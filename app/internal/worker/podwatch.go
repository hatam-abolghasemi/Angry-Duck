// Pod-watch is the one place Angry Duck touches the Kubernetes API at
// all — everywhere else it only ever talks to containerd/CRI (via
// HostExec) and its own controller. It exists because GC and the peer
// mirror both work from containerd's own view of the world (`crictl
// images`, `crictl ps`), and a pod stuck in Init:ImagePullBackOff never
// reaches that view: kubelet holds it before ever calling CreateContainer,
// so crictl has no idea it's even trying. The only place "this node needs
// image X and doesn't have it" is visible at all is the pod's own status,
// via the Kubernetes API.
//
// This is a genuinely different job from the mirror above: the mirror
// intercepts a containerd pull that's actively in flight; this fixes a
// pull that already gave up, for a reason unrelated to peer availability
// (tag resolution against origin itself failed). See Mirror.FallbackImport
// for the actual rescue mechanism — this file is only pod discovery.
package worker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"angryduck/internal/logging"
	"angryduck/internal/metrics"
)

// podImagePullFailuresTotal counts every poll-tick observation of a
// container stuck in ImagePullBackOff/ErrImagePull on this node — "how
// often is this node's kubelet failing to pull," independent of whether
// Angry Duck's own rescue attempt (tagFallbackTotal, in mirror.go)
// succeeds. Deliberately a raw per-node counter with no per-image label:
// cardinality here would be one series per distinct broken image tag
// ever seen, unbounded over the life of a cluster.
var podImagePullFailuresTotal = metrics.NewCounterVec(
	"angryduck_worker_pod_image_pull_failures_total",
	"Poll-tick observations of a container stuck in ImagePullBackOff/ErrImagePull on this node.",
	"node",
)

// k8sInClusterClient is a minimal, dependency-free REST client for the
// one thing pod-watch needs: listing pods scheduled on this node. It
// re-reads the service account token from disk on every call rather than
// caching it, since projected tokens rotate (default ~1h) and a worker
// that runs for weeks must not start failing auth partway through.
type k8sInClusterClient struct {
	baseURL    string
	tokenPath  string
	httpClient *http.Client
}

// NewInClusterK8sClient builds a client from the standard in-cluster
// service account mount and KUBERNETES_SERVICE_HOST/PORT env vars every
// pod gets automatically. Returns an error (not fatal) if either is
// missing, so PodWatch can be skipped gracefully outside a real cluster.
func NewInClusterK8sClient() (*k8sInClusterClient, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, fmt.Errorf("KUBERNETES_SERVICE_HOST/PORT not set (not running in-cluster?)")
	}
	const base = "/var/run/secrets/kubernetes.io/serviceaccount"
	tokenPath := base + "/token"
	if _, err := os.Stat(tokenPath); err != nil {
		return nil, fmt.Errorf("service account token not mounted at %s: %w", tokenPath, err)
	}
	caCert, err := os.ReadFile(base + "/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("reading service account CA cert: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("no certificates parsed from service account ca.crt")
	}
	return &k8sInClusterClient{
		baseURL:   "https://" + net.JoinHostPort(host, port),
		tokenPath: tokenPath,
		httpClient: &http.Client{
			Timeout:   10 * time.Second,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}},
		},
	}, nil
}

// podStuckImage is one container or init container on this node that's
// currently waiting on a pull that isn't going to succeed on its own.
type podStuckImage struct {
	Namespace string
	Pod       string
	Container string
	Image     string
	Reason    string
}

// k8s API response shapes — only the fields pod-watch actually reads.
type podList struct {
	Items []struct {
		Metadata struct {
			Name      string `json:"name"`
			Namespace string `json:"namespace"`
		} `json:"metadata"`
		Status struct {
			ContainerStatuses     []containerStatus `json:"containerStatuses"`
			InitContainerStatuses []containerStatus `json:"initContainerStatuses"`
		} `json:"status"`
	} `json:"items"`
}

type containerStatus struct {
	Name  string `json:"name"`
	Image string `json:"image"` // the exact ref kubelet is trying to run
	State struct {
		Waiting *struct {
			Reason string `json:"reason"`
		} `json:"waiting"`
	} `json:"state"`
}

// stuckReasons are the two kubelet container-status reasons that mean
// "this pull has given up and is backing off," as opposed to transient
// states (ContainerCreating, PodInitializing) that resolve on their own.
var stuckReasons = map[string]bool{
	"ImagePullBackOff": true,
	"ErrImagePull":     true,
}

// listStuckImages lists every pod scheduled on nodeID and returns one
// entry per container/init container currently stuck pulling.
func (c *k8sInClusterClient) listStuckImages(ctx context.Context, nodeID string) ([]podStuckImage, error) {
	tokenBytes, err := os.ReadFile(c.tokenPath)
	if err != nil {
		return nil, fmt.Errorf("reading service account token: %w", err)
	}
	u := c.baseURL + "/api/v1/pods?" + url.Values{"fieldSelector": {"spec.nodeName=" + nodeID}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(tokenBytes)))
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("listing pods: status=%d", resp.StatusCode)
	}
	var parsed podList
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decoding pod list: %w", err)
	}

	var out []podStuckImage
	collect := func(ns, pod string, statuses []containerStatus) {
		for _, cs := range statuses {
			if cs.State.Waiting == nil || !stuckReasons[cs.State.Waiting.Reason] || cs.Image == "" {
				continue
			}
			out = append(out, podStuckImage{
				Namespace: ns, Pod: pod, Container: cs.Name,
				Image: cs.Image, Reason: cs.State.Waiting.Reason,
			})
		}
	}
	for _, item := range parsed.Items {
		collect(item.Metadata.Namespace, item.Metadata.Name, item.Status.ContainerStatuses)
		collect(item.Metadata.Namespace, item.Metadata.Name, item.Status.InitContainerStatuses)
	}
	return out, nil
}

// podLister is the one capability PodWatch needs, factored out so tests
// can supply a fake instead of a real Kubernetes API server.
// *k8sInClusterClient satisfies this.
type podLister interface {
	listStuckImages(ctx context.Context, nodeID string) ([]podStuckImage, error)
}

// PodWatch polls this node's pods for stuck image pulls and hands each
// distinct stuck image to fix. fix may be nil (mirror disabled, so there
// is no rescue mechanism available) — pod-watch still runs and still
// counts failures for visibility, it just never attempts a repair.
type PodWatch struct {
	nodeID   string
	client   podLister
	interval time.Duration
	backoff  time.Duration
	fix      func(ctx context.Context, image string) error

	mu        sync.Mutex
	attempted map[string]time.Time // image -> last attempt, so a permanently-broken image isn't retried every tick
}

// NewPodWatch builds a PodWatch. Call Run to start it.
func NewPodWatch(nodeID string, client *k8sInClusterClient, interval, backoff time.Duration, fix func(context.Context, string) error) *PodWatch {
	return &PodWatch{
		nodeID:    nodeID,
		client:    client,
		interval:  interval,
		backoff:   backoff,
		fix:       fix,
		attempted: make(map[string]time.Time),
	}
}

// Run blocks, polling on every tick until ctx is done.
func (pw *PodWatch) Run(ctx context.Context) {
	ticker := time.NewTicker(pw.interval)
	defer ticker.Stop()
	logging.Infof("angryduck-worker-podwatch: loop started: interval=%s retry_backoff=%s fix_enabled=%v", pw.interval, pw.backoff, pw.fix != nil)
	for {
		select {
		case <-ctx.Done():
			logging.Infof("angryduck-worker-podwatch: loop stopping")
			return
		case <-ticker.C:
			pw.tick(ctx)
		}
	}
}

func (pw *PodWatch) tick(ctx context.Context) {
	stuck, err := pw.client.listStuckImages(ctx, pw.nodeID)
	if err != nil {
		logging.Warnf("angryduck-worker-podwatch: failed to list pods on this node: %v", err)
		return
	}
	if len(stuck) == 0 {
		return
	}

	// Several containers (or several pods of one DaemonSet/Deployment)
	// commonly get stuck on the exact same image at once; one fix
	// attempt covers all of them, so dedupe before calling fix.
	tried := make(map[string]bool, len(stuck))
	for _, s := range stuck {
		podImagePullFailuresTotal.Inc(pw.nodeID)
		logging.Warnf("angryduck-worker-podwatch: pod=%s/%s container=%s stuck (%s) on image=%s",
			s.Namespace, s.Pod, s.Container, s.Reason, s.Image)

		if pw.fix == nil || tried[s.Image] || pw.recentlyAttempted(s.Image) {
			continue
		}
		tried[s.Image] = true
		pw.markAttempted(s.Image)
		if err := pw.fix(ctx, s.Image); err != nil {
			logging.Warnf("angryduck-worker-podwatch: fallback fix for image=%s did not succeed: %v", s.Image, err)
		}
	}
}

func (pw *PodWatch) recentlyAttempted(image string) bool {
	pw.mu.Lock()
	defer pw.mu.Unlock()
	last, ok := pw.attempted[image]
	return ok && time.Since(last) < pw.backoff
}

func (pw *PodWatch) markAttempted(image string) {
	pw.mu.Lock()
	pw.attempted[image] = time.Now()
	pw.mu.Unlock()
}
