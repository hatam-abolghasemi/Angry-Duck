// Pod rescue is the one place Angry Duck touches the Kubernetes API, and
// the one place it moves image bytes between nodes at all. It is
// deliberately small and deliberately NOT a general peer-to-peer
// mechanism (that's Spegel's job): it only ever fires for a pod already
// stuck in ImagePullBackOff/ErrImagePull, it asks the controller for
// AT MOST ONE node that already has the exact image, it makes exactly
// ONE attempt to copy it from there, and on failure it goes quiet for a
// configurable cooldown instead of retrying — no busy-peer loop, no
// fan-out to multiple sources, no continuous interception of every pull.
//
// Why this can skip everything the old mirror needed: that mirror sat in
// front of every containerd pull and had to resolve manifest digests,
// because it never knew in advance which exact bytes a future pull would
// ask for. Rescue only ever acts after kubelet has already told us the
// exact reference a specific pod is stuck on — so it can ask for and
// serve that reference directly, by name, with no digest bookkeeping,
// no tag->digest resolution, and no continuously-updated inventory index
// at all.
package worker

import (
	"bytes"
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"angryduck/internal/logging"
	"angryduck/internal/metrics"
	"angryduck/internal/model"
)

const rescueTokenHeader = "X-Angryduck-Rescue-Token"

// podImagePullFailuresTotal counts every poll-tick observation of a
// container stuck in ImagePullBackOff/ErrImagePull on this node -- how
// often is this node's kubelet failing to pull, independent of whether a
// rescue attempt succeeds. Deliberately a raw per-node counter with no
// per-image label: cardinality here would be one series per distinct
// broken image tag ever seen, unbounded over a cluster's lifetime.
var podImagePullFailuresTotal = metrics.NewCounterVec(
	"angryduck_worker_pod_image_pull_failures_total",
	"Poll-tick observations of a container stuck in ImagePullBackOff/ErrImagePull on this node.",
	"node",
)

// rescueAttemptsTotal counts each one-shot rescue attempt by outcome.
var rescueAttemptsTotal = metrics.NewCounterVec(
	"angryduck_worker_rescue_attempts_total",
	"One-shot pod rescue attempts, by result (success, no_source, dial_failed, transfer_failed).",
	"node", "result",
)

// rescueExportsTotal counts this node acting as a rescue SOURCE.
var rescueExportsTotal = metrics.NewCounterVec(
	"angryduck_worker_rescue_exports_total",
	"Rescue exports served to another node, by result.",
	"node", "result",
)

// --- Kubernetes API access ---------------------------------------------

// k8sInClusterClient is a minimal, dependency-free REST client for the
// one thing rescue needs: listing pods scheduled on this node. It
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
// missing, so rescue can be skipped gracefully outside a real cluster.
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

// k8s API response shapes -- only the fields rescue actually reads.
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

// podLister is the one capability the watch loop needs, factored out so
// tests can supply a fake instead of a real Kubernetes API server.
type podLister interface {
	listStuckImages(ctx context.Context, nodeID string) ([]podStuckImage, error)
}

// --- Watch loop ----------------------------------------------------------

// RescueWatch polls this node's pods for stuck image pulls and makes at
// most one rescue attempt per stuck image per cooldown window. cooldown
// is the entire retry policy: no internal retry loop, no multiple
// sources tried -- one attempt, then silence until cooldown passes.
type RescueWatch struct {
	nodeID            string
	client            podLister
	interval          time.Duration
	cooldown          time.Duration
	rescue            func(ctx context.Context, image string) error
	excludeNamespaces []string
	excludeImages     []string

	mu          sync.Mutex
	attempted   map[string]time.Time // image -> last attempt, enforces the cooldown
	lastWarnLog map[string]time.Time // "namespace/pod/container" -> last WARN, throttles repeat log lines
}

// NewRescueWatch builds a RescueWatch. Call Run to start it. rescue may be
// nil (no way to reach a source, e.g. rescue disabled) -- the watch still
// runs and still counts/logs stuck pulls, it just never attempts a fix.
func NewRescueWatch(nodeID string, client *k8sInClusterClient, interval, cooldown time.Duration, excludeNamespaces, excludeImages []string, rescue func(context.Context, string) error) *RescueWatch {
	return &RescueWatch{
		nodeID:            nodeID,
		client:            client,
		interval:          interval,
		cooldown:          cooldown,
		rescue:            rescue,
		excludeNamespaces: excludeNamespaces,
		excludeImages:     excludeImages,
		attempted:         make(map[string]time.Time),
		lastWarnLog:       make(map[string]time.Time),
	}
}

// isExcluded reports whether a stuck sighting should be ignored entirely
// -- no log, no metric, no rescue attempt. A fleet inevitably accumulates
// abandoned feature-branch deployments and deliberately-broken test/demo
// resources that have been stuck for a long time; without this, rescue
// re-logs every one of them on every node, every poll, forever.
func (rw *RescueWatch) isExcluded(s podStuckImage) bool {
	for _, sub := range rw.excludeNamespaces {
		if sub != "" && strings.Contains(s.Namespace, sub) {
			return true
		}
	}
	for _, sub := range rw.excludeImages {
		if sub != "" && strings.Contains(s.Image, sub) {
			return true
		}
	}
	return false
}

// Run blocks, polling on every tick until ctx is done.
func (rw *RescueWatch) Run(ctx context.Context) {
	ticker := time.NewTicker(rw.interval)
	defer ticker.Stop()
	logging.Infof("angryduck-worker-rescue: watch started: interval=%s cooldown=%s rescue_enabled=%v", rw.interval, rw.cooldown, rw.rescue != nil)
	for {
		select {
		case <-ctx.Done():
			logging.Infof("angryduck-worker-rescue: watch stopping")
			return
		case <-ticker.C:
			rw.tick(ctx)
		}
	}
}

func (rw *RescueWatch) tick(ctx context.Context) {
	stuck, err := rw.client.listStuckImages(ctx, rw.nodeID)
	if err != nil {
		logging.Warnf("angryduck-worker-rescue: failed to list pods on this node: %v", err)
		return
	}
	if len(stuck) == 0 {
		return
	}

	tried := make(map[string]bool, len(stuck))
	for _, s := range stuck {
		if rw.isExcluded(s) {
			continue
		}
		podImagePullFailuresTotal.Inc(rw.nodeID)

		key := s.Namespace + "/" + s.Pod + "/" + s.Container
		if rw.recentlyWarned(key) {
			logging.Debugf("angryduck-worker-rescue: pod=%s/%s container=%s still stuck (%s) on image=%s",
				s.Namespace, s.Pod, s.Container, s.Reason, s.Image)
		} else {
			rw.markWarned(key)
			logging.Warnf("angryduck-worker-rescue: pod=%s/%s container=%s stuck (%s) on image=%s",
				s.Namespace, s.Pod, s.Container, s.Reason, s.Image)
		}

		if rw.rescue == nil || tried[s.Image] || rw.recentlyAttempted(s.Image) {
			continue
		}
		tried[s.Image] = true
		rw.markAttempted(s.Image)
		if err := rw.rescue(ctx, s.Image); err != nil {
			logging.Warnf("angryduck-worker-rescue: one-shot rescue of image=%s did not succeed, will not retry for %s: %v", s.Image, rw.cooldown, err)
		}
	}
}

func (rw *RescueWatch) recentlyAttempted(image string) bool {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	last, ok := rw.attempted[image]
	return ok && time.Since(last) < rw.cooldown
}

func (rw *RescueWatch) markAttempted(image string) {
	rw.mu.Lock()
	rw.attempted[image] = time.Now()
	rw.mu.Unlock()
}

func (rw *RescueWatch) recentlyWarned(key string) bool {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	last, ok := rw.lastWarnLog[key]
	return ok && time.Since(last) < rw.cooldown
}

func (rw *RescueWatch) markWarned(key string) {
	rw.mu.Lock()
	rw.lastWarnLog[key] = time.Now()
	rw.mu.Unlock()
}

// --- The rescue attempt itself: ask, dial, copy, done ---------------------

// Rescuer performs the one-shot import side of a rescue: ask the
// controller for a single source, dial it directly, and copy the image
// across by exact reference -- no digest bookkeeping, no unpack-lock
// juggling (there is no concurrent containerd pull to deadlock against;
// the pull that would have started one already gave up before this ever
// runs), no retry.
type Rescuer struct {
	nodeID            string
	controllerURL     string
	token             string
	hx                *HostExec
	containerdAddress string
	namespace         string
	client            *http.Client
}

// NewRescuer builds a Rescuer.
func NewRescuer(nodeID, controllerURL, token string, hx *HostExec, containerdAddress, namespace string) *Rescuer {
	return &Rescuer{
		nodeID:            nodeID,
		controllerURL:     controllerURL,
		token:             token,
		hx:                hx,
		containerdAddress: containerdAddress,
		namespace:         namespace,
		client:            &http.Client{Timeout: 5 * time.Second},
	}
}

// Attempt is the function passed to RescueWatch: exactly one try, no
// internal retry.
func (rs *Rescuer) Attempt(ctx context.Context, image string) error {
	addr, err := rs.source(ctx, image)
	if err != nil {
		rescueAttemptsTotal.Inc(rs.nodeID, "lookup_failed")
		return fmt.Errorf("asking the controller for a rescue source: %w", err)
	}
	if addr == "" {
		rescueAttemptsTotal.Inc(rs.nodeID, "no_source")
		return fmt.Errorf("no node currently has %s", image)
	}

	conn, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, "tcp", addr)
	if err != nil {
		rescueAttemptsTotal.Inc(rs.nodeID, "dial_failed")
		return fmt.Errorf("dialing rescue source %s: %w", addr, err)
	}

	reqLine := fmt.Sprintf("GET /rescue-export?image=%s HTTP/1.1\r\nHost: %s\r\n%s: %s\r\nConnection: close\r\n\r\n",
		url.QueryEscape(image), addr, rescueTokenHeader, rs.token)
	if _, err := conn.Write([]byte(reqLine)); err != nil {
		conn.Close()
		rescueAttemptsTotal.Inc(rs.nodeID, "dial_failed")
		return fmt.Errorf("sending rescue request to %s: %w", addr, err)
	}

	status, err := readStatusLine(conn)
	if err != nil || status != http.StatusOK {
		conn.Close()
		rescueAttemptsTotal.Inc(rs.nodeID, "transfer_failed")
		return fmt.Errorf("rescue source %s declined %s: status=%d err=%v", addr, image, status, err)
	}

	// From here the socket belongs to ctr. File() dups the descriptor;
	// closing the Go conn leaves the dup (and the socket) alive.
	f, err := conn.(*net.TCPConn).File()
	conn.Close()
	if err != nil {
		rescueAttemptsTotal.Inc(rs.nodeID, "transfer_failed")
		return fmt.Errorf("preparing socket for import: %w", err)
	}
	defer f.Close()

	cmd, err := rs.hx.Command(ctx, "ctr", "-a", rs.containerdAddress, "-n", rs.namespace, "images", "import", "-")
	if err != nil {
		rescueAttemptsTotal.Inc(rs.nodeID, "transfer_failed")
		return fmt.Errorf("building import command: %w", err)
	}
	cmd.Stdin = f
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	start := time.Now()
	if err := cmd.Run(); err != nil {
		rescueAttemptsTotal.Inc(rs.nodeID, "transfer_failed")
		return fmt.Errorf("importing %s from %s: %w: %s", image, addr, err, strings.TrimSpace(stderr.String()))
	}

	rescueAttemptsTotal.Inc(rs.nodeID, "success")
	logging.Infof("angryduck-worker-rescue: recovered %s from %s in %s", image, addr, time.Since(start).Round(time.Millisecond))
	return nil
}

// source asks the controller for one node that already has image.
func (rs *Rescuer) source(ctx context.Context, image string) (string, error) {
	u := rs.controllerURL + "/rescue-source?" + url.Values{"image": {image}, "node": {rs.nodeID}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", err
	}
	resp, err := rs.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("controller status %d", resp.StatusCode)
	}
	var rr model.RescueSourceResponse
	if err := json.NewDecoder(resp.Body).Decode(&rr); err != nil {
		return "", err
	}
	return rr.Address, nil
}

// readStatusLine reads just enough of a raw HTTP response to get the
// status code, leaving the connection positioned right after the header
// terminator for a caller about to hand the raw socket to a subprocess
// instead of reading the body through Go.
func readStatusLine(conn net.Conn) (int, error) {
	buf := make([]byte, 0, 256)
	one := make([]byte, 1)
	seenBlankLine := false
	last4 := make([]byte, 0, 4)
	for {
		n, err := conn.Read(one)
		if n == 0 || err != nil {
			return 0, fmt.Errorf("reading response head: %w", err)
		}
		buf = append(buf, one[0])
		last4 = append(last4, one[0])
		if len(last4) > 4 {
			last4 = last4[1:]
		}
		if string(last4) == "\r\n\r\n" {
			seenBlankLine = true
			break
		}
		if len(buf) > 8192 {
			return 0, fmt.Errorf("response head too large")
		}
	}
	if !seenBlankLine {
		return 0, fmt.Errorf("no header terminator found")
	}
	lines := strings.SplitN(string(buf), "\r\n", 2)
	parts := strings.SplitN(lines[0], " ", 3)
	if len(parts) < 2 {
		return 0, fmt.Errorf("malformed status line: %q", lines[0])
	}
	code, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, fmt.Errorf("malformed status code: %q", parts[1])
	}
	return code, nil
}

// --- The export side: serve one image, by exact reference, once ----------

// RescueExporter serves /rescue-export: streams `ctr images export` for a
// named reference straight into the requester's socket. maxConcurrent
// caps how many of these can run at once on this node (default 1) -- this
// is a rare, occasional repair path, not a throughput mechanism, so it
// deliberately does not need the concurrency the old mirror provisioned
// for continuous peer serving.
type RescueExporter struct {
	nodeID            string
	token             string
	hx                *HostExec
	containerdAddress string
	namespace         string
	slots             chan struct{}
}

// NewRescueExporter builds a RescueExporter.
func NewRescueExporter(nodeID, token string, hx *HostExec, containerdAddress, namespace string, maxConcurrent int) *RescueExporter {
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}
	return &RescueExporter{
		nodeID:            nodeID,
		token:             token,
		hx:                hx,
		containerdAddress: containerdAddress,
		namespace:         namespace,
		slots:             make(chan struct{}, maxConcurrent),
	}
}

// ServeExport handles GET /rescue-export?image=<ref>.
func (e *RescueExporter) ServeExport(w http.ResponseWriter, r *http.Request) {
	if subtle.ConstantTimeCompare([]byte(r.Header.Get(rescueTokenHeader)), []byte(e.token)) != 1 {
		rescueExportsTotal.Inc(e.nodeID, "unauthorized")
		logging.Warnf("angryduck-worker-rescue: rejected export request from %s: bad or missing token", r.RemoteAddr)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	image := r.URL.Query().Get("image")
	if image == "" {
		http.Error(w, "image required", http.StatusBadRequest)
		return
	}

	select {
	case e.slots <- struct{}{}:
		defer func() { <-e.slots }()
	default:
		rescueExportsTotal.Inc(e.nodeID, "busy")
		logging.Debugf("angryduck-worker-rescue: all export slot(s) busy, telling %s to try elsewhere for %s", r.RemoteAddr, image)
		http.Error(w, "busy", http.StatusServiceUnavailable)
		return
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}
	conn, buf, err := hj.Hijack()
	if err != nil {
		rescueExportsTotal.Inc(e.nodeID, "failed")
		return
	}
	defer conn.Close()
	if _, err := buf.WriteString("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n"); err != nil || buf.Flush() != nil {
		rescueExportsTotal.Inc(e.nodeID, "failed")
		return
	}

	f, err := conn.(*net.TCPConn).File()
	if err != nil {
		rescueExportsTotal.Inc(e.nodeID, "failed")
		return
	}
	defer f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd, err := e.hx.Command(ctx, "ctr", "-a", e.containerdAddress, "-n", e.namespace, "images", "export", "-", image)
	if err != nil {
		rescueExportsTotal.Inc(e.nodeID, "failed")
		logging.Warnf("angryduck-worker-rescue: could not build export command for %s: %v", image, err)
		return
	}
	cmd.Stdout = f
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	start := time.Now()
	if err := cmd.Run(); err != nil {
		if isPeerDisconnect(strings.TrimSpace(stderr.String())) {
			logging.Debugf("angryduck-worker-rescue: export of %s to %s ended: requester disconnected", image, r.RemoteAddr)
		} else {
			rescueExportsTotal.Inc(e.nodeID, "failed")
			logging.Warnf("angryduck-worker-rescue: export of %s to %s failed after %s: %v: %s",
				image, r.RemoteAddr, time.Since(start).Round(time.Millisecond), err, strings.TrimSpace(stderr.String()))
		}
		return
	}
	rescueExportsTotal.Inc(e.nodeID, "served")
	logging.Infof("angryduck-worker-rescue: exported %s to %s in %s", image, r.RemoteAddr, time.Since(start).Round(time.Millisecond))
}

// isPeerDisconnect reports whether an export failure's stderr indicates
// the requester simply went away mid-transfer, rather than this node
// being unable to produce the content. A narrow allowlist of known
// disconnect phrasing, not a broad denylist of "real" failures -- the
// safer default is to assume an unrecognized error is worth a WARN.
func isPeerDisconnect(msg string) bool {
	lower := strings.ToLower(msg)
	for _, s := range []string{"broken pipe", "connection reset by peer", "use of closed network connection"} {
		if strings.Contains(lower, s) {
			return true
		}
	}
	return false
}
