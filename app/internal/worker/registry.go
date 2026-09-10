package worker

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"strings"
	"sync"
	"time"

	"angryduck/internal/imageref"
	"angryduck/internal/logging"
	"angryduck/internal/metrics"
)

var registryRequestsTotal = metrics.NewCounterVec(
	"angryduck_worker_registry_requests_total",
	"OCI registry mirror requests handled by AngryDuck.",
	"node", "kind", "result",
)

// RegistryMirror is a thin OCI Distribution mirror in front of the node's
// existing containerd content store. It owns no image data and performs no
// layer graph inspection. A manifest tag is resolved to one digest from
// `ctr images list`; a manifest/blob is then streamed from `ctr content get`.
//
// On a miss, the mirror asks the controller for a small repo-local peer list
// and sends the exact OCI request to those peers. A peer miss becomes a 404,
// which makes containerd continue to its upstream registry automatically.
type RegistryMirror struct {
	ctrPath        string
	controllerURL  string
	nodeID         string
	enabled        bool
	candidateLimit int
	contentSem     chan struct{}
	controlClient  *http.Client
	peerClient     *http.Client

	candidateMu    sync.Mutex
	candidateAt    map[string]time.Time
	candidateCache map[string][]sourceCandidate
	candidateTTL   time.Duration
}

type sourceCandidate struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

func NewRegistryMirror(ctrPath, controllerURL, nodeID string, enabled bool, candidateLimit, maxStreams int, _ time.Duration, candidateTTL time.Duration) *RegistryMirror {
	if candidateLimit < 1 {
		candidateLimit = 1
	}
	if maxStreams < 1 {
		maxStreams = 1
	}
	if candidateTTL <= 0 {
		candidateTTL = 1 * time.Second
	}
	return &RegistryMirror{
		ctrPath:        ctrPath,
		controllerURL:  strings.TrimRight(controllerURL, "/"),
		nodeID:         nodeID,
		enabled:        enabled,
		candidateLimit: candidateLimit,
		contentSem:     make(chan struct{}, maxStreams),
		controlClient:  &http.Client{Timeout: 500 * time.Millisecond},
		peerClient: &http.Client{Transport: &http.Transport{
			DialContext:           (&net.Dialer{Timeout: 200 * time.Millisecond}).DialContext,
			ResponseHeaderTimeout: 500 * time.Millisecond,
			MaxIdleConns:          16,
			MaxIdleConnsPerHost:   2,
			IdleConnTimeout:       30 * time.Second,
		}},
		candidateAt:    make(map[string]time.Time),
		candidateCache: make(map[string][]sourceCandidate),
		candidateTTL:   candidateTTL,
	}
}

// Handle serves the OCI Distribution endpoints used by containerd. The
// `ns` query parameter is the original registry host when containerd routes
// through an _default mirror. When called directly by a peer, the original
// registry is carried in X-AngryDuck-Registry.
func (m *RegistryMirror) Handle(w http.ResponseWriter, r *http.Request) {
	if !m.enabled {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.URL.Path == "/v2/" || r.URL.Path == "/v2" {
		w.Header().Set("Docker-Distribution-Api-Version", "registry/2.0")
		w.WriteHeader(http.StatusOK)
		registryRequestsTotal.Inc(m.nodeID, "api", "ok")
		return
	}

	repo, kind, ref, digest, ok := parseDistributionRequest(r)
	if !ok {
		registryRequestsTotal.Inc(m.nodeID, "unknown", "bad_request")
		http.NotFound(w, r)
		return
	}

	registry := strings.TrimSpace(r.Header.Get("X-AngryDuck-Registry"))
	if registry == "" {
		registry = strings.TrimSpace(r.URL.Query().Get("ns"))
	}
	if registry == "" {
		registry = stripHostPort(r.Host)
	}
	if registry == "" || repo == "" {
		registryRequestsTotal.Inc(m.nodeID, kind, "bad_request")
		http.NotFound(w, r)
		return
	}

	fullImage := registry + "/" + repo
	if ref != "" {
		if strings.HasPrefix(ref, "sha256:") {
			fullImage += "@" + ref
		} else {
			fullImage += ":" + ref
		}
	}
	fullImage = imageref.Normalize(fullImage)

	if kind == "manifest" {
		m.handleManifest(w, r, fullImage, ref)
		return
	}
	m.handleBlob(w, r, fullImage, digest)
}

func (m *RegistryMirror) handleManifest(w http.ResponseWriter, r *http.Request, image, ref string) {
	// A digest manifest is exact; serve it directly from containerd content store.
	// This avoids depending on a digest alias being present in the image list.
	if strings.HasPrefix(ref, "sha256:") && m.serveManifestDigest(w, r, ref) {
		registryRequestsTotal.Inc(m.nodeID, "manifest", "local")
		return
	}

	if localDigest, ok := m.resolveLocal(image); ok {
		// A digest reference is already exact. A tag resolves to the digest in
		// the local image store. We do not inspect the manifest graph.
		if ref == "" || strings.HasPrefix(ref, "sha256:") || localDigest != "" {
			if m.serveManifestDigest(w, r, localDigest) {
				registryRequestsTotal.Inc(m.nodeID, "manifest", "local")
				return
			}
		}
	}

	for _, candidate := range m.sourceCandidates(image) {
		if m.proxyPeer(w, r, candidate.Address) {
			registryRequestsTotal.Inc(m.nodeID, "manifest", "peer")
			return
		}
	}

	registryRequestsTotal.Inc(m.nodeID, "manifest", "origin-fallback")
	m.miss(w)
}

func (m *RegistryMirror) handleBlob(w http.ResponseWriter, r *http.Request, image, digest string) {
	if digest == "" {
		m.miss(w)
		return
	}
	if m.serveBlobDigest(w, r, digest) {
		registryRequestsTotal.Inc(m.nodeID, "blob", "local")
		return
	}

	for _, candidate := range m.sourceCandidates(image) {
		if m.proxyPeer(w, r, candidate.Address) {
			registryRequestsTotal.Inc(m.nodeID, "blob", "peer")
			return
		}
	}

	registryRequestsTotal.Inc(m.nodeID, "blob", "origin-fallback")
	m.miss(w)
}

func (m *RegistryMirror) serveManifestDigest(w http.ResponseWriter, r *http.Request, digest string) bool {
	body, ok := m.readDigest(digest, 4<<20)
	if !ok {
		return false
	}
	contentType := "application/vnd.oci.image.manifest.v1+json"
	var meta struct {
		MediaType string `json:"mediaType"`
	}
	if json.Unmarshal(body, &meta) == nil && meta.MediaType != "" {
		contentType = meta.MediaType
	}

	w.Header().Set("Docker-Distribution-Api-Version", "registry/2.0")
	w.Header().Set("Docker-Content-Digest", digest)
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_, _ = w.Write(body)
	}
	return true
}

func (m *RegistryMirror) serveBlobDigest(w http.ResponseWriter, r *http.Request, digest string) bool {
	if r.Method == http.MethodHead {
		_, err := runCmd(m.ctrPath, "-n", "k8s.io", "content", "info", digest)
		if err != nil {
			return false
		}
		w.Header().Set("Docker-Distribution-Api-Version", "registry/2.0")
		w.Header().Set("Docker-Content-Digest", digest)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		return true
	}

	if !m.acquireContent() {
		// Do not queue image transfers on a busy node; make containerd try its
		// upstream host instead.
		w.WriteHeader(http.StatusServiceUnavailable)
		return true
	}
	defer m.releaseContent()

	cmd := exec.Command(m.ctrPath, "-n", "k8s.io", "content", "get", digest)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return false
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return false
	}

	// Read a single byte before committing the HTTP response. This prevents
	// a missing local blob from becoming an irreversible 200 response; we can
	// still fall through to a peer when ctr reports ENOENT.
	var first [1]byte
	n, readErr := stdout.Read(first[:])
	if readErr != nil && readErr != io.EOF {
		_ = cmd.Wait()
		return false
	}
	if n == 0 {
		if waitErr := cmd.Wait(); waitErr != nil {
			logging.Debugf("angryduck-worker[%s]: local registry blob miss digest=%s stderr=%s", m.nodeID, digest, strings.TrimSpace(stderr.String()))
			return false
		}
		return false
	}

	w.Header().Set("Docker-Distribution-Api-Version", "registry/2.0")
	w.Header().Set("Docker-Content-Digest", digest)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		if _, err := w.Write(first[:n]); err != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			return true
		}
		_, _ = io.Copy(w, stdout)
	}
	if err := cmd.Wait(); err != nil {
		logging.Debugf("angryduck-worker[%s]: local registry blob stream failed digest=%s stderr=%s", m.nodeID, digest, strings.TrimSpace(stderr.String()))
		return false
	}
	return true
}

func (m *RegistryMirror) readDigest(digest string, max int64) ([]byte, bool) {
	if !strings.HasPrefix(digest, "sha256:") {
		return nil, false
	}
	if !m.acquireContent() {
		return nil, false
	}
	defer m.releaseContent()
	cmd := exec.Command(m.ctrPath, "-n", "k8s.io", "content", "get", digest)
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, false
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return nil, false
	}
	body, readErr := io.ReadAll(io.LimitReader(out, max+1))
	waitErr := cmd.Wait()
	if readErr != nil || waitErr != nil || int64(len(body)) > max {
		logging.Debugf("angryduck-worker[%s]: local registry manifest read failed digest=%s stderr=%s", m.nodeID, digest, strings.TrimSpace(stderr.String()))
		return nil, false
	}
	return body, true
}

func (m *RegistryMirror) proxyPeer(w http.ResponseWriter, r *http.Request, address string) bool {
	peerURL := "http://" + address + r.URL.Path
	if q := r.URL.RawQuery; q != "" {
		vals, _ := url.ParseQuery(q)
		vals.Del("ns")
		if encoded := vals.Encode(); encoded != "" {
			peerURL += "?" + encoded
		}
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, peerURL, nil)
	if err != nil {
		return false
	}
	if registry := strings.TrimSpace(r.Header.Get("X-AngryDuck-Registry")); registry != "" {
		req.Header.Set("X-AngryDuck-Registry", registry)
	} else if registry := strings.TrimSpace(r.URL.Query().Get("ns")); registry != "" {
		req.Header.Set("X-AngryDuck-Registry", registry)
	}
	resp, err := m.peerClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	for _, key := range []string{"Content-Type", "Content-Length", "Docker-Content-Digest", "Docker-Distribution-Api-Version"} {
		for _, v := range resp.Header.Values(key) {
			w.Header().Add(key, v)
		}
	}
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return true
	}
	_, err = io.Copy(w, resp.Body)
	return err == nil
}

func (m *RegistryMirror) sourceCandidates(image string) []sourceCandidate {
	repo := imageref.Repo(image)
	m.candidateMu.Lock()
	if at, ok := m.candidateAt[repo]; ok && time.Since(at) < m.candidateTTL {
		cached := m.candidateCache[repo]
		out := append([]sourceCandidate(nil), cached...)
		m.candidateMu.Unlock()
		return out
	}
	m.candidateMu.Unlock()

	if m.controllerURL == "" || repo == "" {
		return nil
	}
	payload := fmt.Sprintf(`{"image":%q,"target_node":%q}`, image, m.nodeID)
	req, err := http.NewRequest(http.MethodPost, m.controllerURL+"/peer/source", strings.NewReader(payload))
	if err != nil {
		return nil
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.controlClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	var out struct {
		Candidates []sourceCandidate `json:"candidates"`
	}
	if err := jsonDecodeLimited(resp.Body, &out); err != nil {
		return nil
	}
	if len(out.Candidates) > m.candidateLimit {
		out.Candidates = out.Candidates[:m.candidateLimit]
	}

	m.candidateMu.Lock()
	m.candidateAt[repo] = time.Now()
	m.candidateCache[repo] = append([]sourceCandidate(nil), out.Candidates...)
	m.candidateMu.Unlock()
	return out.Candidates
}

func (m *RegistryMirror) resolveLocal(image string) (string, bool) {
	// Tag -> digest must not be served from a long-lived cache: tags are
	// mutable and containerd expects a fresh resolve. One small `ctr images
	// list` per manifest request is intentional and avoids ever serving an
	// old image after a tag has been moved. Blob requests never perform this
	// lookup.
	out, err := runCmd(m.ctrPath, "-n", "k8s.io", "images", "list")
	if err != nil {
		return "", false
	}
	digests := parseCtrImagesList(out)
	d, ok := digests[image]
	return d, ok
}

func (m *RegistryMirror) acquireContent() bool {
	select {
	case m.contentSem <- struct{}{}:
		return true
	default:
		return false
	}
}

func (m *RegistryMirror) releaseContent() {
	<-m.contentSem
}

func (m *RegistryMirror) miss(w http.ResponseWriter) {
	w.Header().Set("Docker-Distribution-Api-Version", "registry/2.0")
	w.WriteHeader(http.StatusNotFound)
}

func parseDistributionRequest(r *http.Request) (repo, kind, ref, digest string, ok bool) {
	if !strings.HasPrefix(r.URL.Path, "/v2/") {
		return "", "", "", "", false
	}
	rest := strings.TrimPrefix(r.URL.Path, "/v2/")
	if idx := strings.Index(rest, "/manifests/"); idx >= 0 {
		repo = strings.Trim(rest[:idx], "/")
		ref = strings.Trim(rest[idx+len("/manifests/"):], "/")
		if repo == "" || ref == "" {
			return "", "", "", "", false
		}
		if strings.HasPrefix(ref, "sha256:") {
			digest = ref
		}
		return repo, "manifest", ref, digest, true
	}
	if idx := strings.Index(rest, "/blobs/"); idx >= 0 {
		repo = strings.Trim(rest[:idx], "/")
		digest = strings.Trim(strings.TrimPrefix(rest[idx+len("/blobs/"):], "/"), "/")
		if repo == "" || !strings.HasPrefix(digest, "sha256:") {
			return "", "", "", "", false
		}
		return repo, "blob", "", digest, true
	}
	return "", "", "", "", false
}

func stripHostPort(host string) string {
	if host == "" {
		return ""
	}
	if strings.HasPrefix(host, "[") {
		if i := strings.Index(host, "]"); i >= 0 {
			return strings.Trim(host[1:i], " ")
		}
	}
	if i := strings.LastIndex(host, ":"); i > -1 && strings.Count(host, ":") == 1 {
		return host[:i]
	}
	return host
}

func jsonDecodeLimited(r io.Reader, v interface{}) error {
	dec := bufio.NewReader(io.LimitReader(r, 64*1024))
	return json.NewDecoder(dec).Decode(v)
}
