package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"angryduck/internal/imageref"
	"angryduck/internal/logging"
	"angryduck/internal/metrics"
	"angryduck/internal/model"
	"angryduck/internal/sharedtoken"
)

var (
	mirrorRequestsTotal = metrics.NewCounterVec(
		"angryduck_worker_mirror_requests_total",
		"Content requests containerd sent to this node's mirror, by kind (blob, manifest) and result: local (served from this node), peer (from another node), miss (nobody had it: containerd goes to the registry), error.",
		"node", "kind", "result",
	)
	mirrorBytesTotal = metrics.NewCounterVec(
		"angryduck_worker_mirror_bytes_total",
		"Bytes the mirror served to containerd, by source: local or peer. Every one of them is a byte not downloaded from the registry.",
		"node", "source",
	)
	mirrorServedTotal = metrics.NewCounterVec(
		"angryduck_worker_mirror_peer_served_bytes_total",
		"Bytes this node served to other nodes' mirrors.",
		"node",
	)
)

// MirrorStore is what the mirror needs from containerd. ContainerdStore has it.
type MirrorStore interface {
	Digests(ctx context.Context) (map[string]bool, error)
	ReadBlob(ctx context.Context, digest string, max int64) ([]byte, error)
	OpenContent(ctx context.Context, digest string) (io.ReadCloser, int64, error)
}

// Mirror is a pull-only OCI registry mirror for this node's containerd,
// on 127.0.0.1 only. containerd asks it first for every manifest and blob
// by digest (tags are still resolved at the real registry, so a moved tag
// is never served stale). The mirror answers from this node's content
// store, else streams the content from a peer node that has it (the
// controller's layer inventory says which), else answers 404 and
// containerd goes to the registry as usual. containerd verifies every
// digest, so a peer can't slip in anything else.
//
// It covers every image, announced or not (third-party images too): the
// difference with preheat and propagation is only that nothing is spread
// ahead of time.
type Mirror struct {
	clientToken   string // see RequireClientToken
	store         MirrorStore
	nodeID        string
	token         string
	controllerURL string
	client        *http.Client

	mu      sync.Mutex
	holders map[string]holderEntry
}

type holderEntry struct {
	list []model.Holder
	at   time.Time
}

// holderTTL is how long a controller answer is reused.
const holderTTL = 30 * time.Second

// NewMirror builds a mirror.
// RequireClientToken makes the mirror's registry API answer only requests
// carrying token, which containerd sends from the hosts.toml header. The
// mirror listens on the pod IP, reachable from every pod; without this
// any of them could pull private image content through it.
func (m *Mirror) RequireClientToken(token string) { m.clientToken = token }

func NewMirror(store MirrorStore, nodeID, token, controllerURL string) *Mirror {
	for _, kind := range []string{"blob", "manifest"} {
		for _, r := range []string{"local", "peer", "miss", "error"} {
			mirrorRequestsTotal.Add(0, nodeID, kind, r)
		}
	}
	mirrorBytesTotal.Add(0, nodeID, "local")
	mirrorBytesTotal.Add(0, nodeID, "peer")
	mirrorServedTotal.Add(0, nodeID)
	return &Mirror{
		store: store, nodeID: nodeID, token: token, controllerURL: strings.TrimRight(controllerURL, "/"),
		client: &http.Client{Transport: &http.Transport{
			Proxy:                 nil,
			DialContext:           (&net.Dialer{Timeout: 3 * time.Second}).DialContext,
			ResponseHeaderTimeout: 30 * time.Second,
		}},
		holders: map[string]holderEntry{},
	}
}

// Handler serves the registry API containerd talks to.
func (m *Mirror) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if m.clientToken != "" && !sharedtoken.Valid(r, m.clientToken) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path == "/v2/" || r.URL.Path == "/v2" {
			w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
			_, _ = w.Write([]byte("{}"))
			return
		}
		kind, digest, ok := parseMirrorPath(r.URL.Path)
		if !ok {
			http.NotFound(w, r) // tags and everything else: the registry answers
			return
		}
		m.serve(w, r, kind, digest)
	})
}

// parseMirrorPath reads /v2/<name>/{blobs,manifests}/sha256:<hex>.
func parseMirrorPath(p string) (kind, digest string, ok bool) {
	if !strings.HasPrefix(p, "/v2/") {
		return "", "", false
	}
	for _, k := range []string{"/blobs/", "/manifests/"} {
		if i := strings.LastIndex(p, k); i > len("/v2/") {
			d := p[i+len(k):]
			if validDigest(d) {
				return strings.Trim(k, "/"), d, true
			}
		}
	}
	return "", "", false
}

func validDigest(d string) bool {
	h, ok := strings.CutPrefix(d, "sha256:")
	if !ok || len(h) != 64 {
		return false
	}
	for _, c := range h {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// hasLocal reports whether this node's content store holds digest: a stat
// of the blob file when the store reads blobs directly, else the (cached)
// content listing.
func (m *Mirror) hasLocal(ctx context.Context, digest string) bool {
	if hb, ok := m.store.(interface{ HasBlob(string) (bool, bool) }); ok {
		if have, known := hb.HasBlob(digest); known {
			return have
		}
	}
	have, err := m.store.Digests(ctx)
	return err == nil && have[digest]
}

func (m *Mirror) serve(w http.ResponseWriter, r *http.Request, kind, digest string) {
	kindLabel := strings.TrimSuffix(kind, "s")
	if m.hasLocal(r.Context(), digest) {
		n, err := m.serveLocal(w, r, kind, digest)
		if err == nil {
			mirrorRequestsTotal.Inc(m.nodeID, kindLabel, "local")
			mirrorBytesTotal.Add(n, m.nodeID, "local")
			return
		}
		// Nothing was sent (serveLocal cuts the connection otherwise): go on
		// as if the node didn't have it, to the peers and then a 404, the
		// only answer that sends containerd on to the next host.
		logging.Warnf("angryduck-worker[%s]: mirror: serving %s %s locally: %v", m.nodeID, kindLabel, digest, err)
		clearBlobHeaders(w)
	}
	for _, h := range m.lookup(r.Context(), digest) {
		n, served, err := m.fromPeer(w, r, h, kind, digest)
		if served {
			if err != nil {
				mirrorRequestsTotal.Inc(m.nodeID, kindLabel, "error")
				logging.Warnf("angryduck-worker[%s]: mirror: %s %s from node=%s broke after %d bytes: %v", m.nodeID, kindLabel, digest, h.NodeID, n, err)
				return
			}
			mirrorRequestsTotal.Inc(m.nodeID, kindLabel, "peer")
			mirrorBytesTotal.Add(n, m.nodeID, "peer")
			return
		}
		if err != nil {
			logging.Debugf("angryduck-worker[%s]: mirror: node=%s couldn't serve %s: %v", m.nodeID, h.NodeID, digest, err)
		}
	}
	m.forget(digest)
	mirrorRequestsTotal.Inc(m.nodeID, kindLabel, "miss")
	http.NotFound(w, r)
}

// serveLocal answers from this node's content store.
func (m *Mirror) serveLocal(w http.ResponseWriter, r *http.Request, kind, digest string) (int64, error) {
	w.Header().Set("Docker-Content-Digest", digest)
	if kind == "manifests" {
		b, err := m.store.ReadBlob(r.Context(), digest, 4<<20)
		if err != nil {
			return 0, err
		}
		w.Header().Set("Content-Type", manifestMediaType(b))
		w.Header().Set("Content-Length", fmt.Sprint(len(b)))
		if r.Method == http.MethodHead {
			return 0, nil
		}
		n, err := w.Write(b)
		return int64(n), err
	}
	body, size, err := m.store.OpenContent(r.Context(), digest)
	if err != nil {
		return 0, err
	}
	defer body.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		return 0, nil
	}
	n, err := serveBody(w, body, size)
	if err != nil {
		panic(http.ErrAbortHandler) // cut the stream so containerd sees a short read
	}
	return n, nil
}

// clearBlobHeaders drops what serveLocal set before it failed.
func clearBlobHeaders(w http.ResponseWriter) {
	for _, h := range []string{"Content-Length", "Content-Type", "Docker-Content-Digest"} {
		w.Header().Del(h)
	}
}

// missLocal answers 404 after serveLocal failed without sending anything.
func missLocal(w http.ResponseWriter, r *http.Request) {
	clearBlobHeaders(w)
	http.NotFound(w, r)
}

// manifestMediaType reads mediaType from a manifest, or infers it.
func manifestMediaType(b []byte) string {
	var doc struct {
		MediaType string            `json:"mediaType"`
		Manifests []json.RawMessage `json:"manifests"`
	}
	_ = json.Unmarshal(b, &doc)
	switch {
	case doc.MediaType != "":
		return doc.MediaType
	case doc.Manifests != nil:
		return "application/vnd.oci.image.index.v1+json"
	default:
		return "application/vnd.oci.image.manifest.v1+json"
	}
}

// lookup asks the controller which fresh nodes hold digest.
func (m *Mirror) lookup(ctx context.Context, digest string) []model.Holder {
	m.mu.Lock()
	if e, ok := m.holders[digest]; ok && time.Since(e.at) < holderTTL {
		m.mu.Unlock()
		return e.list
	}
	m.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	u := m.controllerURL + "/layers/holders?digest=" + url.QueryEscape(digest) + "&exclude=" + url.QueryEscape(m.nodeID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil
	}
	sharedtoken.Set(req, m.token)
	var list []model.Holder
	resp, err := m.client.Do(req)
	if err == nil {
		if resp.StatusCode == http.StatusOK {
			_ = json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&list)
		}
		resp.Body.Close()
	}
	m.mu.Lock()
	for d, e := range m.holders {
		if time.Since(e.at) >= holderTTL {
			delete(m.holders, d)
		}
	}
	// Only a list worth reusing is kept: an empty answer (or none, the
	// controller unreachable) is asked again next time, so a blob that
	// lands on a peer a moment later is found.
	if len(list) > 0 {
		m.holders[digest] = holderEntry{list: list, at: time.Now()}
	}
	m.mu.Unlock()
	return list
}

// forget drops digest's cached holders once none of them could serve it.
func (m *Mirror) forget(digest string) {
	m.mu.Lock()
	delete(m.holders, digest)
	m.mu.Unlock()
}

// fromPeer streams digest from h. served is true once a response started
// going out to containerd (so no other peer can be tried after).
func (m *Mirror) fromPeer(w http.ResponseWriter, r *http.Request, h model.Holder, kind, digest string) (n int64, served bool, err error) {
	req, err := http.NewRequestWithContext(r.Context(), r.Method, "http://"+h.Address+"/mirror/content/"+digest+"?kind="+kind, nil)
	if err != nil {
		return 0, false, err
	}
	sharedtoken.Set(req, m.token)
	resp, err := m.client.Do(req)
	if err != nil {
		return 0, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, false, fmt.Errorf("status %d", resp.StatusCode)
	}
	for _, k := range []string{"Content-Type", "Content-Length", "Docker-Content-Digest"} {
		if v := resp.Header.Get(k); v != "" {
			w.Header().Set(k, v)
		}
	}
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return 0, true, nil
	}
	n, err = io.Copy(w, resp.Body)
	if err != nil {
		panic(http.ErrAbortHandler)
	}
	return n, true, nil
}

// RegisterPeer mounts /mirror/content/<digest>, which serves this node's
// own content to other nodes' mirrors, behind the token.
func (m *Mirror) RegisterPeer(mux *http.ServeMux) {
	mux.HandleFunc("/mirror/content/", sharedtoken.Require(m.token, func(w http.ResponseWriter, r *http.Request) {
		digest := strings.TrimPrefix(r.URL.Path, "/mirror/content/")
		if !validDigest(digest) || (r.Method != http.MethodGet && r.Method != http.MethodHead) {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if !m.hasLocal(r.Context(), digest) {
			http.NotFound(w, r)
			return
		}
		kind := "blobs"
		if r.URL.Query().Get("kind") == "manifests" {
			kind = "manifests"
		}
		n, err := m.serveLocal(w, r, kind, digest)
		mirrorServedTotal.Add(n, m.nodeID)
		if err != nil {
			logging.Warnf("angryduck-worker[%s]: mirror: serving %s to %s: %v", m.nodeID, digest, r.RemoteAddr, err)
			// Nothing was sent: a 404, so the asking mirror tries its next
			// holder. An empty 200 would count as served there and reach
			// containerd as a zero-byte blob.
			missLocal(w, r)
		}
	}))
}

// --- containerd configuration ---

// hostsMarker marks the hosts.toml files Angry Duck owns. Files without
// it (written by hand, or by another tool) are never touched.
const hostsMarker = "# managed by angryduck"

// HostsConfig writes containerd's per-registry hosts.toml files that point
// pulls at the mirror.
type HostsConfig struct {
	HostRoot  string   // the node's filesystem as seen from the worker
	ConfigDir string   // containerd's config_path on the node, e.g. /etc/containerd/certs.d
	Mirror    string   // the mirror's address, as containerd reaches it: "<pod IP>:18082"
	Token     string   // sent by containerd to the mirror (hosts.toml header); "" sends none
	Extra     []string // registries to configure even before an image of theirs is local
	NodeID    string

	mu     sync.Mutex
	wrote  bool // Sync ran with the mirror on, so there may be files to remove
	closed bool // set by RemoveOwn: no more writes
	warned map[string]bool
}

// RemoveOwn removes every hosts.toml this worker wrote (marked, and
// pointing at this worker's mirror address), and the registry directory
// when that leaves it empty, so containerd goes straight to the registry
// again. Files another worker pod wrote (a surge rollout) are left alone.
// Sync writes nothing afterwards. It returns the number of files removed.
func (h *HostsConfig) RemoveOwn() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	if h.Mirror == "" || !h.wrote {
		return 0
	}
	root := filepath.Join(h.HostRoot, h.ConfigDir)
	entries, err := os.ReadDir(root)
	if err != nil {
		return 0
	}
	ours := `[host."http://` + h.Mirror + `"]`
	n := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		p := filepath.Join(root, e.Name(), "hosts.toml")
		b, err := os.ReadFile(p)
		if err != nil || !strings.HasPrefix(string(b), hostsMarker) || !strings.Contains(string(b), ours) {
			continue
		}
		if os.Remove(p) == nil {
			n++
			_ = os.Remove(filepath.Join(root, e.Name())) // only if now empty
		}
	}
	return n
}

// Sync writes (or, with enabled=false, removes) hosts.toml for every
// registry named in images plus Extra. It only ever touches files that
// carry hostsMarker. containerd reads these files on each pull, so no
// restart is needed, as long as its CRI registry config_path points at
// ConfigDir.
func (h *HostsConfig) Sync(images []string, enabled bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	if enabled {
		h.wrote = true
	}
	if h.warned == nil {
		h.warned = map[string]bool{}
		if cfg, err := os.ReadFile(filepath.Join(h.HostRoot, "/etc/containerd/config.toml")); err == nil && enabled && !strings.Contains(string(cfg), h.ConfigDir) {
			logging.Warnf("angryduck-worker[%s]: mirror: /etc/containerd/config.toml doesn't set config_path = %q; containerd ignores the mirror until it does", h.NodeID, h.ConfigDir)
		}
	}
	hosts := map[string]bool{}
	for _, e := range h.Extra {
		if e = strings.TrimSpace(e); e != "" {
			hosts[e] = true
		}
	}
	for _, img := range images {
		if strings.HasPrefix(img, "sha256:") {
			continue
		}
		if host := imageref.Host(imageref.Normalize(img)); host != "" && host != "angryduck.local" {
			hosts[host] = true
		}
	}
	root := filepath.Join(h.HostRoot, h.ConfigDir)
	if !enabled {
		entries, _ := os.ReadDir(root)
		for _, e := range entries {
			p := filepath.Join(root, e.Name(), "hosts.toml")
			if b, err := os.ReadFile(p); err == nil && bytes.HasPrefix(b, []byte(hostsMarker)) {
				if os.Remove(p) == nil {
					logging.Infof("angryduck-worker[%s]: mirror off: removed %s", h.NodeID, filepath.Join(h.ConfigDir, e.Name(), "hosts.toml"))
				}
			}
		}
		return
	}
	names := make([]string, 0, len(hosts))
	for host := range hosts {
		names = append(names, host)
	}
	sort.Strings(names)
	for _, host := range names {
		want := hostsToml(host, h.Mirror, h.Token)
		p := filepath.Join(root, host, "hosts.toml")
		b, err := os.ReadFile(p)
		switch {
		case err == nil && !bytes.HasPrefix(b, []byte(hostsMarker)):
			if !h.warned[host] {
				h.warned[host] = true
				logging.Warnf("angryduck-worker[%s]: mirror: %s exists and isn't ours; leaving %s unmirrored", h.NodeID, filepath.Join(h.ConfigDir, host, "hosts.toml"), host)
			}
			continue
		case err == nil && string(b) == want:
			continue
		}
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			logging.Warnf("angryduck-worker[%s]: mirror: %v", h.NodeID, err)
			continue
		}
		if err := os.WriteFile(p, []byte(want), 0o600); err != nil {
			logging.Warnf("angryduck-worker[%s]: mirror: writing hosts.toml for %s: %v", h.NodeID, host, err)
			continue
		}
		logging.Infof("angryduck-worker[%s]: mirror: containerd now asks the mirror first for %s", h.NodeID, host)
	}
}

func hostsToml(host, mirror, token string) string {
	server := "https://" + host
	if host == "docker.io" {
		server = "https://registry-1.docker.io"
	}
	return hostsMarker + ": rewritten by the worker, removed when MIRROR_ENABLED=false.\n" +
		"# Pull-only: tags still resolve at the registry; content comes from\n" +
		"# this node or a peer when one has it, else from the registry.\n" +
		"server = \"" + server + "\"\n\n" +
		"[host.\"http://" + mirror + "\"]\n" +
		"  capabilities = [\"pull\"]\n" + tokenHeader(mirror, token)
}

func tokenHeader(mirror, token string) string {
	if token == "" {
		return ""
	}
	return "  [host.\"http://" + mirror + "\".header]\n" +
		"    Authorization = \"Bearer " + token + "\"\n"
}
