package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	"angryduck/internal/registryclient"
	"angryduck/internal/sharedtoken"
)

var (
	mirrorRequestsTotal = metrics.NewCounterVec(
		"angryduck_worker_mirror_requests_total",
		"Content requests containerd sent to this node's mirror, by kind (blob, manifest) and result: local (served from this node), peer (from another node), stream (from a node still receiving it in a spread), miss (nobody had it: containerd goes to the registry), error.",
		"node", "kind", "result",
	)
	mirrorBytesTotal = metrics.NewCounterVec(
		"angryduck_worker_mirror_bytes_total",
		"Bytes the mirror served to containerd, by source: local, peer, or stream (a peer still receiving it). Every one of them is a byte not downloaded from the registry.",
		"node", "source",
	)
	mirrorServedTotal = metrics.NewCounterVec(
		"angryduck_worker_mirror_peer_served_bytes_total",
		"Bytes this node served to other nodes' mirrors.",
		"node",
	)
	mirrorResolvesTotal = metrics.NewCounterVec(
		"angryduck_worker_mirror_resolves_total",
		"Tags containerd asked this node's mirror to resolve, by what the registry answered (origin: ok, missing, denied, unreachable; unchecked: no node holds the tag, so nobody asked) and where the manifest came from (result: local, peer, miss: containerd asks the registry itself). With any origin but ok, the newest copy a node holds was served.",
		"node", "origin", "result",
	)
	mirrorControllerDown = metrics.NewGaugeVec(
		"angryduck_worker_mirror_controller_unreachable",
		"1 while this node's mirror leaves the controller alone after it didn't answer in time (MIRROR_CONTROLLER_BACKOFF_MIN_MS, doubling up to MIRROR_CONTROLLER_BACKOFF_MAX_MS): content it doesn't hold itself goes straight to the registry.",
		"node",
	)
)

// MirrorStore is what the mirror needs from containerd. ContainerdStore has it.
type MirrorStore interface {
	Digests(ctx context.Context) (map[string]bool, error)
	ReadBlob(ctx context.Context, digest string, max int64) ([]byte, error)
	OpenContent(ctx context.Context, digest string) (io.ReadCloser, int64, error)
	Target(ctx context.Context, image string) (digest string, updated time.Time, err error)
}

// TagOrigin asks a tag's registry what the tag points at
// (registryclient.Client).
type TagOrigin interface {
	ManifestDigest(ctx context.Context, image string) (string, error)
}

// servableOrigin is a TagOrigin that can also tell whether it can serve an
// image it named (registryclient.Client does).
type servableOrigin interface {
	Servable(ctx context.Context, image, platform string) error
}

// Mirror is a pull-only OCI registry mirror for this node's containerd.
// containerd asks it first for every manifest and blob by digest, and,
// with resolve on, for every tag. The mirror answers from this node's
// content store, else streams the content from a peer node that has it
// (the controller's layer inventory says which), else from a peer still
// receiving it in a spread, as the bytes land there, else answers 404 and
// containerd goes to the registry as usual. containerd verifies every
// digest, so a peer can't slip in anything else. The mirror never
// downloads from a registry: that stays containerd's.
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

	origin         TagOrigin // nil: tags are left to the registry
	resolveTimeout time.Duration
	downFor        time.Duration
	askTimeout     time.Duration // for the controller's answers about tags
	verifyPlatform string        // "" : the registry's answer is trusted as is (see EnableVerify)
	verifyTimeout  time.Duration

	ctrlTimeout    time.Duration // for the controller's answers about holders
	ctrlBackoffMin time.Duration // first wait after the controller didn't answer in time...
	ctrlBackoffMax time.Duration // ...doubling up to this (0: asked every time)

	mu          sync.Mutex
	holders     map[string]holderEntry
	tagNodes    map[string]holderEntry // repo -> nodes holding the tag served from node copies (see resolve)
	servable    map[string]verdict     // digest -> whether the registry could serve it (see checkServable)
	down        map[string]time.Time   // registry host -> when to ask it again
	ctrlDown    bool                   // the controller last didn't answer in time
	ctrlUntil   time.Time              // until then, nobody asks it (see controllerSkip)
	ctrlBackoff time.Duration          // the current wait; doubles per timeout in a row
}

type verdict struct {
	ok bool
	at time.Time
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
	mirrorRequestsTotal.Add(0, nodeID, "blob", "stream") // streams carry layers only
	for _, src := range []string{"local", "peer", "stream"} {
		mirrorBytesTotal.Add(0, nodeID, src)
	}
	mirrorServedTotal.Add(0, nodeID)
	mirrorControllerDown.Set(0, nodeID)
	return &Mirror{
		store: store, nodeID: nodeID, token: token, controllerURL: strings.TrimRight(controllerURL, "/"),
		client:         &http.Client{Transport: peerTransport(3*time.Second, 30*time.Second)},
		holders:        map[string]holderEntry{},
		tagNodes:       map[string]holderEntry{},
		servable:       map[string]verdict{},
		ctrlTimeout:    time.Second,
		ctrlBackoffMin: time.Second,
		ctrlBackoffMax: 10 * time.Second,
	}
}

// SetControllerTimeouts sets how long the mirror waits for the controller's
// answers about holders, and how long it leaves a controller that didn't
// answer in time alone: backoffMin after the first timeout, doubling with
// each timeout in a row up to backoffMax (backoffMax 0: asked every time).
func (m *Mirror) SetControllerTimeouts(timeout, backoffMin, backoffMax time.Duration) {
	if timeout > 0 {
		m.ctrlTimeout = timeout
	}
	if backoffMax >= 0 {
		m.ctrlBackoffMax = backoffMax
	}
	if backoffMin > 0 {
		m.ctrlBackoffMin = backoffMin
	}
	if m.ctrlBackoffMin > m.ctrlBackoffMax {
		m.ctrlBackoffMin = m.ctrlBackoffMax
	}
}

// controllerSkip reports whether the controller is to be left alone: it
// didn't answer in time, and its current wait isn't over. A controller that
// is down but still routable (its node died, it is starved of CPU) would
// otherwise cost every layer of every pull on every node a full timeout
// before containerd goes to the registry. Once the wait is over, one request
// asks again while the others still skip. The wait starts short, so a blip
// is over fast, and doubles with each timeout in a row up to its cap, so a
// controller that stays down costs one timeout per node per cap.
func (m *Mirror) controllerSkip() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.ctrlDown {
		return false
	}
	now := time.Now()
	if now.Before(m.ctrlUntil) {
		return true
	}
	m.ctrlUntil = now.Add(m.ctrlTimeout) // this request asks; the rest skip meanwhile
	return false
}

// controllerAnswered records how asking the controller went. Only a
// timeout leaves it alone: a refused connection (no controller pod behind
// the Service) fails at once and costs nothing, so peers are back the
// moment a new controller answers.
func (m *Mirror) controllerAnswered(err error) {
	slow := err != nil && isTimeout(err) && m.ctrlBackoffMax > 0
	m.mu.Lock()
	was := m.ctrlDown
	if slow {
		if !was {
			m.ctrlBackoff = m.ctrlBackoffMin
		} else {
			m.ctrlBackoff = min(2*m.ctrlBackoff, m.ctrlBackoffMax)
		}
		m.ctrlDown, m.ctrlUntil = true, time.Now().Add(m.ctrlBackoff)
	} else {
		m.ctrlDown, m.ctrlUntil, m.ctrlBackoff = false, time.Time{}, 0
	}
	wait := m.ctrlBackoff
	m.mu.Unlock()
	switch {
	case slow && !was:
		mirrorControllerDown.Set(1, m.nodeID)
		logging.Warnf("angryduck-worker[%s]: mirror: the controller didn't answer in time (%v): content this node doesn't hold goes straight to the registry; asking again in %s, then less often up to every %s", m.nodeID, err, wait, m.ctrlBackoffMax)
	case !slow && was:
		mirrorControllerDown.Set(0, m.nodeID)
		logging.Infof("angryduck-worker[%s]: mirror: the controller answers again: peers serve again", m.nodeID)
	}
}

// isTimeout reports whether err is a deadline running out, not a refusal.
func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// EnableResolve makes the mirror resolve tags (see resolve). Each
// registry answer waits at most timeout; a registry that gave none is
// left alone for downFor (0: asked every time). The controller's answers
// about a tag wait at most askTimeout.
// EnableVerify makes the mirror check, before trusting it, a registry
// answer that points a tag at a digest no node holds while nodes hold the
// tag at another one: the registry must serve that image's manifests,
// config and layers for platform within timeout. If it can't (a proxy
// answering tags while its upstream is down), the newest node copy is
// served instead. Verdicts are reused for the unreachable TTL (downFor).
func (m *Mirror) EnableVerify(platform string, timeout time.Duration) {
	if _, ok := m.origin.(servableOrigin); ok {
		m.verifyPlatform, m.verifyTimeout = platform, timeout
	}
}

func (m *Mirror) EnableResolve(origin TagOrigin, timeout, downFor, askTimeout time.Duration) {
	m.origin, m.resolveTimeout, m.downFor, m.askTimeout = origin, timeout, downFor, askTimeout
	m.down = map[string]time.Time{}
	mirrorResolvesTotal.Add(0, m.nodeID, "unchecked", "miss")
	for _, o := range []string{"ok", "missing", "denied", "unreachable", "unservable"} {
		for _, r := range []string{"local", "peer", "miss"} {
			mirrorResolvesTotal.Add(0, m.nodeID, o, r)
		}
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
		// containerd names the registry in ns when it asks a mirror.
		host := r.URL.Query().Get("ns")
		if kind, digest, ok := parseMirrorPath(r.URL.Path); ok {
			m.serve(w, r, kind, digest, m.nodesOfTag(host, repoOfPath(r.URL.Path)))
			return
		}
		if repo, tag, ok := parseTagPath(r.URL.Path); ok && m.origin != nil && host != "" {
			m.resolve(w, r, imageref.Normalize(host+"/"+repo+":"+tag), host, repo)
			return
		}
		http.NotFound(w, r) // everything else: the registry answers
	})
}

// parseTagPath reads /v2/<name>/manifests/<tag>.
func parseTagPath(p string) (repo, tag string, ok bool) {
	i := strings.LastIndex(p, "/manifests/")
	if !strings.HasPrefix(p, "/v2/") || i <= len("/v2/") {
		return "", "", false
	}
	repo, tag = p[len("/v2/"):i], p[i+len("/manifests/"):]
	return repo, tag, validTag(tag)
}

// validTag follows the distribution spec: [a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}.
func validTag(t string) bool {
	if t == "" || len(t) > 128 || t[0] == '.' || t[0] == '-' {
		return false
	}
	for _, c := range t {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '.' || c == '-') {
			return false
		}
	}
	return true
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

// repoOfPath reads <name> from /v2/<name>/{blobs,manifests}/<ref>.
func repoOfPath(p string) string {
	for _, k := range []string{"/blobs/", "/manifests/"} {
		if i := strings.LastIndex(p, k); i > len("/v2/") && strings.HasPrefix(p, "/v2/") {
			return p[len("/v2/"):i]
		}
	}
	return ""
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

// serve answers digest from this node or a peer, else 404, and returns
// which: local, peer, miss or error. tagNodes, tried after the holders
// the layer inventory names, are nodes holding the tag this content
// belongs to (see resolve).
func (m *Mirror) serve(w http.ResponseWriter, r *http.Request, kind, digest string, tagNodes []model.Holder) string {
	kindLabel := strings.TrimSuffix(kind, "s")
	if m.hasLocal(r.Context(), digest) {
		n, err := m.serveLocal(w, r, kind, digest)
		if err == nil {
			mirrorRequestsTotal.Inc(m.nodeID, kindLabel, "local")
			mirrorBytesTotal.Add(n, m.nodeID, "local")
			return "local"
		}
		// Nothing was sent (serveLocal cuts the connection otherwise): go on
		// as if the node didn't have it, to the peers and then a 404, the
		// only answer that sends containerd on to the next host.
		logging.Warnf("angryduck-worker[%s]: mirror: serving %s %s locally: %v", m.nodeID, kindLabel, digest, err)
		clearBlobHeaders(w)
	}
	holders := m.lookup(r.Context(), digest)
	if len(tagNodes) > 0 {
		tried := make(map[string]bool, len(holders))
		for _, h := range holders {
			tried[h.NodeID] = true
		}
		for _, h := range tagNodes {
			if !tried[h.NodeID] && h.NodeID != m.nodeID {
				holders = append(holders, h)
			}
		}
	}
	for _, h := range holders {
		source := "peer"
		if h.Arriving {
			if kind != "blobs" {
				continue // spreads carry layers; manifests come whole from holders
			}
			source = "stream"
		}
		n, served, err := m.fromPeer(w, r, h, kind, digest)
		if served {
			if err != nil {
				mirrorRequestsTotal.Inc(m.nodeID, kindLabel, "error")
				logging.Warnf("angryduck-worker[%s]: mirror: %s %s from node=%s (%s) broke after %d bytes: %v", m.nodeID, kindLabel, digest, h.NodeID, source, n, err)
				return "error"
			}
			mirrorRequestsTotal.Inc(m.nodeID, kindLabel, source)
			mirrorBytesTotal.Add(n, m.nodeID, source)
			return source
		}
		if err != nil {
			logging.Debugf("angryduck-worker[%s]: mirror: node=%s couldn't serve %s: %v", m.nodeID, h.NodeID, digest, err)
		}
	}
	m.forget(digest)
	mirrorRequestsTotal.Inc(m.nodeID, kindLabel, "miss")
	http.NotFound(w, r)
	return "miss"
}

// resolve answers a tag. The registry decides what the tag means when it
// answers within resolveTimeout, so a moved tag is never served stale.
// When it can't (down, refusing Angry Duck's credentials, or no longer
// knowing the tag), the newest copy a node holds is served: freshness
// never stops peers from sharing. A tag no node holds is a 404 without
// asking the registry, so containerd goes there itself, as it would
// without the mirror.
//
// The nodes holding the tag are kept for repo (holderTTL), so the content
// containerd asks next (the platform manifest, config and layers) is
// found on them even when the layer inventory doesn't place it yet.
func (m *Mirror) resolve(w http.ResponseWriter, r *http.Request, image, host, repo string) {
	if m.askTag(r.Context(), image, false).Holders == 0 {
		mirrorResolvesTotal.Inc(m.nodeID, "unchecked", "miss")
		http.NotFound(w, r)
		return
	}
	origin, digest := m.atOrigin(r.Context(), image, host)
	if origin == "ok" && !m.checkServable(r.Context(), image, digest) {
		origin = "unservable"
	}
	var nodes []model.Holder
	if origin != "ok" {
		ans := m.askTag(r.Context(), image, true)
		digest, nodes = ans.Digest, ans.Nodes
		m.keepTagNodes(host, repo, nodes)
	}
	result := "miss"
	if digest != "" {
		result = m.serve(w, r, "manifests", digest, nodes)
	} else {
		http.NotFound(w, r)
	}
	mirrorResolvesTotal.Inc(m.nodeID, origin, result)
	if origin != "ok" {
		logging.Infof("angryduck-worker[%s]: mirror: image=%s: registry answered %s; newest node copy %q: %s", m.nodeID, image, origin, digest, result)
	}
}

// checkServable reports whether the registry's digest for image can be
// trusted. It is checked only when no node holds that digest but some
// node holds the tag at another one, so a healthy registry costs nothing
// extra and a tag moved upstream costs one check per digest per TTL.
func (m *Mirror) checkServable(ctx context.Context, image, digest string) bool {
	so, ok := m.origin.(servableOrigin)
	if !ok || m.verifyPlatform == "" || digest == "" || m.hasLocal(ctx, digest) {
		return true
	}
	m.mu.Lock()
	v, cached := m.servable[digest]
	m.mu.Unlock()
	if cached && time.Since(v.at) < m.downFor {
		return v.ok
	}
	if len(m.lookup(ctx, digest)) > 0 {
		return true
	}
	newest := m.askTag(ctx, image, true).Digest
	if newest == "" || newest == digest {
		return true
	}
	vctx, cancel := context.WithTimeout(ctx, m.verifyTimeout)
	defer cancel()
	err := so.Servable(vctx, imageref.Repo(image)+"@"+digest, m.verifyPlatform)
	v = verdict{ok: err == nil, at: time.Now()}
	m.mu.Lock()
	for d, e := range m.servable {
		if time.Since(e.at) >= m.downFor {
			delete(m.servable, d)
		}
	}
	if m.downFor > 0 {
		m.servable[digest] = v
	}
	m.mu.Unlock()
	if err != nil {
		logging.Infof("angryduck-worker[%s]: mirror: image=%s: registry named %s, which no node holds, but can't serve it (%v); serving the newest node copy %s", m.nodeID, image, digest, err, newest)
	}
	return v.ok
}

// keepTagNodes remembers, for holderTTL, which nodes hold a tag of
// host/repo that was served from node copies.
func (m *Mirror) keepTagNodes(host, repo string, nodes []model.Holder) {
	if len(nodes) == 0 || repo == "" {
		return
	}
	key := host + "/" + repo
	m.mu.Lock()
	for k, e := range m.tagNodes {
		if time.Since(e.at) >= holderTTL {
			delete(m.tagNodes, k)
		}
	}
	m.tagNodes[key] = holderEntry{list: nodes, at: time.Now()}
	m.mu.Unlock()
}

// nodesOfTag returns the nodes keepTagNodes remembered for host/repo.
func (m *Mirror) nodesOfTag(host, repo string) []model.Holder {
	if repo == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.tagNodes[host+"/"+repo]
	if !ok || time.Since(e.at) >= holderTTL {
		return nil
	}
	return e.list
}

// atOrigin asks host what image's tag points at, unless host gave no
// answer less than downFor ago. It returns registryclient.Availability's
// verdict and, when ok, the digest.
func (m *Mirror) atOrigin(ctx context.Context, image, host string) (string, string) {
	m.mu.Lock()
	until, down := m.down[host]
	m.mu.Unlock()
	if down && time.Now().Before(until) {
		return "unreachable", ""
	}
	ctx, cancel := context.WithTimeout(ctx, m.resolveTimeout)
	defer cancel()
	digest, err := m.origin.ManifestDigest(ctx, image)
	status := registryclient.Availability(err)
	m.mu.Lock()
	if status == "unreachable" && m.downFor > 0 {
		m.down[host] = time.Now().Add(m.downFor)
	} else {
		delete(m.down, host)
	}
	m.mu.Unlock()
	return status, digest
}

// askTag asks the controller how many fresh nodes hold image and, with
// newest, which digest the newest of their copies points at.
func (m *Mirror) askTag(ctx context.Context, image string, newest bool) model.TagHolders {
	var ans model.TagHolders
	if m.controllerSkip() {
		return ans
	}
	ctx, cancel := context.WithTimeout(ctx, m.askTimeout)
	defer cancel()
	u := m.controllerURL + "/images/tag?image=" + url.QueryEscape(image)
	if newest {
		u += "&newest=1"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return ans
	}
	sharedtoken.Set(req, m.token)
	resp, err := m.client.Do(req)
	m.controllerAnswered(err)
	if err != nil {
		logging.Debugf("angryduck-worker[%s]: mirror: asking the controller about %s: %v", m.nodeID, image, err)
		return ans
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		_ = json.NewDecoder(io.LimitReader(resp.Body, 4<<10)).Decode(&ans)
	}
	return ans
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
	if m.controllerSkip() {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, m.ctrlTimeout)
	defer cancel()
	u := m.controllerURL + "/layers/holders?digest=" + url.QueryEscape(digest) + "&exclude=" + url.QueryEscape(m.nodeID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil
	}
	sharedtoken.Set(req, m.token)
	var list []model.Holder
	resp, err := m.client.Do(req)
	m.controllerAnswered(err)
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
// going out to containerd (so no other peer can be tried after). A holder
// still receiving the blob (Arriving) serves it from /spread/content, which
// sends the bytes as they land there, or the committed blob once it has.
func (m *Mirror) fromPeer(w http.ResponseWriter, r *http.Request, h model.Holder, kind, digest string) (n int64, served bool, err error) {
	u := "http://" + h.Address + "/mirror/content/" + digest + "?kind=" + kind
	if h.Arriving {
		u = "http://" + h.Address + "/spread/content/" + digest
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, u, nil)
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
	n, err = relay(w, resp.Body)
	if err != nil {
		panic(http.ErrAbortHandler)
	}
	return n, true, nil
}

// RegisterPeer mounts, behind the token, /mirror/content/<digest>, which
// serves this node's own content to other nodes' mirrors, and
// /mirror/tag?image=, which tells the controller what a tag points at
// here and since when.
func (m *Mirror) RegisterPeer(mux *http.ServeMux) {
	mux.HandleFunc("/mirror/tag", sharedtoken.Require(m.token, func(w http.ResponseWriter, r *http.Request) {
		digest, updated, err := m.store.Target(r.Context(), r.URL.Query().Get("image"))
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(model.TagTarget{Digest: digest, Updated: updated})
	}))
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
	Resolve   bool     // containerd asks the mirror for tags too
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
		want := hostsToml(host, h.Mirror, h.Token, h.Resolve)
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

func hostsToml(host, mirror, token string, resolve bool) string {
	server := "https://" + host
	if host == "docker.io" {
		server = "https://registry-1.docker.io"
	}
	tags, capabilities := "# Pull-only: tags resolve at the registry; content comes from\n", `["pull"]`
	if resolve {
		tags, capabilities = "# Pull-only: the mirror resolves tags with the registry, or a node's\n# newest copy when the registry can't answer; content comes from\n", `["pull", "resolve"]`
	}
	return hostsMarker + ": rewritten by the worker, removed when MIRROR_ENABLED=false.\n" +
		tags + "# this node or a peer when one has it, else from the registry.\n" +
		"server = \"" + server + "\"\n\n" +
		"[host.\"http://" + mirror + "\"]\n" +
		"  capabilities = " + capabilities + "\n" + tokenHeader(mirror, token)
}

func tokenHeader(mirror, token string) string {
	if token == "" {
		return ""
	}
	return "  [host.\"http://" + mirror + "\".header]\n" +
		"    Authorization = \"Bearer " + token + "\"\n"
}
