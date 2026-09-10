package worker

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"angryduck/internal/logging"
	"angryduck/internal/metrics"
	"angryduck/internal/model"
)

// How the mirror works, end to end:
//
//  1. containerd resolves a tag against the origin registry itself (one
//     HEAD, a few hundred bytes) and gets the image's digest. The mirror
//     is registered with capabilities = ["pull"] only, so tag resolution
//     never comes here — which means the digest we match on is always
//     the one origin says is current, and a moved tag can never make us
//     import a stale image.
//  2. containerd asks this worker (127.0.0.1) for manifests/<digest>
//     before origin. That request is held.
//  3. The worker asks the controller which fresh nodes reported that
//     digest, and orders one of them to export it. The peer answers with
//     `ctr images export` writing directly into the TCP socket; here that
//     same socket becomes `ctr images import`'s stdin. Image bytes move
//     node ctr -> socket -> socket -> node ctr, entirely in the kernel —
//     the worker process never reads or copies a single one of them.
//  4. Whether the import worked or not, the held request is answered 404.
//     containerd moves on to origin, fetches the small manifest, and for
//     every layer finds the blob already in its content store, so it
//     downloads nothing else and goes straight to unpacking. If no peer had the image, the
//     404 comes back immediately and the pull is an ordinary origin pull.
//
// The whole image moves as one unit, identified only by name and digest:
// no per-layer lookups, no layer serving. Blob requests are always 404'd
// on the spot.
//
// "Held" means a parked goroutine waiting on a channel or a process exit
// — no polling, no CPU. The worker's only active work per transfer is a
// handful of syscalls to wire file descriptors together.

var (
	mirrorRequestsTotal = metrics.NewCounterVec(
		"angryduck_worker_mirror_requests_total",
		"Registry requests containerd sent to this node's mirror, by kind and outcome.",
		"node", "kind", "result",
	)
	mirrorTransfersTotal = metrics.NewCounterVec(
		"angryduck_worker_mirror_transfers_total",
		"Peer image transfers attempted by this node (one per distinct digest, however many pulls wait on it), by result.",
		"node", "result",
	)
	mirrorExportsTotal = metrics.NewCounterVec(
		"angryduck_worker_mirror_exports_total",
		"Export orders this node received from peers, by result.",
		"node", "result",
	)
	mirrorBytesTotal = metrics.NewCounterVec(
		"angryduck_worker_mirror_bytes_total",
		"Image bytes moved between nodes by peer transfer (read from TCP_INFO; the worker never touches them). direction=in is origin traffic avoided.",
		"node", "direction",
	)
)

const tokenHeader = "X-Angryduck-Token"
const refHeader = "X-Angryduck-Ref"

// MirrorConfig carries every mirror tunable.
type MirrorConfig struct {
	NodeID            string
	ControllerURL     string
	Token             string
	ContainerdAddress string // socket path as seen on the node
	Namespace         string
	HoldTimeout       time.Duration // max time a containerd request is held
	QueueWait         time.Duration // max wait for a free transfer slot
	MaxExports        int
	MaxImports        int
}

// Mirror is both halves of peer transfer: the containerd-facing /v2/
// endpoint (requester side) and the /export endpoint (source side).
type Mirror struct {
	cfg    MirrorConfig
	hx     *HostExec
	inv    *Inventory
	client *http.Client

	exportSlots chan struct{}
	importSlots chan struct{}

	mu      sync.Mutex
	flights map[string]*flight
}

type flight struct {
	done   chan struct{}
	result string
}

// NewMirror builds a Mirror.
func NewMirror(cfg MirrorConfig, hx *HostExec, inv *Inventory) *Mirror {
	if cfg.MaxExports < 1 {
		cfg.MaxExports = 1
	}
	if cfg.MaxImports < 1 {
		cfg.MaxImports = 1
	}
	return &Mirror{
		cfg:         cfg,
		hx:          hx,
		inv:         inv,
		client:      &http.Client{Timeout: 2 * time.Second},
		exportSlots: make(chan struct{}, cfg.MaxExports),
		importSlots: make(chan struct{}, cfg.MaxImports),
		flights:     make(map[string]*flight),
	}
}

// ---------------------------------------------------------------------------
// Requester side: containerd -> /v2/
// ---------------------------------------------------------------------------

// ServeRegistry answers containerd's mirror requests. Every answer is a
// 404 except /v2/ itself; the only question is how long the 404 waits.
func (m *Mirror) ServeRegistry(w http.ResponseWriter, r *http.Request) {
	if !isLoopback(r.RemoteAddr) {
		// Only this node's containerd has any business here.
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/v2/")
	if rest == "" {
		w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{}"))
		return
	}
	kind, name, ref := parseV2Path(rest)
	if kind != "manifests" || !isDigest(ref) || (r.Method != http.MethodGet && r.Method != http.MethodHead) {
		if kind == "" {
			kind = "other"
		}
		mirrorRequestsTotal.Inc(m.cfg.NodeID, kind, "passthrough")
		notFound(w)
		return
	}
	ns := r.URL.Query().Get("ns")
	if ns == "" {
		mirrorRequestsTotal.Inc(m.cfg.NodeID, kind, "passthrough")
		notFound(w)
		return
	}
	image := ns + "/" + name + "@" + ref
	result := m.await(r.Context(), ref, image)
	mirrorRequestsTotal.Inc(m.cfg.NodeID, kind, result)
	notFound(w)
}

// await joins the in-flight transfer for digest, or starts one. Many pods
// on one node needing the same image cause exactly one transfer.
func (m *Mirror) await(reqCtx context.Context, digest, image string) string {
	if m.inv.Has(digest) {
		return "local"
	}
	m.mu.Lock()
	f, joined := m.flights[digest]
	if !joined {
		f = &flight{done: make(chan struct{})}
		m.flights[digest] = f
		go m.fly(f, digest, image)
	}
	m.mu.Unlock()

	select {
	case <-f.done:
		if joined {
			return "joined_" + f.result
		}
		return f.result
	case <-reqCtx.Done():
		// containerd gave up (pod deleted, kubelet cancelled). The
		// transfer carries on to completion or HoldTimeout: a retry will
		// join it or find the image already local.
		return "cancelled"
	}
}

// fly runs one transfer with its own deadline, detached from any single
// request so a cancelled first waiter can't abort it for the others.
func (m *Mirror) fly(f *flight, digest, image string) {
	ctx, cancel := context.WithTimeout(context.Background(), m.cfg.HoldTimeout)
	defer cancel()
	start := time.Now()
	f.result = m.transfer(ctx, digest, image)
	mirrorTransfersTotal.Inc(m.cfg.NodeID, f.result)
	logging.Infof("angryduck-worker-mirror: %s image=%s result=%s in %s", "transfer", image, f.result, time.Since(start).Round(time.Millisecond))
	m.mu.Lock()
	delete(m.flights, digest)
	m.mu.Unlock()
	close(f.done)
}

// transfer finds a source and imports from it. Result strings are metric
// labels: hit | miss | failed | busy | timeout.
func (m *Mirror) transfer(ctx context.Context, digest, image string) string {
	if !acquire(ctx, m.importSlots, m.cfg.QueueWait) {
		// Every import slot on this node is busy. Don't queue behind
		// them for the whole hold window — origin is faster than that.
		return "busy"
	}
	defer func() { <-m.importSlots }()

	for {
		peers, err := m.peers(ctx, digest)
		if err != nil {
			logging.Warnf("angryduck-worker-mirror: peer lookup for %s failed, letting containerd go to origin: %v", image, err)
			return "miss"
		}
		if len(peers) == 0 {
			return "miss"
		}
		sawBusy := false
		for _, peer := range peers {
			switch m.importFrom(ctx, peer, digest, image) {
			case outcomeOK:
				return "hit"
			case outcomeBusy:
				sawBusy = true
			}
			if ctx.Err() != nil {
				return "timeout"
			}
		}
		if !sawBusy {
			return "failed"
		}
		// Every source was mid-export. Sources multiply as nodes finish
		// importing and announce themselves, so ask the controller again
		// in a second rather than queueing on the same saturated seeds.
		t := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			t.Stop()
			return "timeout"
		case <-t.C:
		}
	}
}

type outcome int

const (
	outcomeOK outcome = iota
	outcomeBusy
	outcomeFailed
)

// importFrom orders peer to export digest and pipes the socket into the
// node's `ctr images import`.
func (m *Mirror) importFrom(ctx context.Context, peer, digest, image string) outcome {
	d := net.Dialer{Timeout: 2 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", peer)
	if err != nil {
		logging.Debugf("angryduck-worker-mirror: dial %s: %v", peer, err)
		return outcomeFailed
	}
	tcp := conn.(*net.TCPConn)
	// The peer may queue us for up to its QueueWait before answering.
	_ = tcp.SetDeadline(time.Now().Add(m.cfg.QueueWait + 5*time.Second))
	req := "GET /export?" + url.Values{"digest": {digest}}.Encode() + " HTTP/1.1\r\n" +
		"Host: " + peer + "\r\n" + tokenHeader + ": " + m.cfg.Token + "\r\nConnection: close\r\n\r\n"
	if _, err := tcp.Write([]byte(req)); err != nil {
		tcp.Close()
		return outcomeFailed
	}
	status, hdr, err := readResponseHead(tcp)
	if err != nil || status != http.StatusOK {
		tcp.Close()
		if status == http.StatusServiceUnavailable {
			return outcomeBusy
		}
		logging.Debugf("angryduck-worker-mirror: peer %s declined %s: status=%d err=%v", peer, digest, status, err)
		return outcomeFailed
	}
	// From here the socket belongs to ctr. File() dups the descriptor;
	// closing the Go conn leaves the dup (and the socket) alive.
	f, err := tcp.File()
	tcp.Close()
	if err != nil {
		return outcomeFailed
	}
	defer f.Close()

	// --no-unpack is load-bearing, not an optimization. containerd's own
	// pull — the one whose manifest request we are holding — takes an
	// unpack lock on that manifest digest before fetching it. An import
	// that unpacks needs the same lock, so it waits on the pull, the pull
	// waits on us, and we wait on the import: a deadlock until the hold
	// timeout. (Reproduced against real containerd; the import sat
	// blocked in Unpacker.lockBlobDescriptor with every byte received.)
	// Landing blobs only in the content store touches no unpack lock, and
	// the pull unpacks them once, exactly as it would after any download.
	cmd, err := m.hx.Command(ctx, "ctr", "-a", m.cfg.ContainerdAddress, "-n", m.cfg.Namespace, "images", "import", "--no-unpack", "-")
	if err != nil {
		logging.Errorf("angryduck-worker-mirror: %v", err)
		return outcomeFailed
	}
	stderr := &tailBuffer{max: 2048}
	cmd.Stdin = f
	cmd.Stderr = stderr
	start := time.Now()
	err = cmd.Run()
	_, in := tcpBytes(f)
	if in > 0 {
		mirrorBytesTotal.Add(int64(in), m.cfg.NodeID, "in")
	}
	if err != nil {
		logging.Warnf("angryduck-worker-mirror: import of %s from %s failed after %s (%s received): %v: %s",
			image, peer, time.Since(start).Round(time.Millisecond), humanBytes(in), err, strings.TrimSpace(stderr.String()))
		return outcomeFailed
	}
	ref := hdr.Get(refHeader)
	if ref == "" {
		ref = image
	}
	m.inv.Add(digest, ref)
	go m.announce(digest)
	logging.Infof("angryduck-worker-mirror: imported %s from %s: %s in %s", ref, peer, humanBytes(in), time.Since(start).Round(time.Millisecond))
	return outcomeOK
}

func (m *Mirror) peers(ctx context.Context, digest string) ([]string, error) {
	u := m.cfg.ControllerURL + "/peers?" + url.Values{"digest": {digest}, "node": {m.cfg.NodeID}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("controller status %d", resp.StatusCode)
	}
	var pr model.PeersResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return nil, err
	}
	return pr.Peers, nil
}

func (m *Mirror) announce(digest string) {
	body, _ := json.Marshal(model.Announce{NodeID: m.cfg.NodeID, Digest: digest})
	resp, err := m.client.Post(m.cfg.ControllerURL+"/announce", "application/json", bytes.NewReader(body))
	if err != nil {
		logging.Debugf("angryduck-worker-mirror: announce %s: %v (next report will cover it)", digest, err)
		return
	}
	resp.Body.Close()
}

// ---------------------------------------------------------------------------
// Source side: peer -> /export
// ---------------------------------------------------------------------------

// ServeExport streams a local image to a peer by making the peer's socket
// `ctr images export`'s stdout. Content goes containerd -> ctr -> socket.
func (m *Mirror) ServeExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get(tokenHeader)), []byte(m.cfg.Token)) != 1 {
		mirrorExportsTotal.Inc(m.cfg.NodeID, "unauthorized")
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	digest := r.URL.Query().Get("digest")
	if !isDigest(digest) {
		http.Error(w, "bad digest", http.StatusBadRequest)
		return
	}
	ref, ok := m.inv.Lookup(digest)
	if !ok {
		mirrorExportsTotal.Inc(m.cfg.NodeID, "not_found")
		http.Error(w, "not here", http.StatusNotFound)
		return
	}
	if !acquire(r.Context(), m.exportSlots, m.cfg.QueueWait) {
		mirrorExportsTotal.Inc(m.cfg.NodeID, "busy")
		http.Error(w, "busy", http.StatusServiceUnavailable)
		return
	}
	defer func() { <-m.exportSlots }()

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "no hijack", http.StatusInternalServerError)
		return
	}
	conn, _, err := hj.Hijack()
	if err != nil {
		return
	}
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		conn.Close()
		return
	}
	head := "HTTP/1.1 200 OK\r\nContent-Type: application/x-tar\r\n" + refHeader + ": " + ref + "\r\nConnection: close\r\n\r\n"
	if _, err := tcp.Write([]byte(head)); err != nil {
		tcp.Close()
		return
	}
	f, err := tcp.File()
	tcp.Close()
	if err != nil {
		return
	}
	defer f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), m.cfg.HoldTimeout)
	defer cancel()
	cmd, err := m.hx.Command(ctx, "ctr", "-a", m.cfg.ContainerdAddress, "-n", m.cfg.Namespace, "images", "export", "-", ref)
	if err != nil {
		logging.Errorf("angryduck-worker-mirror: %v", err)
		mirrorExportsTotal.Inc(m.cfg.NodeID, "failed")
		return
	}
	stderr := &tailBuffer{max: 2048}
	cmd.Stdout = f
	cmd.Stderr = stderr
	start := time.Now()
	err = cmd.Run()
	out, _ := tcpBytes(f)
	if out > 0 {
		mirrorBytesTotal.Add(int64(out), m.cfg.NodeID, "out")
	}
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		// "not found" means this node's content store is missing blobs
		// for the image — stop advertising it. A broken pipe just means
		// the requester went away and says nothing about us.
		if strings.Contains(msg, "not found") {
			m.inv.MarkUnexportable(digest)
		}
		mirrorExportsTotal.Inc(m.cfg.NodeID, "failed")
		logging.Warnf("angryduck-worker-mirror: export of %s to %s failed after %s: %v: %s", ref, r.RemoteAddr, time.Since(start).Round(time.Millisecond), err, msg)
		return
	}
	mirrorExportsTotal.Inc(m.cfg.NodeID, "served")
	logging.Infof("angryduck-worker-mirror: exported %s to %s: %s in %s", ref, r.RemoteAddr, humanBytes(out), time.Since(start).Round(time.Millisecond))
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func acquire(ctx context.Context, slots chan struct{}, wait time.Duration) bool {
	select {
	case slots <- struct{}{}:
		return true
	default:
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case slots <- struct{}{}:
		return true
	case <-t.C:
		return false
	case <-ctx.Done():
		return false
	}
}

// readResponseHead reads an HTTP response head one byte at a time. That
// is deliberate: a buffered reader would read ahead into the tar stream,
// and those bytes would be lost when the socket is handed to ctr. Heads
// are ~150 bytes, so this is ~150 tiny reads once per transfer.
func readResponseHead(c net.Conn) (int, http.Header, error) {
	var buf []byte
	one := make([]byte, 1)
	for len(buf) < 4096 {
		if _, err := c.Read(one); err != nil {
			return 0, nil, err
		}
		buf = append(buf, one[0])
		if bytes.HasSuffix(buf, []byte("\r\n\r\n")) {
			break
		}
	}
	lines := strings.Split(strings.TrimSuffix(string(buf), "\r\n\r\n"), "\r\n")
	parts := strings.SplitN(lines[0], " ", 3)
	if len(parts) < 2 || !strings.HasPrefix(parts[0], "HTTP/") {
		return 0, nil, errors.New("malformed response")
	}
	status, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, nil, err
	}
	hdr := http.Header{}
	for _, l := range lines[1:] {
		if k, v, ok := strings.Cut(l, ":"); ok {
			hdr.Add(strings.TrimSpace(k), strings.TrimSpace(v))
		}
	}
	return status, hdr, nil
}

// tcpBytes reads tcpi_bytes_acked / tcpi_bytes_received (Linux >= 4.1)
// from a socket, which is how the worker counts bytes it never touched.
func tcpBytes(f *os.File) (acked, received uint64) {
	rc, err := f.SyscallConn()
	if err != nil {
		return 0, 0
	}
	var info [256]byte
	size := uint32(len(info))
	var errno syscall.Errno
	_ = rc.Control(func(fd uintptr) {
		_, _, errno = syscall.Syscall6(syscall.SYS_GETSOCKOPT, fd, syscall.IPPROTO_TCP, syscall.TCP_INFO,
			uintptr(unsafe.Pointer(&info[0])), uintptr(unsafe.Pointer(&size)), 0)
	})
	if errno != 0 || size < 136 {
		return 0, 0
	}
	return binary.LittleEndian.Uint64(info[120:128]), binary.LittleEndian.Uint64(info[128:136])
}

// parseV2Path splits "<name>/manifests/<ref>" or "<name>/blobs/<digest>".
// name itself may contain slashes, so split on the last marker.
func parseV2Path(rest string) (kind, name, ref string) {
	for _, k := range []string{"manifests", "blobs"} {
		marker := "/" + k + "/"
		if i := strings.LastIndex(rest, marker); i > 0 {
			return k, rest[:i], rest[i+len(marker):]
		}
	}
	return "", "", ""
}

func isDigest(s string) bool {
	if len(s) != 71 || !strings.HasPrefix(s, "sha256:") {
		return false
	}
	for _, c := range s[7:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func isLoopback(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func notFound(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	_, _ = w.Write([]byte(`{"errors":[{"code":"MANIFEST_UNKNOWN"}]}`))
}

func humanBytes(n uint64) string {
	const mib = 1 << 20
	if n >= mib {
		return fmt.Sprintf("%.1f MiB", float64(n)/mib)
	}
	return fmt.Sprintf("%d B", n)
}
