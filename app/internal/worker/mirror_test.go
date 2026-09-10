package worker

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"angryduck/internal/model"
)

const testDigest = "sha256:9467d14fc385f36a8b42cfeca64f90b22f67e1f62a80ec5605c96846f8072bf9"

// --- HostExec ---------------------------------------------------------------

func TestHostExecResolvesFromHostRootPreferringUsrLocal(t *testing.T) {
	root := t.TempDir()
	for _, p := range []string{"/usr/bin/ctr", "/usr/local/bin/ctr", "/usr/bin/crictl"} {
		mustWriteExec(t, filepath.Join(root, p))
	}
	hx, err := NewHostExec(root)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := hx.Resolve("ctr"); got != "/usr/local/bin/ctr" {
		t.Fatalf("ctr resolved to %q, want /usr/local/bin/ctr (Kubespray location wins, like a shell PATH)", got)
	}
	if got, _ := hx.Resolve("crictl"); got != "/usr/bin/crictl" {
		t.Fatalf("crictl resolved to %q, want /usr/bin/crictl (apt location)", got)
	}
	if _, err := hx.Resolve("docker"); err == nil {
		t.Fatal("expected an error for a binary the node doesn't have")
	}
}

func TestHostExecAcceptsAbsoluteSymlinkWithoutFollowingIt(t *testing.T) {
	// Seen through /proc/1/root, an absolute link target would be resolved
	// against the CONTAINER's root and look dangling. The chrooted exec
	// resolves it correctly, so Resolve must not reject it.
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "usr/local/bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/opt/containerd/bin/ctr", filepath.Join(root, "usr/local/bin/ctr")); err != nil {
		t.Fatal(err)
	}
	hx, _ := NewHostExec(root)
	if got, err := hx.Resolve("ctr"); err != nil || got != "/usr/local/bin/ctr" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestHostExecIgnoresNonExecutableFiles(t *testing.T) {
	root := t.TempDir()
	p := filepath.Join(root, "usr/bin/ctr")
	_ = os.MkdirAll(filepath.Dir(p), 0o755)
	_ = os.WriteFile(p, []byte("x"), 0o644)
	hx, _ := NewHostExec(root)
	if _, err := hx.Resolve("ctr"); err == nil {
		t.Fatal("a non-executable regular file must not be picked")
	}
}

func TestHostExecRejectsMissingRoot(t *testing.T) {
	if _, err := NewHostExec("/definitely/not/here"); err == nil {
		t.Fatal("expected error")
	}
}

func TestHostExecRunWithoutChroot(t *testing.T) {
	hx, _ := NewHostExec("")
	out, err := hx.Run("sh", "-c", "echo $PATH; echo hi >&2; exit 0")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "/usr/local/bin") {
		t.Fatalf("child should get the node-style PATH, got %q", out)
	}
	_, err = hx.Run("sh", "-c", "echo boom >&2; exit 3")
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("stderr should surface in the error, got %v", err)
	}
}

func TestHostExecChildDoesNotInheritPodEnv(t *testing.T) {
	t.Setenv("CONTAINER_RUNTIME_ENDPOINT", "unix:///wrong.sock")
	hx, _ := NewHostExec("")
	out, _ := hx.Run("sh", "-c", "echo ${CONTAINER_RUNTIME_ENDPOINT:-unset}")
	if strings.TrimSpace(out) != "unset" {
		t.Fatalf("pod env leaked into node binary: %q", out)
	}
}

func TestRedactArgs(t *testing.T) {
	got := redactArgs([]string{"pull", "--creds", "u:secret", "img"})
	if strings.Contains(got, "secret") {
		t.Fatalf("credentials leaked: %s", got)
	}
}

func TestTailBufferKeepsOnlyTheEnd(t *testing.T) {
	b := &tailBuffer{max: 8}
	_, _ = b.Write([]byte("0123456789"))
	_, _ = b.Write([]byte("ab"))
	if b.String() != "456789ab" {
		t.Fatalf("got %q", b.String())
	}
	_, _ = b.Write([]byte("c"))
	if b.String() != "56789abc" {
		t.Fatalf("got %q", b.String())
	}
}

func mustWriteExec(t *testing.T, p string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
}

// --- Inventory --------------------------------------------------------------

type countingRuntime struct {
	*fakeRuntime
	mu    sync.Mutex
	calls int
}

func (c *countingRuntime) LocalImages() ([]string, map[string]string, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	return c.fakeRuntime.LocalImages()
}

func TestInventoryReusesFreshListing(t *testing.T) {
	rt := &countingRuntime{fakeRuntime: newFakeRuntime()}
	inv := NewInventory(rt)
	_, _, _ = inv.Get(time.Minute)
	_, _, _ = inv.Get(time.Minute)
	if rt.calls != 1 {
		t.Fatalf("second Get within maxAge should reuse the listing; runtime called %d times", rt.calls)
	}
	_, _, _ = inv.Get(0)
	if rt.calls != 2 {
		t.Fatalf("maxAge 0 must refresh; runtime called %d times", rt.calls)
	}
}

func TestInventoryIndexesManifestDigestsFromDigestPinnedRefs(t *testing.T) {
	rt := newFakeRuntime()
	rt.local["reg/app:v1"] = true
	rt.digests["reg/app:v1"] = "sha256:config"
	rt.digests["reg/app@"+testDigest] = "sha256:config"
	rt.digests["sha256:config"] = "sha256:config"
	inv := NewInventory(rt)
	_, _, _ = inv.Get(0)
	ref, ok := inv.Lookup(testDigest)
	if !ok || ref != "reg/app@"+testDigest {
		t.Fatalf("Lookup = %q, %v", ref, ok)
	}
	if d := inv.Digests(); len(d) != 1 || d[0] != testDigest {
		t.Fatalf("Digests = %v, want only the manifest digest (not tag or config id)", d)
	}
}

func TestInventoryKeepsImportsNewerThanTheListing(t *testing.T) {
	rt := newFakeRuntime()
	inv := NewInventory(rt)
	started := time.Now()
	inv.Add(testDigest, "reg/app@"+testDigest) // lands while a listing is running
	inv.replace(nil, map[string]string{}, started)
	if !inv.Has(testDigest) {
		t.Fatal("an import that finished after the listing began must survive that listing")
	}
	inv.replace(nil, map[string]string{}, time.Now())
	if inv.Has(testDigest) {
		t.Fatal("a listing that started after the import and doesn't show it is authoritative")
	}
}

func TestInventoryUnexportableIsHiddenUntilImageLeaves(t *testing.T) {
	rt := newFakeRuntime()
	rt.digests["reg/app@"+testDigest] = "sha256:c"
	inv := NewInventory(rt)
	_, _, _ = inv.Get(0)
	inv.MarkUnexportable(testDigest)
	if inv.Has(testDigest) || len(inv.Digests()) != 0 {
		t.Fatal("unexportable digest must not be advertised")
	}
	_, _, _ = inv.Get(0)
	if inv.Has(testDigest) {
		t.Fatal("still present locally -> stays hidden")
	}
	delete(rt.digests, "reg/app@"+testDigest)
	_, _, _ = inv.Get(0)
	rt.digests["reg/app@"+testDigest] = "sha256:c"
	_, _, _ = inv.Get(0)
	if !inv.Has(testDigest) {
		t.Fatal("after the image left and came back (re-pull) it should be advertised again")
	}
}

// --- hosts.toml -------------------------------------------------------------

func TestHostsTOMLWritesRemovesAndRespectsForeignFiles(t *testing.T) {
	dir := t.TempDir()
	hx, _ := NewHostExec("")
	h := NewHostsTOML(hx, dir, []string{"registry.internal-registry.example.com", "http://127.0.0.1:5000", "docker.io"}, "http://127.0.0.1:18081")

	foreign := filepath.Join(dir, "docker.io", "hosts.toml")
	_ = os.MkdirAll(filepath.Dir(foreign), 0o755)
	_ = os.WriteFile(foreign, []byte("server = \"https://registry-1.docker.io\"\n"), 0o644)

	h.Ensure()
	b, err := os.ReadFile(filepath.Join(dir, "registry.internal-registry.example.com", "hosts.toml"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, want := range []string{hostsMarker, `server = "https://registry.internal-registry.example.com"`, `[host."http://127.0.0.1:18081"]`, `capabilities = ["pull"]`} {
		if !strings.Contains(s, want) {
			t.Fatalf("hosts.toml missing %q:\n%s", want, s)
		}
	}
	if strings.Contains(s, "resolve") {
		t.Fatal("the mirror must never get resolve capability: tags must always resolve at origin")
	}
	plain, _ := os.ReadFile(filepath.Join(dir, "127.0.0.1:5000", "hosts.toml"))
	if !strings.Contains(string(plain), `server = "http://127.0.0.1:5000"`) {
		t.Fatalf("http:// entry should keep its scheme and use host:port as the dir:\n%s", plain)
	}
	if got, _ := os.ReadFile(foreign); strings.Contains(string(got), hostsMarker) {
		t.Fatal("a hosts.toml angryduck didn't write must never be overwritten")
	}

	// Spegel-style wipe, then re-assert.
	_ = os.RemoveAll(filepath.Join(dir, "registry.internal-registry.example.com"))
	h.Ensure()
	if _, err := os.Stat(filepath.Join(dir, "registry.internal-registry.example.com", "hosts.toml")); err != nil {
		t.Fatal("Ensure must restore a removed file")
	}

	h.Remove()
	if _, err := os.Stat(filepath.Join(dir, "registry.internal-registry.example.com")); !os.IsNotExist(err) {
		t.Fatal("Remove should delete our file and its now-empty dir")
	}
	if _, err := os.Stat(foreign); err != nil {
		t.Fatal("Remove must leave foreign files alone")
	}
}

// --- mirror protocol helpers ------------------------------------------------

func TestParseV2Path(t *testing.T) {
	cases := []struct{ in, kind, name, ref string }{
		{"devops/generic/app/manifests/" + testDigest, "manifests", "devops/generic/app", testDigest},
		{"library/nginx/blobs/" + testDigest, "blobs", "library/nginx", testDigest},
		{"a/manifests/b/manifests/v1", "manifests", "a/manifests/b", "v1"},
		{"garbage", "", "", ""},
	}
	for _, c := range cases {
		k, n, r := parseV2Path(c.in)
		if k != c.kind || n != c.name || r != c.ref {
			t.Errorf("parseV2Path(%q) = %q %q %q", c.in, k, n, r)
		}
	}
}

func TestIsDigest(t *testing.T) {
	if !isDigest(testDigest) {
		t.Fatal("valid digest rejected")
	}
	for _, bad := range []string{"v1", "sha256:abc", "sha512:" + testDigest[7:], testDigest[:70] + "Z", "latest"} {
		if isDigest(bad) {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestReadResponseHeadDoesNotConsumeBody(t *testing.T) {
	a, b := net.Pipe()
	go func() {
		_, _ = a.Write([]byte("HTTP/1.1 200 OK\r\nX-Angryduck-Ref: reg/app@" + testDigest + "\r\n\r\nTARSTREAM"))
		a.Close()
	}()
	status, hdr, err := readResponseHead(b)
	if err != nil || status != 200 || hdr.Get(refHeader) != "reg/app@"+testDigest {
		t.Fatalf("status=%d hdr=%v err=%v", status, hdr, err)
	}
	rest, _ := io.ReadAll(b)
	if string(rest) != "TARSTREAM" {
		t.Fatalf("body bytes must be left for ctr, got %q", rest)
	}
}

func TestTCPBytesCountsWhatTheKernelMoved(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	payload := make([]byte, 3<<20)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		_, _ = c.Write(payload)
		c.Close()
	}()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, c)
	f, err := c.(*net.TCPConn).File()
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	_, received := tcpBytes(f)
	// The FIN occupies one byte of sequence space, so a closed stream
	// reads payload+1. Anything else means the struct offset is wrong.
	if received != uint64(len(payload)) && received != uint64(len(payload))+1 {
		t.Fatalf("tcpi_bytes_received = %d, want %d (wrong struct offset?)", received, len(payload))
	}
}

func TestAcquireTimesOutWhenFull(t *testing.T) {
	slots := make(chan struct{}, 1)
	slots <- struct{}{}
	start := time.Now()
	if acquire(context.Background(), slots, 30*time.Millisecond) {
		t.Fatal("acquired a full semaphore")
	}
	if time.Since(start) < 25*time.Millisecond {
		t.Fatal("should wait up to the queue window before giving up")
	}
}

// --- mirror handlers --------------------------------------------------------

func newTestMirror(t *testing.T, controller http.Handler) (*Mirror, *Inventory) {
	t.Helper()
	srv := httptest.NewServer(controller)
	t.Cleanup(srv.Close)
	hx, _ := NewHostExec("")
	inv := NewInventory(newFakeRuntime())
	m := NewMirror(MirrorConfig{
		NodeID: "n2", ControllerURL: srv.URL, Token: "0123456789abcdef",
		Namespace: "k8s.io", HoldTimeout: 2 * time.Second, QueueWait: 50 * time.Millisecond,
		MaxExports: 1, MaxImports: 1,
	}, hx, inv)
	return m, inv
}

func manifestReq(remote string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/v2/devops/app/manifests/"+testDigest+"?ns=registry.internal-registry.example.com", nil)
	r.RemoteAddr = remote
	return r
}

func TestMirrorAnswers404AtOnceWhenNoPeerHasIt(t *testing.T) {
	var asked sync.WaitGroup
	asked.Add(1)
	m, _ := newTestMirror(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer asked.Done()
		if r.URL.Query().Get("digest") != testDigest || r.URL.Query().Get("node") != "n2" {
			t.Errorf("unexpected lookup %s", r.URL.RawQuery)
		}
		_ = json.NewEncoder(w).Encode(model.PeersResponse{})
	}))
	w := httptest.NewRecorder()
	start := time.Now()
	m.ServeRegistry(w, manifestReq("127.0.0.1:5555"))
	asked.Wait()
	if w.Code != http.StatusNotFound {
		t.Fatalf("status %d", w.Code)
	}
	if time.Since(start) > time.Second {
		t.Fatal("a miss must not hold containerd")
	}
}

func TestMirrorNeverHoldsBlobOrTagRequests(t *testing.T) {
	m, _ := newTestMirror(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("controller must not be asked for %s", r.URL)
	}))
	for _, p := range []string{"/v2/devops/app/blobs/" + testDigest + "?ns=x", "/v2/devops/app/manifests/v1?ns=x", "/v2/devops/app/manifests/" + testDigest} {
		r := httptest.NewRequest(http.MethodGet, p, nil)
		r.RemoteAddr = "127.0.0.1:1"
		w := httptest.NewRecorder()
		m.ServeRegistry(w, r)
		if w.Code != http.StatusNotFound {
			t.Errorf("%s -> %d", p, w.Code)
		}
	}
}

func TestMirrorRejectsNonLoopbackCallers(t *testing.T) {
	m, _ := newTestMirror(t, http.NotFoundHandler())
	w := httptest.NewRecorder()
	m.ServeRegistry(w, manifestReq("10.0.0.7:4444"))
	if w.Code != http.StatusForbidden {
		t.Fatalf("status %d", w.Code)
	}
}

func TestMirrorCollapsesConcurrentRequestsIntoOneTransfer(t *testing.T) {
	var mu sync.Mutex
	lookups := 0
	release := make(chan struct{})
	m, _ := newTestMirror(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		lookups++
		mu.Unlock()
		<-release // hold the first lookup so the others must join it
		_ = json.NewEncoder(w).Encode(model.PeersResponse{})
	}))
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.ServeRegistry(httptest.NewRecorder(), manifestReq("127.0.0.1:1"))
		}()
	}
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()
	if lookups != 1 {
		t.Fatalf("5 concurrent pulls of one digest caused %d transfers, want 1", lookups)
	}
}

func TestMirrorSkipsTransferWhenImageIsLocal(t *testing.T) {
	m, inv := newTestMirror(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("no lookup expected for a local image")
	}))
	inv.Add(testDigest, "reg/app@"+testDigest)
	w := httptest.NewRecorder()
	m.ServeRegistry(w, manifestReq("127.0.0.1:1"))
	if w.Code != http.StatusNotFound {
		t.Fatalf("status %d", w.Code)
	}
}

func TestExportRequiresTokenAndKnownDigest(t *testing.T) {
	m, _ := newTestMirror(t, http.NotFoundHandler())
	r := httptest.NewRequest(http.MethodGet, "/export?digest="+testDigest, nil)
	w := httptest.NewRecorder()
	m.ServeExport(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("no token -> %d", w.Code)
	}
	r = httptest.NewRequest(http.MethodGet, "/export?digest="+testDigest, nil)
	r.Header.Set(tokenHeader, "0123456789abcdef")
	w = httptest.NewRecorder()
	m.ServeExport(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown digest -> %d", w.Code)
	}
}

func TestExportSaysBusyWhenSlotsAreFull(t *testing.T) {
	m, inv := newTestMirror(t, http.NotFoundHandler())
	inv.Add(testDigest, "reg/app@"+testDigest)
	m.exportSlots <- struct{}{} // MaxExports=1, now full
	r := httptest.NewRequest(http.MethodGet, "/export?digest="+testDigest, nil)
	r.Header.Set(tokenHeader, "0123456789abcdef")
	w := httptest.NewRecorder()
	m.ServeExport(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503 so the requester tries another source", w.Code)
	}
}
