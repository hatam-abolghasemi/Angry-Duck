package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestContainerdRoot(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "config.toml")
	cases := map[string]string{
		"": "/var/lib/containerd",
		"version = 2\nroot = \"/data/containerd\"\n":   "/data/containerd",
		"root = '/x' # comment\n":                      "/x",
		"version = 2\n[plugins]\n  root = \"/nope\"\n": "/var/lib/containerd",
		"root = \"relative\"\n":                        "/var/lib/containerd",
	}
	for in, want := range cases {
		os.WriteFile(p, []byte(in), 0o644)
		if got := containerdRoot(p); got != want {
			t.Errorf("containerdRoot(%q) = %q, want %q", in, got, want)
		}
	}
	if got := containerdRoot(filepath.Join(dir, "missing")); got != "/var/lib/containerd" {
		t.Errorf("missing file: %q", got)
	}
}

func blobStore(t *testing.T) (*CtrStore, string, []byte) {
	t.Helper()
	root := t.TempDir()
	blobDir := filepath.Join(root, "var/lib/containerd/io.containerd.content.v1.content/blobs/sha256")
	os.MkdirAll(blobDir, 0o755)
	data := bytes.Repeat([]byte("layer"), 1000)
	sum := sha256.Sum256(data)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	os.WriteFile(filepath.Join(blobDir, hex.EncodeToString(sum[:])), data, 0o644)
	hx, err := NewHostExec(root)
	if err != nil {
		t.Fatal(err)
	}
	s := NewCtrStore(hx, "k8s.io", "")
	if got := s.UseBlobDir(""); got != blobDir {
		t.Fatalf("UseBlobDir = %q, want %q", got, blobDir)
	}
	return s, digest, data
}

func TestDirectBlobReads(t *testing.T) {
	s, digest, data := blobStore(t)
	ctx := context.Background()

	var buf bytes.Buffer
	if err := s.StreamBlob(ctx, digest, int64(len(data)), &buf); err != nil || !bytes.Equal(buf.Bytes(), data) {
		t.Fatalf("StreamBlob: %v (%d bytes)", err, buf.Len())
	}
	if err := s.StreamBlob(ctx, digest, int64(len(data))+1, &bytes.Buffer{}); err == nil {
		t.Fatal("StreamBlob accepted a size mismatch")
	}
	b, err := s.ReadBlob(ctx, digest, 1<<20)
	if err != nil || !bytes.Equal(b, data) {
		t.Fatalf("ReadBlob: %v", err)
	}
	if _, err := s.ReadBlob(ctx, digest, 10); err == nil {
		t.Fatal("ReadBlob ignored max")
	}
	cw := &countWriter{w: &bytes.Buffer{}}
	if err := s.StreamContent(ctx, digest, cw); err != nil || cw.n != int64(len(data)) {
		t.Fatalf("StreamContent: %v, %d bytes", err, cw.n)
	}
	// Not on disk (and no ctr here): falls back to ctr, which fails.
	missing := "sha256:" + hex.EncodeToString(make([]byte, 32))
	if _, ok := func() (int64, bool) { _, n, ok := s.openBlob(missing); return n, ok }(); ok {
		t.Fatal("openBlob found a missing blob")
	}
	if _, ok := func() (int64, bool) { _, n, ok := s.openBlob("sha256:../../etc/passwd"); return n, ok }(); ok {
		t.Fatal("openBlob accepted an invalid digest")
	}
}

func TestListingCoalescesConcurrentCallers(t *testing.T) {
	var l listing
	var calls atomic.Int32
	release := make(chan struct{})
	fetch := func(context.Context) (map[string]bool, error) {
		calls.Add(1)
		<-release
		return map[string]bool{"sha256:a": true}, nil
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if m, err := l.get(context.Background(), fetch); err != nil || !m["sha256:a"] {
				t.Errorf("get: %v %v", m, err)
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	if n := calls.Load(); n != 1 {
		t.Fatalf("%d fetches for 20 concurrent callers, want 1", n)
	}
	// Fresh result is served from cache.
	l.get(context.Background(), fetch)
	if n := calls.Load(); n != 1 {
		t.Fatalf("cached result refetched: %d", n)
	}
}

func TestListingInvalidateDuringFetch(t *testing.T) {
	var l listing
	release := make(chan struct{})
	var calls atomic.Int32
	fetch := func(context.Context) (map[string]bool, error) {
		n := calls.Add(1)
		if n == 1 {
			<-release
			return map[string]bool{"old": true}, nil
		}
		return map[string]bool{"new": true}, nil
	}
	done := make(chan map[string]bool)
	go func() { m, _ := l.get(context.Background(), fetch); done <- m }()
	time.Sleep(20 * time.Millisecond)
	l.invalidate() // e.g. an import finished while the listing ran
	m, _ := l.get(context.Background(), fetch)
	if !m["new"] {
		t.Fatalf("caller after invalidate got %v", m)
	}
	close(release)
	<-done
	if m, _ := l.get(context.Background(), fetch); !m["new"] {
		t.Fatalf("stale in-flight result was cached: %v", m)
	}
}

func TestListingCallerCancelDoesNotFailOthers(t *testing.T) {
	var l listing
	release := make(chan struct{})
	fetch := func(context.Context) (map[string]bool, error) {
		<-release
		return map[string]bool{"x": true}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error)
	go func() { _, err := l.get(ctx, fetch); errc <- err }()
	res := make(chan map[string]bool)
	go func() { m, _ := l.get(context.Background(), fetch); res <- m }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-errc; err == nil {
		t.Fatal("cancelled caller got no error")
	}
	close(release)
	if m := <-res; !m["x"] {
		t.Fatalf("other caller got %v", m)
	}
}

func TestHostExecListingGateSerializes(t *testing.T) {
	hx, _ := NewHostExec("/")
	var cur, peak atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rel, err := hx.AcquireListing(context.Background())
			if err != nil {
				t.Error(err)
				return
			}
			if n := cur.Add(1); n > peak.Load() {
				peak.Store(n)
			}
			time.Sleep(5 * time.Millisecond)
			cur.Add(-1)
			rel()
		}()
	}
	wg.Wait()
	if peak.Load() != 1 {
		t.Fatalf("peak concurrent listings %d, want 1", peak.Load())
	}
	ctx, cancel := context.WithCancel(context.Background())
	rel, _ := hx.AcquireListing(context.Background())
	cancel()
	if _, err := hx.AcquireListing(ctx); err == nil {
		t.Fatal("AcquireListing ignored a cancelled context")
	}
	rel()
}
