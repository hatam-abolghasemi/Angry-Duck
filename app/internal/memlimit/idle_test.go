package memlimit

import (
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"testing"
	"time"
)

var sink [][]byte

func retained() uint64 {
	s := []metrics.Sample{{Name: "/memory/classes/heap/free:bytes"}, {Name: "/memory/classes/heap/unused:bytes"}}
	metrics.Read(s)
	return s[0].Value.Uint64() + s[1].Value.Uint64()
}

// ReleaseWhenIdle only sets the threshold: it returns at once, and nothing
// runs until a burst of work ends (Soon).
func TestReleaseWhenIdleRunsNothingOnItsOwn(t *testing.T) {
	defer minRetained.Store(0)
	done := make(chan struct{})
	go func() {
		ReleaseWhenIdle(context.Background(), time.Millisecond, 1<<20)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ReleaseWhenIdle didn't return")
	}
	if minRetained.Load() != 1<<20 {
		t.Fatalf("threshold %d", minRetained.Load())
	}
}

// Garbage a burst leaves before any collection reaches it is held for
// nothing too: a burst that ends between two collections must still be
// handed back.
func TestSoonReleasesUncollectedGarbage(t *testing.T) {
	old := quietAfter
	quietAfter = 50 * time.Millisecond
	defer func() { quietAfter = old }()
	minRetained.Store(1 << 20)
	defer minRetained.Store(0)
	runtime.GC()
	debug.FreeOSMemory()
	// No collection runs during the burst or the wait: only the release
	// itself may reach the garbage, as when a burst ends between two.
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	for i := 0; i < 4; i++ {
		sink = append(sink, make([]byte, 4<<20))
	}
	sink = nil
	before := heapHeld()
	Soon()
	time.Sleep(300 * time.Millisecond)
	if after := heapHeld(); after+8<<20 > before {
		t.Fatalf("held %d KiB before, %d KiB after: a 16 MiB burst's garbage wasn't handed back", before>>10, after>>10)
	}
}

// heapHeld is the heap memory taken from the OS and not handed back.
func heapHeld() uint64 {
	s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}, {Name: "/memory/classes/heap/unused:bytes"}, {Name: "/memory/classes/heap/free:bytes"}}
	metrics.Read(s)
	return s[0].Value.Uint64() + s[1].Value.Uint64() + s[2].Value.Uint64()
}

func TestSoonReleasesAfterQuiet(t *testing.T) {
	old := quietAfter
	quietAfter = 50 * time.Millisecond
	defer func() { quietAfter = old }()
	minRetained.Store(1 << 20)
	defer minRetained.Store(0)
	for i := 0; i < 64; i++ {
		sink = append(sink, make([]byte, 1<<20))
	}
	sink = nil
	runtime.GC()
	if r := retained(); r < 32<<20 {
		t.Skipf("runtime already returned the burst (%d MiB retained)", r>>20)
	}
	Soon()
	Soon() // pushed back, not doubled
	time.Sleep(300 * time.Millisecond)
	if r := retained(); r > 8<<20 {
		t.Fatalf("still retaining %d MiB of freed heap", r>>20)
	}
}

// Probes and scrapes arrive on a schedule, work or not: they must not
// restart the quiet wait, or a node whose probes land close enough
// together never gets to quiet.
func TestAfterEachIgnoresProbesAndScrapes(t *testing.T) {
	soon.mu.Lock()
	if soon.t != nil {
		soon.t.Stop()
	}
	soon.t = nil
	soon.mu.Unlock()
	armed := func() bool {
		soon.mu.Lock()
		defer soon.mu.Unlock()
		return soon.t != nil
	}
	h := AfterEach(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	for _, path := range []string{"/healthz", "/metrics"} {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
		if armed() {
			t.Fatalf("%s started the quiet wait", path)
		}
	}
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v2/library/app/blobs/sha256:00", nil))
	if !armed() {
		t.Fatal("a real request didn't start the quiet wait")
	}
}
