package memlimit

import (
	"context"
	"runtime"
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
