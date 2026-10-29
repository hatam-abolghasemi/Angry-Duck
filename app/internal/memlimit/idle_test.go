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

func TestReleaseWhenIdle(t *testing.T) {
	for i := 0; i < 64; i++ { // a burst: 64 MiB, then garbage
		sink = append(sink, make([]byte, 1<<20))
	}
	sink = nil
	runtime.GC()
	if r := retained(); r < 32<<20 {
		t.Skipf("runtime already returned the burst (%d MiB retained)", r>>20)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	ReleaseWhenIdle(ctx, 50*time.Millisecond, 1<<20)
	if r := retained(); r > 8<<20 {
		t.Fatalf("still retaining %d MiB of freed heap", r>>20)
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
