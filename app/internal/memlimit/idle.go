package memlimit

import (
	"context"
	"runtime/debug"
	"runtime/metrics"
	"time"

	"angryduck/internal/logging"
)

// ReleaseWhenIdle hands freed heap memory back to the OS. Go keeps heap it
// has freed for reuse and returns it only gradually; after a burst (a
// rescue, a big listing) that is a few MiB held for nothing until the next
// burst. Every interval, if more than minRetained bytes of freed heap are
// held, it runs a collection and returns them. The check itself reads two
// counters; the collection costs well under a millisecond at this heap
// size. angryduck_process_memory_bytes shows the effect.
func ReleaseWhenIdle(ctx context.Context, interval time.Duration, minRetained uint64) {
	s := []metrics.Sample{{Name: "/memory/classes/heap/free:bytes"}, {Name: "/memory/classes/heap/unused:bytes"}}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		metrics.Read(s)
		if retained := s[0].Value.Uint64() + s[1].Value.Uint64(); retained > minRetained {
			debug.FreeOSMemory()
			logging.Debugf("memlimit: released %d KiB of idle heap to the OS", retained/1024)
		}
	}
}
