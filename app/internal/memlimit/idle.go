package memlimit

import (
	"context"
	"net/http"
	"runtime/debug"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"time"

	"angryduck/internal/logging"
)

// minRetained is the freed heap worth handing back; set by
// ReleaseWhenIdle.
var minRetained atomic.Uint64

// ReleaseWhenIdle hands freed heap memory back to the OS. Go keeps heap it
// has freed for reuse and returns it only gradually; after a burst (a
// rescue, a big listing) that is a few MiB held for nothing until the next
// burst. Every interval, and shortly after each burst of work ends (see
// Soon), if more than minRetained bytes of freed heap are held, it runs a
// collection and returns them. The check itself reads two counters; the
// collection costs well under a millisecond at this heap size.
// angryduck_process_memory_bytes shows the effect.
func ReleaseWhenIdle(ctx context.Context, interval time.Duration, retained uint64) {
	minRetained.Store(retained)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		release()
	}
}

func release() {
	min := minRetained.Load()
	if min == 0 {
		return // ReleaseWhenIdle isn't running
	}
	s := []metrics.Sample{{Name: "/memory/classes/heap/free:bytes"}, {Name: "/memory/classes/heap/unused:bytes"}}
	metrics.Read(s)
	if retained := s[0].Value.Uint64() + s[1].Value.Uint64(); retained > min {
		debug.FreeOSMemory()
		logging.Debugf("memlimit: released %d KiB of idle heap to the OS", retained/1024)
	}
}

// quietAfter is how long things must stay quiet after a burst of work
// before freed heap is handed back.
var quietAfter = 5 * time.Second

var soon struct {
	mu sync.Mutex
	t  *time.Timer
}

// Soon says a burst of work (a transfer, a request, a pass over the images)
// just ended: freed heap goes back to the OS once nothing else has called
// Soon for a few seconds, instead of at the next interval. Back-to-back
// work keeps pushing it back, so a busy stretch never pays for it.
func Soon() {
	soon.mu.Lock()
	defer soon.mu.Unlock()
	if soon.t == nil {
		soon.t = time.AfterFunc(quietAfter, release)
		return
	}
	soon.t.Reset(quietAfter)
}

// AfterEach wraps h so every request it handles ends with Soon.
func AfterEach(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer Soon()
		h.ServeHTTP(w, r)
	})
}
