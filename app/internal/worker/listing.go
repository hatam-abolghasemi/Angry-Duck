package worker

import (
	"context"
	"sync"
	"time"
)

// listGate lets one full listing (images, containers, content or
// snapshots) run at a time. Each walks the whole store and builds a large
// response, and the periodic loops (reporter, layer scan, cleanup, preheat
// monitor) would otherwise line up on the same ticks. Transfers don't
// take it.
var listGate = make(chan struct{}, 1)

// acquireListing waits for the listing gate; call the returned func to
// release it. It fails only when ctx ends first.
func acquireListing(ctx context.Context) (func(), error) {
	select {
	case listGate <- struct{}{}:
		return func() { <-listGate }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// listingTimeout bounds a listing, so a wedged containerd can't hold the
// gate forever.
const listingTimeout = 2 * time.Minute

// listingTTL is how long a content or snapshot listing is reused.
const listingTTL = 15 * time.Second

// listing caches one full listing and coalesces concurrent refreshes.
type listing struct {
	mu       sync.Mutex
	gen      uint64 // bumped by invalidate; a run started earlier is stale
	at       time.Time
	keys     map[string]bool // read-only once published
	inflight *listCall
}

type listCall struct {
	done chan struct{}
	keys map[string]bool
	err  error
}

// get returns the cached keys if fresh, or runs fetch once for everyone
// asking at the same time. The map is shared: callers must not modify it.
func (l *listing) get(ctx context.Context, fetch func(context.Context) (map[string]bool, error)) (map[string]bool, error) {
	l.mu.Lock()
	if l.keys != nil && time.Since(l.at) < listingTTL {
		keys := l.keys
		l.mu.Unlock()
		return keys, nil
	}
	c := l.inflight
	if c == nil {
		c = &listCall{done: make(chan struct{})}
		l.inflight = c
		gen := l.gen
		go func() {
			// Detached from any one caller: the result serves all of
			// them, and one caller giving up must not fail the others.
			fctx, cancel := context.WithTimeout(context.Background(), listingTimeout)
			defer cancel()
			c.keys, c.err = fetch(fctx)
			l.mu.Lock()
			if l.inflight == c {
				l.inflight = nil
			}
			if c.err == nil && l.gen == gen {
				l.keys, l.at = c.keys, time.Now()
			}
			l.mu.Unlock()
			close(c.done)
		}()
	}
	l.mu.Unlock()
	select {
	case <-c.done:
		return c.keys, c.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (l *listing) invalidate() {
	l.mu.Lock()
	l.gen++
	l.keys, l.inflight = nil, nil // new callers start a fresh run
	l.mu.Unlock()
}
