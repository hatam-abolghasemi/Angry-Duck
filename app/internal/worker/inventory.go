package worker

import (
	"sync"
	"time"
)

// Inventory is the one place this worker's view of local images lives.
//
// Before it existed, the reporter (every 15s) ran the runtime's full image
// listing on its own timer even when a very recent listing was already
// available. Now a caller reuses the cached listing if it's young enough
// instead of re-running the same subprocess and JSON parse.
type Inventory struct {
	runtime Runtime

	fetchMu sync.Mutex // serializes refreshes so two callers never list at once

	mu       sync.RWMutex
	refs     []string
	listedAt time.Time
	haveData bool
}

// NewInventory builds an empty inventory backed by rt.
func NewInventory(rt Runtime) *Inventory {
	return &Inventory{runtime: rt}
}

// Get returns the local image listing, reusing the cached one if it is no
// older than maxAge, otherwise refreshing it with one runtime call.
func (inv *Inventory) Get(maxAge time.Duration) ([]string, error) {
	if refs, ok := inv.cached(maxAge); ok {
		return refs, nil
	}
	inv.fetchMu.Lock()
	defer inv.fetchMu.Unlock()
	// Another caller may have refreshed while we waited for fetchMu.
	if refs, ok := inv.cached(maxAge); ok {
		return refs, nil
	}
	started := time.Now()
	refs, err := inv.runtime.LocalImages()
	if err != nil {
		return nil, err
	}
	inv.mu.Lock()
	inv.refs = refs
	inv.listedAt, inv.haveData = started, true
	inv.mu.Unlock()
	return refs, nil
}

func (inv *Inventory) cached(maxAge time.Duration) ([]string, bool) {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	if !inv.haveData || time.Since(inv.listedAt) > maxAge {
		return nil, false
	}
	return inv.refs, true
}
