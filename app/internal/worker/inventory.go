package worker

import (
	"sync"
	"time"
)

// Inventory is the one place this worker's view of local images lives.
//
// Before it existed the reporter (every 15s) and GC (every 60s) each ran
// the runtime's full image listing on their own timers — the same
// subprocess and the same JSON parse, twice, for the same answer. Now
// whichever of them runs first refreshes the listing and the other reuses
// it if it's young enough.
//
// Reusing a listing that is up to one report interval old is safe for GC:
// an image pulled after the listing simply isn't evaluated until the next
// one (it can't be wrongly removed if it isn't seen), and an image removed
// after it only produces a harmless "not found" from RemoveImage.
type Inventory struct {
	runtime Runtime

	fetchMu sync.Mutex // serializes refreshes so two callers never list at once

	mu       sync.RWMutex
	refs     []string
	digests  map[string]string // any alias -> runtime's canonical id (GC input)
	listedAt time.Time
	haveData bool
}

// NewInventory builds an empty inventory backed by rt.
func NewInventory(rt Runtime) *Inventory {
	return &Inventory{runtime: rt}
}

// Get returns the local image listing, reusing the cached one if it is no
// older than maxAge, otherwise refreshing it with one runtime call.
func (inv *Inventory) Get(maxAge time.Duration) ([]string, map[string]string, error) {
	if refs, digests, ok := inv.cached(maxAge); ok {
		return refs, digests, nil
	}
	inv.fetchMu.Lock()
	defer inv.fetchMu.Unlock()
	// Another caller may have refreshed while we waited for fetchMu.
	if refs, digests, ok := inv.cached(maxAge); ok {
		return refs, digests, nil
	}
	started := time.Now()
	refs, digests, err := inv.runtime.LocalImages()
	if err != nil {
		return nil, nil, err
	}
	inv.mu.Lock()
	inv.refs, inv.digests = refs, digests
	inv.listedAt, inv.haveData = started, true
	inv.mu.Unlock()
	return refs, digests, nil
}

func (inv *Inventory) cached(maxAge time.Duration) ([]string, map[string]string, bool) {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	if !inv.haveData || time.Since(inv.listedAt) > maxAge {
		return nil, nil, false
	}
	return inv.refs, inv.digests, true
}
