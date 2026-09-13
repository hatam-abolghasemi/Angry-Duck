package worker

import (
	"testing"
	"time"
)

// TestPruneExpiredDropsOnlyPastGrace confirms PruneExpired removes entries
// whose grace period has fully elapsed and leaves everything else (both
// still-in-grace entries and InGracePeriod's own logic) untouched. Without
// this, orderedAt grows by one entry per unique image ever ordered, for the
// lifetime of the process.
func TestPruneExpiredDropsOnlyPastGrace(t *testing.T) {
	rt := newFakeRuntime()
	p := NewPuller(rt, 50*time.Millisecond, "test-node", false)

	p.mu.Lock()
	p.orderedAt["img:expired-1"] = time.Now().Add(-time.Hour)
	p.orderedAt["img:expired-2"] = time.Now().Add(-time.Minute)
	p.orderedAt["img:fresh"] = time.Now()
	p.mu.Unlock()

	pruned := p.PruneExpired()
	if pruned != 2 {
		t.Fatalf("PruneExpired() = %d, want 2", pruned)
	}

	p.mu.Lock()
	_, expired1Present := p.orderedAt["img:expired-1"]
	_, expired2Present := p.orderedAt["img:expired-2"]
	_, freshPresent := p.orderedAt["img:fresh"]
	remaining := len(p.orderedAt)
	p.mu.Unlock()

	if expired1Present || expired2Present {
		t.Errorf("expired entries were not pruned")
	}
	if !freshPresent {
		t.Errorf("fresh entry was incorrectly pruned")
	}
	if remaining != 1 {
		t.Errorf("orderedAt has %d entries after prune, want 1", remaining)
	}
}

// TestPruneExpiredNoOpWhenNothingExpired confirms a prune pass with
// everything still in grace removes nothing and reports zero.
func TestPruneExpiredNoOpWhenNothingExpired(t *testing.T) {
	rt := newFakeRuntime()
	p := NewPuller(rt, time.Hour, "test-node", false)

	p.mu.Lock()
	p.orderedAt["img:a"] = time.Now()
	p.orderedAt["img:b"] = time.Now()
	p.mu.Unlock()

	if pruned := p.PruneExpired(); pruned != 0 {
		t.Fatalf("PruneExpired() = %d, want 0", pruned)
	}
	p.mu.Lock()
	remaining := len(p.orderedAt)
	p.mu.Unlock()
	if remaining != 2 {
		t.Errorf("orderedAt has %d entries, want 2 (nothing should have been pruned)", remaining)
	}
}

// TestGCTickPrunesExpiredOrders confirms the GC loop actually invokes
// PruneExpired on every tick (not just that the method works in
// isolation), so this doesn't silently regress if the wiring in gc.go
// changes.
func TestGCTickPrunesExpiredOrders(t *testing.T) {
	rt := newFakeRuntime()
	p := NewPuller(rt, 10*time.Millisecond, "test-node", false)
	gc := NewGC(rt, NewInventory(rt), 0, p, time.Hour, 5, true, nil, "test-node", false)

	p.mu.Lock()
	p.orderedAt["img:long-gone"] = time.Now().Add(-time.Hour)
	p.mu.Unlock()

	gc.tick()

	p.mu.Lock()
	_, present := p.orderedAt["img:long-gone"]
	p.mu.Unlock()
	if present {
		t.Errorf("gc.tick() did not prune an expired puller order")
	}
}
