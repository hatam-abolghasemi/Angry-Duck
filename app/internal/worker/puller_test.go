package worker

import (
	"strings"
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
	p := NewPuller(rt, 50*time.Millisecond, "test-node", false, "")

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
	p := NewPuller(rt, time.Hour, "test-node", false, "")

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
	p := NewPuller(rt, 10*time.Millisecond, "test-node", false, "")
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

// TestPreheatedReposPrunesPastRetention confirms PreheatedRepos returns
// only repos preheated within the given retention window, and prunes
// anything older while it's at it — the same pattern PruneExpired uses
// for orderedAt, but on its own, much longer-lived map (see the Puller
// struct comment for why preheatedAt is separate from orderedAt).
func TestPreheatedReposPrunesPastRetention(t *testing.T) {
	rt := newFakeRuntime()
	p := NewPuller(rt, time.Minute, "test-node", false, "")

	p.mu.Lock()
	p.preheatedAt["repo/long-gone"] = time.Now().Add(-2 * time.Hour)
	p.preheatedAt["repo/recent"] = time.Now()
	p.mu.Unlock()

	got := p.PreheatedRepos(time.Hour)
	if got["repo/long-gone"] {
		t.Errorf("expected repo/long-gone to be pruned (past retention), got present")
	}
	if !got["repo/recent"] {
		t.Errorf("expected repo/recent to be present (within retention)")
	}
	if len(got) != 1 {
		t.Errorf("got %d repos, want 1", len(got))
	}

	// The stale entry should actually be gone from the map now, not just
	// excluded from this one result.
	p.mu.Lock()
	_, stillPresent := p.preheatedAt["repo/long-gone"]
	p.mu.Unlock()
	if stillPresent {
		t.Errorf("expected PreheatedRepos to prune the stale entry, but it's still in the map")
	}
}

// TestHandlePullRecordsPreheatedRepoOnSuccess confirms a successful pull
// through the normal HandlePull path (not a direct map write, like the
// test above) ends up recorded in preheatedAt under the image's bare
// repo, which is what PreheatMonitor actually depends on in production.
func TestHandlePullRecordsPreheatedRepoOnSuccess(t *testing.T) {
	rt := newFakeRuntime()
	p := NewPuller(rt, time.Minute, "test-node", false, "")

	orderPull(t, p, rt, "docker.io/library/nginx:1.25")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.PreheatedRepos(time.Hour)["docker.io/library/nginx"] {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected repo docker.io/library/nginx to be recorded as preheated after a successful pull")
}

// TestHandlePullObservesDurationHistogram confirms a successful pull
// records exactly one observation in pullDurationSeconds under the same
// node/result/registry labels pullsTotal uses — the two metrics time and
// count the same event, from the same call site.
func TestHandlePullObservesDurationHistogram(t *testing.T) {
	rt := newFakeRuntime()
	const nodeID = "test-node-duration"
	p := NewPuller(rt, time.Minute, nodeID, false, "")

	orderPull(t, p, rt, "docker.io/library/nginx:1.25")

	want := `angryduck_worker_pull_duration_seconds_count{node="` + nodeID + `",result="success",registry=""} 1`
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(scrapeMetrics(t), want) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected %q in metrics output after a successful pull", want)
}

// TestSpegelPresenceDisabledByDefault confirms an empty
// spegelImageSubstring (the default) reports "" — detection off, no
// runtime call made at all.
func TestSpegelPresenceDisabledByDefault(t *testing.T) {
	rt := newFakeRuntime()
	p := NewPuller(rt, time.Minute, "test-node", false, "")
	if got := p.spegelPresence(); got != "" {
		t.Errorf("spegelPresence() = %q, want empty string when disabled", got)
	}
}

// TestSpegelPresenceDetectsRunningContainer confirms enabling detection
// with a substring correctly reports "true" when a matching container is
// running, and "false" when none is.
func TestSpegelPresenceDetectsRunningContainer(t *testing.T) {
	rt := newFakeRuntime()
	rt.running["ghcr.io/spegel-org/spegel:v0.7.4"] = true

	p := NewPuller(rt, time.Minute, "test-node", false, "spegel")
	if got := p.spegelPresence(); got != "true" {
		t.Errorf("spegelPresence() = %q, want \"true\"", got)
	}

	rtNoSpegel := newFakeRuntime()
	rtNoSpegel.running["docker.io/library/nginx:1.25"] = true
	pNoSpegel := NewPuller(rtNoSpegel, time.Minute, "test-node", false, "spegel")
	if got := pNoSpegel.spegelPresence(); got != "false" {
		t.Errorf("spegelPresence() = %q, want \"false\"", got)
	}
}

// TestHandlePullLabelsSpegelPresence confirms a real pull through
// HandlePull actually threads the Spegel-presence label into the
// duration histogram, not just that the standalone method works.
func TestHandlePullLabelsSpegelPresence(t *testing.T) {
	rt := newFakeRuntime()
	rt.running["ghcr.io/spegel-org/spegel:v0.7.4"] = true

	const nodeID = "test-node-spegel-label"
	p := NewPuller(rt, time.Minute, nodeID, false, "spegel")

	orderPull(t, p, rt, "docker.io/library/nginx:1.25")

	want := `angryduck_worker_pull_duration_seconds_count{node="` + nodeID + `",result="success",registry="",spegel="true"} 1`
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(scrapeMetrics(t), want) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected %q in metrics output", want)
}
