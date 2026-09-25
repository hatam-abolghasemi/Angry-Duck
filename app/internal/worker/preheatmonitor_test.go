package worker

import (
	"strings"
	"testing"
	"time"
)

// TestPreheatMonitorTickCountsOnlyPreheatedRepos proves a tick reports a
// count per repo (not per pod, not per tag) for repos this worker has
// actually preheated, and leaves every other currently-running repo out
// entirely rather than reporting it as a "false" data point.
func TestPreheatMonitorTickCountsOnlyPreheatedRepos(t *testing.T) {
	rt := newFakeRuntime()
	// Two different tags of the same repo -> two containers, one repo.
	rt.running["docker.io/library/nginx:1.25"] = true
	rt.running["docker.io/library/nginx:1.26"] = true
	// A different repo entirely, never preheated.
	rt.running["registry.example.com/pause:3.10.1"] = true

	const nodeID = "test-node-preheat-monitor"
	p := NewPuller(rt, nodeID, false, "", 0)
	p.mu.Lock()
	p.preheatedAt["docker.io/library/nginx"] = time.Now()
	p.mu.Unlock()

	m := NewPreheatMonitor(rt, p, time.Hour, time.Hour, nodeID)
	m.tick()

	out := scrapeMetrics(t)
	want := `angryduck_worker_preheated_containers_running{node="` + nodeID + `",repo="docker.io/library/nginx"} 2`
	if !strings.Contains(out, want) {
		t.Errorf("expected %q in output, got:\n%s", want, out)
	}
	if strings.Contains(out, `node="`+nodeID+`",repo="registry.example.com/pause"`) {
		t.Errorf("expected the non-preheated pause repo to be absent entirely, got:\n%s", out)
	}
}

// TestPreheatMonitorTickResetsWhenNothingPreheated confirms a node with
// no recent preheats reports no series at all — not a zero-valued one —
// and doesn't error just because nothing matched.
func TestPreheatMonitorTickResetsWhenNothingPreheated(t *testing.T) {
	rt := newFakeRuntime()
	rt.running["docker.io/library/redis:7"] = true

	const nodeID = "test-node-preheat-monitor-empty"
	p := NewPuller(rt, nodeID, false, "", 0)
	// No preheatedAt entries at all.

	m := NewPreheatMonitor(rt, p, time.Hour, time.Hour, nodeID)
	m.tick()

	out := scrapeMetrics(t)
	if strings.Contains(out, `node="`+nodeID+`"`) {
		t.Errorf("expected no series for a node with nothing preheated, got:\n%s", out)
	}
}

// TestPreheatMonitorTickExpiresOldPreheats confirms a repo preheated
// outside the retention window no longer counts, matching
// Puller.PreheatedRepos' own pruning.
func TestPreheatMonitorTickExpiresOldPreheats(t *testing.T) {
	rt := newFakeRuntime()
	rt.running["docker.io/library/nginx:1.25"] = true

	const nodeID = "test-node-preheat-monitor-expired"
	p := NewPuller(rt, nodeID, false, "", 0)
	p.mu.Lock()
	p.preheatedAt["docker.io/library/nginx"] = time.Now().Add(-2 * time.Hour)
	p.mu.Unlock()

	m := NewPreheatMonitor(rt, p, time.Hour, time.Hour, nodeID) // retention = 1h, entry is 2h old
	m.tick()

	out := scrapeMetrics(t)
	if strings.Contains(out, `node="`+nodeID+`"`) {
		t.Errorf("expected the expired preheat to no longer count, got:\n%s", out)
	}
}
