package controller

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"angryduck/internal/model"
)

// pullOrderRecorder is a fake worker /pull endpoint that records every
// image it was ordered to pull, keyed by which test server received it.
type pullOrderRecorder struct {
	mu      sync.Mutex
	pulls   []string
	server  *httptest.Server
	address string // host:port, matching what workerEntry.Address expects
}

func newPullOrderRecorder(t *testing.T) *pullOrderRecorder {
	t.Helper()
	rec := &pullOrderRecorder{}
	mux := http.NewServeMux()
	mux.HandleFunc("/pull", func(w http.ResponseWriter, r *http.Request) {
		var order model.PullOrder
		_ = json.NewDecoder(r.Body).Decode(&order)
		rec.mu.Lock()
		rec.pulls = append(rec.pulls, order.Image)
		rec.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	})
	rec.server = httptest.NewServer(mux)
	rec.address = rec.server.Listener.Addr().String()
	t.Cleanup(rec.server.Close)
	return rec
}

func (r *pullOrderRecorder) received() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.pulls))
	copy(out, r.pulls)
	return out
}

// waitForPulls polls briefly since sendPullOrder fires from a goroutine.
func waitForPulls(t *testing.T, recs ...*pullOrderRecorder) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		total := 0
		for _, r := range recs {
			total += len(r.received())
		}
		if total > 0 {
			time.Sleep(50 * time.Millisecond) // let any remaining sends land
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestRankerExcludesMatchingNodesFromSelection proves the actual production
// need this exists for: master/control-plane nodes must keep reporting and
// keep running their own GC (they're never removed from the registry, and
// nothing here stops their worker pod from running), but must never be
// picked as a preheat target, since no real pod is ever scheduled onto a
// master to benefit from the pre-pull. Without exclusion, masters look
// artificially idle (no real workload) and win the ranking constantly.
func TestRankerExcludesMatchingNodesFromSelection(t *testing.T) {
	master := newPullOrderRecorder(t)
	worker := newPullOrderRecorder(t)

	registry := NewRegistry(time.Minute, time.Minute)
	registry.Update(model.WorkerReport{
		NodeID: "sahand-k8s-stg-master2-104025", Address: master.address,
		Utilization: 0.01, Timestamp: time.Now(), // near-empty: would win on utilization alone
	})
	registry.Update(model.WorkerReport{
		NodeID: "sahand-k8s-stg-worker5-104033", Address: worker.address,
		Utilization: 0.40, Timestamp: time.Now(),
	})

	rk := NewRanker(registry, 2, time.Hour, []string{"master", "control-plane"}, true)
	ordered := rk.OrderNow("registry.example.com/app:1.0.0")

	waitForPulls(t, master, worker)

	if len(master.received()) != 0 {
		t.Fatalf("master node received a pull order despite matching an excluded substring: %v", master.received())
	}
	if len(worker.received()) != 1 {
		t.Fatalf("expected exactly one pull order to the non-excluded worker, got %v", worker.received())
	}
	for _, nodeID := range ordered {
		if nodeID == "sahand-k8s-stg-master2-104025" {
			t.Fatalf("OrderNow returned an excluded node in its ordered list: %v", ordered)
		}
	}
}

// TestRankerFallsBackWhenAllFreshWorkersAreExcluded proves the ranker fails
// safe (logs and returns nil, orders nothing) rather than falling back to
// ordering an excluded node, if every currently-fresh worker happens to
// match an excluded substring.
func TestRankerFallsBackWhenAllFreshWorkersAreExcluded(t *testing.T) {
	master := newPullOrderRecorder(t)

	registry := NewRegistry(time.Minute, time.Minute)
	registry.Update(model.WorkerReport{
		NodeID: "sahand-k8s-stg-master1-104024", Address: master.address,
		Utilization: 0.01, Timestamp: time.Now(),
	})

	rk := NewRanker(registry, 2, time.Hour, []string{"master"}, true)
	ordered := rk.OrderNow("registry.example.com/app:1.0.0")

	if ordered != nil {
		t.Fatalf("expected no nodes ordered when every fresh worker is excluded, got %v", ordered)
	}
	waitForPulls(t, master)
	if len(master.received()) != 0 {
		t.Fatalf("excluded master received a pull order as a fallback: %v", master.received())
	}
}

// TestRankerWithNoExclusionsBehavesAsBefore is a compatibility check: an
// empty/nil exclude list (the default, RANK_EXCLUDE_NODE_SUBSTRINGS unset)
// must rank and order every fresh worker exactly as it did before this
// feature existed.
func TestRankerWithNoExclusionsBehavesAsBefore(t *testing.T) {
	master := newPullOrderRecorder(t)

	registry := NewRegistry(time.Minute, time.Minute)
	registry.Update(model.WorkerReport{
		NodeID: "sahand-k8s-stg-master3-104026", Address: master.address,
		Utilization: 0.01, Timestamp: time.Now(),
	})

	rk := NewRanker(registry, 1, time.Hour, nil, true) // no exclusions
	ordered := rk.OrderNow("registry.example.com/app:1.0.0")

	waitForPulls(t, master)

	if len(ordered) != 1 || ordered[0] != "sahand-k8s-stg-master3-104026" {
		t.Fatalf("expected the only fresh worker to be ordered with no exclusions configured, got %v", ordered)
	}
	if len(master.received()) != 1 {
		t.Fatalf("expected exactly one pull order with no exclusions configured, got %v", master.received())
	}
}

// TestRankerDoesNotReorderSameImageToSameNode is the regression test for
// the production bug this fix addresses: with RANK_INTERVAL_S well under
// TARGET_TTL_S, utilization barely moves between ticks, so the same
// lowest-N nodes were being re-ordered to pull the same image on every
// tick for the whole TTL window. A node that already has an outstanding
// order for the current target image must not receive a second one.
func TestRankerDoesNotReorderSameImageToSameNode(t *testing.T) {
	node := newPullOrderRecorder(t)

	registry := NewRegistry(time.Minute, time.Minute)
	registry.Update(model.WorkerReport{
		NodeID: "sahand2-prd-k8s-worker1", Address: node.address,
		Utilization: 0.10, Timestamp: time.Now(),
	})

	rk := NewRanker(registry, 1, time.Hour, nil, true)

	first := rk.tickForTest("registry.example.com/app:1.0.0")
	waitForPulls(t, node)
	if len(first) != 1 {
		t.Fatalf("expected the first tick to order the node, got %v", first)
	}

	// Simulate the next ranker tick, same target image still active,
	// utilization unchanged — this used to re-fire the same order.
	second := rk.tickForTest("registry.example.com/app:1.0.0")
	if len(second) != 0 {
		t.Fatalf("expected the second tick to order nobody (already ordered for this image), got %v", second)
	}

	time.Sleep(50 * time.Millisecond) // let any errant send land
	if len(node.received()) != 1 {
		t.Fatalf("expected exactly one /pull delivered to the node across two ticks, got %v", node.received())
	}
}

// TestRankerReordersOnNewTargetImage proves the dedup set is scoped to one
// target image, not permanent: once a new image is pushed, previously
// ordered nodes are eligible again.
func TestRankerReordersOnNewTargetImage(t *testing.T) {
	node := newPullOrderRecorder(t)

	registry := NewRegistry(time.Minute, time.Minute)
	registry.Update(model.WorkerReport{
		NodeID: "sahand2-prd-k8s-worker2", Address: node.address,
		Utilization: 0.10, Timestamp: time.Now(),
	})

	rk := NewRanker(registry, 1, time.Hour, nil, true)

	rk.tickForTest("registry.example.com/app:1.0.0")
	waitForPulls(t, node)

	second := rk.tickForTest("registry.example.com/app:2.0.0")
	if len(second) != 1 {
		t.Fatalf("expected the node to be re-ordered for a new target image, got %v", second)
	}

	time.Sleep(50 * time.Millisecond)
	got := node.received()
	if len(got) != 2 || got[0] != "registry.example.com/app:1.0.0" || got[1] != "registry.example.com/app:2.0.0" {
		t.Fatalf("expected pulls for both images in order, got %v", got)
	}
}

// TestRankerPrefersNodeWithRepoOverLowerUtilization proves the core preheat
// locality behavior: a node that already has the target image's repo
// present locally (any tag) is chosen ahead of an emptier node that
// doesn't have it at all, even though the empty node would win on raw
// utilization alone.
func TestRankerPrefersNodeWithRepoOverLowerUtilization(t *testing.T) {
	hasRepo := newPullOrderRecorder(t)
	empty := newPullOrderRecorder(t)

	registry := NewRegistry(time.Minute, time.Minute)
	registry.Update(model.WorkerReport{
		NodeID: "node-has-repo", Address: hasRepo.address,
		Utilization: 0.70, // much fuller...
		Repos:       []string{"registry.example.com/app"},
		Timestamp:   time.Now(),
	})
	registry.Update(model.WorkerReport{
		NodeID: "node-empty", Address: empty.address,
		Utilization: 0.05, // ...than this nearly-empty node
		Timestamp:   time.Now(),
	})

	rk := NewRanker(registry, 1, time.Hour, nil, true)
	ordered := rk.OrderNow("registry.example.com/app:2.0.0")

	waitForPulls(t, hasRepo, empty)

	if len(ordered) != 1 || ordered[0] != "node-has-repo" {
		t.Fatalf("expected the node with the repo already present to be chosen despite higher utilization, got %v", ordered)
	}
	if len(hasRepo.received()) != 1 {
		t.Fatalf("expected exactly one pull order to node-has-repo, got %v", hasRepo.received())
	}
	if len(empty.received()) != 0 {
		t.Fatalf("expected no pull order to the emptier node without the repo, got %v", empty.received())
	}
}

// TestRankerFallsBackToUtilizationWhenLocalityDisabled proves
// preferImageLocality=false restores the old behavior exactly, even when a
// node happens to have the repo already.
func TestRankerFallsBackToUtilizationWhenLocalityDisabled(t *testing.T) {
	hasRepo := newPullOrderRecorder(t)
	empty := newPullOrderRecorder(t)

	registry := NewRegistry(time.Minute, time.Minute)
	registry.Update(model.WorkerReport{
		NodeID: "node-has-repo", Address: hasRepo.address,
		Utilization: 0.70,
		Repos:       []string{"registry.example.com/app"},
		Timestamp:   time.Now(),
	})
	registry.Update(model.WorkerReport{
		NodeID: "node-empty", Address: empty.address,
		Utilization: 0.05,
		Timestamp:   time.Now(),
	})

	rk := NewRanker(registry, 1, time.Hour, nil, false)
	ordered := rk.OrderNow("registry.example.com/app:2.0.0")

	waitForPulls(t, hasRepo, empty)

	if len(ordered) != 1 || ordered[0] != "node-empty" {
		t.Fatalf("expected utilization-only ranking to pick the emptier node, got %v", ordered)
	}
}

// TestRankerLocalityMatchesByRepoNotTag proves the match is repo-level: a
// node holding an OLDER tag of the same repo still counts as "has it",
// while a node holding a DIFFERENT repo entirely does not.
func TestRankerLocalityMatchesByRepoNotTag(t *testing.T) {
	oldTag := newPullOrderRecorder(t)
	otherRepo := newPullOrderRecorder(t)

	registry := NewRegistry(time.Minute, time.Minute)
	registry.Update(model.WorkerReport{
		NodeID: "node-old-tag", Address: oldTag.address,
		Utilization: 0.50,
		Repos:       []string{"registry.example.com/app"}, // same repo, was holding 1.0.0
		Timestamp:   time.Now(),
	})
	registry.Update(model.WorkerReport{
		NodeID: "node-other-repo", Address: otherRepo.address,
		Utilization: 0.10, // emptier, but wrong repo entirely
		Repos:       []string{"registry.example.com/unrelated-service"},
		Timestamp:   time.Now(),
	})

	rk := NewRanker(registry, 1, time.Hour, nil, true)
	ordered := rk.OrderNow("registry.example.com/app:2.0.0")

	waitForPulls(t, oldTag, otherRepo)

	if len(ordered) != 1 || ordered[0] != "node-old-tag" {
		t.Fatalf("expected the node holding an older tag of the same repo to win over an emptier node with an unrelated repo, got %v", ordered)
	}
}

// tickForTest exposes orderLowestN for tests without making it part of the
// package's real public API — production callers only ever go through
// OrderNow (webhook-triggered) or the ticker inside Run.
func (rk *Ranker) tickForTest(image string) []string {
	return rk.orderLowestN(image)
}

// TestRankerSeedsExactlyTopNPerImage is the stg regression: with the old
// dedup, every tick ordered topN NEW nodes until the TTL ran out, so one
// push preheated the whole fleet. Now later ticks order nobody.
func TestRankerSeedsExactlyTopNPerImage(t *testing.T) {
	registry := NewRegistry(time.Minute, time.Minute)
	var recs []*pullOrderRecorder
	for i := 0; i < 6; i++ {
		rec := newPullOrderRecorder(t)
		recs = append(recs, rec)
		registry.Update(model.WorkerReport{
			NodeID: "worker" + string(rune('a'+i)), Address: rec.address,
			Utilization: float64(i) / 10, Timestamp: time.Now(),
		})
	}
	rk := NewRanker(registry, 2, time.Hour, nil, true)

	if got := rk.tickForTest("registry.example.com/app:1"); len(got) != 2 {
		t.Fatalf("first tick should seed 2, got %v", got)
	}
	waitForPulls(t, recs...)
	for i := 0; i < 5; i++ {
		if got := rk.tickForTest("registry.example.com/app:1"); len(got) != 0 {
			t.Fatalf("tick %d ordered %v after the seeds were placed; preheat must not fan out", i+2, got)
		}
	}
	total := 0
	for _, r := range recs {
		total += len(r.received())
	}
	if total != 2 {
		t.Fatalf("%d pull orders delivered, want 2", total)
	}
}

// TestRankerReplacesAFailedSeed: a rejected order frees its slot, the next
// tick fills it with a different node, and the failed node isn't retried.
func TestRankerReplacesAFailedSeed(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer bad.Close()
	good := newPullOrderRecorder(t)

	registry := NewRegistry(time.Minute, time.Minute)
	registry.Update(model.WorkerReport{NodeID: "broken", Address: bad.Listener.Addr().String(), Utilization: 0.01, Timestamp: time.Now()})
	registry.Update(model.WorkerReport{NodeID: "healthy", Address: good.address, Utilization: 0.50, Timestamp: time.Now()})
	rk := NewRanker(registry, 1, time.Hour, nil, true)

	if got := rk.tickForTest("registry.example.com/app:1"); len(got) != 1 || got[0] != "broken" {
		t.Fatalf("lowest utilization should be tried first, got %v", got)
	}
	time.Sleep(100 * time.Millisecond) // the rejected send releases the slot
	if got := rk.tickForTest("registry.example.com/app:1"); len(got) != 1 || got[0] != "healthy" {
		t.Fatalf("freed slot should go to the next node, got %v", got)
	}
	waitForPulls(t, good)
	if got := rk.tickForTest("registry.example.com/app:1"); len(got) != 0 {
		t.Fatalf("slot filled; nothing more expected, got %v", got)
	}
}
