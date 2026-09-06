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

	rk := NewRanker(registry, 2, time.Hour, []string{"master", "control-plane"})
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

	rk := NewRanker(registry, 2, time.Hour, []string{"master"})
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

	rk := NewRanker(registry, 1, time.Hour, nil) // no exclusions
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

	rk := NewRanker(registry, 1, time.Hour, nil)

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

	rk := NewRanker(registry, 1, time.Hour, nil)

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

// tickForTest exposes orderLowestN for tests without making it part of the
// package's real public API — production callers only ever go through
// OrderNow (webhook-triggered) or the ticker inside Run.
func (rk *Ranker) tickForTest(image string) []string {
	return rk.orderLowestN(image)
}
