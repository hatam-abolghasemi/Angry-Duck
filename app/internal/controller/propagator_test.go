package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"angryduck/internal/model"
)

// fleet is a set of fake workers that accept every order and remember
// who was ordered, from which primary source.
type fleet struct {
	mu        sync.Mutex
	orders    map[string][]string // target -> primary sources it was ordered from
	fail      map[string]bool
	cancelled map[string]bool
	seq       []string // targets in the order they were ordered
}

func (f *fleet) worker(t *testing.T, node string) string {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/pull/cancel" {
			f.mu.Lock()
			f.cancelled[node] = true
			f.mu.Unlock()
			_ = json.NewEncoder(w).Encode(model.PullAck{Accepted: true})
			return
		}
		var o model.RescueOrder
		_ = json.NewDecoder(r.Body).Decode(&o)
		f.mu.Lock()
		f.orders[node] = append(f.orders[node], o.Sources[0].NodeID)
		f.seq = append(f.seq, node)
		fail := f.fail[node]
		f.mu.Unlock()
		if o.Reason != "propagate" {
			t.Errorf("reason = %q", o.Reason)
		}
		if fail {
			w.WriteHeader(http.StatusBadGateway)
			_ = json.NewEncoder(w).Encode(model.RescueResult{Error: "boom"})
			return
		}
		_ = json.NewEncoder(w).Encode(model.RescueResult{OK: true, Source: o.Sources[0].NodeID})
	}))
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

func newFleet() *fleet {
	return &fleet{orders: map[string][]string{}, fail: map[string]bool{}, cancelled: map[string]bool{}}
}

func testPropagator(reg *Registry) *Propagator {
	return NewPropagator(reg, rescueToken, PropagatorConfig{
		Interval: time.Hour, Window: time.Hour, MaxConcurrent: 10, PerSource: 2, MaxUtilization: 0.7,
		ExcludeNodeSubstrings: []string{"master"}, RetryAfter: time.Minute, BackoffMax: time.Hour, Timeout: 5 * time.Second,
	})
}

func TestPropagator_FansOutLikeATreeWithoutDuplicates(t *testing.T) {
	f := newFleet()
	reg := NewRegistry(time.Hour, time.Minute)
	report(reg, "seed", f.worker(t, "seed"), 0.1, img)
	for _, n := range []string{"w1", "w2", "w3", "w4", "w5", "w6"} {
		report(reg, n, f.worker(t, n), 0.2)
	}
	report(reg, "master1", f.worker(t, "master1"), 0.1) // excluded
	report(reg, "full", f.worker(t, "full"), 0.9)       // above MaxUtilization

	p := testPropagator(reg)
	p.Start(img)

	// Round 1: one holder, two transfers at most.
	p.tick(context.Background())
	p.wg.Wait()
	if n := len(f.orders); n != 2 {
		t.Fatalf("round 1: %d nodes ordered, want 2 (per-source cap): %v", n, f.orders)
	}
	// Round 2: three holders now, 4 nodes left: all of them go.
	p.tick(context.Background())
	p.wg.Wait()
	if n := len(f.orders); n != 6 {
		t.Fatalf("round 2: %d nodes ordered, want 6: %v", n, f.orders)
	}
	perSource := map[string]int{}
	for node, srcs := range f.orders {
		if len(srcs) != 1 {
			t.Fatalf("node %s ordered %d times", node, len(srcs))
		}
		perSource[srcs[0]]++
	}
	for src, n := range perSource {
		if src == "seed" && n > 2+2 {
			t.Fatalf("seed served %d transfers", n)
		}
	}
	if _, ok := f.orders["master1"]; ok {
		t.Fatal("excluded node was ordered")
	}
	if _, ok := f.orders["full"]; ok {
		t.Fatal("node above MaxUtilization was ordered")
	}

	st := p.Status()
	if len(st) != 1 || len(st[0].Have) != 7 || len(st[0].Missing) != 0 || len(st[0].Skipped) != 2 {
		t.Fatalf("status = %+v", st)
	}
	// Round 3: every node with room has it, but "full" may get room back
	// (the cleanup frees space): the job stays.
	p.tick(context.Background())
	if len(p.Status()) != 1 {
		t.Fatal("propagation ended with a too-full node still missing the image")
	}
	report(reg, "full", reg.FreshWorkers()[len(reg.FreshWorkers())-1].Address, 0.5)
	p.tick(context.Background())
	p.wg.Wait()
	if len(f.orders["full"]) != 1 {
		t.Fatalf("node with room again not ordered: %v", f.orders)
	}
	p.tick(context.Background())
	if len(p.Status()) != 0 {
		t.Fatal("finished propagation still active")
	}
}

func TestPropagator_OneAtATimePerSourceDoubles(t *testing.T) {
	f := newFleet()
	reg := NewRegistry(time.Hour, time.Minute)
	report(reg, "seed", f.worker(t, "seed"), 0.1, img)
	for i := 0; i < 7; i++ {
		n := "w" + string(rune('a'+i))
		report(reg, n, f.worker(t, n), 0.2)
	}
	p := NewPropagator(reg, rescueToken, PropagatorConfig{Interval: time.Hour, MaxConcurrent: 50, PerSource: 1, MaxUtilization: 0.7, RetryAfter: time.Minute, BackoffMax: time.Hour, Timeout: 5 * time.Second})
	p.Start(img)
	want := []int{1, 3, 7} // holders 1 -> 2 -> 4 -> 8
	for round, w := range want {
		p.tick(context.Background())
		p.wg.Wait()
		if len(f.orders) != w {
			t.Fatalf("round %d: %d ordered, want %d", round+1, len(f.orders), w)
		}
	}
}

func TestPropagator_LeavesSeedsAloneThenCancelsSlowOnes(t *testing.T) {
	f := newFleet()
	reg := NewRegistry(time.Hour, time.Minute)
	report(reg, "seed1", f.worker(t, "seed1"), 0.1, img) // done
	report(reg, "seed2", f.worker(t, "seed2"), 0.1)      // still pulling
	report(reg, "w1", f.worker(t, "w1"), 0.2)
	p := NewPropagator(reg, rescueToken, PropagatorConfig{Interval: time.Hour, MaxConcurrent: 10, PerSource: 2, MaxUtilization: 0.7,
		RetryAfter: time.Minute, BackoffMax: time.Hour, Timeout: 5 * time.Second,
		SeedTimeoutMin: time.Minute, SeedTimeoutMax: 10 * time.Minute, SeedTimeoutFactor: 3})
	orderedAt := time.Now()
	p.SetSeeds(func(string) map[string]time.Time { return map[string]time.Time{"seed1": orderedAt, "seed2": orderedAt} })
	p.Start(img)
	p.tick(context.Background())
	p.wg.Wait()
	if _, ok := f.orders["seed2"]; ok {
		t.Fatal("a seed still pulling must not be a propagation target")
	}
	if st := p.Status(); len(st[0].Seeding) != 1 {
		t.Fatalf("status = %+v", st)
	}
	// seed1 finished at once, so the timeout is the 1m floor; move seed2's
	// order back past it.
	orderedAt = time.Now().Add(-2 * time.Minute)
	p.mu.Lock()
	p.jobs[img].firstSeedTook = time.Second
	p.mu.Unlock()
	p.SetSeeds(func(string) map[string]time.Time {
		return map[string]time.Time{"seed1": orderedAt, "seed2": orderedAt}
	})
	p.tick(context.Background())
	p.wg.Wait()
	if !f.cancelled["seed2"] || len(f.orders["seed2"]) != 1 {
		t.Fatalf("slow seed: cancelled=%v orders=%v", f.cancelled, f.orders)
	}
}

func TestPropagator_WaitingPodsFirstAndSupersede(t *testing.T) {
	f := newFleet()
	reg := NewRegistry(time.Hour, time.Minute)
	report(reg, "seed", f.worker(t, "seed"), 0.1, img)
	report(reg, "idle", f.worker(t, "idle"), 0.1)
	report(reg, "busy", f.worker(t, "busy"), 0.6)
	p := NewPropagator(reg, rescueToken, PropagatorConfig{Interval: time.Hour, MaxConcurrent: 10, PerSource: 1, MaxUtilization: 0.7, RetryAfter: time.Minute, BackoffMax: time.Hour, Timeout: 5 * time.Second})
	p.SetWaiting(func() map[string]map[string]bool { return map[string]map[string]bool{img: {"busy": true}} })
	p.Start(img)
	p.tick(context.Background())
	p.wg.Wait()
	if len(f.seq) != 1 || f.seq[0] != "busy" {
		t.Fatalf("node with a waiting pod should go first, got %v", f.seq)
	}
	p.Start("registry.example.com/team/app:1.6.0")
	if imgs := p.Images(); imgs[img] || len(imgs) != 1 {
		t.Fatalf("older tag of the same repo not superseded: %v", imgs)
	}
}

func TestPropagator_WaitsForSeedsAndBacksOffFailures(t *testing.T) {
	f := newFleet()
	f.fail["w1"] = true
	reg := NewRegistry(time.Hour, time.Minute)
	report(reg, "seed", f.worker(t, "seed"), 0.1) // hasn't reported the image yet
	report(reg, "w1", f.worker(t, "w1"), 0.2)

	p := testPropagator(reg)
	p.Start(img)
	p.tick(context.Background())
	p.wg.Wait()
	if len(f.orders) != 0 {
		t.Fatal("ordered transfers before any node had the image")
	}

	report(reg, "seed", reg.FreshWorkers()[0].Address, 0.1, img) // seed finished its pull
	p.tick(context.Background())
	p.wg.Wait()
	p.tick(context.Background()) // within backoff: no second order
	p.wg.Wait()
	if len(f.orders["w1"]) != 1 {
		t.Fatalf("w1 ordered %d times, want 1 then backoff", len(f.orders["w1"]))
	}
	if st := p.Status(); len(st[0].Failing) != 1 {
		t.Fatalf("status = %+v", st)
	}
}

func TestPropagator_WindowExpires(t *testing.T) {
	reg := NewRegistry(time.Hour, time.Minute)
	report(reg, "w1", "10.0.0.1:1", 0.2)
	p := testPropagator(reg)
	p.Start(img)
	p.mu.Lock()
	p.jobs[img].started = time.Now().Add(-2 * time.Hour)
	p.mu.Unlock()
	p.tick(context.Background())
	if len(p.Status()) != 0 {
		t.Fatal("expired propagation still active")
	}
}
