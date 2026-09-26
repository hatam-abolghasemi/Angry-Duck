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
	mu     sync.Mutex
	orders map[string][]string // target -> primary sources it was ordered from
	fail   map[string]bool
}

func (f *fleet) worker(t *testing.T, node string) string {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var o model.RescueOrder
		_ = json.NewDecoder(r.Body).Decode(&o)
		f.mu.Lock()
		f.orders[node] = append(f.orders[node], o.Sources[0].NodeID)
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

func newFleet() *fleet { return &fleet{orders: map[string][]string{}, fail: map[string]bool{}} }

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
	// Round 3: everyone eligible has it; the propagation finishes.
	p.tick(context.Background())
	if len(p.Status()) != 0 {
		t.Fatal("finished propagation still active")
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
