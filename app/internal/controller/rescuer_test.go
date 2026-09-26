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

	"angryduck/internal/kube"
	"angryduck/internal/model"
)

const rescueToken = "0123456789abcdef0123456789abcdef"

type fakePods struct{ pods []kube.Pod }

func (f fakePods) ListPods(context.Context, string) ([]kube.Pod, error) { return f.pods, nil }

func stuckPod(ns, name, node, image, reason, policy string) kube.Pod {
	var p kube.Pod
	p.Metadata.Namespace, p.Metadata.Name = ns, name
	p.Spec.NodeName = node
	p.Spec.Containers = []kube.Container{{Name: "app", Image: image, ImagePullPolicy: policy}}
	cs := kube.ContainerStatus{Name: "app", Image: image}
	cs.State.Waiting = &struct {
		Reason string `json:"reason"`
	}{Reason: reason}
	p.Status.ContainerStatuses = []kube.ContainerStatus{cs}
	return p
}

// orderSink is a fake target worker recording the orders it gets.
type orderSink struct {
	mu     sync.Mutex
	orders []model.RescueOrder
	auth   []string
}

func (s *orderSink) server(t *testing.T) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var o model.RescueOrder
		_ = json.NewDecoder(r.Body).Decode(&o)
		s.mu.Lock()
		s.orders = append(s.orders, o)
		s.auth = append(s.auth, r.Header.Get("Authorization"))
		s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(model.RescueResult{OK: true, Source: o.Sources[0].NodeID, Blobs: 3})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newTestRescuer(reg *Registry, pods []kube.Pod) *Rescuer {
	return NewRescuer(reg, fakePods{pods}, rescueToken, RescuerConfig{
		Interval: time.Hour, RetryAfter: time.Hour, Timeout: 5 * time.Second, MaxConcurrent: 4, MaxSources: 2,
	})
}

func report(reg *Registry, node, addr string, util float64, images ...string) {
	reg.Update(model.WorkerReport{NodeID: node, Address: addr, Utilization: util, Images: images, Timestamp: time.Now()})
}

const img = "registry.example.com/team/app:1.5.0"

func TestRescuer_OrdersStuckNodeWithSourcesByUtilization(t *testing.T) {
	sink := &orderSink{}
	target := sink.server(t)
	reg := NewRegistry(time.Minute, time.Minute)
	report(reg, "master1", strings.TrimPrefix(target.URL, "http://"), 0.3)
	report(reg, "worker14", "10.0.0.14:18081", 0.7, img)
	report(reg, "worker2", "10.0.0.2:18081", 0.2, img)
	report(reg, "worker9", "10.0.0.9:18081", 0.1, "registry.example.com/team/app:1.4.10") // other tag: not a source

	rs := newTestRescuer(reg, []kube.Pod{
		stuckPod("angryduck", "a", "master1", img, "ImagePullBackOff", "IfNotPresent"),
		stuckPod("angryduck", "b", "master1", img, "ErrImagePull", "IfNotPresent"), // same node+image: one order
	})
	rs.tick(context.Background())
	rs.wg.Wait()

	if len(sink.orders) != 1 {
		t.Fatalf("got %d orders, want 1", len(sink.orders))
	}
	o := sink.orders[0]
	if o.Image != img || len(o.Sources) != 2 || o.Sources[0].NodeID != "worker2" || o.Sources[1].NodeID != "worker14" {
		t.Fatalf("order = %+v", o)
	}
	if sink.auth[0] != "Bearer "+rescueToken {
		t.Fatalf("order sent without the token: %q", sink.auth[0])
	}

	// Same pair again within RetryAfter: no second order.
	rs.tick(context.Background())
	rs.wg.Wait()
	if len(sink.orders) != 1 {
		t.Fatalf("re-ordered within RetryAfter: %d orders", len(sink.orders))
	}
}

func TestRescuer_Skips(t *testing.T) {
	sink := &orderSink{}
	target := sink.server(t)
	addr := strings.TrimPrefix(target.URL, "http://")
	reg := NewRegistry(time.Minute, time.Minute)
	report(reg, "node-a", addr, 0.1, img) // already has it
	report(reg, "node-b", addr, 0.1)
	report(reg, "node-c", addr, 0.1)
	report(reg, "worker14", "10.0.0.14:18081", 0.1, img)

	rs := newTestRescuer(reg, []kube.Pod{
		stuckPod("ns", "has-it", "node-a", img, "ImagePullBackOff", "IfNotPresent"),
		stuckPod("ns", "always", "node-b", img, "ImagePullBackOff", "Always"),
		stuckPod("ns", "no-source", "node-c", "registry.example.com/team/other:1", "ImagePullBackOff", "IfNotPresent"),
		stuckPod("ns", "no-worker", "node-d", img, "ImagePullBackOff", "IfNotPresent"),
		stuckPod("ns", "crashing", "node-c", img, "CrashLoopBackOff", "IfNotPresent"),
	})
	rs.tick(context.Background())
	rs.wg.Wait()
	if len(sink.orders) != 0 {
		t.Fatalf("expected no orders, got %+v", sink.orders)
	}
}

func TestFindStuck_NormalizesAndCoversInitContainers(t *testing.T) {
	p := stuckPod("ns", "p", "n1", "nginx", "ImagePullBackOff", "IfNotPresent")
	p.Spec.InitContainers = p.Spec.Containers
	p.Spec.Containers = nil
	p.Status.InitContainerStatuses = p.Status.ContainerStatuses
	p.Status.ContainerStatuses = nil

	unscheduled := stuckPod("ns", "q", "", "nginx", "ImagePullBackOff", "IfNotPresent")

	got := findStuck([]kube.Pod{p, unscheduled})
	if len(got) != 1 || got[0].Image != "docker.io/library/nginx:latest" || got[0].Node != "n1" {
		t.Fatalf("got %+v", got)
	}
}

// failingTarget is a worker whose rescues always fail.
func failingTarget(t *testing.T, calls *int) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*calls++
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(model.RescueResult{Error: "import: boom"})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestRescuer_BacksOffExponentiallyAndResetsWhenResolved(t *testing.T) {
	calls := 0
	target := failingTarget(t, &calls)
	reg := NewRegistry(time.Hour, time.Minute)
	report(reg, "master1", strings.TrimPrefix(target.URL, "http://"), 0.3)
	report(reg, "worker14", "10.0.0.14:18081", 0.1, img)
	pods := &fakePodsMut{pods: []kube.Pod{stuckPod("ns", "p", "master1", img, "ImagePullBackOff", "IfNotPresent")}}
	rs := NewRescuer(reg, pods, rescueToken, RescuerConfig{
		Interval: time.Hour, RetryAfter: time.Minute, BackoffMax: 10 * time.Minute, Timeout: 5 * time.Second, MaxConcurrent: 1, MaxSources: 1,
	})

	rs.tick(context.Background())
	rs.wg.Wait()
	st := rs.Status()
	if calls != 1 || len(st) != 1 || st[0].ConsecutiveFailures != 1 || st[0].LastResult != "failure" {
		t.Fatalf("after 1st attempt: calls=%d status=%+v", calls, st)
	}
	if wait := time.Until(st[0].NextAttempt); wait < 50*time.Second || wait > time.Minute {
		t.Fatalf("first backoff = %s, want ~1m", wait)
	}

	// Pretend time passed: make it due, fail again, expect ~2m.
	for i, want := range []time.Duration{2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 10 * time.Minute, 10 * time.Minute} {
		rs.mu.Lock()
		for _, p := range rs.pairs {
			p.next = time.Time{}
		}
		rs.mu.Unlock()
		rs.tick(context.Background())
		rs.wg.Wait()
		st = rs.Status()
		wait := time.Until(st[0].NextAttempt)
		if st[0].ConsecutiveFailures != i+2 || wait < want-10*time.Second || wait > want {
			t.Fatalf("failure %d: backoff %s, want %s (%+v)", i+2, wait, want, st[0])
		}
	}

	// Not due yet: no new attempt.
	before := calls
	rs.tick(context.Background())
	rs.wg.Wait()
	if calls != before {
		t.Fatal("retried before the backoff expired")
	}

	// The pod recovers: the pair and its backoff are forgotten.
	pods.pods = nil
	rs.tick(context.Background())
	if len(rs.Status()) != 0 {
		t.Fatalf("resolved pair still tracked: %+v", rs.Status())
	}
}

func TestRescuer_SkipsDoNotBackOff(t *testing.T) {
	reg := NewRegistry(time.Hour, time.Minute)
	report(reg, "node-c", "10.0.0.3:18081", 0.1)
	rs := NewRescuer(reg, fakePods{[]kube.Pod{stuckPod("ns", "p", "node-c", img, "ImagePullBackOff", "IfNotPresent")}}, rescueToken, RescuerConfig{
		Interval: time.Hour, RetryAfter: time.Minute, BackoffMax: time.Hour, Timeout: time.Second,
	})
	rs.tick(context.Background())
	st := rs.Status()
	if len(st) != 1 || st[0].LastResult != "no_source" || st[0].ConsecutiveFailures != 0 {
		t.Fatalf("%+v", st)
	}
}

type fakePodsMut struct{ pods []kube.Pod }

func (f *fakePodsMut) ListPods(context.Context, string) ([]kube.Pod, error) { return f.pods, nil }
