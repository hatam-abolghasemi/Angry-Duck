package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"angryduck/internal/kube"
)

func fastWake(t *testing.T) {
	old := wakeCoalesce
	wakeCoalesce = 10 * time.Millisecond
	t.Cleanup(func() { wakeCoalesce = old })
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestWaker_CollapsesAndNeverBlocks(t *testing.T) {
	fastWake(t)
	w := newWaker()
	for i := 0; i < 5; i++ {
		w.wake()
	}
	<-w
	if !w.settle(context.Background()) {
		t.Fatal("settle returned false")
	}
	select {
	case <-w:
		t.Fatal("wake-ups were not collapsed")
	default:
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	wakeCoalesce = time.Hour
	if w.settle(ctx) {
		t.Fatal("settle ignored a cancelled context")
	}
}

func TestPropagator_FinishedTransferStartsNextRoundWithoutWaitingForTick(t *testing.T) {
	fastWake(t)
	f := newFleet()
	reg := NewRegistry(time.Hour, time.Minute)
	report(reg, "seed", f.worker(t, "seed"), 0.1, img)
	for i := 0; i < 7; i++ {
		n := "w" + string(rune('a'+i))
		report(reg, n, f.worker(t, n), 0.2)
	}
	p := NewPropagator(reg, rescueToken, PropagatorConfig{Interval: time.Hour, MaxConcurrent: 50, PerSource: 1, MaxUtilization: 0.7, RetryAfter: time.Minute, BackoffMax: time.Hour, Timeout: 5 * time.Second})
	p.Start(img)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.tick(ctx)
	stopped := make(chan struct{})
	go func() { p.Run(ctx); close(stopped) }()
	waitFor(t, "every node to be ordered with a one-hour interval", func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.orders) == 7
	})
	cancel()
	<-stopped
	p.wg.Wait()
	for n, from := range f.orders {
		if len(from) != 1 {
			t.Errorf("%s ordered %d times", n, len(from))
		}
	}
}

func TestRescuer_FinishedRescueStartsBacklogWithoutWaitingForTick(t *testing.T) {
	fastWake(t)
	sink := &orderSink{}
	target := sink.server(t)
	addr := strings.TrimPrefix(target.URL, "http://")
	reg := NewRegistry(time.Minute, time.Minute)
	report(reg, "node1", addr, 0.3)
	report(reg, "node2", addr, 0.3)
	report(reg, "node3", addr, 0.3)
	report(reg, "holder", "10.0.0.9:18081", 0.1, img)
	rs := NewRescuer(reg, fakePods{[]kube.Pod{
		stuckPod("ns", "a", "node1", img, "ImagePullBackOff", "IfNotPresent"),
		stuckPod("ns", "b", "node2", img, "ImagePullBackOff", "IfNotPresent"),
		stuckPod("ns", "c", "node3", img, "ErrImagePull", "IfNotPresent"),
	}}, rescueToken, RescuerConfig{Interval: time.Hour, RetryAfter: time.Hour, Timeout: 5 * time.Second, MaxConcurrent: 1, MaxSources: 1})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rs.tick(ctx)
	stopped := make(chan struct{})
	go func() { rs.Run(ctx); close(stopped) }()
	waitFor(t, "all three stuck nodes to be rescued one at a time", func() bool {
		sink.mu.Lock()
		defer sink.mu.Unlock()
		return len(sink.orders) == 3
	})
	cancel()
	<-stopped
	rs.wg.Wait()
}
