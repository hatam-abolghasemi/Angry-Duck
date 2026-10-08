package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/containerd/events"
)

// fakeEvents hands out one subscription per Subscribe call.
type fakeEvents struct {
	mu   sync.Mutex
	subs []chan *events.Envelope
	errs []chan error
	got  []string
}

func (f *fakeEvents) Subscribe(_ context.Context, filters ...string) (<-chan *events.Envelope, <-chan error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch, errs := make(chan *events.Envelope, 16), make(chan error, 1)
	f.subs, f.errs, f.got = append(f.subs, ch), append(f.errs, errs), filters
	return ch, errs
}

func (f *fakeEvents) last() (chan *events.Envelope, chan error, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.subs[len(f.subs)-1], f.errs[len(f.errs)-1], len(f.subs)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestInventoryWatchSettlesBurstsAndResyncsAfterReconnecting(t *testing.T) {
	f := &fakeEvents{}
	var changed atomic.Int32
	var liveMu sync.Mutex
	var lives []bool
	w := NewInventoryWatch(f, "k8s.io", "n1", func() { changed.Add(1) }, func(on bool) {
		liveMu.Lock()
		lives = append(lives, on)
		liveMu.Unlock()
	})
	w.settle = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)
	waitFor(t, "a subscription", func() bool { f.mu.Lock(); defer f.mu.Unlock(); return len(f.subs) == 1 })
	if len(f.got) != len(inventoryTopics) || f.got[0] != `namespace=="k8s.io",topic=="/images/create"` {
		t.Fatalf("filters = %q", f.got)
	}
	if changed.Load() != 0 {
		t.Fatal("the first subscription must not report: the reporter's first report covers it")
	}
	ch, errs, _ := f.last()
	for _, topic := range []string{"/images/create", "/images/create", "/images/update"} {
		ch <- &events.Envelope{Namespace: "k8s.io", Topic: topic}
	}
	waitFor(t, "one report for the burst", func() bool { return changed.Load() == 1 })
	time.Sleep(60 * time.Millisecond)
	if n := changed.Load(); n != 1 {
		t.Fatalf("%d reports for one burst, want 1", n)
	}
	errs <- errors.New("containerd restarted")
	waitFor(t, "a resubscription", func() bool { _, _, n := f.last(); return n == 2 })
	waitFor(t, "a report on reconnecting", func() bool { return changed.Load() == 2 })
	liveMu.Lock()
	defer liveMu.Unlock()
	if len(lives) != 3 || !lives[0] || lives[1] || !lives[2] {
		t.Fatalf("live = %v, want on, off while down, on again", lives)
	}
}
