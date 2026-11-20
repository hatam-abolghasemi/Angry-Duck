package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/containerd/events"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/anypb"
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

// taskExit encodes a TaskExit the way containerd sends it: container_id
// (1), id (2), pid (3), exit_status (4).
func taskExit(containerID, id string) *events.Envelope {
	var b []byte
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendString(b, containerID)
	b = protowire.AppendTag(b, 2, protowire.BytesType)
	b = protowire.AppendString(b, id)
	b = protowire.AppendTag(b, 3, protowire.VarintType)
	b = protowire.AppendVarint(b, 3883358)
	b = protowire.AppendTag(b, 4, protowire.VarintType)
	b = protowire.AppendVarint(b, 0)
	return &events.Envelope{Namespace: "k8s.io", Topic: "/tasks/exit",
		Event: &anypb.Any{TypeUrl: "containerd.events.TaskExit", Value: b}}
}

func TestContainerEventsSkipExecExits(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  *events.Envelope
		want bool
	}{
		{"init process exit", taskExit("d809c2", "d809c2"), true},
		{"exec probe exit", taskExit("d809c2", "a47ab7"), false},
		{"task start", &events.Envelope{Topic: "/tasks/start"}, true},
		{"container delete", &events.Envelope{Topic: "/containers/delete"}, true},
		{"exit without payload", &events.Envelope{Topic: "/tasks/exit"}, true},
		{"unreadable exit", &events.Envelope{Topic: "/tasks/exit", Event: &anypb.Any{Value: []byte{0x0a, 0xff}}}, true},
	} {
		if got := followContainerEvent(tc.env); got != tc.want {
			t.Errorf("%s: followed = %t, want %t", tc.name, got, tc.want)
		}
	}
}

func TestInventoryWatchIgnoresExecProbes(t *testing.T) {
	f := &fakeEvents{}
	var cont atomic.Int32
	w := NewInventoryWatch(f, "k8s.io", "n1", func() {}, func(bool) {})
	w.OnContainers(func() { cont.Add(1) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)
	waitFor(t, "a subscription", func() bool { f.mu.Lock(); defer f.mu.Unlock(); return len(f.subs) == 1 })
	ch, _, _ := f.last()
	for i := 0; i < 5; i++ {
		ch <- taskExit("c1", "exec")
	}
	ch <- taskExit("c1", "c1")
	waitFor(t, "the container's own exit", func() bool { return cont.Load() == 1 })
	time.Sleep(30 * time.Millisecond)
	if n := cont.Load(); n != 1 {
		t.Fatalf("image cleanup woken %d times, want 1 (exec exits ignored)", n)
	}
}
