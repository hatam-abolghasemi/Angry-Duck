package kube

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func podJSON(name, rv string) map[string]any {
	return map[string]any{"metadata": map[string]any{"namespace": "ns", "name": name, "resourceVersion": rv},
		"spec": map[string]any{"nodeName": "w1"}}
}

// fakeAPI serves a pod list and scripted watch streams.
type fakeAPI struct {
	mu        sync.Mutex
	lists     int
	listItems []map[string]any
	listRV    string
	watches   []string           // resourceVersion each watch asked for
	streams   [][]map[string]any // events per watch call, in order
	watchCode int                // non-zero: refuse watches with it
	block     chan struct{}      // a watch with no scripted stream waits here
}

func (f *fakeAPI) handler(t *testing.T) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("fieldSelector") != "status.phase=Pending" {
			t.Errorf("selector %q", r.URL.Query().Get("fieldSelector"))
		}
		f.mu.Lock()
		if r.URL.Query().Get("watch") == "" {
			f.lists++
			body := map[string]any{"items": f.listItems, "metadata": map[string]any{"resourceVersion": f.listRV}}
			f.mu.Unlock()
			_ = json.NewEncoder(w).Encode(body)
			return
		}
		if f.watchCode != 0 {
			code := f.watchCode
			f.mu.Unlock()
			http.Error(w, "no", code)
			return
		}
		f.watches = append(f.watches, r.URL.Query().Get("resourceVersion"))
		var evs []map[string]any
		if len(f.streams) > 0 {
			evs, f.streams = f.streams[0], f.streams[1:]
		} else {
			f.mu.Unlock()
			select {
			case <-f.block:
			case <-r.Context().Done():
			}
			return
		}
		f.mu.Unlock()
		enc := json.NewEncoder(w)
		for _, ev := range evs {
			_ = enc.Encode(ev)
			w.(http.Flusher).Flush()
		}
	})
}

func (w *PodWatch) names() []string {
	pods, _ := w.ListPods(context.Background(), "status.phase=Pending")
	var out []string
	for _, p := range pods {
		out = append(out, p.Metadata.Name)
	}
	sort.Strings(out)
	return out
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out: %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestPodWatchListsOnceThenFollowsAndResumes(t *testing.T) {
	f := &fakeAPI{listItems: []map[string]any{podJSON("a", "10")}, listRV: "10", block: make(chan struct{}),
		streams: [][]map[string]any{
			{{"type": "ADDED", "object": podJSON("b", "11")}, {"type": "MODIFIED", "object": podJSON("a", "12")}},
			{{"type": "DELETED", "object": podJSON("a", "13")}, {"type": "BOOKMARK", "object": podJSON("", "15")}},
		}}
	srv := httptest.NewServer(f.handler(t))
	defer srv.Close()
	defer close(f.block)
	var changes atomic.Int32
	w := NewPodWatch(New(srv.URL, "", srv.Client()), "status.phase=Pending", time.Hour, time.Hour, func() { changes.Add(1) })
	if _, err := w.ListPods(context.Background(), "status.phase=Pending"); err == nil {
		t.Fatal("answered before listing")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)
	eventually(t, "third watch", func() bool { f.mu.Lock(); defer f.mu.Unlock(); return len(f.watches) == 3 })
	f.mu.Lock()
	watches, lists := append([]string(nil), f.watches...), f.lists
	f.mu.Unlock()
	if lists != 1 {
		t.Fatalf("%d lists, want 1: a watch that ends on its timeout resumes, it doesn't relist", lists)
	}
	if fmt.Sprint(watches) != "[10 12 15]" {
		t.Fatalf("watches resumed at %v, want [10 12 15] (the bookmark moves it too)", watches)
	}
	if got := fmt.Sprint(w.names()); got != "[b]" {
		t.Fatalf("pods = %s, want [b]", got)
	}
	if n := changes.Load(); n != 4 {
		t.Fatalf("%d changes, want 4: the list and three pod events, not the bookmark", n)
	}
	if _, err := w.ListPods(context.Background(), "status.phase=Running"); err == nil {
		t.Fatal("another selector must be refused")
	}
}

func TestPodWatchRelistsWhenItsVersionIsGone(t *testing.T) {
	f := &fakeAPI{listItems: []map[string]any{podJSON("a", "10")}, listRV: "10", block: make(chan struct{}),
		streams: [][]map[string]any{{{"type": "ERROR", "object": map[string]any{"kind": "Status", "code": 410}}}}}
	srv := httptest.NewServer(f.handler(t))
	defer srv.Close()
	defer close(f.block)
	w := NewPodWatch(New(srv.URL, "", srv.Client()), "status.phase=Pending", time.Hour, time.Hour, func() {})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)
	eventually(t, "a relist after 410", func() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.lists == 2 })
}

func TestPodWatchPollsWithoutWatchPermission(t *testing.T) {
	f := &fakeAPI{listItems: []map[string]any{podJSON("a", "10")}, listRV: "10", watchCode: http.StatusForbidden}
	srv := httptest.NewServer(f.handler(t))
	defer srv.Close()
	w := NewPodWatch(New(srv.URL, "", srv.Client()), "status.phase=Pending", time.Hour, 20*time.Millisecond, func() {})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)
	eventually(t, "periodic lists", func() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.lists >= 4 })
	if got := fmt.Sprint(w.names()); got != "[a]" {
		t.Fatalf("pods = %s", got)
	}
}
