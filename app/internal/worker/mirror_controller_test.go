package worker

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"angryduck/internal/blobship"
	"angryduck/internal/model"
)

// A controller that is routable but doesn't answer (its node died, it is
// starved of CPU) costs one timeout, then the mirror leaves it alone for a
// while: misses go straight to the registry. Once the wait is over it is
// asked again, and peers serve again as soon as it answers.
func TestMirrorLeavesASlowControllerAlone(t *testing.T) {
	peerStore := blobship.NewMemStore()
	d := peerStore.Put([]byte("layer"))
	mux := http.NewServeMux()
	NewMirror(peerStore, "b", testToken, "").RegisterPeer(mux)
	peerSrv := httptest.NewServer(mux)
	defer peerSrv.Close()

	var hang atomic.Bool
	hang.Store(true)
	var asks atomic.Int32
	release := make(chan struct{})
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asks.Add(1)
		if hang.Load() {
			select {
			case <-r.Context().Done():
			case <-release:
			}
			return
		}
		_ = json.NewEncoder(w).Encode([]model.Holder{{NodeID: "b", Address: strings.TrimPrefix(peerSrv.URL, "http://")}})
	}))
	defer controller.Close()
	defer close(release)

	m := NewMirror(blobship.NewMemStore(), "a", testToken, controller.URL)
	m.SetControllerTimeouts(100*time.Millisecond, 300*time.Millisecond, time.Second)
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	get := func() (int, time.Duration) {
		start := time.Now()
		resp, err := http.Get(srv.URL + "/v2/team/app/blobs/" + d)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode, time.Since(start)
	}

	if c, took := get(); c != 404 || took < 100*time.Millisecond {
		t.Fatalf("first request: status %d after %s, want a 404 after the timeout", c, took)
	}
	for i := 0; i < 5; i++ {
		if c, took := get(); c != 404 || took > 50*time.Millisecond {
			t.Fatalf("while left alone: status %d after %s, want an immediate 404", c, took)
		}
	}
	if n := asks.Load(); n != 1 {
		t.Fatalf("controller asked %d times while left alone, want 1", n)
	}

	hang.Store(false)
	time.Sleep(350 * time.Millisecond)
	if c, _ := get(); c != 200 {
		t.Fatalf("after the wait, with the controller answering: status %d, want the blob from the peer", c)
	}
}

// A refused connection (no controller pod behind the Service) fails at
// once: it costs nothing, so the controller isn't left alone and peers are
// back the moment a new controller answers.
func TestMirrorKeepsAskingARefusingController(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close() // nothing listens: connections are refused

	m := NewMirror(blobship.NewMemStore(), "a", testToken, "http://"+addr)
	m.SetControllerTimeouts(time.Second, time.Hour, time.Hour)
	if got := m.lookup(t.Context(), "sha256:"+strings.Repeat("a", 64)); got != nil {
		t.Fatalf("lookup with no controller: %v", got)
	}
	if m.controllerSkip() {
		t.Fatal("a refused connection must not leave the controller alone")
	}
}

// The wait starts at the minimum, doubles with each timeout in a row up to
// the cap, and starts over once the controller answers.
func TestMirrorControllerBackoffDoubles(t *testing.T) {
	m := NewMirror(blobship.NewMemStore(), "a", testToken, "http://127.0.0.1:1")
	m.SetControllerTimeouts(time.Second, time.Second, 10*time.Second)
	want := []time.Duration{1, 2, 4, 8, 10, 10}
	for i, w := range want {
		m.controllerAnswered(context.DeadlineExceeded)
		if m.ctrlBackoff != w*time.Second {
			t.Fatalf("timeout %d in a row: wait %s, want %s", i+1, m.ctrlBackoff, w*time.Second)
		}
	}
	m.controllerAnswered(nil)
	if m.controllerSkip() {
		t.Fatal("an answer must end the wait")
	}
	m.controllerAnswered(context.DeadlineExceeded)
	if m.ctrlBackoff != time.Second {
		t.Fatalf("after an answer: wait %s, want the minimum again", m.ctrlBackoff)
	}
}
