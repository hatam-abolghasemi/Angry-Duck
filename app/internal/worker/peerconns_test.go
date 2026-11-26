package worker

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"angryduck/internal/blobship"
	"angryduck/internal/model"
)

// countPeerConns counts the peer connections of what the test builds
// next, and restores the package's settings after it.
func countPeerConns(t *testing.T, idle time.Duration) (out, in func() int64, inHook func(net.Conn, http.ConnState)) {
	t.Helper()
	prevIdle, prevOut, prevIn := peerIdleTimeout, peerConnsOut, peerConnsIn
	t.Cleanup(func() { peerIdleTimeout, peerConnsOut, peerConnsIn = prevIdle, prevOut, prevIn })
	inHook = SetPeerConns("n1", idle)
	return peerConnsOut.get, peerConnsIn.get, inHook
}

func (c *connCount) get() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// waitConns waits until count reads want: a server sees a closed
// connection a moment after the client closed it.
func waitConns(t *testing.T, what string, count func() int64, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for count() != want {
		if time.Now().After(deadline) {
			t.Fatalf("%d connection(s) %s, want %d", count(), what, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A spread's connection to a peer stays for the job's next blobs, and is
// closed once the job's last work here ends.
func TestSpreadClosesPeerConnectionsWhenItsLastJobEnds(t *testing.T) {
	out, _, _ := countPeerConns(t, 0)
	blob := []byte(strings.Repeat("peer", 2048))
	d := digestOf(blob)
	srcStore := newFakeSpreadStore()
	srcStore.blobs[d] = blob
	_, srcSrv := newTestSpread(t, srcStore, &fakeRegistry{}, nil, "src")
	store, ctl := newFakeSpreadStore(), newControllerSink(t)
	sp, srv := newTestSpread(t, store, &fakeRegistry{}, ctl, "n1")

	resp := postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Digest: d, Size: int64(len(blob)), Path: model.SpreadPathPeer,
		Peer: &model.Holder{NodeID: "src", Address: strings.TrimPrefix(srcSrv.URL, "http://")}})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d", resp.StatusCode)
	}
	sp.wg.Wait()
	if got := ctl.byPath()[model.SpreadPathPeer]; got != model.SpreadResultOK {
		t.Fatalf("report = %q", got)
	}
	if n := out(); n != 1 {
		t.Fatalf("%d connection(s) to peers after the transfer, want the one kept for the job's next blobs", n)
	}
	postSpread(t, srv, "/spread/drop", model.SpreadDrop{Job: "s1"})
	if n := out(); n != 0 {
		t.Fatalf("%d connection(s) to peers left after the job ended, want 0", n)
	}
}

// A rescue's connections to its sources are closed once it ends.
func TestRescueClosesPeerConnectionsWhenItEnds(t *testing.T) {
	out, _, _ := countPeerConns(t, 0)
	srcStore := blobship.NewMemStore()
	srcStore.AddTestImage(testImage, "base", "app")
	_, src := peer(t, "worker14", srcStore)
	_, dstSrv := peer(t, "master1", blobship.NewMemStore())
	code, res := order(t, dstSrv, model.RescueOrder{Image: testImage, Sources: []model.RescueSource{{NodeID: "worker14", Address: addr(src)}}}, testToken)
	if code != http.StatusOK || !res.OK {
		t.Fatalf("rescue failed: %d %+v", code, res)
	}
	if n := out(); n != 0 {
		t.Fatalf("%d connection(s) to peers left after the rescue, want 0", n)
	}
}

// A repair's connections to peers are closed once it ends.
func TestImageRepairClosesPeerConnectionsWhenItEnds(t *testing.T) {
	out, _, _ := countPeerConns(t, 0)
	peerStore := blobship.NewMemStore()
	peerStore.AddTestImage(testImage, "base", "app")
	ctl := peerAndController(t, peerStore)
	store := newRepairStore()
	ti := store.AddTestImage(testImage, "base", "app")
	breakImage(store.MemStore, ti)
	newTestRepair(store, &fakeRepairRegistry{store: peerStore}, ctl).Pass(context.Background())
	runnable(t, store, testImage)
	if n := out(); n != 0 {
		t.Fatalf("%d connection(s) to peers left after the repair, want 0", n)
	}
}

// What no event covers (the mirror) is closed after IDLE_CONN_TIMEOUT_S,
// and both sides are counted, on the gauge too.
func TestPeerConnectionsCloseAfterTheIdleTimeoutAndAreCounted(t *testing.T) {
	out, in, inHook := countPeerConns(t, 50*time.Millisecond)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ok")) }))
	srv.Config.ConnState = inHook
	srv.Start()
	t.Cleanup(srv.Close)
	c := &http.Client{Transport: peerTransport(time.Second, time.Second)}
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body) // read to the end: the connection is kept
	resp.Body.Close()
	if n := out(); n != 1 {
		t.Fatalf("%d connection(s) out, want 1", n)
	}
	waitConns(t, "in", in, 1)
	if m := scrapeMetrics(t); !strings.Contains(m, `angryduck_worker_peer_connections{node="n1",direction="out"} 1`) ||
		!strings.Contains(m, `angryduck_worker_peer_connections{node="n1",direction="in"} 1`) {
		t.Fatalf("gauge doesn't show the open connection:\n%s", m)
	}
	waitConns(t, "out after the idle timeout", out, 0)
	waitConns(t, "in after the idle timeout", in, 0)
}
