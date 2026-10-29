package worker

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"angryduck/internal/model"
)

func TestKeepRunningJudgesOnRecentSpeed(t *testing.T) {
	t0 := time.Now()
	size := int64(100 << 20)
	at := func(p *xferProgress, sec float64, got int64) {
		p.add(int(got-p.got), t0.Add(time.Duration(sec*float64(time.Second))))
	}
	// 10 MB/s steady, 50 MB in: 5s to go; restarting at 100 MB/s takes 1s.
	var p xferProgress
	p.start(t0)
	for s := 1; s <= 5; s++ {
		at(&p, float64(s), int64(s)*10<<20)
	}
	if keep, why := keepRunning(&p, size, 100<<20, t0.Add(5*time.Second)); keep {
		t.Fatalf("kept a transfer 5x slower than a restart: %s", why)
	}
	// Restarting at 12 MB/s (8.3s) isn't worth it against 5s to go.
	if keep, why := keepRunning(&p, size, 12<<20, t0.Add(5*time.Second)); !keep {
		t.Fatalf("restarted a transfer that finishes sooner: %s", why)
	}
	// Stalled for a while: restart.
	if keep, why := keepRunning(&p, size, 5<<20, t0.Add(12*time.Second)); keep {
		t.Fatalf("kept a stalled transfer: %s", why)
	}
	// Unknown rate for the new source: keep.
	if keep, _ := keepRunning(&p, size, 0, t0.Add(5*time.Second)); !keep {
		t.Fatal("restarted without an expected rate")
	}
	// Slow for a long time, fast lately (its source got a faster copy):
	// judged on the recent speed, kept.
	var q xferProgress
	q.start(t0)
	for s := 1; s <= 20; s++ {
		at(&q, float64(s), int64(s)*1<<20) // 1 MB/s for 20s
	}
	at(&q, 21.5, 60<<20)
	at(&q, 23, 90<<20) // then 20 MB/s
	if keep, why := keepRunning(&q, size, 40<<20, t0.Add(23*time.Second)); !keep {
		t.Fatalf("restarted a transfer that just sped up: %s", why)
	}
}

// gatedPeer serves a blob up to a point, then waits for the test.
func gatedPeer(t *testing.T, blob []byte, upto int, release <-chan struct{}) string {
	return httptestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconvItoa(len(blob)))
		_, _ = w.Write(blob[:upto])
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write(blob[upto:])
	}))
}

func strconvItoa(n int) string { b, _ := json.Marshal(n); return string(b) }

// A late peer transfer is restarted from a faster holder, and the old
// one's end is reported under its own ID.
func TestSpreadReplaceRestartsALateTransfer(t *testing.T) {
	blob := []byte(strings.Repeat("r", 200000))
	d := digestOf(blob)
	store, ctl := newFakeSpreadStore(), newControllerSink(t)
	sp, srv := newTestSpread(t, store, &fakeRegistry{}, ctl, "c")
	never := make(chan struct{})
	t.Cleanup(func() { close(never) })
	slow := gatedPeer(t, blob, 1000, never)
	holder := newFakeSpreadStore()
	holder.blobs[d] = blob
	_, fast := newTestSpread(t, holder, &fakeRegistry{}, nil, "h")

	r := postSpreadURL(t, srv.URL, "/spread/fetch", model.SpreadFetch{Job: "s1", Digest: d, Size: int64(len(blob)), Path: model.SpreadPathPeer,
		Peer: &model.Holder{NodeID: "b", Address: strings.TrimPrefix(slow, "http://")}, Stream: true, ID: "x1"})
	if r.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d", r.StatusCode)
	}
	time.Sleep(200 * time.Millisecond)
	r = postSpreadURL(t, srv.URL, "/spread/fetch", model.SpreadFetch{Job: "s1", Digest: d, Size: int64(len(blob)), Path: model.SpreadPathPeer,
		Peer: &model.Holder{NodeID: "h", Address: strings.TrimPrefix(fast.URL, "http://")}, ID: "x2", Replace: true, ExpectRate: 50 << 20})
	var ack model.SpreadFetchAck
	_ = json.NewDecoder(r.Body).Decode(&ack)
	if r.StatusCode != http.StatusAccepted || !ack.Accepted {
		t.Fatalf("replace: status %d ack %+v, want accepted (the old one is stalled)", r.StatusCode, ack)
	}
	sp.wg.Wait()
	ctl.mu.Lock()
	byID := map[string]string{}
	for _, rep := range ctl.reports {
		byID[rep.ID] = rep.Result
	}
	ctl.mu.Unlock()
	if byID["x1"] != model.SpreadResultCancelled || byID["x2"] != model.SpreadResultOK {
		t.Fatalf("reports by id %v, want x1 cancelled and x2 ok", byID)
	}
	if !bytes.Equal(store.blobs[d], blob) {
		t.Fatal("blob not committed from the holder")
	}
}

// A transfer going well is kept: 409 with Kept, and it finishes.
func TestSpreadReplaceKeepsATransferGoingWell(t *testing.T) {
	blob := []byte(strings.Repeat("k", 300000))
	d := digestOf(blob)
	store, ctl := newFakeSpreadStore(), newControllerSink(t)
	sp, srv := newTestSpread(t, store, &fakeRegistry{}, ctl, "c")
	release := make(chan struct{})
	src := gatedPeer(t, blob, 290000, release) // almost all here, quickly
	holder := newFakeSpreadStore()
	holder.blobs[d] = blob
	_, other := newTestSpread(t, holder, &fakeRegistry{}, nil, "h")

	postSpreadURL(t, srv.URL, "/spread/fetch", model.SpreadFetch{Job: "s1", Digest: d, Size: int64(len(blob)), Path: model.SpreadPathPeer,
		Peer: &model.Holder{NodeID: "b", Address: strings.TrimPrefix(src, "http://")}, Stream: true, ID: "x1"})
	time.Sleep(200 * time.Millisecond)
	r := postSpreadURL(t, srv.URL, "/spread/fetch", model.SpreadFetch{Job: "s1", Digest: d, Size: int64(len(blob)), Path: model.SpreadPathPeer,
		Peer: &model.Holder{NodeID: "h", Address: strings.TrimPrefix(other.URL, "http://")}, ID: "x2", Replace: true, ExpectRate: 100000})
	var ack model.SpreadFetchAck
	_ = json.NewDecoder(r.Body).Decode(&ack)
	if r.StatusCode != http.StatusConflict || !ack.Kept {
		t.Fatalf("status %d ack %+v, want 409 kept", r.StatusCode, ack)
	}
	close(release)
	sp.wg.Wait()
	if got := ctl.byPath()[model.SpreadPathPeer]; got != model.SpreadResultOK {
		t.Fatalf("kept transfer = %q", got)
	}
}

// A stream fed by a stalled registry pull moves to the peer copy of the
// same blob coming in faster, before either commits.
func TestSpreadStreamMovesToTheCopyFurtherAhead(t *testing.T) {
	blob := []byte(strings.Repeat("0123456789", 20000))
	d := digestOf(blob)
	store, reg, ctl := newIngestStore(t), newPacedRegistry(), newControllerSink(t)
	reg.size[d] = int64(len(blob))
	sp, srv := newTestSpread(t, store.fakeSpreadStore, nil, ctl, "b")
	sp.store, sp.registry, sp.ingest = store, reg, store

	startRegistryFetch(t, srv.URL, reg, "s1", d, int64(len(blob)))
	_, _ = reg.pipe(d).Write(blob[:1000]) // then the registry stalls
	resp := getContent(t, srv.URL, d)
	readN(t, resp.Body, 1000)

	release := make(chan struct{})
	peer := gatedPeer(t, blob, 150000, release)
	postSpreadURL(t, srv.URL, "/spread/fetch", model.SpreadFetch{Job: "s1", Digest: d, Size: int64(len(blob)), Path: model.SpreadPathPeer,
		Peer: &model.Holder{NodeID: "p", Address: strings.TrimPrefix(peer, "http://")}})
	// The stream gets past what the registry delivered while both are
	// still running.
	if got := readN(t, resp.Body, 100000); !bytes.Equal(got, blob[1000:101000]) {
		t.Fatal("bytes from the faster copy differ")
	}
	close(release)
	rest, err := io.ReadAll(resp.Body)
	if err != nil || !bytes.Equal(rest, blob[101000:]) {
		t.Fatalf("rest: %d bytes, err %v", len(rest), err)
	}
	sp.wg.Wait() // the peer copy wins; the worker cancels the registry pull itself
	if got := ctl.byPath(); got[model.SpreadPathPeer] != model.SpreadResultOK || got[model.SpreadPathRegistry] != model.SpreadResultCancelled {
		t.Fatalf("reports %v", got)
	}
}

// A restart is expected no faster than this node's link allows: its
// fastest finished transfer less what its other transfers use. Without
// that history, a node already receiving other transfers keeps what runs.
func TestSpreadRestartExpectationRespectsTheNodesOwnLink(t *testing.T) {
	now := time.Now()
	mk := func(rate float64) *spreadXfer {
		x := &spreadXfer{}
		x.progress.start(now.Add(-10 * time.Second))
		x.progress.add(int(rate*10), now)
		return x
	}
	sp := &Spread{xfers: map[xferKey]*spreadXfer{}}
	old := mk(1 << 20)
	sp.xfers[xferKey{"a", "peer"}] = old
	if got := sp.expectLocked(old, 50<<20, now); got != 50<<20 {
		t.Fatalf("alone, no history: expect %v, want the offered 50 MB/s", got)
	}
	sp.xfers[xferKey{"b", "peer"}] = mk(2 << 20)
	if got := sp.expectLocked(old, 50<<20, now); got > 1.01*(1<<20) {
		t.Fatalf("busy, no history: expect %v, want old's own 1 MB/s (keep)", got)
	}
	sp.peakRecv = 10 << 20
	if got := sp.expectLocked(old, 50<<20, now); got < 7.9*(1<<20) || got > 8.1*(1<<20) {
		t.Fatalf("link 10 MB/s, others 2 MB/s: expect %v, want 8 MB/s", got)
	}
	// What that means for the decision on a 100 MB blob, 10 MB in at 1 MB/s.
	old.progress.add(10<<20-int(old.progress.got), now)
	if keep, why := keepRunning(&old.progress, 100<<20, sp.expectLocked(old, 50<<20, now), now); keep {
		t.Fatalf("90 s to go against ~12 s from a holder: %s", why)
	}
}

// A node already receiving the blob from the registry, busy with other
// transfers too (its link is the limit), declines a second copy from a
// peer; without ExpectRate (older controllers) it races as before.
func TestSpreadDeclinesASecondCopyThatWouldOnlySplitItsLink(t *testing.T) {
	blob := []byte(strings.Repeat("s", 100000))
	d := digestOf(blob)
	reg := newPacedRegistry()
	reg.size[d] = int64(len(blob))
	sp, srv := newTestSpread(t, newFakeSpreadStore(), nil, newControllerSink(t), "n1")
	sp.registry = reg
	startRegistryFetch(t, srv.URL, reg, "s1", d, int64(len(blob)))
	_, _ = reg.pipe(d).Write(blob[:10000])
	other := &spreadXfer{}
	other.progress.start(time.Now())
	sp.mu.Lock()
	sp.xfers[xferKey{"sha256:other", "peer"}] = other // another transfer coming in
	sp.mu.Unlock()
	holder := newFakeSpreadStore()
	holder.blobs[d] = blob
	_, peer := newTestSpread(t, holder, &fakeRegistry{}, nil, "h")
	push := model.SpreadFetch{Job: "s1", Digest: d, Size: int64(len(blob)), Path: model.SpreadPathPeer,
		Peer: &model.Holder{NodeID: "h", Address: strings.TrimPrefix(peer.URL, "http://")}, ExpectRate: 50 << 20}
	r := postSpreadURL(t, srv.URL, "/spread/fetch", push)
	var ack model.SpreadFetchAck
	_ = json.NewDecoder(r.Body).Decode(&ack)
	if r.StatusCode != http.StatusConflict || !ack.Kept {
		t.Fatalf("status %d ack %+v, want the second copy declined", r.StatusCode, ack)
	}
	push.ExpectRate = 0
	if r := postSpreadURL(t, srv.URL, "/spread/fetch", push); r.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d, want the old race without ExpectRate", r.StatusCode)
	}
	sp.mu.Lock()
	delete(sp.xfers, xferKey{"sha256:other", "peer"})
	sp.mu.Unlock()
	sp.wg.Wait()
}
