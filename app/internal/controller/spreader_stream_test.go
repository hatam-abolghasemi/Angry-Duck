package controller

import (
	"context"
	"testing"
	"time"

	"angryduck/internal/model"
)

// streamSetup is spreadSetup with every worker streaming and streaming on:
// blobs of 15 MiB and more (dL1, dL2; not dL0), chains of 2.
func streamSetup(t *testing.T, perSource int) (*Spreader, *spreadFleet, *Registry) {
	s, f, reg := spreadSetup(t, perSource)
	s.cfg.Stream, s.cfg.StreamMinBytes, s.cfg.StreamMaxDepth = true, 15<<20, 2
	reg.mu.Lock()
	for _, w := range reg.workers {
		w.Streams = true
	}
	reg.mu.Unlock()
	return s, f, reg
}

func streams(recs []fetchRec) []fetchRec {
	var out []fetchRec
	for _, r := range recs {
		if r.Stream {
			out = append(out, r)
		}
	}
	return out
}

func TestSpreader_StreamsLargeBlobsFromNodesStillPullingThem(t *testing.T) {
	s, f, _ := streamSetup(t, 1)
	if _, err := s.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	pulls := f.take() // registry pulls, all accepted
	pulling := map[string]map[string]bool{}
	for _, r := range pulls {
		if pulling[r.Digest] == nil {
			pulling[r.Digest] = map[string]bool{}
		}
		pulling[r.Digest][r.node] = true
	}
	s.tick(context.Background())
	s.wg.Wait()
	got := streams(f.take())
	if len(got) == 0 {
		t.Fatal("no stream orders, want the idle node streamed a large blob from a node pulling it")
	}
	sources := map[string]int{}
	for _, r := range got {
		if r.Path != model.SpreadPathPeer || r.Peer == nil {
			t.Fatalf("stream order %+v not a peer fetch", r)
		}
		if r.Digest == dL0 {
			t.Fatalf("10 MiB blob streamed, below the 15 MiB minimum")
		}
		if !pulling[r.Digest][r.Peer.NodeID] {
			t.Fatalf("%s streams %s from %s, which isn't pulling it", r.node, r.Digest, r.Peer.NodeID)
		}
		if pulling[r.Digest][r.node] {
			t.Fatalf("%s already pulls %s, it must not get a stream of it too", r.node, r.Digest)
		}
		sources[r.Peer.NodeID]++
	}
	for src, k := range sources {
		if k > 1 {
			t.Fatalf("%s serves %d streams, PerSource=1", src, k)
		}
	}
}

func TestSpreader_StreamChainsStayWithinTheDepthCap(t *testing.T) {
	s, f, _ := streamSetup(t, 10)
	if _, err := s.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	f.take()
	for round := 0; round < 3; round++ {
		s.tick(context.Background())
		s.wg.Wait()
		f.take()
	}
	// Nothing finished: every node receiving a layer must be at depth 1
	// (registry) or 2 (streaming from a depth-1 node), never 3.
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.jobs[img]
	holders := make([][]string, len(j.res.Layers))
	sawStream := false
	for node, n := range j.nodes {
		for _, x := range n.xfers {
			if x.layer < 0 {
				continue
			}
			d := j.depth(node, x.layer, holders, 5)
			if d > 2 {
				t.Fatalf("%s receives layer %d at depth %d, cap is 2", node, x.layer, d)
			}
			if x.stream {
				sawStream = true
				if j.depth(x.from, x.layer, holders, 5) != 1 {
					t.Fatalf("%s streams from %s, which isn't at depth 1", node, x.from)
				}
			}
		}
	}
	if !sawStream {
		t.Fatal("no streams at all")
	}
}

func TestSpreader_NoStreamsFromWorkersThatDontStreamOrWhenOff(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*Spreader, *Registry)
	}{
		{"old workers", func(_ *Spreader, reg *Registry) {
			reg.mu.Lock()
			for _, w := range reg.workers {
				w.Streams = false
			}
			reg.mu.Unlock()
		}},
		{"depth 1", func(s *Spreader, _ *Registry) { s.cfg.StreamMaxDepth = 1 }},
		{"off", func(s *Spreader, _ *Registry) { s.cfg.Stream = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, f, reg := streamSetup(t, 10)
			tc.setup(s, reg)
			if _, err := s.Start(context.Background(), img); err != nil {
				t.Fatal(err)
			}
			s.wg.Wait()
			s.tick(context.Background())
			s.wg.Wait()
			if got := streams(f.take()); len(got) != 0 {
				t.Fatalf("%d stream orders, want none", len(got))
			}
		})
	}
}

// A stream that breaks because its source's own transfer stopped doesn't
// back the receiver off, and that source streams no more for the job.
func TestSpreader_BrokenStreamIsTheSourcesFailure(t *testing.T) {
	s, f, _ := streamSetup(t, 1)
	if _, err := s.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	f.take()
	s.tick(context.Background())
	s.wg.Wait()
	got := streams(f.take())
	if len(got) == 0 {
		t.Fatal("no stream orders")
	}
	r := got[0]
	job := s.jobFor(img)
	s.done(model.SpreadDone{Node: r.node, Job: job.id, Digest: r.Digest, Path: model.SpreadPathPeer, Result: model.SpreadResultFailed, Upstream: true, Error: "unexpected EOF"})
	s.mu.Lock()
	n := job.nodes[r.node]
	backedOff := !n.next.IsZero() || n.fails != 0
	banned := job.noStream[r.Peer.NodeID]
	s.mu.Unlock()
	if backedOff {
		t.Fatal("receiver backed off for its source's failure")
	}
	if !banned {
		t.Fatal("source still allowed to stream after its stream broke on its side")
	}
	s.tick(context.Background())
	s.wg.Wait()
	for _, o := range streams(f.take()) {
		if o.Peer.NodeID == r.Peer.NodeID {
			t.Fatalf("%s streams from %s again", o.node, r.Peer.NodeID)
		}
	}
	if v := metricValue(t, `angryduck_controller_spread_transfers_total{path="peer",result="upstream"}`); v < 1 {
		t.Fatalf("upstream failure not counted: %v", v)
	}

	// The same report without Upstream is the receiver's own failure.
	s.mu.Lock()
	var other *sxfer
	var otherNode string
	for node, nn := range job.nodes {
		for _, x := range nn.xfers {
			if x.stream {
				other, otherNode = x, node
			}
		}
	}
	s.mu.Unlock()
	if other == nil {
		return
	}
	s.done(model.SpreadDone{Node: otherNode, Job: job.id, Digest: job.res.Layers[other.layer].Digest, Path: model.SpreadPathPeer, Result: model.SpreadResultFailed, Error: "disk full"})
	s.mu.Lock()
	defer s.mu.Unlock()
	if job.nodes[otherNode].fails != 1 {
		t.Fatal("receiver's own failure not backed off")
	}
}

// While a broken stream isn't reported yet, its receiver must not become a
// stream source for the node that fed it.
func TestSpreader_NeverStreamsInACircle(t *testing.T) {
	s, _, _ := streamSetup(t, 10)
	s.cfg.StreamMaxDepth = 5
	if _, err := s.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.jobs[img]
	// w2 streams layer 2 from w1; w3 from w2.
	for _, n := range j.nodes {
		n.xfers = nil
	}
	j.nodes["w2"].xfers = []*sxfer{{layer: 2, path: model.SpreadPathPeer, from: "w1", stream: true, accepted: true}}
	j.nodes["w3"].xfers = []*sxfer{{layer: 2, path: model.SpreadPathPeer, from: "w2", stream: true, accepted: true}}
	list := []streamer{{node: "w2", depth: 2}, {node: "w3", depth: 3}}
	if got := j.pickStreamer(list, 2, "w1", map[string]int{}, 10, 5); got != "" {
		t.Fatalf("w1 would stream from %s, which it feeds", got)
	}
	if got := j.pickStreamer(list, 2, "w4", map[string]int{}, 10, 5); got != "w2" {
		t.Fatalf("w4 picks %q, want the shorter chain w2", got)
	}
}

// A receiver that predates streaming can't tell a broken stream from its
// own failure: it only gets blobs from nodes that hold them.
func TestSpreader_NoStreamsToWorkersThatDontStream(t *testing.T) {
	s, f, reg := streamSetup(t, 10)
	reg.mu.Lock()
	reg.workers["w6"].Streams = false
	reg.mu.Unlock()
	if _, err := s.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	s.tick(context.Background())
	s.wg.Wait()
	got := streams(f.take())
	if len(got) == 0 {
		t.Fatal("no streams to the workers that do stream")
	}
	for _, r := range got {
		if r.node == "w6" {
			t.Fatalf("w6 (no streaming) got a stream of %s from %s", r.Digest, r.Peer.NodeID)
		}
	}
}

// With no cap (0), chains grow past 2 as receivers' orders are taken.
func TestSpreader_UncappedChainsGrowBeyondTwo(t *testing.T) {
	s, f, _ := streamSetup(t, 1)
	s.cfg.StreamMaxDepth = 0
	s.cfg.MaxRegistry = 1 // one puller, everyone else in line behind it
	if _, err := s.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	f.take()
	for round := 0; round < 6; round++ {
		s.tick(context.Background())
		s.wg.Wait()
		f.take()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.jobs[img]
	holders := make([][]string, len(j.res.Layers))
	deepest := 0
	for node, n := range j.nodes {
		for _, x := range n.xfers {
			if x.layer >= 0 {
				if d := j.depth(node, x.layer, holders, 10); d > deepest {
					deepest = d
				}
			}
		}
	}
	if deepest < 3 {
		t.Fatalf("deepest chain %d, want past 2 with no cap", deepest)
	}
}

// A large blob whose registry pull streams on isn't raced: nodes join the
// stream instead of splitting the registry's bandwidth. Small ones still are.
func TestSpreader_NoRegistryRaceForAStreamedBlob(t *testing.T) {
	s, f, _ := streamSetup(t, 1)
	if _, err := s.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	for round := 0; round < 3; round++ {
		s.tick(context.Background())
		s.wg.Wait()
	}
	per := map[string]int{}
	for _, r := range byPath(f.take(), model.SpreadPathRegistry) {
		per[r.Digest]++
	}
	if per[dL2] != 1 || per[dL1] != 1 {
		t.Fatalf("registry pulls per blob %v: the streamed 20/30 MB blobs must be pulled once", per)
	}

	// Streaming off: the race is back.
	s2, f2, _ := streamSetup(t, 1)
	s2.cfg.Stream = false
	if _, err := s2.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	s2.wg.Wait()
	per = map[string]int{}
	for _, r := range byPath(f2.take(), model.SpreadPathRegistry) {
		per[r.Digest]++
	}
	if per[dL2] != 2 {
		t.Fatalf("without streaming the largest blob must still be raced: %v", per)
	}
}

// Small blobs have their own budget per source: a node serving a large
// transfer still serves a small blob at the same time.
func TestSpreader_SmallBlobsDontWaitBehindLargeTransfers(t *testing.T) {
	for _, tc := range []struct {
		express int64
		want    bool
	}{{12 << 20, true}, {0, false}} {
		s, f, _ := spreadSetup(t, 1)
		s.cfg.ExpressMaxBytes = tc.express
		if _, err := s.Start(context.Background(), img); err != nil {
			t.Fatal(err)
		}
		s.wg.Wait()
		job := s.jobFor(img)
		// One node holds everything; nothing else does.
		var src string
		for _, r := range f.take() {
			src = r.node
			break
		}
		for _, d := range []string{dL0, dL1, dL2} {
			s.done(model.SpreadDone{Node: src, Job: job.id, Digest: d, Path: model.SpreadPathRegistry, Result: model.SpreadResultOK})
		}
		s.tick(context.Background())
		s.wg.Wait()
		small, large := 0, 0
		for _, r := range byPath(f.take(), model.SpreadPathPeer) {
			if r.Peer.NodeID != src {
				continue
			}
			if r.Digest == dL0 {
				small++
			} else {
				large++
			}
		}
		if large != 1 {
			t.Fatalf("express=%d: %d large transfers from %s at once, PerSource=1", tc.express, large, src)
		}
		if got := small == 1; got != tc.want {
			t.Fatalf("express=%d: small transfers from %s = %d, want express slot %v", tc.express, src, small, tc.want)
		}
	}
}

// guardSetup: streaming on, guard on, typical speeds known (10 MB/s both
// paths), one registry puller.
func guardSetup(t *testing.T) (*Spreader, *spreadFleet, *Registry) {
	s, f, reg := streamSetup(t, 1)
	s.cfg.StreamMaxDepth, s.cfg.SlowFactor, s.cfg.MaxRegistry = 0, 2, 1
	s.rates = map[string]float64{model.SpreadPathRegistry: 10 << 20, model.SpreadPathPeer: 10 << 20}
	return s, f, reg
}

// age moves every transfer of the job back by d.
func age(s *Spreader, j *sjob, d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range j.nodes {
		for _, x := range n.xfers {
			x.at = x.at.Add(-d)
		}
	}
}

func TestSpreader_LateStreamedRegistryPullIsRaced(t *testing.T) {
	s, f, _ := guardSetup(t)
	s.cfg.MaxRegistry = 2 // room for a race once the small blobs are in
	if _, err := s.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	job := s.jobFor(img)
	var head string
	// Every blob but the largest lands; the largest (30 MB, 3 s expected)
	// keeps pulling.
	for round := 0; round < 4; round++ {
		for _, r := range byPath(f.take(), model.SpreadPathRegistry) {
			if r.Digest == dL2 {
				head = r.node
				continue
			}
			s.done(model.SpreadDone{Node: r.node, Job: job.id, Digest: r.Digest, Path: model.SpreadPathRegistry, Result: model.SpreadResultOK, ID: r.ID})
		}
		s.tick(context.Background())
		s.wg.Wait()
	}
	if head == "" {
		t.Fatal("no registry pull of the largest blob")
	}
	for _, r := range byPath(f.take(), model.SpreadPathRegistry) {
		if r.Digest == dL2 {
			t.Fatalf("%s races a streamed pull that isn't late", r.node)
		}
	}
	// 30 MB at 10 MB/s is 3 s; factor 2: late after 6 s.
	age(s, job, 10*time.Second)
	s.tick(context.Background())
	s.wg.Wait()
	raced := false
	for _, r := range byPath(f.take(), model.SpreadPathRegistry) {
		if r.Digest == dL2 && r.node != head {
			raced = true
		}
	}
	if !raced {
		t.Fatal("a late streamed registry pull wasn't raced")
	}
	if v := metricValue(t, `angryduck_controller_spread_slow_total{action="race"}`); v < 1 {
		t.Fatalf("race not counted: %v", v)
	}
}

// A late stream is offered a restart from a holder; declined, it goes on
// and isn't offered again for a while; accepted, the late one is gone and
// its own end, reported under its ID, leaves the new transfer alone.
func TestSpreader_LateStreamIsOfferedARestartFromAHolder(t *testing.T) {
	s, f, _ := guardSetup(t)
	if _, err := s.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	head := byPath(f.take(), model.SpreadPathRegistry)[0]
	s.tick(context.Background())
	s.wg.Wait()
	var st *fetchRec
	for _, r := range streams(f.take()) {
		if r.Digest == dL2 && r.Peer.NodeID == head.node {
			r := r
			st = &r
		}
	}
	if st == nil {
		t.Fatal("no stream of the head's blob")
	}
	job := s.jobFor(img)
	// The head finishes; its stream lags behind. Another node holds the
	// blob too (a restart from the head itself would be pointless).
	s.done(model.SpreadDone{Node: head.node, Job: job.id, Digest: dL2, Path: model.SpreadPathRegistry, Result: model.SpreadResultOK, ID: head.ID})
	var other string
	for _, nd := range []string{"w1", "w2", "w3", "w4", "w5", "w6"} {
		if nd != head.node && nd != st.node {
			other = nd
			break
		}
	}
	s.done(model.SpreadDone{Node: other, Job: job.id, Digest: dL2, Path: model.SpreadPathRegistry, Result: model.SpreadResultOK})
	age(s, job, 20*time.Second)

	f.mu.Lock()
	f.kept = map[string]bool{st.node: true}
	f.mu.Unlock()
	s.tick(context.Background())
	s.wg.Wait()
	var offer *fetchRec
	for _, r := range f.take() {
		if r.node == st.node && r.Replace {
			r := r
			offer = &r
		}
	}
	if offer == nil || offer.Digest != dL2 || offer.ExpectRate <= 0 || offer.Stream || offer.Peer.NodeID == head.node {
		t.Fatalf("restart offer %+v", offer)
	}
	s.mu.Lock()
	n := job.nodes[st.node]
	keptOK := len(n.xfers) == 1 && n.xfers[0].id == st.ID && n.fails == 0 && n.next.IsZero()
	s.mu.Unlock()
	if !keptOK {
		t.Fatal("after a declined offer the stream must go on, without backoff")
	}
	s.tick(context.Background())
	s.wg.Wait()
	for _, r := range f.take() {
		if r.node == st.node && r.Replace {
			t.Fatal("offered again right after a decline")
		}
	}

	// Accepted.
	f.mu.Lock()
	f.kept = nil
	f.mu.Unlock()
	s.mu.Lock()
	n.xfers[0].offered = time.Time{}
	s.mu.Unlock()
	s.tick(context.Background())
	s.wg.Wait()
	var restart *fetchRec
	for _, r := range f.take() {
		if r.node == st.node && r.Replace {
			r := r
			restart = &r
		}
	}
	if restart == nil {
		t.Fatal("no second offer")
	}
	s.mu.Lock()
	replaced := len(n.xfers) == 1 && n.xfers[0].id == restart.ID
	s.mu.Unlock()
	if !replaced {
		t.Fatal("accepted restart must replace the late stream")
	}
	// The late one's own end arrives after: the restart stays.
	s.done(model.SpreadDone{Node: st.node, Job: job.id, Digest: dL2, Path: model.SpreadPathPeer, Result: model.SpreadResultCancelled, ID: st.ID})
	s.done(model.SpreadDone{Node: st.node, Job: job.id, Digest: dL2, Path: model.SpreadPathPeer, Result: model.SpreadResultFailed, ID: st.ID, Error: "context canceled"})
	s.mu.Lock()
	still := len(n.xfers) == 1 && n.xfers[0].id == restart.ID && n.fails == 0
	s.mu.Unlock()
	if !still {
		t.Fatal("the replaced transfer's report cleared the restart or backed the node off")
	}
}

func TestSpreader_TypicalSpeedsFromWholeLargeTransfersOnly(t *testing.T) {
	s, _, _ := spreadSetup(t, 1)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.learnRateLocked(model.SpreadPathPeer, 1<<20, 0.01) // too small to judge by
	if s.rates[model.SpreadPathPeer] != 0 {
		t.Fatal("learned from a small transfer")
	}
	s.learnRateLocked(model.SpreadPathPeer, 100<<20, 1)
	s.learnRateLocked(model.SpreadPathPeer, 100<<20, 10)
	r := s.rates[model.SpreadPathPeer]
	if r <= 10<<20 || r >= 100<<20 {
		t.Fatalf("rate %.0f, want smoothed between 10 and 100 MB/s", r)
	}
	s.cfg.SlowFactor = 2
	x := &sxfer{layer: 0, path: model.SpreadPathPeer, at: time.Now().Add(-4 * time.Second)}
	if s.lateLocked(x, 1<<30, time.Now()) {
		t.Fatal("late before the floor")
	}
	x.at = time.Now().Add(-40 * time.Second)
	if !s.lateLocked(x, 100<<20, time.Now()) {
		t.Fatal("100 MB in 40 s at a typical 30+ MB/s must be late")
	}
	s.cfg.SlowFactor = 0
	if s.lateLocked(x, 100<<20, time.Now()) {
		t.Fatal("late with the guard off")
	}
}

// The guard doesn't wait for the periodic tick: a wake-up is scheduled for
// when the next running transfer turns late.
func TestSpreader_WakesWhenTheNextTransferTurnsLate(t *testing.T) {
	s, _, _ := guardSetup(t)
	s.wakeup = newWaker()
	if _, err := s.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	s.mu.Lock()
	timer := s.lateTimer != nil
	s.mu.Unlock()
	if !timer {
		t.Fatal("no wake-up scheduled for a running transfer")
	}
	// 30 MB at 10 MB/s, factor 2: late at 6 s. Move it to just before.
	age(s, s.jobFor(img), 5950*time.Millisecond)
	s.tick(context.Background())
	s.wg.Wait()
	select { // wake-ups the orders' acknowledgements posted: not this one
	case <-s.wakeup:
	default:
	}
	select {
	case <-s.wakeup:
	case <-time.After(2 * time.Second):
		t.Fatal("no wake-up when the transfer turned late")
	}
	s.cfg.SlowFactor = 0
	s.tick(context.Background())
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lateTimer != nil {
		t.Fatal("a wake-up scheduled with the guard off")
	}
}

// A late copy from a slow holder (not a stream) is offered a restart from
// another holder too.
func TestSpreader_LateCopyFromASlowHolderIsOfferedARestart(t *testing.T) {
	s, f, _ := guardSetup(t)
	s.cfg.Stream = false
	if _, err := s.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	head := byPath(f.take(), model.SpreadPathRegistry)[0]
	job := s.jobFor(img)
	s.done(model.SpreadDone{Node: head.node, Job: job.id, Digest: head.Digest, Path: model.SpreadPathRegistry, Result: model.SpreadResultOK, ID: head.ID})
	s.tick(context.Background())
	s.wg.Wait()
	var copyRec *fetchRec
	for _, r := range byPath(f.take(), model.SpreadPathPeer) {
		if r.Digest == head.Digest && !r.Stream {
			r := r
			copyRec = &r
			break
		}
	}
	if copyRec == nil {
		t.Fatal("no peer copy from the holder")
	}
	var other string
	for _, nd := range []string{"w1", "w2", "w3", "w4", "w5", "w6"} {
		if nd != head.node && nd != copyRec.node {
			other = nd
			break
		}
	}
	s.done(model.SpreadDone{Node: other, Job: job.id, Digest: head.Digest, Path: model.SpreadPathRegistry, Result: model.SpreadResultOK})
	age(s, job, 30*time.Second)
	s.tick(context.Background())
	s.wg.Wait()
	for _, r := range f.take() {
		if r.node == copyRec.node && r.Replace && r.Digest == head.Digest {
			if r.Peer.NodeID == head.node {
				t.Fatal("restart offered from the slow holder itself")
			}
			return
		}
	}
	t.Fatal("no restart offered for a late copy from a slow holder")
}
