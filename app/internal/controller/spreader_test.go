package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"angryduck/internal/blobship"
	"angryduck/internal/layerindex"
	"angryduck/internal/metrics"
	"angryduck/internal/model"
	"angryduck/internal/registryclient"
)

const (
	dL0 = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	dL1 = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	dL2 = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
)

type fakeImageResolver struct{ res registryclient.Resolved }

func (f fakeImageResolver) Resolve(context.Context, string, string) (registryclient.Resolved, error) {
	return f.res, nil
}

func testResolved() registryclient.Resolved {
	return registryclient.Resolved{
		Top:      blobship.Descriptor{MediaType: blobship.MediaTypeOCIManifest, Digest: "sha256:top", Size: 3},
		Metadata: map[string][]byte{"sha256:top": []byte("top")},
		Layers: []layerindex.Layer{
			{Digest: dL0, ChainID: "sha256:c0", Size: 10 << 20},
			{Digest: dL1, ChainID: "sha256:c1", Size: 20 << 20},
			{Digest: dL2, ChainID: "sha256:c2", Size: 30 << 20},
		},
	}
}

// spreadFleet is a set of fake workers: they accept every order and
// remember it. Tests play the workers' SpreadDone reports themselves.
type spreadFleet struct {
	mu        sync.Mutex
	fetches   []fetchRec
	finalizes []string
	drops     []string
	busy      map[string]bool // answer fetches with 409
}

type fetchRec struct {
	node string
	model.SpreadFetch
}

func (f *spreadFleet) worker(t *testing.T, node string) string {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.URL.Path {
		case "/spread/fetch":
			var o model.SpreadFetch
			_ = json.NewDecoder(r.Body).Decode(&o)
			if f.busy[node] {
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(model.SpreadFetchAck{Reason: "busy"})
				return
			}
			f.fetches = append(f.fetches, fetchRec{node, o})
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(model.SpreadFetchAck{Accepted: true})
		case "/spread/finalize":
			var o model.SpreadFinalize
			_ = json.NewDecoder(r.Body).Decode(&o)
			if string(o.Metadata[o.TopDig]) != "top" {
				t.Errorf("finalize without the metadata: %+v", o)
			}
			f.finalizes = append(f.finalizes, node)
			_ = json.NewEncoder(w).Encode(model.SpreadFinalizeResult{OK: true})
		case "/spread/drop":
			f.drops = append(f.drops, node)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected %s on %s", r.URL.Path, node)
		}
	}))
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

func (f *spreadFleet) take() []fetchRec {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.fetches
	f.fetches = nil
	return out
}

func testSpreader(reg *Registry, perSource int) *Spreader {
	return NewSpreader(reg, fakeImageResolver{testResolved()}, rescueToken, SpreaderConfig{
		Interval: time.Hour, MaxRegistry: 5, RacePerBlob: 2, PerSource: perSource, MaxUtilization: 0.7,
		Exclude: []string{"master"}, RetryAfter: time.Minute, BackoffMax: time.Hour, TransferTimeout: time.Hour,
		ResolveTimeout: time.Second, Platform: "linux/amd64",
	})
}

func spreadSetup(t *testing.T, perSource int) (*Spreader, *spreadFleet, *Registry) {
	f := &spreadFleet{busy: map[string]bool{}}
	reg := NewRegistry(time.Hour, time.Minute)
	for _, n := range []string{"w1", "w2", "w3", "w4", "w5", "w6"} {
		report(reg, n, f.worker(t, n), 0.2)
	}
	report(reg, "master1", f.worker(t, "master1"), 0.1) // excluded
	report(reg, "full", f.worker(t, "full"), 0.9)       // too full to receive
	return testSpreader(reg, perSource), f, reg
}

func (s *Spreader) jobFor(image string) *sjob {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.jobs[image]
}

func byPath(recs []fetchRec, path string) []fetchRec {
	var out []fetchRec
	for _, r := range recs {
		if r.Path == path {
			out = append(out, r)
		}
	}
	return out
}

func TestSpreader_RegistryOncePerBlobPlusBoundedRaces(t *testing.T) {
	s, f, _ := spreadSetup(t, 1)
	ordered, err := s.Start(context.Background(), img)
	if err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	recs := f.take()
	if len(recs) != 5 || len(ordered) != 5 {
		t.Fatalf("%d orders, %d reported: want MaxRegistry=5 registry pulls and nothing else (no holders yet)", len(recs), len(ordered))
	}
	perLayer := map[string]int{}
	perNode := map[string]int{}
	for _, r := range recs {
		if r.Path != model.SpreadPathRegistry || r.Image != img {
			t.Fatalf("unexpected order %+v", r)
		}
		perLayer[r.Digest]++
		perNode[r.node]++
		if r.node == "master1" || r.node == "full" {
			t.Fatalf("%s must not receive", r.node)
		}
	}
	for d, n := range perLayer {
		if n > 2 {
			t.Fatalf("blob %s pulled from the registry %d times at once, want at most RacePerBlob=2", d, n)
		}
	}
	if len(perLayer) != 3 {
		t.Fatalf("every blob must be started before any is raced: %v", perLayer)
	}
	for n, c := range perNode {
		if c > 1 {
			t.Fatalf("node %s has %d registry pulls, want one at a time", n, c)
		}
	}
	// The largest blob is started first, and raced first.
	if perLayer[dL2] != 2 || perLayer[dL0] != 1 {
		t.Fatalf("pulls per blob = %v, want the 30MB one raced and the 10MB one alone", perLayer)
	}
}

func TestSpreader_BlobSpreadsEastWestAsSoonAsAnyNodeHasIt(t *testing.T) {
	s, f, _ := spreadSetup(t, 1)
	if _, err := s.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	first := f.take()
	var puller string
	for _, r := range first {
		if r.Digest == dL1 {
			puller = r.node
			break
		}
	}
	job := s.jobFor(img)
	s.done(model.SpreadDone{Node: puller, Job: job.id, Digest: dL1, Path: model.SpreadPathRegistry, Result: model.SpreadResultOK, Bytes: 20 << 20})
	s.tick(context.Background())
	s.wg.Wait()
	recs := f.take()
	peers := byPath(recs, model.SpreadPathPeer)
	if len(peers) != 1 || peers[0].Digest != dL1 || peers[0].Peer.NodeID != puller {
		t.Fatalf("peer orders = %+v, want exactly one (PerSource=1) for the blob %s now holds, from it", peers, puller)
	}
	for _, r := range byPath(recs, model.SpreadPathRegistry) {
		if r.Digest == dL1 {
			t.Fatalf("%s ordered to pull %s from the registry although %s has it", r.node, dL1, puller)
		}
	}
}

func TestSpreader_RacesARegistryPullWithAPeerPushOnTheSameNode(t *testing.T) {
	s, f, _ := spreadSetup(t, 10)
	if _, err := s.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	var pullers []string
	for _, r := range f.take() {
		if r.Digest == dL2 {
			pullers = append(pullers, r.node)
		}
	}
	if len(pullers) != 2 {
		t.Fatalf("want the largest blob raced by 2 nodes, got %v", pullers)
	}
	winner, slow := pullers[0], pullers[1]
	job := s.jobFor(img)
	s.done(model.SpreadDone{Node: winner, Job: job.id, Digest: dL2, Path: model.SpreadPathRegistry, Result: model.SpreadResultOK})
	s.tick(context.Background())
	s.wg.Wait()
	pushed := false
	for _, r := range byPath(f.take(), model.SpreadPathPeer) {
		if r.node == slow && r.Digest == dL2 && r.Peer.NodeID == winner {
			pushed = true
		}
	}
	if !pushed {
		t.Fatalf("%s still pulling %s from the registry must also get it pushed from %s", slow, dL2, winner)
	}
	s.mu.Lock()
	n := job.nodes[slow]
	both := n.reg != nil && n.peer != nil && n.reg.layer == 2 && n.peer.layer == 2
	s.mu.Unlock()
	if !both {
		t.Fatal("the slow node must have both paths running for the same blob")
	}
	// The peer push wins: the worker cancels its own registry pull and
	// reports it; both slots end up free.
	s.done(model.SpreadDone{Node: slow, Job: job.id, Digest: dL2, Path: model.SpreadPathPeer, Result: model.SpreadResultOK})
	s.done(model.SpreadDone{Node: slow, Job: job.id, Digest: dL2, Path: model.SpreadPathRegistry, Result: model.SpreadResultCancelled})
	s.mu.Lock()
	free := n.reg == nil && n.peer == nil && n.have[2]
	s.mu.Unlock()
	if !free {
		t.Fatal("after the race both slots must be free and the blob counted")
	}
}

func TestSpreader_FinalizesWhenEveryLayerIsThereThenCompletes(t *testing.T) {
	s, f, reg := spreadSetup(t, 10)
	if _, err := s.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	job := s.jobFor(img)
	// Every eligible node receives every layer.
	for round := 0; round < 10; round++ {
		recs := f.take()
		if len(recs) == 0 {
			break
		}
		for _, r := range recs {
			s.done(model.SpreadDone{Node: r.node, Job: job.id, Digest: r.Digest, Path: r.Path, Result: model.SpreadResultOK})
		}
		s.tick(context.Background())
		s.wg.Wait()
	}
	f.mu.Lock()
	finals := len(f.finalizes)
	f.mu.Unlock()
	if finals != 6 {
		t.Fatalf("%d finalizes, want one per eligible node (6)", finals)
	}
	// Every finalize succeeded, but the full node may still get it once
	// cleanup makes room (as with propagation): the job stays.
	s.tick(context.Background())
	s.wg.Wait()
	if s.jobFor(img) == nil {
		t.Fatal("job ended while a node that was too full may still receive it")
	}
	// Once that node has it too, the next tick ends the job and clears its
	// per-image series.
	report(reg, "full", reg.worker("full").Address, 0.5, img)
	s.tick(context.Background())
	s.wg.Wait()
	if s.jobFor(img) != nil {
		t.Fatal("job still running after every node registered the image")
	}
	out := scrape(t)
	if strings.Contains(out, "angryduck_controller_spread_nodes{") || strings.Contains(out, "angryduck_controller_spread_node_missing_bytes{") {
		t.Fatalf("per-image series left behind after the spread ended:\n%s", out)
	}
	if !strings.Contains(out, `angryduck_controller_spreads_total{result="complete"}`) {
		t.Fatal("completion not counted")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.drops) != 0 {
		t.Fatalf("drops %v: nodes that registered the image hold nothing to release", f.drops)
	}
}

func TestSpreader_SupersedeDropsTheOldJobOnNodesThatGotOrders(t *testing.T) {
	s, f, _ := spreadSetup(t, 1)
	if _, err := s.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	ordered := map[string]bool{}
	for _, r := range f.take() {
		ordered[r.node] = true
	}
	if _, err := s.Start(context.Background(), "registry.example.com/team/app:1.6.0"); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.drops) != len(ordered) {
		t.Fatalf("drops to %v, want every node ordered for the old tag (%d)", f.drops, len(ordered))
	}
	for _, n := range f.drops {
		if !ordered[n] {
			t.Fatalf("drop sent to %s, which got nothing", n)
		}
	}
}

func TestSpreader_BusyWorkerFreesTheSlotAndALostReportExpires(t *testing.T) {
	s, f, _ := spreadSetup(t, 1)
	for _, n := range []string{"w1", "w2", "w3", "w4", "w5", "w6"} {
		f.busy[n] = true // every order answered 409
	}
	if _, err := s.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	job := s.jobFor(img)
	s.mu.Lock()
	held := 0
	for _, n := range job.nodes {
		if n.reg != nil || n.peer != nil {
			t.Fatal("a 409 must free the slot")
		}
		if !n.next.IsZero() {
			held++
		}
	}
	s.mu.Unlock()
	if held == 0 {
		t.Fatal("a 409 must hold the node back briefly")
	}
	// Workers free again: orders go out; one report gets lost.
	f.mu.Lock()
	f.busy = map[string]bool{}
	f.mu.Unlock()
	s.mu.Lock()
	for _, n := range job.nodes {
		n.next = time.Time{}
	}
	s.mu.Unlock()
	s.tick(context.Background())
	s.wg.Wait()
	s.mu.Lock()
	var stuck *snode
	for _, n := range job.nodes {
		if n.reg != nil {
			stuck = n
			n.reg.at = time.Now().Add(-2 * time.Hour)
			break
		}
	}
	s.mu.Unlock()
	if stuck == nil {
		t.Fatal("no registry pull ordered once workers were free")
	}
	s.tick(context.Background())
	s.wg.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	if stuck.reg != nil && time.Since(stuck.reg.at) > time.Hour {
		t.Fatal("silent transfer not freed")
	}
}

func scrape(t *testing.T) string {
	t.Helper()
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

func TestSpreader_KeepsRetryingTheRegistryWhenNoNodeHasTheWholeImage(t *testing.T) {
	s, f, _ := spreadSetup(t, 1)
	if _, err := s.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	job := s.jobFor(img)
	// The registry fails the largest blob three times.
	for _, r := range f.take() {
		if r.Digest == dL2 {
			s.done(model.SpreadDone{Node: r.node, Job: job.id, Digest: dL2, Path: model.SpreadPathRegistry, Result: model.SpreadResultFailed})
		}
	}
	s.mu.Lock()
	job.regFails[2] = 3
	for _, n := range job.nodes {
		n.next = time.Time{} // backoff over
	}
	s.mu.Unlock()
	s.tick(context.Background())
	s.wg.Wait()
	retried := false
	for _, r := range f.take() {
		if r.Digest == dL2 && r.Path == model.SpreadPathRegistry {
			retried = true
		}
	}
	if !retried {
		t.Fatal("no node has the whole image to fall back on: the registry must be retried, not given up on")
	}
}

// No cluster-wide cap on east-west traffic: once a blob has a holder, every
// node lacking it gets a peer transfer, limited only by the holders'
// PerSource (until 1.8.9, PROPAGATE_MAX_CONCURRENT=8 capped the cluster).
func TestSpreader_PeerTransfersLimitedOnlyPerNode(t *testing.T) {
	f := &spreadFleet{busy: map[string]bool{}}
	reg := NewRegistry(time.Hour, time.Minute)
	var nodes []string
	for i := 1; i <= 12; i++ {
		n := fmt.Sprintf("w%02d", i)
		nodes = append(nodes, n)
		report(reg, n, f.worker(t, n), 0.2)
	}
	s := testSpreader(reg, 20)
	if _, err := s.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	var puller string
	for _, r := range f.take() {
		if r.Digest == dL1 {
			puller = r.node
			break
		}
	}
	job := s.jobFor(img)
	s.done(model.SpreadDone{Node: puller, Job: job.id, Digest: dL1, Path: model.SpreadPathRegistry, Result: model.SpreadResultOK, Bytes: 20 << 20})
	s.tick(context.Background())
	s.wg.Wait()
	peers := byPath(f.take(), model.SpreadPathPeer)
	if len(peers) != len(nodes)-1 {
		t.Fatalf("%d peer orders, want %d: one per node lacking the blob, no cluster-wide cap", len(peers), len(nodes)-1)
	}
	seen := map[string]bool{}
	for _, p := range peers {
		if p.Digest != dL1 || p.Peer.NodeID != puller || p.node == puller || seen[p.node] {
			t.Fatalf("bad peer order %+v", p)
		}
		seen[p.node] = true
	}
}
