package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"angryduck/internal/model"
)

// resumeStore is ingestStore with containerd's resumable writes: an
// ingest file stays under its ref until it is committed or aborted, and a
// write under a ref goes on from what its file holds, as containerd's
// Status offset says.
type resumeStore struct {
	*ingestStore
	amu     sync.Mutex
	aborted int
}

func newResumeStore(t *testing.T) *resumeStore {
	s := &resumeStore{ingestStore: newIngestStore(t)}
	s.chunk = 512
	return s
}

func (s *resumeStore) abortedCount() int {
	s.amu.Lock()
	defer s.amu.Unlock()
	return s.aborted
}

func (s *resumeStore) WriteBlobFrom(ctx context.Context, lease, ref, d string, size int64, open BlobSource, keep func(error) bool) (res WriteResult, err error) {
	if have, _ := s.HasBlob(d); have {
		_, err := s.HoldContent(ctx, lease, d)
		return res, err
	}
	path := filepath.Join(s.dir, ref)
	var off int64
	if st, e := os.Stat(path); e == nil && st.Size() <= size {
		off = st.Size()
	}
	abort := func() {
		os.Remove(path)
		s.amu.Lock()
		s.aborted++
		s.amu.Unlock()
	}
	body, start, err := open(off)
	if err != nil {
		if off == 0 || keep == nil || !keep(err) {
			abort()
		} else {
			res.Kept = true
		}
		return res, err
	}
	defer body.Close()
	if start != off {
		res.Dropped, off = off, 0
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return res, err
	}
	if err := f.Truncate(off); err != nil {
		f.Close()
		return res, err
	}
	res.Resumed = off
	obs := writeObserverFrom(ctx)
	if obs != nil {
		obs.opened(off)
	}
	buf := make([]byte, s.chunk)
	total := off
	r := io.LimitReader(body, size-off+1)
	for {
		k, rerr := r.Read(buf)
		if k > 0 {
			if _, werr := f.WriteAt(buf[:k], total); werr != nil {
				err = werr
				break
			}
			total += int64(k)
			if obs != nil {
				obs.wrote(total)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			err = rerr
			break
		}
	}
	f.Close()
	res.Received = total - off
	if err == nil && total < size {
		err = fmt.Errorf("got %d bytes, want %d", total, size)
	}
	if err != nil {
		if total <= size && total > 0 && keep != nil && keep(err) {
			res.Kept = true
		} else {
			abort()
		}
		return res, err
	}
	if total > size {
		abort()
		return res, fmt.Errorf("got %d bytes, want %d", total, size)
	}
	b, _ := os.ReadFile(path)
	if digestOf(b) != d {
		abort()
		return res, errors.New("content mismatch at commit")
	}
	os.Remove(path)
	s.fakeSpreadStore.mu.Lock()
	defer s.fakeSpreadStore.mu.Unlock()
	if s.leases[lease] == nil {
		return res, errors.New("no such lease " + lease)
	}
	s.blobs[d] = b
	s.leases[lease][d] = true
	return res, nil
}

// newResumeSpread is a Spread whose store resumes.
func newResumeSpread(t *testing.T, store *resumeStore, reg BlobOpener, ctl *controllerSink, node string) (*Spread, *httptest.Server) {
	sp, srv := newTestSpread(t, store.fakeSpreadStore, nil, ctl, node)
	sp.store, sp.registry, sp.ingest = store, reg, store
	if !sp.EnableResume() {
		t.Fatal("the store can't resume")
	}
	return sp, srv
}

// brokenRegistry sends the first cut bytes of a blob, then fails; with
// ranges it honours Range requests (OpenBlobFrom), and fails no more
// once it has broken once.
type brokenRegistry struct {
	blob   []byte
	cut    int
	ranges bool
	mu     sync.Mutex
	broke  bool
	froms  []int64
}

func (r *brokenRegistry) OpenBlob(ctx context.Context, image, d string) (io.ReadCloser, int64, error) {
	body, _, n, err := r.open(0)
	return body, n, err
}

func (r *brokenRegistry) open(from int64) (io.ReadCloser, int64, int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.froms = append(r.froms, from)
	if !r.broke {
		r.broke = true
		return io.NopCloser(io.MultiReader(bytes.NewReader(r.blob[:r.cut]), failReader{errors.New("connection reset")})), 0, int64(len(r.blob)), nil
	}
	return io.NopCloser(bytes.NewReader(r.blob[from:])), from, int64(len(r.blob)) - from, nil
}

type failReader struct{ err error }

func (e failReader) Read([]byte) (int, error) { return 0, e.err }

// rangedRegistry is brokenRegistry with Range support.
type rangedRegistry struct{ *brokenRegistry }

func (r rangedRegistry) OpenBlobFrom(ctx context.Context, image, d string, from int64) (io.ReadCloser, int64, int64, error) {
	return r.open(from)
}

// peerWith serves blob as a peer of this version would (it is a Spread
// holding the blob) and records the Range header of every request.
func peerWith(t *testing.T, node string, blob []byte) (addr string, ranges func() []string) {
	t.Helper()
	store := newFakeSpreadStore()
	store.blobs[digestOf(blob)] = blob
	sp, _ := newTestSpread(t, store, &fakeRegistry{}, nil, node)
	mux := http.NewServeMux()
	sp.Register(mux)
	var mu sync.Mutex
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get("Range"))
		mu.Unlock()
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://"), func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...)
	}
}

// metricValue reads one series' value from the worker's metrics.
func metricValue(t *testing.T, series string) int64 {
	t.Helper()
	for _, line := range strings.Split(scrapeMetrics(t), "\n") {
		if v, ok := strings.CutPrefix(line, series+" "); ok {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				t.Fatalf("%s: %v", line, err)
			}
			return n
		}
	}
	t.Fatalf("no series %s", series)
	return 0
}

func discarded(t *testing.T, node, reason string) int64 {
	return metricValue(t, `angryduck_worker_spread_discarded_bytes_total{node="`+node+`",reason="`+reason+`"}`)
}

func resumed(t *testing.T, node, path string) int64 {
	return metricValue(t, `angryduck_worker_spread_resumed_bytes_total{node="`+node+`",path="`+path+`"}`)
}

func (c *controllerSink) all() []model.SpreadDone {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]model.SpreadDone(nil), c.reports...)
}

// A registry pull that breaks part-way leaves its bytes; the next transfer
// of the blob, from a peer, asks only for the rest.
func TestSpreadResumesAFailedTransferFromAnotherSource(t *testing.T) {
	node := "rs-" + randomSuffix()[:8] // metrics are per process: a fresh node per run
	blob := []byte(strings.Repeat("resume-me", 2000))
	d, size := digestOf(blob), int64(len(blob))
	store, ctl := newResumeStore(t), newControllerSink(t)
	reg := &brokenRegistry{blob: blob, cut: 6000}
	sp, srv := newResumeSpread(t, store, reg, ctl, node)

	postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Image: "r.io/app:1", Digest: d, Size: size, Path: model.SpreadPathRegistry})
	sp.wg.Wait()
	if r := ctl.all(); len(r) != 1 || r[0].Result != model.SpreadResultFailed || r[0].Bytes != 6000 {
		t.Fatalf("reports %+v, want one failed after 6000 bytes", r)
	}
	if store.abortedCount() != 0 {
		t.Fatal("the failed transfer's ingest was aborted")
	}

	addr, ranges := peerWith(t, "rs-src", blob)
	postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Digest: d, Size: size, Path: model.SpreadPathPeer, Peer: &model.Holder{NodeID: "rs-src", Address: addr}})
	sp.wg.Wait()
	r := ctl.all()
	if last := r[len(r)-1]; last.Result != model.SpreadResultOK || last.Bytes != size-6000 {
		t.Fatalf("second transfer %+v, want ok with only the rest (%d bytes)", last, size-6000)
	}
	if got := ranges(); len(got) != 1 || got[0] != "bytes=6000-" {
		t.Fatalf("peer saw Range %q, want bytes=6000-", got)
	}
	if !bytes.Equal(store.blobs[d], blob) {
		t.Fatal("committed blob differs")
	}
	if got := resumed(t, node, model.SpreadPathPeer); got != 6000 {
		t.Fatalf("resumed bytes %d, want 6000", got)
	}
	if got := discarded(t, node, "failed"); got != 0 {
		t.Fatalf("discarded failed %d, want 0", got)
	}
	sp.mu.Lock()
	left := len(sp.partials)
	sp.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d partials left after the commit", left)
	}
}

// The registry honours Range: the retry over the same path fetches the
// rest from it.
func TestSpreadResumesFromTheRegistryWithARange(t *testing.T) {
	node := "rs-" + randomSuffix()[:8] // metrics are per process: a fresh node per run
	blob := []byte(strings.Repeat("registry-range", 1500))
	d, size := digestOf(blob), int64(len(blob))
	store, ctl := newResumeStore(t), newControllerSink(t)
	reg := rangedRegistry{&brokenRegistry{blob: blob, cut: 9000}}
	sp, srv := newResumeSpread(t, store, reg, ctl, node)
	order := model.SpreadFetch{Job: "s1", Image: "r.io/app:1", Digest: d, Size: size, Path: model.SpreadPathRegistry}
	postSpread(t, srv, "/spread/fetch", order)
	sp.wg.Wait()
	postSpread(t, srv, "/spread/fetch", order)
	sp.wg.Wait()
	if !bytes.Equal(store.blobs[d], blob) {
		t.Fatal("committed blob differs")
	}
	if fmt.Sprint(reg.froms) != "[0 9000]" {
		t.Fatalf("registry asked from %v, want [0 9000]", reg.froms)
	}
	if got := resumed(t, node, model.SpreadPathRegistry); got != 9000 {
		t.Fatalf("resumed %d, want 9000", got)
	}
}

// A source that can only send the whole blob (a peer before 1.15.0, a
// registry ignoring Range): what was here goes, the blob is still right.
func TestSpreadStartsOverWhenTheSourceSendsTheWholeBlob(t *testing.T) {
	node := "rs-" + randomSuffix()[:8] // metrics are per process: a fresh node per run
	blob := []byte(strings.Repeat("old-peer", 2500))
	d, size := digestOf(blob), int64(len(blob))
	store, ctl := newResumeStore(t), newControllerSink(t)
	sp, srv := newResumeSpread(t, store, &brokenRegistry{blob: blob, cut: 5000}, ctl, node)
	postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Image: "r.io/app:1", Digest: d, Size: size, Path: model.SpreadPathRegistry})
	sp.wg.Wait()

	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(blob)))
		_, _ = w.Write(blob) // ignores Range, as before 1.15.0
	}))
	t.Cleanup(old.Close)
	postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Digest: d, Size: size, Path: model.SpreadPathPeer,
		Peer: &model.Holder{NodeID: "old", Address: strings.TrimPrefix(old.URL, "http://")}})
	sp.wg.Wait()
	if !bytes.Equal(store.blobs[d], blob) {
		t.Fatal("committed blob differs")
	}
	if got := discarded(t, node, "restarted"); got != 5000 {
		t.Fatalf("discarded restarted %d, want 5000", got)
	}
	if got := resumed(t, node, model.SpreadPathPeer); got != 0 {
		t.Fatalf("resumed %d, want 0", got)
	}
}

// muchFaster is an ExpectRate no running transfer in a test comes close to.
const muchFaster = 1 << 50

// stallingPeer sends the first k bytes of blob, then nothing until the
// request is cancelled.
func stallingPeer(t *testing.T, blob []byte, k int) string {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(blob)))
		_, _ = w.Write(blob[:k])
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

// The slow-chain guard's restart takes the late transfer's bytes over: the
// faster peer sends only the rest.
func TestSpreadRestartGoesOnFromTheLateTransfersBytes(t *testing.T) {
	node := "rs-" + randomSuffix()[:8] // metrics are per process: a fresh node per run
	blob := []byte(strings.Repeat("restart", 3000))
	d, size := digestOf(blob), int64(len(blob))
	store, ctl := newResumeStore(t), newControllerSink(t)
	sp, srv := newResumeSpread(t, store, &fakeRegistry{}, ctl, node)

	slow := stallingPeer(t, blob, 7000)
	postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Digest: d, Size: size, Path: model.SpreadPathPeer, ID: "x1",
		Peer: &model.Holder{NodeID: "slow", Address: slow}})
	waitFor(t, "the slow transfer to write 7000 bytes", func() bool {
		sp.mu.Lock()
		defer sp.mu.Unlock()
		x := sp.xfers[xferKey{d, model.SpreadPathPeer}]
		return x != nil && x.progress.have() == 7000
	})
	fast, ranges := peerWith(t, "fast", blob)
	resp := postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Digest: d, Size: size, Path: model.SpreadPathPeer, ID: "x2",
		Peer: &model.Holder{NodeID: "fast", Address: fast}, Replace: true, ExpectRate: muchFaster})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("replace: status %d", resp.StatusCode)
	}
	sp.wg.Wait()
	if !bytes.Equal(store.blobs[d], blob) {
		t.Fatal("committed blob differs")
	}
	if got := ranges(); len(got) != 1 || got[0] != "bytes=7000-" {
		t.Fatalf("fast peer saw Range %q, want bytes=7000-", got)
	}
	byID := map[string]model.SpreadDone{}
	for _, r := range ctl.all() {
		byID[r.ID] = r
	}
	if byID["x1"].Result != model.SpreadResultCancelled || byID["x2"].Result != model.SpreadResultOK || byID["x2"].Bytes != size-7000 {
		t.Fatalf("reports %+v", byID)
	}
	if got := resumed(t, node, model.SpreadPathPeer); got != 7000 {
		t.Fatalf("resumed %d, want 7000", got)
	}
	for _, reason := range discardReasons {
		if got := discarded(t, node, reason); got != 0 {
			t.Fatalf("discarded %s %d, want 0", reason, got)
		}
	}
	if store.abortedCount() != 0 {
		t.Fatal("an ingest was aborted")
	}
}

// A peer copy of a blob still coming from the registry takes the pull's
// bytes over instead of racing it; a stream reading the blob meanwhile
// goes on across the hand-off.
func TestSpreadPeerCopyTakesTheRegistryPullOver(t *testing.T) {
	node := "rs-" + randomSuffix()[:8] // metrics are per process: a fresh node per run
	blob := []byte(strings.Repeat("takeover", 4000))
	d, size := digestOf(blob), int64(len(blob))
	store, reg, ctl := newResumeStore(t), newPacedRegistry(), newControllerSink(t)
	reg.size[d] = size
	sp, srv := newResumeSpread(t, store, reg, ctl, node)

	resp := postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Image: "r.io/app:1", Digest: d, Size: size, Path: model.SpreadPathRegistry, ID: "reg"})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("registry fetch: %d", resp.StatusCode)
	}
	<-reg.ready
	if _, err := reg.pipe(d).Write(blob[:8000]); err != nil {
		t.Fatal(err)
	}
	stream := getContent(t, srv.URL, d)
	if got := readN(t, stream.Body, 8000); !bytes.Equal(got, blob[:8000]) {
		t.Fatal("stream's first bytes differ")
	}
	addr, ranges := peerWith(t, "holder", blob)
	resp = postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Digest: d, Size: size, Path: model.SpreadPathPeer, ID: "peer",
		Peer: &model.Holder{NodeID: "holder", Address: addr}, ExpectRate: muchFaster})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("peer fetch: %d", resp.StatusCode)
	}
	rest, err := io.ReadAll(stream.Body)
	if err != nil || !bytes.Equal(rest, blob[8000:]) {
		t.Fatalf("stream across the hand-off: %d bytes, err %v", len(rest), err)
	}
	sp.wg.Wait()
	if !bytes.Equal(store.blobs[d], blob) {
		t.Fatal("committed blob differs")
	}
	if got := ranges(); len(got) != 1 || got[0] != "bytes=8000-" {
		t.Fatalf("holder saw Range %q, want bytes=8000-", got)
	}
	byID := map[string]model.SpreadDone{}
	for _, r := range ctl.all() {
		byID[r.ID] = r
	}
	if byID["reg"].Result != model.SpreadResultCancelled || byID["peer"].Result != model.SpreadResultOK || byID["peer"].Bytes != size-8000 {
		t.Fatalf("reports %+v", byID)
	}
	if got := discarded(t, node, "lost_race"); got != 0 {
		t.Fatalf("lost_race %d, want 0", got)
	}
	if got := resumed(t, node, model.SpreadPathPeer); got != 8000 {
		t.Fatalf("resumed %d, want 8000", got)
	}
}

// A peer copy that wouldn't finish clearly sooner is declined, as before:
// the running pull keeps its bytes.
func TestSpreadKeepsTheRunningPullWhenThePeerIsNoFaster(t *testing.T) {
	blob := []byte(strings.Repeat("keep", 4000))
	d, size := digestOf(blob), int64(len(blob))
	store, reg, ctl := newResumeStore(t), newPacedRegistry(), newControllerSink(t)
	reg.size[d] = size
	sp, srv := newResumeSpread(t, store, reg, ctl, "rs-keep")
	postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Image: "r.io/app:1", Digest: d, Size: size, Path: model.SpreadPathRegistry})
	<-reg.ready
	_, _ = reg.pipe(d).Write(blob[:4000])
	waitFor(t, "4000 bytes written", func() bool {
		sp.mu.Lock()
		defer sp.mu.Unlock()
		return sp.xfers[xferKey{d, model.SpreadPathRegistry}].progress.have() == 4000
	})
	addr, _ := peerWith(t, "holder2", blob)
	resp := postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Digest: d, Size: size, Path: model.SpreadPathPeer,
		Peer: &model.Holder{NodeID: "holder2", Address: addr}, ExpectRate: 1})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status %d, want 409 kept", resp.StatusCode)
	}
	_, _ = reg.pipe(d).Write(blob[4000:])
	reg.pipe(d).Close()
	sp.wg.Wait()
	if !bytes.Equal(store.blobs[d], blob) {
		t.Fatal("committed blob differs")
	}
}

// Orders without ExpectRate still race; the loser's bytes are counted as
// thrown away.
func TestSpreadCountsTheBytesALostRaceThrowsAway(t *testing.T) {
	node := "rs-" + randomSuffix()[:8] // metrics are per process: a fresh node per run
	blob := []byte(strings.Repeat("race", 4000))
	d, size := digestOf(blob), int64(len(blob))
	store, reg, ctl := newResumeStore(t), newPacedRegistry(), newControllerSink(t)
	reg.size[d] = size
	sp, srv := newResumeSpread(t, store, reg, ctl, node)
	postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Image: "r.io/app:1", Digest: d, Size: size, Path: model.SpreadPathRegistry})
	<-reg.ready
	_, _ = reg.pipe(d).Write(blob[:3000])
	waitFor(t, "3000 bytes written", func() bool {
		sp.mu.Lock()
		defer sp.mu.Unlock()
		return sp.xfers[xferKey{d, model.SpreadPathRegistry}].progress.have() == 3000
	})
	addr, ranges := peerWith(t, "holder3", blob)
	resp := postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Digest: d, Size: size, Path: model.SpreadPathPeer,
		Peer: &model.Holder{NodeID: "holder3", Address: addr}})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d", resp.StatusCode)
	}
	sp.wg.Wait()
	if !bytes.Equal(store.blobs[d], blob) {
		t.Fatal("committed blob differs")
	}
	if got := ranges(); len(got) != 1 || got[0] != "" {
		t.Fatalf("racing peer saw Range %q, want none", got)
	}
	if got := discarded(t, node, "lost_race"); got != 3000 {
		t.Fatalf("lost_race %d, want 3000", got)
	}
}

// Bytes containerd rejects at commit are never kept: the next transfer
// starts from 0.
func TestSpreadDropsAResumedWriteThatFailsItsDigest(t *testing.T) {
	node := "rs-" + randomSuffix()[:8] // metrics are per process: a fresh node per run
	blob := []byte(strings.Repeat("verify", 3000))
	d, size := digestOf(blob), int64(len(blob))
	bad := append([]byte(nil), blob...)
	copy(bad, "GARBAGE!")
	store, ctl := newResumeStore(t), newControllerSink(t)
	sp, srv := newResumeSpread(t, store, &brokenRegistry{blob: bad, cut: 4000}, ctl, node)
	postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Image: "r.io/app:1", Digest: d, Size: size, Path: model.SpreadPathRegistry})
	sp.wg.Wait()

	addr, ranges := peerWith(t, "holder4", blob)
	peer := model.SpreadFetch{Job: "s1", Digest: d, Size: size, Path: model.SpreadPathPeer, Peer: &model.Holder{NodeID: "holder4", Address: addr}}
	postSpread(t, srv, "/spread/fetch", peer)
	sp.wg.Wait()
	if r := ctl.all(); r[len(r)-1].Result != model.SpreadResultFailed {
		t.Fatalf("resumed onto bad bytes: %+v, want failed", r[len(r)-1])
	}
	if got := discarded(t, node, "failed"); got != size {
		t.Fatalf("discarded failed %d, want %d", got, size)
	}
	postSpread(t, srv, "/spread/fetch", peer)
	sp.wg.Wait()
	if !bytes.Equal(store.blobs[d], blob) {
		t.Fatal("blob not committed after starting over")
	}
	if got := ranges(); len(got) != 2 || got[0] != "bytes=4000-" || got[1] != "" {
		t.Fatalf("Ranges %q, want the resume, then the whole blob", got)
	}
}

// Dropping the job forgets what it left part-way.
func TestSpreadDropForgetsPartials(t *testing.T) {
	node := "rs-" + randomSuffix()[:8] // metrics are per process: a fresh node per run
	blob := []byte(strings.Repeat("drop", 3000))
	d, size := digestOf(blob), int64(len(blob))
	store, ctl := newResumeStore(t), newControllerSink(t)
	sp, srv := newResumeSpread(t, store, &brokenRegistry{blob: blob, cut: 2500}, ctl, node)
	postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Image: "r.io/app:1", Digest: d, Size: size, Path: model.SpreadPathRegistry})
	sp.wg.Wait()
	if r := postSpread(t, srv, "/spread/drop", model.SpreadDrop{Job: "s1"}); r.StatusCode != http.StatusNoContent {
		t.Fatalf("drop: %d", r.StatusCode)
	}
	sp.mu.Lock()
	left := len(sp.partials)
	sp.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d partials after the drop", left)
	}
	if got := discarded(t, node, "unused"); got != 2500 {
		t.Fatalf("unused %d, want 2500", got)
	}
}

// Peers serve the rest of a blob from a byte on, committed or arriving.
func TestSpreadServesTheRestOfABlob(t *testing.T) {
	blob := []byte(strings.Repeat("0123456789", 3000))
	d, size := digestOf(blob), int64(len(blob))
	get := func(base, rng string) *http.Response {
		req, _ := http.NewRequest(http.MethodGet, base+"/spread/content/"+d, nil)
		req.Header.Set("Authorization", "Bearer "+spreadToken)
		if rng != "" {
			req.Header.Set("Range", rng)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}
	held := newFakeSpreadStore()
	held.blobs[d] = blob
	_, srv := newTestSpread(t, held, &fakeRegistry{}, nil, "rs-serve")
	resp := get(srv.URL, "bytes=1234-")
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusPartialContent || resp.Header.Get("Content-Range") != fmt.Sprintf("bytes 1234-%d/%d", size-1, size) || !bytes.Equal(b, blob[1234:]) {
		t.Fatalf("committed range: %d %q %d bytes", resp.StatusCode, resp.Header.Get("Content-Range"), len(b))
	}
	for _, other := range []string{"bytes=0-99", "bytes=-100", "bytes=999999-", "items=5-"} {
		resp := get(srv.URL, other)
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK || !bytes.Equal(b, blob) {
			t.Fatalf("Range %q: %d, %d bytes; want the whole blob", other, resp.StatusCode, len(b))
		}
	}

	store, reg := newResumeStore(t), newPacedRegistry()
	reg.size[d] = size
	sp, src := newResumeSpread(t, store, reg, newControllerSink(t), "rs-serve2")
	postSpread(t, src, "/spread/fetch", model.SpreadFetch{Job: "s1", Image: "r.io/app:1", Digest: d, Size: size, Path: model.SpreadPathRegistry})
	<-reg.ready
	_, _ = reg.pipe(d).Write(blob[:20000])
	resp = get(src.URL, "bytes=15000-")
	if resp.StatusCode != http.StatusPartialContent || resp.Header.Get(StreamHeader) != "1" || resp.ContentLength != size-15000 {
		t.Fatalf("arriving range: %d stream %q length %d", resp.StatusCode, resp.Header.Get(StreamHeader), resp.ContentLength)
	}
	if got := readN(t, resp.Body, 5000); !bytes.Equal(got, blob[15000:20000]) {
		t.Fatal("arriving range: first bytes differ")
	}
	_, _ = reg.pipe(d).Write(blob[20000:])
	reg.pipe(d).Close()
	rest, err := io.ReadAll(resp.Body)
	if err != nil || !bytes.Equal(rest, blob[20000:]) {
		t.Fatalf("arriving range rest: %d bytes, %v", len(rest), err)
	}
	sp.wg.Wait()
}

// A streamed transfer that fails on this node's own side (containerd, not
// the source) is the node's failure, not upstream: the controller backs
// it off.
func TestSpreadStoreFailureIsNotBlamedOnTheStreamSource(t *testing.T) {
	blob := []byte(strings.Repeat("own-fault", 2000))
	d, size := digestOf(blob), int64(len(blob))
	store, ctl := &failingResumeStore{newResumeStore(t)}, newControllerSink(t)
	sp, srv := newTestSpread(t, store.fakeSpreadStore, nil, ctl, "rs-own")
	sp.store, sp.ingest = store, store
	if !sp.EnableResume() {
		t.Fatal("can't resume")
	}
	addr, _ := peerWith(t, "streamer", blob)
	postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Digest: d, Size: size, Path: model.SpreadPathPeer, Stream: true,
		Peer: &model.Holder{NodeID: "streamer", Address: addr}})
	sp.wg.Wait()
	r := ctl.all()
	if len(r) != 1 || r[0].Result != model.SpreadResultFailed || r[0].Upstream {
		t.Fatalf("reports %+v, want failed and not upstream", r)
	}
}

// failingResumeStore fails before it opens any source, as containerd
// refusing the write would.
type failingResumeStore struct{ *resumeStore }

func (s *failingResumeStore) WriteBlobFrom(context.Context, string, string, string, int64, BlobSource, func(error) bool) (WriteResult, error) {
	return WriteResult{}, errors.New("containerd: no space left on device")
}

// Bytes a stopped transfer left that the transfer committing the blob
// didn't go on from (a race over the other path) count as thrown away.
func TestSpreadCountsAPartialTheCommitDidntUse(t *testing.T) {
	node := "rs-" + randomSuffix()[:8]
	blob := []byte(strings.Repeat("unused-partial", 2000))
	d, size := digestOf(blob), int64(len(blob))
	store, ctl := newResumeStore(t), newControllerSink(t)
	reg := &brokenRegistry{blob: blob, cut: 7000}
	sp, srv := newResumeSpread(t, store, reg, ctl, node)
	postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Image: "r.io/app:1", Digest: d, Size: size, Path: model.SpreadPathRegistry})
	sp.wg.Wait()
	// The partial is taken by the next transfer: put it back aside, as a
	// transfer racing on another ingest would leave it.
	sp.mu.Lock()
	p := sp.partials[d]
	delete(sp.partials, d)
	sp.mu.Unlock()
	gate := make(chan struct{})
	held := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-gate // answers only once the partial is back
		w.Header().Set("Content-Length", strconv.Itoa(len(blob)))
		_, _ = w.Write(blob)
	}))
	t.Cleanup(held.Close)
	postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Digest: d, Size: size, Path: model.SpreadPathPeer,
		Peer: &model.Holder{NodeID: "holder5", Address: strings.TrimPrefix(held.URL, "http://")}})
	sp.mu.Lock()
	sp.partials[d] = p
	sp.mu.Unlock()
	close(gate)
	sp.wg.Wait()
	if got := discarded(t, node, "unused"); got != 7000 {
		t.Fatalf("unused %d, want 7000", got)
	}
}
