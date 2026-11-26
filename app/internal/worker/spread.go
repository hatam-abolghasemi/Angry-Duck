package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"angryduck/internal/blobship"
	"angryduck/internal/imageref"
	"angryduck/internal/logging"
	"angryduck/internal/memlimit"
	"angryduck/internal/metrics"
	"angryduck/internal/model"
	"angryduck/internal/registryclient"
	"angryduck/internal/sharedtoken"
)

var (
	spreadTransfersTotal = metrics.NewCounterVec(
		"angryduck_worker_spread_transfers_total",
		"Blobs this node fetched for a spread, by path (registry, peer) and result: ok, failed, or cancelled (the blob arrived by the other path first, or the job was dropped).",
		"node", "path", "result",
	)
	spreadBytesTotal = metrics.NewCounterVec(
		"angryduck_worker_spread_bytes_total",
		"Bytes moved for spreads, by path: registry and peer are received, served is sent to other nodes.",
		"node", "path",
	)
	spreadMillisTotal = metrics.NewCounterVec(
		"angryduck_worker_spread_transfer_milliseconds_total",
		"Time spent on spread transfers, by path (registry, peer). spread_bytes_total / this x 1000 is the average throughput in bytes per second.",
		"node", "path",
	)
	spreadActive = metrics.NewGaugeVec(
		"angryduck_worker_spread_active",
		"Blobs this node is fetching right now, by path. Any number of different blobs at once; one blob at most once per path.",
		"node", "path",
	)
	spreadFinalizing = metrics.NewGaugeVec(
		"angryduck_worker_spread_finalizing",
		"Images this node is registering (importing and unpacking) right now. Different images run side by side; one image one at a time.",
		"node",
	)
	spreadFinalizesTotal = metrics.NewCounterVec(
		"angryduck_worker_spread_finalizes_total",
		"Images registered from spread blobs, by result.",
		"node", "result",
	)
)

// spreadLeaseTTL is how long a job's blobs stay held without the image
// that will reference them. A crashed or abandoned job frees them on its
// own; a longer job moves them to a fresh lease before this runs out.
const spreadLeaseTTL = time.Hour

// spreadMaxBlob refuses absurd sizes in an order, so a broken controller
// can't make a node fill its disk with one fetch.
const spreadMaxBlob = 64 << 30

// spreadMaxMetadata caps the metadata a finalize carries (real images: a
// few KB).
const spreadMaxMetadata = 4 << 20

// SpreadStore is what spreading needs from containerd. ContainerdStore
// has it.
type SpreadStore interface {
	HasBlob(dgst string) (have, known bool)
	Digests(ctx context.Context) (map[string]bool, error)
	WriteBlob(ctx context.Context, lease, ref, dgst string, size int64, r io.Reader) (int64, error)
	CreateLease(ctx context.Context, id string, ttl time.Duration) error
	MoveLease(ctx context.Context, from, to string, ttl time.Duration) error
	DeleteLease(ctx context.Context, id string) error
	Import(ctx context.Context, r io.Reader, platform string) error
	Resolve(ctx context.Context, image string) (mediaType, digest string, err error)
	OpenContent(ctx context.Context, dgst string) (io.ReadCloser, int64, error)
	// HoldContent and HoldSnapshot put a blob or a committed snapshot that
	// is already here under a job's lease, and report whether it is here,
	// asked fresh. A layer counted as "already here" must be held: until
	// the image is registered nothing else references it, and containerd's
	// garbage collector can free it before the finalize needs it (kubelet
	// or the cleanup removed the image it came with).
	HoldContent(ctx context.Context, lease, dgst string) (bool, error)
	HoldSnapshot(ctx context.Context, lease, key string) (bool, error)
}

// BlobOpener streams one blob of an image's repository from its registry.
type BlobOpener interface {
	OpenBlob(ctx context.Context, image, digest string) (io.ReadCloser, int64, error)
}

// Spread is this node's part of spreading an image blob by blob.
//
// The blob is the unit: the node fetches any number of different blobs at
// once, each at most once per path (registry, peer). The same blob may be
// coming over both paths, a race the controller starts on purpose: when
// one commits it, the other is cancelled right here, from the goroutine
// that committed, without waiting on anyone. Every transfer ends with a
// SpreadDone to the controller. How many run at once is the controller's
// call (registry and per-source limits), not this node's.
//
// Blobs land in containerd's content store under the job's lease (see
// spreadLeaseTTL) and are served to peers by digest at /spread/content.
// When the node holds every layer, /spread/finalize registers the image
// from the metadata the controller sends. Different images register side
// by side; the same image one finalize at a time.
//
// No goroutine runs while nothing is being transferred.
type Spread struct {
	store         SpreadStore
	registry      BlobOpener
	token         string
	nodeID        string
	platform      string
	controllerURL string
	epoch         string        // random per process: lease names never collide with a previous run's
	timeout       time.Duration // one transfer, start to commit
	peers         *http.Client
	ctl           *http.Client

	mu    sync.Mutex
	jobs  map[string]*spreadJob
	xfers map[xferKey]*spreadXfer // running transfers

	finalizing *keyedLock // by image: one finalize per image at a time
	running    int        // finalizes running now, under mu
	onFinalize func()
	onStale    func()         // the controller's view of this node's layers is off
	wg         sync.WaitGroup // transfers, for tests

	// Streaming (see spreadstream.go): nil ingest when this node can't.
	ingest   IngestStore
	streamMu sync.Mutex
	arriving map[string]*arriving // by digest: blobs coming in now
	refs     atomic.Int64         // names every transfer's ingest apart
	peakRecv float64              // fastest finished transfer here, bytes/s: what this node's link takes; under mu

	// Resuming (see resume.go): nil resumer when the store can't continue
	// a write. partials are the ingests transfers stopped part-way left
	// for the next transfer of their blob, by digest, under mu.
	resumer  ResumingStore
	partials map[string]partial

	// Unpacking layers as they arrive (see spreadunpack.go): nil applier
	// when off.
	applier       LayerApplier
	streamApplier LayerStreamApplier    // nil unless stream unpacking is on
	unpacks       map[string]*unpackJob // by job, under mu
	// Parallel unpacking (spreadparallel.go): nil extractor when off;
	// extracting counts the layers being unpacked now across jobs, under mu.
	extractor  LayerExtractor
	extractMax int
	extracting int
	// Every layer unpacking at once while blobs arrive (see
	// spreadwhole.go): nil when stream unpacking is off or can't extract.
	streamExtractor LayerStreamExtractor
	slotsHook       sync.Once // registers the unpack slots gauge's scrape hook once
}

type spreadJob struct {
	mu       sync.Mutex // serializes this job's lease calls
	lease    string     // "" until the first transfer
	gen      int
	leasedAt time.Time
	touched  time.Time
	started  time.Time // first lease of the job here: where spreadReadySeconds starts; under Spread.mu
}

type spreadXfer struct {
	job, digest, path string
	ref               string // its ingest
	cancel            context.CancelFunc
	cancelled         atomic.Bool  // cancelled on purpose: why says why
	why               atomic.Int32 // a stop* reason, once cancelled
	feed              *feed        // what peers stream from while it runs; nil when this node can't stream
	progress          xferProgress
	// stopped is closed once the transfer no longer writes its ingest.
	// after is the transfer it took the ingest over from (handOff): it
	// writes only once that one has stopped.
	stopped chan struct{}
	after   *spreadXfer
}

// spreadRef names the ingest of a spread transfer of dgst over path: n
// sets transfers of this process apart, epoch apart from a previous one's.
func (s *Spread) spreadRef(path, dgst string) string {
	return "angryduck-spread-" + path + "-" + strings.TrimPrefix(dgst, "sha256:") + "-" + s.epoch + "-" + strconv.FormatInt(s.refs.Add(1), 10)
}

// xferKey names a running transfer: one blob over one path.
type xferKey struct{ digest, path string }

func otherPath(path string) string {
	if path == model.SpreadPathPeer {
		return model.SpreadPathRegistry
	}
	return model.SpreadPathPeer
}

// activeLocked publishes how many transfers run on path. Caller holds s.mu.
func (s *Spread) activeLocked(path string) {
	n := 0
	for k := range s.xfers {
		if k.path == path {
			n++
		}
	}
	spreadActive.Set(float64(n), s.nodeID, path)
}

// NewSpread builds the worker's spread handlers.
func NewSpread(store SpreadStore, registry BlobOpener, token, nodeID, platform, controllerURL string, timeout time.Duration) *Spread {
	for _, path := range []string{"registry", "peer"} {
		for _, r := range []string{"ok", "failed", "cancelled"} {
			spreadTransfersTotal.Add(0, nodeID, path, r)
		}
		spreadMillisTotal.Add(0, nodeID, path)
		spreadActive.Set(0, nodeID, path)
	}
	for _, path := range []string{"registry", "peer", "served"} {
		spreadBytesTotal.Add(0, nodeID, path)
	}
	spreadFinalizesTotal.Add(0, nodeID, "success")
	spreadFinalizesTotal.Add(0, nodeID, "failure")
	spreadFinalizing.Set(0, nodeID)
	spreadStreamedBytes.Add(0, nodeID)
	sp := &Spread{
		store: store, registry: registry, token: token, nodeID: nodeID, platform: platform,
		controllerURL: strings.TrimRight(controllerURL, "/"), timeout: timeout, epoch: randomSuffix()[:8],
		// Peers stream for as long as a blob takes; ctx bounds it.
		peers:      &http.Client{Transport: peerTransport(5*time.Second, 30*time.Second)},
		ctl:        &http.Client{Timeout: 5 * time.Second},
		jobs:       map[string]*spreadJob{},
		xfers:      map[xferKey]*spreadXfer{},
		finalizing: newKeyedLock(),
		arriving:   map[string]*arriving{},
		unpacks:    map[string]*unpackJob{},
		partials:   map[string]partial{},
	}
	if is, ok := store.(IngestStore); ok && is.CanStream() {
		sp.ingest = is
	}
	for _, r := range discardReasons {
		spreadDiscardedBytes.Add(0, nodeID, r)
	}
	return sp
}

// Streams reports whether this node serves blobs to peers while still
// receiving them.
func (s *Spread) Streams() bool { return s.ingest != nil }

// DisableStreaming serves only committed blobs, as before streaming.
// Call before Register.
func (s *Spread) DisableStreaming() { s.ingest = nil }

// OnFinalize registers fn to run after an image is registered (the
// reporter's Kick, so the controller sees the image right away).
func (s *Spread) OnFinalize(fn func()) { s.onFinalize = fn }

// OnStale registers fn to run when a finalize finds layers gone that the
// controller counted as here (the reporter's Kick, so its next layer
// inventory goes out right away and the controller re-plans them).
func (s *Spread) OnStale(fn func()) { s.onStale = fn }

// Register mounts the spread endpoints, all behind the shared token.
func (s *Spread) Register(mux *http.ServeMux) {
	mux.HandleFunc("/spread/fetch", sharedtoken.Require(s.token, s.HandleFetch))
	mux.HandleFunc("/spread/finalize", sharedtoken.Require(s.token, s.HandleFinalize))
	mux.HandleFunc("/spread/drop", sharedtoken.Require(s.token, s.HandleDrop))
	mux.HandleFunc("/spread/content/", sharedtoken.Require(s.token, s.HandleContent))
}

// validJobID keeps job IDs safe inside lease and ingest names.
func validJobID(id string) bool {
	if id == "" || len(id) > 64 {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}

// have reports whether blob dgst is committed here.
func (s *Spread) have(ctx context.Context, dgst string) bool {
	if have, known := s.store.HasBlob(dgst); known {
		return have
	}
	all, err := s.store.Digests(ctx)
	return err == nil && all[dgst]
}

// HandleFetch starts one transfer and answers at once (see SpreadFetchAck).
func (s *Spread) HandleFetch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var o model.SpreadFetch
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&o); err != nil {
		http.Error(w, "invalid fetch order", http.StatusBadRequest)
		return
	}
	switch {
	case !validJobID(o.Job), !validDigest(o.Digest), o.Size <= 0, o.Size > spreadMaxBlob:
		http.Error(w, "invalid fetch order", http.StatusBadRequest)
		return
	case o.Path == model.SpreadPathRegistry && o.Image == "",
		o.Path == model.SpreadPathPeer && (o.Peer == nil || o.Peer.Address == ""),
		o.Path != model.SpreadPathRegistry && o.Path != model.SpreadPathPeer:
		http.Error(w, "invalid fetch order", http.StatusBadRequest)
		return
	}
	if s.have(r.Context(), o.Digest) && s.holdPresent(r.Context(), o.Job, o.Digest) {
		s.landed(o.Job, o)
		writeJSON(w, http.StatusOK, model.SpreadFetchAck{Present: true})
		return
	}
	key := xferKey{o.Digest, o.Path}
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	x := &spreadXfer{job: o.Job, digest: o.Digest, path: o.Path, ref: s.spreadRef(o.Path, o.Digest), cancel: cancel, stopped: make(chan struct{})}
	x.progress.start(time.Now())
	now := time.Now()
	s.mu.Lock()
	// prev is the running transfer of the blob this one stops: restarted
	// from a faster source, or taken over.
	var prev *spreadXfer
	var why string
	if old, running := s.xfers[key]; running {
		if !o.Replace {
			s.mu.Unlock()
			cancel()
			writeJSON(w, http.StatusConflict, model.SpreadFetchAck{Reason: "already fetching " + o.Digest + " from " + o.Path})
			return
		}
		keep, reason := keepRunning(&old.progress, o.Size, s.keptFor(old, o.Path), s.expectLocked(old, o.ExpectRate, now), now)
		if keep {
			s.mu.Unlock()
			cancel()
			writeJSON(w, http.StatusConflict, model.SpreadFetchAck{Kept: true, Reason: reason})
			return
		}
		// Restart from the faster source. The old transfer reports its
		// own end (cancelled, under its own ID).
		prev, why = old, reason
	}
	// The blob is already coming over the other path: start this second
	// copy only if it should finish clearly sooner (a node whose own link
	// is the limit would only split it). Orders without ExpectRate, from
	// controllers before 1.9.0, race as before.
	if other := s.xfers[xferKey{o.Digest, otherPath(o.Path)}]; other != nil && o.ExpectRate > 0 && !o.Replace {
		keep, reason := keepRunning(&other.progress, o.Size, s.keptFor(other, o.Path), s.expectLocked(other, o.ExpectRate, now), now)
		switch {
		case keep:
			s.mu.Unlock()
			cancel()
			writeJSON(w, http.StatusConflict, model.SpreadFetchAck{Kept: true, Reason: reason})
			return
		case s.handsOff(o.Path):
			// A hand-off, not a race: the other copy stops, and this one
			// goes on from its bytes instead of fetching them again.
			prev, why = other, reason
		}
	}
	// Its ingest: the one it takes over (a peer goes on from those
	// bytes), else what an earlier transfer of the blob left part-way,
	// else its own, new one.
	if prev != nil && s.handsOff(o.Path) {
		x.ref, x.after = prev.ref, prev
	} else {
		s.takePartialLocked(x)
	}
	s.xfers[key] = x
	s.activeLocked(o.Path)
	s.pruneJobsLocked(now)
	// Streamable from the moment the order is accepted (the controller
	// may send peers here as soon as it sees the 202), and before the
	// transfer it replaces stops: a stream of the blob never finds it
	// gone in between.
	x.feed = s.feedStart(o.Digest, x.ref, o.Size)
	switch {
	case prev == nil:
	case x.after != nil:
		prev.stop(stopHandOff)
		if prev.path == o.Path {
			logging.Infof("angryduck-worker[%s]: spread: restarting %s from %s, going on from its %d bytes: %s", s.nodeID, o.Digest, o.Peer.NodeID, prev.progress.have(), why)
		} else {
			logging.Infof("angryduck-worker[%s]: spread: %s goes on from %s instead of the %s, from its %d bytes: %s", s.nodeID, o.Digest, o.Peer.NodeID, prev.path, prev.progress.have(), why)
		}
	default:
		prev.stop(stopRestart)
		logging.Infof("angryduck-worker[%s]: spread: restarting %s from %s: %s", s.nodeID, o.Digest, o.Peer.NodeID, why)
	}
	s.mu.Unlock()
	// Its layer may be unpacked while it arrives (stream unpacking).
	s.arrives(o.Job, o)

	s.wg.Add(1)
	working := memlimit.Working()
	go func() {
		defer working()
		s.run(ctx, x, o)
	}()
	writeJSON(w, http.StatusAccepted, model.SpreadFetchAck{Accepted: true})
}

// holdPresent puts a blob that is already here under job's lease, so it
// is still here at finalize. False (fetch it after all) when it is gone or
// can't be held: a fetch of a blob that turns out to be here costs one
// containerd call, a "present" for one that then vanishes cost a broken
// image.
func (s *Spread) holdPresent(ctx context.Context, job, dgst string) bool {
	lease, err := s.lease(ctx, job)
	if err != nil {
		logging.Warnf("angryduck-worker[%s]: spread: holding %s: %v (fetching it instead)", s.nodeID, dgst, err)
		return false
	}
	held, err := s.store.HoldContent(ctx, lease, dgst)
	if err != nil {
		logging.Warnf("angryduck-worker[%s]: spread: holding %s: %v (fetching it instead)", s.nodeID, dgst, err)
		return false
	}
	return held
}

// pruneJobsLocked forgets jobs idle for longer than their lease lives:
// their lease has expired, so there is nothing left to hold or drop.
func (s *Spread) pruneJobsLocked(now time.Time) {
	for id, j := range s.jobs {
		if now.Sub(j.touched) > spreadLeaseTTL && !s.busyLocked(id) {
			delete(s.jobs, id)
		}
	}
	s.forgetPartialsLocked("", now)
	s.pruneUnpacksLocked(now)
}

func (s *Spread) busyLocked(job string) bool {
	for _, x := range s.xfers {
		if x.job == job {
			return true
		}
	}
	return false
}

// lease returns the lease holding job's blobs here, creating it, or moving
// its blobs to a fresh one when half its life is gone.
func (s *Spread) lease(ctx context.Context, job string) (string, error) {
	s.mu.Lock()
	j := s.jobs[job]
	if j == nil {
		j = &spreadJob{started: time.Now()}
		s.jobs[job] = j
	}
	j.touched = time.Now()
	s.mu.Unlock()

	j.mu.Lock()
	defer j.mu.Unlock()
	switch {
	case j.lease == "":
		name := "angryduck-spread-" + job + "-" + s.epoch + "-0"
		if err := s.store.CreateLease(ctx, name, spreadLeaseTTL); err != nil {
			return "", fmt.Errorf("creating lease: %w", err)
		}
		j.lease, j.leasedAt = name, time.Now()
	case time.Since(j.leasedAt) > spreadLeaseTTL/2:
		name := "angryduck-spread-" + job + "-" + s.epoch + "-" + strconv.Itoa(j.gen+1)
		if err := s.store.MoveLease(ctx, j.lease, name, spreadLeaseTTL); err != nil {
			return "", fmt.Errorf("renewing lease: %w", err)
		}
		j.gen++
		j.lease, j.leasedAt = name, time.Now()
	}
	return j.lease, nil
}

// run does one transfer, then forgets it and reports.
func (s *Spread) run(ctx context.Context, x *spreadXfer, o model.SpreadFetch) {
	defer s.wg.Done()
	defer memlimit.Soon() // its buffers are garbage now
	start := time.Now()
	w, upstream, err := s.fetch(ctx, x, o)
	n := w.Received
	close(x.stopped) // its ingest is free for a transfer taking it over
	x.cancel()
	took := time.Since(start)
	// Before the other path is cancelled below: streams then go on from
	// the committed blob.
	s.feedEnd(o.Digest, x.feed, err == nil)

	result := model.SpreadResultOK
	switch {
	case err == nil:
		if w.Resumed == 0 && !w.Present {
			observeBlobFetch(o.Path, o.Size, took)
		}
		// Won: stop the other path if it is fetching this same blob.
		s.mu.Lock()
		if n >= recvRateMinBytes && took > 0 {
			if r := float64(n) / took.Seconds(); r > s.peakRecv {
				s.peakRecv = r
			}
		}
		if other := s.xfers[xferKey{o.Digest, otherPath(o.Path)}]; other != nil {
			other.stop(stopOtherPath)
		}
		// Nothing left to continue: the blob is here. Bytes another
		// transfer left for it, which this one didn't go on from, go.
		if p, ok := s.partials[o.Digest]; ok {
			delete(s.partials, o.Digest)
			spreadDiscardedBytes.Add(p.bytes, s.nodeID, "unused")
		}
		s.mu.Unlock()
		spreadResumedBytes.Add(w.Resumed, s.nodeID, o.Path)
	case x.cancelled.Load():
		result = model.SpreadResultCancelled
	default:
		result = model.SpreadResultFailed
	}
	s.settle(x, o, w, err)

	s.mu.Lock()
	if key := (xferKey{o.Digest, o.Path}); s.xfers[key] == x {
		delete(s.xfers, key)
	}
	s.activeLocked(o.Path)
	s.mu.Unlock()
	spreadTransfersTotal.Inc(s.nodeID, o.Path, result)
	spreadBytesTotal.Add(n, s.nodeID, o.Path)
	spreadMillisTotal.Add(took.Milliseconds(), s.nodeID, o.Path)

	done := model.SpreadDone{Node: s.nodeID, Job: o.Job, Digest: o.Digest, Path: o.Path, Result: result, Bytes: n, Seconds: took.Seconds(), ID: o.ID}
	from := "registry"
	if o.Peer != nil {
		from = o.Peer.NodeID
	}
	switch result {
	case model.SpreadResultOK:
		s.landed(o.Job, o)
		if w.Resumed > 0 {
			logging.Infof("angryduck-worker[%s]: spread: got %s (%d bytes, %d of them kept from before) from %s in %s", s.nodeID, o.Digest, n+w.Resumed, w.Resumed, from, took.Round(time.Millisecond))
		} else {
			logging.Infof("angryduck-worker[%s]: spread: got %s (%d bytes) from %s in %s", s.nodeID, o.Digest, n, from, took.Round(time.Millisecond))
		}
	case model.SpreadResultCancelled:
		logging.Debugf("angryduck-worker[%s]: spread: stopped fetching %s from %s after %d bytes: %s", s.nodeID, o.Digest, from, n, stopReason(x.why.Load()))
	default:
		done.Error = err.Error()
		done.Upstream = upstream
		if upstream {
			logging.Infof("angryduck-worker[%s]: spread: streaming %s from %s ended after %d bytes: %v (the source's own transfer stopped)", s.nodeID, o.Digest, from, n, err)
		} else {
			logging.Warnf("angryduck-worker[%s]: spread: fetching %s from %s failed after %d bytes: %v", s.nodeID, o.Digest, from, n, err)
		}
	}
	s.report(done)
}

// fetch streams the blob from its source into the content store, going on
// from what an earlier transfer of it left here when it can. upstream is
// set when a stream from a peer still receiving the blob failed on the
// peer's side (see SpreadDone.Upstream).
func (s *Spread) fetch(ctx context.Context, x *spreadXfer, o model.SpreadFetch) (w WriteResult, upstream bool, err error) {
	if x.after != nil {
		// Taking over an ingest: write only once its last writer stopped
		// (it was stopped for this one, so that is soon). Even when this
		// one is stopped meanwhile: a transfer taking over from it in turn
		// waits on this one alone.
		<-x.after.stopped
		if err := ctx.Err(); err != nil {
			return w, false, err
		}
	}
	lease, err := s.lease(ctx, o.Job)
	if err != nil {
		return w, false, err
	}
	var tracker *readTracker
	var openErr error // the source couldn't be opened
	open := func(off int64) (io.ReadCloser, int64, error) {
		opening := time.Now()
		body, start, length, err := s.openFrom(ctx, o, off)
		if err != nil {
			openErr = err
			return nil, 0, err
		}
		observeBlobOpen(o.Path, opening)
		if length >= 0 && length != o.Size-start {
			body.Close()
			return nil, 0, fmt.Errorf("source announced %d bytes from byte %d, want %d", length, start, o.Size-start)
		}
		x.progress.resumedAt(start)
		var src io.Reader = &progressReader{r: body, p: &x.progress}
		if o.Stream {
			tracker = &readTracker{r: src}
			src = tracker
		}
		return readCloser2{src, body}, start, nil
	}
	if x.feed != nil {
		ctx = withWriteObserver(ctx, feedObserver{s: s, digest: o.Digest, f: x.feed})
	}
	if s.resumer != nil {
		w, err = s.resumer.WriteBlobFrom(ctx, lease, x.ref, o.Digest, o.Size, open, x.keep)
	} else {
		var body io.ReadCloser
		if body, _, err = open(0); err == nil {
			w.Received, err = s.store.WriteBlob(ctx, lease, x.ref, o.Digest, o.Size, body)
			body.Close()
		}
	}
	switch {
	case err != nil && openErr != nil:
		upstream = o.Stream && ctx.Err() == nil
	case err != nil && tracker != nil && tracker.failed() && !x.cancelled.Load():
		upstream = true
	}
	return w, upstream, err
}

// readCloser2 reads from r and closes c.
type readCloser2 struct {
	io.Reader
	c io.Closer
}

func (r readCloser2) Close() error { return r.c.Close() }

// openFrom opens o's source from byte off: start is where the body really
// starts (off, or 0 from a source that can only send it whole), length the
// body's announced size or -1.
func (s *Spread) openFrom(ctx context.Context, o model.SpreadFetch, off int64) (body io.ReadCloser, start, length int64, err error) {
	if o.Path == model.SpreadPathRegistry {
		if ro, ok := s.registry.(RangeOpener); ok && off > 0 {
			return ro.OpenBlobFrom(ctx, imageref.Normalize(o.Image), o.Digest, off)
		}
		body, length, err = s.registry.OpenBlob(ctx, imageref.Normalize(o.Image), o.Digest)
		return body, 0, length, err
	}
	return s.openPeer(ctx, o.Peer.Address, o.Digest, off, o.Size)
}

// openPeer asks a peer for blob dgst of size bytes from byte off (a Range
// request when off > 0). Peers before 1.15.0 send it whole: start is 0.
func (s *Spread) openPeer(ctx context.Context, addr, dgst string, off, size int64) (body io.ReadCloser, start, length int64, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/spread/content/"+dgst, nil)
	if err != nil {
		return nil, 0, 0, err
	}
	sharedtoken.Set(req, s.token)
	if off > 0 {
		req.Header.Set("Range", "bytes="+strconv.FormatInt(off, 10)+"-")
	}
	resp, err := s.peers.Do(req)
	if err != nil {
		return nil, 0, 0, err
	}
	switch {
	case resp.StatusCode == http.StatusOK:
		return resp.Body, 0, resp.ContentLength, nil
	case resp.StatusCode == http.StatusPartialContent && off > 0:
		if got, ok := registryclient.RangeStart(resp.Header.Get("Content-Range")); ok && got == off {
			return resp.Body, off, resp.ContentLength, nil
		}
		resp.Body.Close()
		return nil, 0, 0, fmt.Errorf("peer %s: asked for %s from byte %d, got range %q", addr, dgst, off, resp.Header.Get("Content-Range"))
	}
	msg := readError(resp)
	resp.Body.Close()
	return nil, 0, 0, fmt.Errorf("peer %s: %s", addr, msg)
}

// report tells the controller a transfer ended. A lost report only costs
// time: the controller also learns of the blob from this node's next layer
// inventory, and frees a silent slot after its transfer timeout.
func (s *Spread) report(done model.SpreadDone) {
	if s.controllerURL == "" {
		return
	}
	body, _ := json.Marshal(done)
	for attempt, wait := 0, time.Second; attempt < 3; attempt, wait = attempt+1, wait*2 {
		req, err := http.NewRequest(http.MethodPost, s.controllerURL+"/spread/done", bytes.NewReader(body))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		sharedtoken.Set(req, s.token)
		resp, err := s.ctl.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode < 500 {
				return
			}
		}
		time.Sleep(wait)
	}
	logging.Warnf("angryduck-worker[%s]: spread: couldn't report %s of %s to the controller", s.nodeID, done.Result, done.Digest)
}

// HandleContent serves one blob by digest to a peer: committed, or still
// arriving here (streamed, see spreadstream.go).
func (s *Spread) HandleContent(w http.ResponseWriter, r *http.Request) {
	dgst := strings.TrimPrefix(r.URL.Path, "/spread/content/")
	if !validDigest(dgst) || (r.Method != http.MethodGet && r.Method != http.MethodHead) {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if !s.have(r.Context(), dgst) {
		if !s.serveStream(w, r, dgst) {
			http.NotFound(w, r)
		}
		return
	}
	body, size, err := s.store.OpenContent(r.Context(), dgst)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer body.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Docker-Content-Digest", dgst)
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		return
	}
	// A receiver going on from what it has: the rest only.
	if from := rangeFrom(r, size); from > 0 {
		if err := skipTo(body, from); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		partialHeaders(w, from, size)
		n, err := copyToWriter(w, &io.LimitedReader{R: body, N: size - from})
		spreadBytesTotal.Add(n, s.nodeID, "served")
		if err == nil && n < size-from {
			err = io.ErrUnexpectedEOF
		}
		if err != nil {
			logging.Warnf("angryduck-worker[%s]: spread: serving %s from byte %d to %s failed after %d bytes: %v", s.nodeID, dgst, from, r.RemoteAddr, n, err)
			panic(http.ErrAbortHandler)
		}
		return
	}
	n, err := serveBody(w, body, size)
	spreadBytesTotal.Add(n, s.nodeID, "served")
	if err != nil {
		logging.Warnf("angryduck-worker[%s]: spread: serving %s to %s failed after %d bytes: %v", s.nodeID, dgst, r.RemoteAddr, n, err)
		// Cut the connection: with Content-Length promised, the receiver
		// sees a short read rather than an error page.
		panic(http.ErrAbortHandler)
	}
}

// HandleDrop cancels a job's transfers here and releases its blobs.
func (s *Spread) HandleDrop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var d model.SpreadDrop
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<10)).Decode(&d); err != nil || !validJobID(d.Job) {
		http.Error(w, "invalid drop", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	for _, x := range s.xfers {
		if x.job == d.Job {
			x.stop(stopDropped)
		}
	}
	s.mu.Unlock()
	s.unpackStop(r.Context(), d.Job, true)
	s.release(r.Context(), d.Job)
	w.WriteHeader(http.StatusNoContent)
}

// release deletes job's lease and forgets the job.
func (s *Spread) release(ctx context.Context, job string) {
	s.mu.Lock()
	j := s.jobs[job]
	if !s.busyLocked(job) {
		delete(s.jobs, job)
	}
	s.forgetPartialsLocked(job, time.Now())
	quiet := len(s.jobs) == 0 && len(s.xfers) == 0
	s.mu.Unlock()
	if quiet {
		// The last job here ended: its connections to peers are idle now
		// (see peerconns.go). One in use (a transfer of a newer job that
		// started meanwhile) is left alone.
		s.peers.CloseIdleConnections()
	}
	if j == nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.lease == "" {
		return
	}
	if err := s.store.DeleteLease(ctx, j.lease); err != nil {
		logging.Warnf("angryduck-worker[%s]: spread: releasing lease %s: %v (it expires on its own)", s.nodeID, j.lease, err)
		return
	}
	j.lease = ""
}

// HandleFinalize registers the image once every layer is here.
func (s *Spread) HandleFinalize(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var f model.SpreadFinalize
	// Metadata travels base64-encoded: allow for that on top of its cap.
	if err := json.NewDecoder(io.LimitReader(r.Body, 2*spreadMaxMetadata)).Decode(&f); err != nil {
		http.Error(w, "invalid finalize", http.StatusBadRequest)
		return
	}
	if err := checkFinalize(f); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// One finalize per image at a time (a retry while the first still
	// runs waits for it); different images don't wait on each other.
	// Unpacking is bounded by the CPU and IO given to containerd, not here.
	image := imageref.Normalize(f.Image)
	queued := time.Now()
	unlock, err := s.finalizing.lock(r.Context(), image)
	if err != nil {
		return
	}
	defer unlock()
	waited := time.Since(queued)
	s.finalizingAdd(1)
	defer s.finalizingAdd(-1)
	// Not cut short by the controller giving up: an unpack halfway is
	// wasted work, and the next report shows the controller the image.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 15*time.Minute)
	defer cancel()
	start := time.Now()
	// Layers still being unpacked as they arrived are finished first:
	// the import then finds them unpacked and skips them.
	drained := s.unpackStop(ctx, f.Job, false)
	drainTook := time.Since(start)
	held, err := s.finalize(ctx, f, image)
	if err != nil {
		spreadFinalizesTotal.Inc(s.nodeID, "failure")
		logging.Warnf("angryduck-worker[%s]: spread: registering image=%s failed: %v", s.nodeID, image, err)
		// The controller planned this finalize from an inventory that
		// turned out wrong: drop cached listings and report again now, so
		// its retry fetches what is missing instead of finalizing again.
		if inv, ok := s.store.(interface{ Invalidate() }); ok {
			inv.Invalidate()
		}
		if s.onStale != nil {
			s.onStale()
		}
		writeJSON(w, http.StatusBadGateway, model.SpreadFinalizeResult{Error: err.Error()})
		return
	}
	if drained {
		spreadFinalizeSeconds.Observe(drainTook.Seconds(), hnode(), "drain")
	}
	spreadFinalizeSeconds.Observe(held.Seconds(), hnode(), "hold")
	s.mu.Lock()
	var began time.Time
	if j := s.jobs[f.Job]; j != nil {
		began = j.started
	}
	s.mu.Unlock()
	if !began.IsZero() {
		observeSince(spreadReadySeconds, began)
	} else {
		logging.Debugf("angryduck-worker[%s]: spread: job %s for image=%s was forgotten before it finished: its time to ready is not recorded", s.nodeID, f.Job, image)
	}
	s.release(ctx, f.Job) // the image references its blobs now
	spreadFinalizesTotal.Inc(s.nodeID, "success")
	if waited >= 100*time.Millisecond {
		logging.Infof("angryduck-worker[%s]: spread: image=%s registered in %s (waited %s for another finalize of it)", s.nodeID, image, time.Since(start).Round(time.Millisecond), waited.Round(time.Millisecond))
	} else {
		logging.Infof("angryduck-worker[%s]: spread: image=%s registered in %s", s.nodeID, image, time.Since(start).Round(time.Millisecond))
	}
	if s.onFinalize != nil {
		s.onFinalize()
	}
	writeJSON(w, http.StatusOK, model.SpreadFinalizeResult{OK: true})
}

// finalizingAdd moves the count of finalizes running here.
func (s *Spread) finalizingAdd(d int) {
	s.mu.Lock()
	s.running += d
	n := s.running
	s.mu.Unlock()
	spreadFinalizing.Set(float64(n), s.nodeID)
}

// checkFinalize validates a finalize order: every metadata blob must match
// its digest, and the top one must be among them.
func checkFinalize(f model.SpreadFinalize) error {
	if !validJobID(f.Job) || f.Image == "" || !validDigest(f.TopDig) || f.TopType == "" {
		return errors.New("invalid finalize")
	}
	var total int
	for d, b := range f.Metadata {
		total += len(b)
		sum := sha256.Sum256(b)
		if d != "sha256:"+hex.EncodeToString(sum[:]) {
			return fmt.Errorf("metadata blob %s doesn't match its digest", d)
		}
	}
	if total > spreadMaxMetadata {
		return errors.New("metadata too large")
	}
	if top, ok := f.Metadata[f.TopDig]; !ok || int64(len(top)) != f.TopSize {
		return errors.New("top blob missing from metadata")
	}
	return nil
}

// finalize imports an archive of just the metadata, naming the image: the
// layers are already in the content store (or unpacked), so containerd
// registers the name and unpacks what isn't unpacked yet.
//
// The controller sends it once its view says every layer is here, a view
// up to a layer scan old. So first every layer is held under the job's
// lease and checked: the highest one already unpacked (its snapshot keeps
// those below it), and the blob of each one above that. A layer that is
// gone fails the finalize before anything is imported; the import itself
// only publishes the name once the image is unpacked, so even a layer
// vanishing after this check can't leave a broken image behind.
//
// held is how long the check and pinning took (spread_finalize_seconds
// {phase="hold"}, recorded by the caller once the whole finalize worked).
func (s *Spread) finalize(ctx context.Context, f model.SpreadFinalize, image string) (held time.Duration, err error) {
	top := blobship.Descriptor{MediaType: f.TopType, Digest: f.TopDig, Size: f.TopSize}
	holding := time.Now()
	if err := s.holdLayers(ctx, f, top); err != nil {
		return 0, err
	}
	held = time.Since(holding)
	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(func() error {
			aw, err := blobship.NewArchiveWriter(pw, top, image)
			if err != nil {
				return err
			}
			for _, d := range sortedKeys(f.Metadata) {
				if err := aw.AddBytes(d, f.Metadata[d]); err != nil {
					return err
				}
			}
			return aw.Close()
		}())
	}()
	err = s.store.Import(withImportCaller(ctx, importBySpread), pr, s.platform)
	pr.CloseWithError(errImportEnded)
	if err != nil {
		return held, err
	}
	if _, got, err := s.store.Resolve(ctx, image); err != nil || got != f.TopDig {
		return held, fmt.Errorf("after import, %s resolves to %q (err %v), want %s", image, got, err, f.TopDig)
	}
	return held, nil
}

// holdLayers is finalize's check (see there). When the layers can't be
// worked out from the metadata, it holds nothing and leaves it to the
// import, which then fails cleanly if something is missing.
func (s *Spread) holdLayers(ctx context.Context, f model.SpreadFinalize, top blobship.Descriptor) error {
	layers, err := blobship.LayersFromMetadata(top, f.Metadata, s.platform)
	if err != nil || len(layers) == 0 {
		if err != nil {
			logging.Debugf("angryduck-worker[%s]: spread: image=%s: layers not checked before finalize: %v", s.nodeID, f.Image, err)
		}
		return nil
	}
	lease, err := s.lease(ctx, f.Job)
	if err != nil {
		return err
	}
	unpacked := -1
	for i := len(layers) - 1; i >= 0; i-- {
		ok, err := s.store.HoldSnapshot(ctx, lease, layers[i].ChainID)
		if err != nil {
			return fmt.Errorf("holding snapshot of layer %d: %w", i, err)
		}
		if ok {
			unpacked = i
			break
		}
	}
	var missing []string
	for i := unpacked + 1; i < len(layers); i++ {
		ok, err := s.store.HoldContent(ctx, lease, layers[i].Blob.Digest)
		if err != nil {
			return fmt.Errorf("holding blob of layer %d: %w", i, err)
		}
		if !ok {
			missing = append(missing, layers[i].Blob.Digest)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("%d layer(s) counted as here are gone (removed with another image meanwhile): %s; not registering the image",
			len(missing), strings.Join(missing, ", "))
	}
	return nil
}
