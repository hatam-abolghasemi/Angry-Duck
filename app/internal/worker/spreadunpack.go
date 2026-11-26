package worker

import (
	"context"
	"errors"
	"io"
	"sort"
	"time"

	"angryduck/internal/logging"
	"angryduck/internal/memlimit"
	"angryduck/internal/metrics"
	"angryduck/internal/model"
)

var spreadUnpacksTotal = metrics.NewCounterVec(
	"angryduck_worker_spread_unpacks_total",
	"Layers unpacked as they arrived, before the image was registered, by result: \"applied\" (once the blob landed), \"streamed\" (while the blob was still arriving), \"present\" (already unpacked), \"stream_failed\" (unpacking while arriving failed: unpacked once it landed instead) or \"failed\" (left to the image's own unpack).",
	"node", "result",
)

// LayerApplier unpacks a layer blob onto the layer below it. ContainerdStore
// has it.
type LayerApplier interface {
	ApplyLayer(ctx context.Context, lease string, l LayerSpec) (applied bool, err error)
}

// LayerStreamApplier unpacks a layer from its blob's bytes as they arrive
// (see streamunpack.go). ContainerdStore has it.
type LayerStreamApplier interface {
	ApplyLayerStream(ctx context.Context, lease string, l LayerSpec, r io.Reader) (applied bool, err error)
}

// unpackTimeout bounds one layer's unpack, as finalize bounds a whole image.
const unpackTimeout = 15 * time.Minute

// errStreamUnpackDone ends the reading side of a stream unpack.
var errStreamUnpackDone = errors.New("stream unpack ended")

// pendingLayer is a layer waiting to be unpacked: landed, or (stream
// unpacking) still arriving.
type pendingLayer struct {
	LayerSpec
	arriving bool
}

// unpackJob is one spread job's unpacking on this node. Layers are
// unpacked as their blobs land, bottom up: a layer whose layer below isn't
// unpacked yet waits for it. One goroutine per job runs while there is
// something to unpack; none while there isn't.
//
// Layers this node never fetched for the job (it held their blob before)
// aren't known here and stop the chain at that point; the image's own
// unpack at finalize does the rest, as it always did.
type unpackJob struct {
	ctx    context.Context
	cancel context.CancelFunc

	pending  map[string]pendingLayer // not unpacked yet, by chainID
	ready    map[string]bool         // chainIDs unpacked here (by us, or already)
	waiting  map[string]bool         // tried once, the layer below was missing
	noStream map[string]bool         // unpacking while arriving failed: wait for the blob

	// Parallel unpacking (see parallelunpack.go and parallelKickLocked):
	// layers being worked on (unpacked or streamed), unpacked but not
	// committed yet, tried for a commit since their layer below was last
	// unpacked here, the order they were queued in, and the goroutines
	// running for the job.
	busy        map[string]bool
	extracted   map[string]extractedLayer
	commitTried map[string]bool
	seq         map[string]int
	nextSeq     int
	workers     int
	streaming   bool // a layer is being unpacked while it arrives
	committing  bool

	running bool
	again   bool          // a layer landed during a run
	idle    chan struct{} // closed when the run ends
	stopped bool          // finalize or drop: start nothing more
	ended   time.Time     // when it stopped; forgotten a lease's life later
}

// EnableUnpack unpacks layers as they arrive (SPREAD_UNPACK_ENABLED).
func (s *Spread) EnableUnpack(a LayerApplier) {
	for _, r := range []string{"applied", "present", "failed"} {
		spreadUnpacksTotal.Add(0, s.nodeID, r)
	}
	s.applier = a
}

// EnableStreamUnpack unpacks layers while their blobs still arrive
// (SPREAD_UNPACK_STREAM_ENABLED). It needs unpacking on arrival on and the
// blobs still arriving readable here (the content store mounted, streaming
// on); false when they aren't.
func (s *Spread) EnableStreamUnpack(a LayerStreamApplier) bool {
	if s.applier == nil || s.ingest == nil {
		return false
	}
	for _, r := range []string{"streamed", "stream_failed"} {
		spreadUnpacksTotal.Add(0, s.nodeID, r)
	}
	s.streamApplier = a
	if x, ok := a.(LayerStreamExtractor); ok {
		s.streamExtractor = x
		if s.extractMax < 1 {
			s.extractMax = 2
		}
	}
	return true
}

// SetUnpackSlots sets how many layers this node unpacks at once across all
// spreads (SPREAD_UNPACK_PARALLEL_MAX), for parallel unpacking and for
// unpacking every layer while blobs arrive.
func (s *Spread) SetUnpackSlots(max int) {
	if max > 0 {
		s.mu.Lock()
		s.extractMax = max
		s.mu.Unlock()
	}
}

// layerOf is the layer a fetch order places, if it says.
func layerOf(o model.SpreadFetch) (LayerSpec, bool) {
	u := o.Unpack
	if u == nil || !validDigest(u.ChainID) || u.Parent != "" && !validDigest(u.Parent) ||
		u.MediaType == "" || len(u.MediaType) > 255 {
		return LayerSpec{}, false
	}
	return LayerSpec{Digest: o.Digest, Size: o.Size, MediaType: u.MediaType, ChainID: u.ChainID, Parent: u.Parent}, true
}

// arrives queues the layer of a blob whose transfer just started, for
// unpacking while it arrives (stream unpacking only).
func (s *Spread) arrives(job string, o model.SpreadFetch) {
	if s.streamApplier == nil {
		return
	}
	if l, ok := layerOf(o); ok && streamUnpackable(l.MediaType) {
		s.queueUnpack(job, l, true)
	}
}

// streamUnpackable: layers unpacked while they arrive are gzip or plain
// tar. A zstd layer's decoder holds the frame's whole window (8 MiB with
// common settings), about 13 MiB more for the worker while it runs: those
// are unpacked once they land, through containerd, as before.
func streamUnpackable(mediaType string) bool {
	switch mediaType {
	case "application/vnd.oci.image.layer.v1.tar",
		"application/vnd.oci.image.layer.v1.tar+gzip",
		"application/vnd.docker.image.rootfs.diff.tar.gzip",
		"application/vnd.docker.image.rootfs.diff.tar":
		return true
	}
	return false
}

// landed queues the layer of a blob that is here now for unpacking.
func (s *Spread) landed(job string, o model.SpreadFetch) {
	if s.applier == nil {
		return
	}
	if l, ok := layerOf(o); ok {
		s.queueUnpack(job, l, false)
	}
}

// queueUnpack adds a layer to job's unpacking and starts it if idle.
func (s *Spread) queueUnpack(job string, l LayerSpec, arriving bool) {
	s.mu.Lock()
	j := s.unpacks[job]
	if j == nil {
		ctx, cancel := context.WithCancel(context.Background())
		j = &unpackJob{ctx: ctx, cancel: cancel, pending: map[string]pendingLayer{}, ready: map[string]bool{},
			waiting: map[string]bool{}, noStream: map[string]bool{}, busy: map[string]bool{},
			extracted: map[string]extractedLayer{}, commitTried: map[string]bool{}, seq: map[string]int{}}
		s.unpacks[job] = j
	}
	if j.stopped || j.ready[l.ChainID] {
		s.mu.Unlock()
		return
	}
	if _, ok := j.extracted[l.ChainID]; ok {
		s.mu.Unlock() // unpacked on its own, waiting to be committed
		return
	}
	if p, ok := j.pending[l.ChainID]; ok && arriving && !p.arriving {
		s.mu.Unlock() // landed already: a later transfer of it changes nothing
		return
	}
	j.pending[l.ChainID] = pendingLayer{LayerSpec: l, arriving: arriving}
	if _, ok := j.seq[l.ChainID]; !ok {
		j.seq[l.ChainID] = j.nextSeq
		j.nextSeq++
	}
	if s.extractor != nil || s.streamExtractor != nil {
		s.parallelKickLocked(job, j)
		s.mu.Unlock()
		return
	}
	if arriving && j.noStream[l.ChainID] {
		s.mu.Unlock() // waits for its blob
		return
	}
	if j.running {
		j.again = true
		s.mu.Unlock()
		return
	}
	j.running, j.idle = true, make(chan struct{})
	s.mu.Unlock()
	s.wg.Add(1)
	working := memlimit.Working()
	go func() {
		defer working()
		s.unpackRun(job, j)
	}()
}

// afterUnpack ends every unpack goroutine: with stream unpack on, the worker
// wrote the layer's files itself, and their page cache and inode records
// are charged to it until handed back (memlimit.ReclaimSoon).
func (s *Spread) afterUnpack() {
	if s.streamApplier != nil {
		memlimit.ReclaimSoon()
	}
}

// unpackRun unpacks what can be unpacked, until nothing more can.
func (s *Spread) unpackRun(job string, j *unpackJob) {
	defer s.afterUnpack()
	defer s.wg.Done()
	for {
		s.unpackPass(job, j)
		s.mu.Lock()
		if !j.again || j.stopped {
			j.running = false
			close(j.idle)
			s.mu.Unlock()
			return
		}
		j.again = false
		s.mu.Unlock()
	}
}

// unpackPass tries every pending layer whose layer below is unpacked, or
// that hasn't been tried yet, repeating while that unpacks something.
func (s *Spread) unpackPass(job string, j *unpackJob) {
	for progress := true; progress; {
		progress = false
		s.mu.Lock()
		if j.stopped {
			s.mu.Unlock()
			return
		}
		var todo []pendingLayer
		for _, l := range j.pending {
			if l.arriving && (s.streamApplier == nil || j.noStream[l.ChainID]) {
				continue // waits for its blob
			}
			if l.Parent == "" || j.ready[l.Parent] || !j.waiting[l.ChainID] {
				todo = append(todo, l)
			}
		}
		// Landed layers first: they don't wait on any transfer.
		sort.SliceStable(todo, func(a, b int) bool { return !todo[a].arriving && todo[b].arriving })
		s.mu.Unlock()
		if len(todo) == 0 {
			return
		}
		lease, err := s.lease(j.ctx, job)
		if err != nil {
			logging.Warnf("angryduck-worker[%s]: spread: not unpacking layers early: %v (the image unpacks them at finalize)", s.nodeID, err)
			return
		}
		for _, l := range todo {
			if j.ctx.Err() != nil {
				return
			}
			start := time.Now()
			var applied bool
			var err error
			if l.arriving {
				applied, err = s.unpackStreaming(j.ctx, lease, l.LayerSpec)
			} else {
				ctx, cancel := context.WithTimeout(j.ctx, unpackTimeout)
				applied, err = s.applier.ApplyLayer(ctx, lease, l.LayerSpec)
				cancel()
			}
			streamFailed := false
			s.mu.Lock()
			switch {
			case err == nil:
				delete(j.pending, l.ChainID)
				delete(j.waiting, l.ChainID)
				j.ready[l.ChainID] = true
				progress = true
			case errors.Is(err, errParentMissing):
				j.waiting[l.ChainID] = true
			case l.arriving && j.ctx.Err() == nil:
				// Unpacked once its blob lands instead, through containerd.
				// If it landed meanwhile, landed() asked for another pass.
				j.noStream[l.ChainID] = true
				streamFailed = true
			default:
				delete(j.pending, l.ChainID)
			}
			s.mu.Unlock()
			switch {
			case err == nil && applied && l.arriving:
				spreadUnpacksTotal.Inc(s.nodeID, "streamed")
				logging.Debugf("angryduck-worker[%s]: spread: unpacked %s (%d bytes) while it arrived, done %s after starting", s.nodeID, l.Digest, l.Size, time.Since(start).Round(time.Millisecond))
			case err == nil && applied:
				spreadUnpacksTotal.Inc(s.nodeID, "applied")
				logging.Debugf("angryduck-worker[%s]: spread: unpacked %s (%d bytes) in %s", s.nodeID, l.Digest, l.Size, time.Since(start).Round(time.Millisecond))
			case err == nil:
				spreadUnpacksTotal.Inc(s.nodeID, "present")
			case errors.Is(err, errParentMissing):
			case j.ctx.Err() != nil:
				return // dropped
			case streamFailed:
				spreadUnpacksTotal.Inc(s.nodeID, "stream_failed")
				logging.Warnf("angryduck-worker[%s]: spread: unpacking %s while it arrived failed: %v (unpacking it once it lands instead)", s.nodeID, l.Digest, err)
			default:
				spreadUnpacksTotal.Inc(s.nodeID, "failed")
				logging.Warnf("angryduck-worker[%s]: spread: unpacking %s early failed: %v (the image unpacks it at finalize)", s.nodeID, l.Digest, err)
			}
		}
	}
}

// unpackStreaming unpacks l from its blob as it arrives: the bytes come
// from whatever transfer of it runs here, then the committed blob, as a
// peer streaming from this node gets them (see stream).
func (s *Spread) unpackStreaming(jctx context.Context, lease string, l LayerSpec) (applied bool, err error) {
	err = s.withBlobStream(jctx, l, func(ctx context.Context, r io.Reader) error {
		var aerr error
		applied, aerr = s.streamApplier.ApplyLayerStream(ctx, lease, l, r)
		return aerr
	})
	return applied, err
}

// withBlobStream runs fn on l's blob as it arrives here, then the committed
// blob, as a peer streaming from this node gets them (see stream).
func (s *Spread) withBlobStream(jctx context.Context, l LayerSpec, fn func(context.Context, io.Reader) error) error {
	ctx, cancel := context.WithTimeout(jctx, s.timeout+unpackTimeout)
	defer cancel()
	pr, pw := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := s.stream(ctx, pw, l.Digest, l.Size)
		pw.CloseWithError(err) // nil: the whole blob, then EOF
	}()
	err := fn(ctx, pr)
	pr.CloseWithError(errStreamUnpackDone)
	cancel()
	<-done
	return err
}

// unpackStop ends a job's early unpacking. Finalize waits for a layer
// being unpacked right now (it is useful, and containerd's unpack would
// otherwise unpack it again side by side); a drop cancels it. waited
// says whether it had to wait for one.
func (s *Spread) unpackStop(ctx context.Context, job string, cancel bool) (waited bool) {
	s.mu.Lock()
	j := s.unpacks[job]
	if j == nil {
		s.mu.Unlock()
		return false
	}
	// Kept, stopped, so a blob landing late doesn't start the job over;
	// forgotten by pruneUnpacksLocked.
	j.stopped, j.ended = true, time.Now()
	idle := j.idle
	running := j.running
	s.mu.Unlock()
	if cancel || !running {
		j.cancel()
		s.discardExtracted(j)
		return false
	}
	select {
	case <-idle:
	case <-ctx.Done():
	}
	j.cancel()
	s.discardExtracted(j)
	return true
}

// pruneUnpacksLocked forgets stopped jobs once no late transfer of theirs
// can still land, and running-less jobs whose spread job was forgotten.
func (s *Spread) pruneUnpacksLocked(now time.Time) {
	for id, j := range s.unpacks {
		switch {
		case j.running:
		case j.stopped && now.Sub(j.ended) > spreadLeaseTTL:
			delete(s.unpacks, id)
		case !j.stopped && s.jobs[id] == nil:
			j.cancel()
			delete(s.unpacks, id)
		}
	}
}
