package worker

import (
	"context"
	"errors"
	"time"

	"angryduck/internal/logging"
	"angryduck/internal/metrics"
	"angryduck/internal/model"
)

var spreadUnpacksTotal = metrics.NewCounterVec(
	"angryduck_worker_spread_unpacks_total",
	"Layers unpacked as they arrived, before the image was registered, by result: \"applied\", \"present\" (already unpacked) or \"failed\" (left to the image's own unpack).",
	"node", "result",
)

// LayerApplier unpacks a layer blob onto the layer below it. ContainerdStore
// has it.
type LayerApplier interface {
	ApplyLayer(ctx context.Context, lease string, l LayerSpec) (applied bool, err error)
}

// unpackTimeout bounds one layer's unpack, as finalize bounds a whole image.
const unpackTimeout = 15 * time.Minute

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

	pending map[string]LayerSpec // landed, not unpacked yet, by chainID
	ready   map[string]bool      // chainIDs unpacked here (by us, or already)
	waiting map[string]bool      // tried once, the layer below was missing

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

// landed queues the layer of a blob that is here now for unpacking.
func (s *Spread) landed(job string, o model.SpreadFetch) {
	u := o.Unpack
	if s.applier == nil || u == nil || !validDigest(u.ChainID) || u.Parent != "" && !validDigest(u.Parent) ||
		u.MediaType == "" || len(u.MediaType) > 255 {
		return
	}
	l := LayerSpec{Digest: o.Digest, Size: o.Size, MediaType: u.MediaType, ChainID: u.ChainID, Parent: u.Parent}
	s.mu.Lock()
	j := s.unpacks[job]
	if j == nil {
		ctx, cancel := context.WithCancel(context.Background())
		j = &unpackJob{ctx: ctx, cancel: cancel, pending: map[string]LayerSpec{}, ready: map[string]bool{}, waiting: map[string]bool{}}
		s.unpacks[job] = j
	}
	if j.stopped || j.ready[l.ChainID] {
		s.mu.Unlock()
		return
	}
	j.pending[l.ChainID] = l
	if j.running {
		j.again = true
		s.mu.Unlock()
		return
	}
	j.running, j.idle = true, make(chan struct{})
	s.mu.Unlock()
	s.wg.Add(1)
	go s.unpackRun(job, j)
}

// unpackRun unpacks what can be unpacked, until nothing more can.
func (s *Spread) unpackRun(job string, j *unpackJob) {
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
		var todo []LayerSpec
		for _, l := range j.pending {
			if l.Parent == "" || j.ready[l.Parent] || !j.waiting[l.ChainID] {
				todo = append(todo, l)
			}
		}
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
			ctx, cancel := context.WithTimeout(j.ctx, unpackTimeout)
			applied, err := s.applier.ApplyLayer(ctx, lease, l)
			cancel()
			s.mu.Lock()
			switch {
			case err == nil:
				delete(j.pending, l.ChainID)
				delete(j.waiting, l.ChainID)
				j.ready[l.ChainID] = true
				progress = true
			case errors.Is(err, errParentMissing):
				j.waiting[l.ChainID] = true
			default:
				delete(j.pending, l.ChainID)
			}
			s.mu.Unlock()
			switch {
			case err == nil && applied:
				spreadUnpacksTotal.Inc(s.nodeID, "applied")
				logging.Debugf("angryduck-worker[%s]: spread: unpacked %s (%d bytes) in %s", s.nodeID, l.Digest, l.Size, time.Since(start).Round(time.Millisecond))
			case err == nil:
				spreadUnpacksTotal.Inc(s.nodeID, "present")
			case errors.Is(err, errParentMissing):
			case j.ctx.Err() != nil:
				return // dropped
			default:
				spreadUnpacksTotal.Inc(s.nodeID, "failed")
				logging.Warnf("angryduck-worker[%s]: spread: unpacking %s early failed: %v (the image unpacks it at finalize)", s.nodeID, l.Digest, err)
			}
		}
	}
}

// unpackStop ends a job's early unpacking. Finalize waits for a layer
// being unpacked right now (it is useful, and containerd's unpack would
// otherwise unpack it again side by side); a drop cancels it.
func (s *Spread) unpackStop(ctx context.Context, job string, cancel bool) {
	s.mu.Lock()
	j := s.unpacks[job]
	if j == nil {
		s.mu.Unlock()
		return
	}
	// Kept, stopped, so a blob landing late doesn't start the job over;
	// forgotten by pruneUnpacksLocked.
	j.stopped, j.ended = true, time.Now()
	idle := j.idle
	running := j.running
	s.mu.Unlock()
	if cancel || !running {
		j.cancel()
		return
	}
	select {
	case <-idle:
	case <-ctx.Done():
	}
	j.cancel()
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
