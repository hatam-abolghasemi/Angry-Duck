package worker

import (
	"context"
	"errors"
	"io"
	"sort"
	"time"

	"angryduck/internal/logging"
	"angryduck/internal/memlimit"
)

// Every layer at once (stream unpacking, on any containerd): a layer's
// bytes start arriving and it starts unpacking, whatever the layers below
// are doing. One whose layer below is already unpacked is unpacked straight
// onto it; one whose layer below isn't yet is unpacked into a snapshot of
// its own (ExtractLayerStream) and attached onto the layer below once that
// one is, bottom up (CommitExtracted: renames, instant). Before this, a
// layer waited for the one below to finish, so of an image's layers all
// arriving at once only the lowest was unpacked while it arrived and the
// rest one after another once landed.
//
// Landed layers go to containerd's own unpack when the layer below is
// there (its files are then charged to containerd, not the worker); the
// worker unpacks a landed layer only when waiting for the layer below
// would leave it idle. At most SPREAD_UNPACK_PARALLEL_MAX layers unpack at
// once on the node, across spreads.

// LayerStreamExtractor unpacks a layer on its own while its blob arrives,
// and attaches or discards it (see attach.go). ContainerdStore has it.
type LayerStreamExtractor interface {
	ExtractLayerStream(ctx context.Context, lease string, l LayerSpec, r io.Reader) (ExtractedLayer, error)
	CommitExtracted(ctx context.Context, lease string, l LayerSpec, x ExtractedLayer) (bool, error)
	DiscardExtracted(ctx context.Context, lease string, x ExtractedLayer)
}

// committer is who commits or discards x: the worker's own extraction for
// a layer it unpacked, containerd's parallel unpack otherwise.
func (s *Spread) committer(x ExtractedLayer) interface {
	CommitExtracted(ctx context.Context, lease string, l LayerSpec, x ExtractedLayer) (bool, error)
	DiscardExtracted(ctx context.Context, lease string, x ExtractedLayer)
} {
	if x.ByWorker || s.extractor == nil {
		return s.streamExtractor
	}
	return s.extractor
}

// wholeKickLocked starts what job can do now, with every layer unpacking
// at once: layers arriving first (their bytes are coming now), lowest
// first, then landed ones, within the node's slots; and committing what
// was unpacked on its own. Caller holds s.mu.
func (s *Spread) wholeKickLocked(job string, j *unpackJob) {
	if j.stopped {
		return
	}
	var arriving, landed []pendingLayer
	for c, l := range j.pending {
		switch {
		case j.busy[c]:
		case l.arriving:
			if !j.noStream[c] {
				arriving = append(arriving, l)
			} // else: unpacked once it lands
		case j.waiting[c] && l.Parent != "" && !j.ready[l.Parent]:
			// Waits for the layer below (unpacking it here failed once).
		default:
			landed = append(landed, l)
		}
	}
	bySeq := func(ls []pendingLayer) {
		sort.Slice(ls, func(a, b int) bool { return j.seq[ls[a].ChainID] < j.seq[ls[b].ChainID] })
	}
	bySeq(arriving)
	bySeq(landed)
	for _, l := range append(arriving, landed...) {
		if s.extracting >= s.extractMax {
			break
		}
		s.extracting++
		j.busy[l.ChainID] = true
		s.workerStartLocked(j)
		working := memlimit.Working()
		go func() {
			defer working()
			if l.arriving {
				s.wholeStreamWorker(job, j, l.LayerSpec)
			} else {
				s.wholeLandedWorker(job, j, l.LayerSpec)
			}
		}()
	}
	if len(j.extracted) > 0 && !j.committing {
		j.committing = true
		s.workerStartLocked(j)
		working := memlimit.Working()
		go func() {
			defer working()
			s.commitWorker(job, j)
		}()
	}
}

// wholeStreamWorker unpacks a layer while its blob arrives: onto the layer
// below if that is here, else on its own, attached later.
func (s *Spread) wholeStreamWorker(job string, j *unpackJob, l LayerSpec) {
	defer s.afterUnpack()
	start := time.Now()
	lease, err := s.lease(j.ctx, job)
	applied := false
	var x ExtractedLayer
	if err == nil {
		applied, err = s.unpackStreaming(j.ctx, lease, l)
		if errors.Is(err, errParentMissing) {
			x, err = s.extractStreaming(j.ctx, lease, l)
		}
	}
	s.mu.Lock()
	s.extracting--
	delete(j.busy, l.ChainID)
	dropped := j.ctx.Err() != nil
	switch {
	case err == nil && x.Key != "" && !dropped:
		delete(j.pending, l.ChainID)
		j.extracted[l.ChainID] = extractedLayer{LayerSpec: l, x: x, result: "streamed"}
		logging.Debugf("angryduck-worker[%s]: spread: unpacked %s (%d bytes) on its own while it arrived in %s, attaching it once the layer below is", s.nodeID, l.Digest, l.Size, time.Since(start).Round(time.Millisecond))
	case err == nil && x.Key == "":
		delete(j.pending, l.ChainID)
		delete(j.waiting, l.ChainID)
		j.ready[l.ChainID] = true
		clear(j.commitTried) // what waited on it may commit now
		if applied {
			spreadUnpacksTotal.Inc(s.nodeID, "streamed")
			logging.Debugf("angryduck-worker[%s]: spread: unpacked %s (%d bytes) while it arrived, done %s after starting", s.nodeID, l.Digest, l.Size, time.Since(start).Round(time.Millisecond))
		} else {
			spreadUnpacksTotal.Inc(s.nodeID, "present")
		}
	case !dropped:
		// Unpacked once its blob lands instead (by containerd when the
		// layer below is there).
		j.noStream[l.ChainID] = true
		j.waiting[l.ChainID] = true
		spreadUnpacksTotal.Inc(s.nodeID, "stream_failed")
		logging.Warnf("angryduck-worker[%s]: spread: unpacking %s while it arrived failed: %v (unpacking it once it lands instead)", s.nodeID, l.Digest, err)
	}
	s.kickAllLocked()
	s.workerDoneLocked(j)
	s.mu.Unlock()
	if err == nil && x.Key != "" && dropped {
		s.streamExtractor.DiscardExtracted(context.Background(), lease, x)
	}
}

// wholeLandedWorker unpacks a landed layer: through containerd onto the
// layer below when that is here, else (rather than wait) by the worker on
// its own from the landed blob, attached later.
func (s *Spread) wholeLandedWorker(job string, j *unpackJob, l LayerSpec) {
	defer s.afterUnpack()
	start := time.Now()
	lease, err := s.lease(j.ctx, job)
	applied := false
	var x ExtractedLayer
	if err == nil {
		ctx, cancel := context.WithTimeout(j.ctx, unpackTimeout)
		applied, err = s.applier.ApplyLayer(ctx, lease, l)
		cancel()
		s.mu.Lock()
		own := !j.noStream[l.ChainID]
		s.mu.Unlock()
		switch {
		case !errors.Is(err, errParentMissing):
		case s.extractor != nil:
			// containerd unpacks it on its own and commits it onto the
			// layer below later (parallel unpacking, containerd 2.2+).
			ctx, cancel := context.WithTimeout(j.ctx, unpackTimeout)
			x, err = s.extractor.ExtractLayer(ctx, lease, l)
			cancel()
			if err == nil && x.Present {
				x = ExtractedLayer{}
			}
		case own && streamUnpackable(l.MediaType):
			x, err = s.extractStreaming(withUnpackMethod(j.ctx, unpackLandedOwn), lease, l)
		}
	}
	s.mu.Lock()
	s.extracting--
	delete(j.busy, l.ChainID)
	dropped := j.ctx.Err() != nil
	switch {
	case err == nil && x.Key != "" && !dropped:
		delete(j.pending, l.ChainID)
		j.extracted[l.ChainID] = extractedLayer{LayerSpec: l, x: x, result: "applied"}
		logging.Debugf("angryduck-worker[%s]: spread: unpacked %s (%d bytes) on its own in %s, attaching it once the layer below is", s.nodeID, l.Digest, l.Size, time.Since(start).Round(time.Millisecond))
	case err == nil && x.Key == "":
		delete(j.pending, l.ChainID)
		delete(j.waiting, l.ChainID)
		j.ready[l.ChainID] = true
		clear(j.commitTried)
		if applied {
			spreadUnpacksTotal.Inc(s.nodeID, "applied")
			logging.Debugf("angryduck-worker[%s]: spread: unpacked %s (%d bytes) in %s", s.nodeID, l.Digest, l.Size, time.Since(start).Round(time.Millisecond))
		} else {
			spreadUnpacksTotal.Inc(s.nodeID, "present")
		}
	case errors.Is(err, errParentMissing) && !dropped:
		j.waiting[l.ChainID] = true // containerd unpacks it once the layer below is here
	case !dropped:
		delete(j.pending, l.ChainID)
		spreadUnpacksTotal.Inc(s.nodeID, "failed")
		logging.Warnf("angryduck-worker[%s]: spread: unpacking %s early failed: %v (the image unpacks it at finalize)", s.nodeID, l.Digest, err)
	}
	s.kickAllLocked()
	s.workerDoneLocked(j)
	s.mu.Unlock()
	if err == nil && x.Key != "" && dropped {
		s.committer(x).DiscardExtracted(context.Background(), lease, x)
	}
}

// extractStreaming unpacks l on its own from its blob, arriving or landed.
func (s *Spread) extractStreaming(jctx context.Context, lease string, l LayerSpec) (x ExtractedLayer, err error) {
	err = s.withBlobStream(jctx, l, func(ctx context.Context, r io.Reader) error {
		var xerr error
		x, xerr = s.streamExtractor.ExtractLayerStream(ctx, lease, l, r)
		return xerr
	})
	if err == nil && x.Present {
		return ExtractedLayer{}, nil // already unpacked: nothing to attach
	}
	return x, err
}
