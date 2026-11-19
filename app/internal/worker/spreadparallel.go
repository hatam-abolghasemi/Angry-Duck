package worker

import (
	"context"
	"errors"
	"sort"
	"time"

	"angryduck/internal/logging"
)

// LayerExtractor unpacks layers side by side and commits them bottom up
// (see parallelunpack.go). ContainerdStore has it.
type LayerExtractor interface {
	ExtractLayer(ctx context.Context, lease string, l LayerSpec) (ExtractedLayer, error)
	CommitExtracted(ctx context.Context, lease string, l LayerSpec, x ExtractedLayer) (bool, error)
	DiscardExtracted(ctx context.Context, lease string, x ExtractedLayer)
}

type extractedLayer struct {
	LayerSpec
	x      ExtractedLayer
	result string // counted once committed: "applied" (default) or "streamed"
}

// EnableParallelUnpack unpacks landed layers side by side, at most max at
// once on this node across all spreads (SPREAD_UNPACK_PARALLEL_ENABLED,
// where containerd supports it). It needs unpacking on arrival on.
func (s *Spread) EnableParallelUnpack(x LayerExtractor, max int) bool {
	if s.applier == nil || max < 1 {
		return false
	}
	s.extractor, s.extractMax = x, max
	return true
}

// parallelKickLocked starts what job can do now: unpacking while it arrives
// the lowest layer whose layer below is unpacked, or may be (stream
// unpacking, one at a time), unpacking landed layers within the node's
// slots, and committing unpacked ones. Caller holds s.mu.
//
// A layer below that this spread didn't unpack may be on the node already
// (a shared base): the layer is tried once, and the applier says
// errParentMissing, having read nothing, if it isn't. Then it waits for the
// layer below to be unpacked here, as the sequential path does.
func (s *Spread) parallelKickLocked(job string, j *unpackJob) {
	if s.streamExtractor != nil {
		s.wholeKickLocked(job, j)
		return
	}
	if j.stopped {
		return
	}
	var stream, landed []pendingLayer
	for c, l := range j.pending {
		if j.busy[c] {
			continue
		}
		switch {
		case !l.arriving:
			landed = append(landed, l)
		case s.streamApplier != nil && !j.noStream[c] && (l.Parent == "" || j.ready[l.Parent] || !j.waiting[c]):
			stream = append(stream, l)
		}
	}
	bySeq := func(ls []pendingLayer) {
		sort.Slice(ls, func(a, b int) bool { return j.seq[ls[a].ChainID] < j.seq[ls[b].ChainID] })
	}
	if len(stream) > 0 && !j.streaming {
		bySeq(stream)
		l := stream[0]
		j.busy[l.ChainID], j.streaming = true, true
		s.workerStartLocked(j)
		go s.streamWorker(job, j, l.LayerSpec)
	}
	bySeq(landed)
	for _, l := range landed {
		if s.extracting >= s.extractMax {
			break
		}
		s.extracting++
		j.busy[l.ChainID] = true
		s.workerStartLocked(j)
		go s.extractWorker(job, j, l.LayerSpec)
	}
	if len(j.extracted) > 0 && !j.committing {
		j.committing = true
		s.workerStartLocked(j)
		go s.commitWorker(job, j)
	}
}

// kickAllLocked gives every job a chance at slots just freed.
func (s *Spread) kickAllLocked() {
	for job, j := range s.unpacks {
		s.parallelKickLocked(job, j)
	}
}

func (s *Spread) workerStartLocked(j *unpackJob) {
	if j.workers == 0 {
		j.running, j.idle = true, make(chan struct{})
	}
	j.workers++
	s.wg.Add(1)
}

func (s *Spread) workerDoneLocked(j *unpackJob) {
	j.workers--
	if j.workers == 0 {
		j.running = false
		close(j.idle)
	}
	s.wg.Done()
}

// extractWorker has containerd unpack one landed layer on its own.
func (s *Spread) extractWorker(job string, j *unpackJob, l LayerSpec) {
	defer s.afterUnpack()
	start := time.Now()
	var x ExtractedLayer
	lease, err := s.lease(j.ctx, job)
	if err == nil {
		ctx, cancel := context.WithTimeout(j.ctx, unpackTimeout)
		x, err = s.extractor.ExtractLayer(ctx, lease, l)
		cancel()
	}
	s.mu.Lock()
	s.extracting--
	delete(j.busy, l.ChainID)
	dropped := j.ctx.Err() != nil
	switch {
	case err == nil && x.Present:
		delete(j.pending, l.ChainID)
		j.ready[l.ChainID] = true
		spreadUnpacksTotal.Inc(s.nodeID, "present")
	case err == nil && !dropped:
		delete(j.pending, l.ChainID)
		j.extracted[l.ChainID] = extractedLayer{LayerSpec: l, x: x}
		logging.Debugf("angryduck-worker[%s]: spread: unpacked %s (%d bytes) on its own in %s, committing it once the layer below is", s.nodeID, l.Digest, l.Size, time.Since(start).Round(time.Millisecond))
	case !dropped:
		delete(j.pending, l.ChainID)
		spreadUnpacksTotal.Inc(s.nodeID, "failed")
		logging.Warnf("angryduck-worker[%s]: spread: unpacking %s early failed: %v (the image unpacks it at finalize)", s.nodeID, l.Digest, err)
	}
	s.kickAllLocked()
	s.workerDoneLocked(j)
	s.mu.Unlock()
	if err == nil && dropped {
		s.extractor.DiscardExtracted(context.Background(), lease, x)
	}
}

// streamWorker unpacks the lowest layer still arriving onto the one below.
func (s *Spread) streamWorker(job string, j *unpackJob, l LayerSpec) {
	defer s.afterUnpack()
	start := time.Now()
	lease, err := s.lease(j.ctx, job)
	applied := false
	if err == nil {
		applied, err = s.unpackStreaming(j.ctx, lease, l)
	}
	s.mu.Lock()
	j.streaming = false
	delete(j.busy, l.ChainID)
	dropped := j.ctx.Err() != nil
	switch {
	case err == nil:
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
	case errors.Is(err, errParentMissing) && !dropped:
		// The layer below isn't here yet: tried again once it is unpacked
		// here, if the blob is still arriving (else it is unpacked once landed).
		j.waiting[l.ChainID] = true
	case !dropped:
		// Unpacked once its blob lands instead.
		j.noStream[l.ChainID] = true
		if !errors.Is(err, errParentMissing) {
			spreadUnpacksTotal.Inc(s.nodeID, "stream_failed")
			logging.Warnf("angryduck-worker[%s]: spread: unpacking %s while it arrived failed: %v (unpacking it once it lands instead)", s.nodeID, l.Digest, err)
		}
	}
	s.parallelKickLocked(job, j)
	s.workerDoneLocked(j)
	s.mu.Unlock()
}

// commitWorker commits unpacked layers onto the layers below them, bottom
// up, for as long as one can be: one whose layer below was unpacked here,
// or (tried once) one whose layer below was already on the node.
func (s *Spread) commitWorker(job string, j *unpackJob) {
	defer s.afterUnpack()
	for {
		s.mu.Lock()
		var next *extractedLayer
		for c, e := range j.extracted {
			if e.Parent != "" && !j.ready[e.Parent] && j.commitTried[c] {
				continue
			}
			if next == nil || j.seq[c] < j.seq[next.ChainID] {
				e := e
				next = &e
			}
		}
		if next == nil || j.ctx.Err() != nil {
			j.committing = false
			s.workerDoneLocked(j)
			s.mu.Unlock()
			return
		}
		j.commitTried[next.ChainID] = true
		s.mu.Unlock()

		lease, err := s.lease(j.ctx, job)
		applied := false
		if err == nil {
			ctx, cancel := context.WithTimeout(j.ctx, time.Minute)
			applied, err = s.committer(next.x).CommitExtracted(ctx, lease, next.LayerSpec, next.x)
			cancel()
		}
		s.mu.Lock()
		switch {
		case errors.Is(err, errParentMissing):
			// Waits for the layer below.
		case err == nil:
			delete(j.extracted, next.ChainID)
			j.ready[next.ChainID] = true
			clear(j.commitTried)
			switch {
			case !applied:
				spreadUnpacksTotal.Inc(s.nodeID, "present")
			case next.result != "":
				spreadUnpacksTotal.Inc(s.nodeID, next.result)
			default:
				spreadUnpacksTotal.Inc(s.nodeID, "applied")
			}
		default:
			delete(j.extracted, next.ChainID)
			if j.ctx.Err() == nil {
				spreadUnpacksTotal.Inc(s.nodeID, "failed")
				logging.Warnf("angryduck-worker[%s]: spread: committing %s failed: %v (the image unpacks it at finalize)", s.nodeID, next.Digest, err)
			}
		}
		// A layer just unpacked may let the next one stream.
		s.parallelKickLocked(job, j)
		s.mu.Unlock()
		if err != nil && !errors.Is(err, errParentMissing) {
			s.committer(next.x).DiscardExtracted(context.Background(), lease, next.x)
		}
	}
}

// discardExtracted removes what was unpacked but never committed (the
// layer below never arrived here): finalize's own unpack does those.
func (s *Spread) discardExtracted(j *unpackJob) {
	if s.extractor == nil && s.streamExtractor == nil {
		return
	}
	s.mu.Lock()
	var left []ExtractedLayer
	for c, e := range j.extracted {
		left = append(left, e.x)
		delete(j.extracted, c)
	}
	s.mu.Unlock()
	for _, x := range left {
		s.committer(x).DiscardExtracted(context.Background(), "", x)
	}
}
