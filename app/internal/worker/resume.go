package worker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"angryduck/internal/metrics"
	"angryduck/internal/model"
	"angryduck/internal/registryclient"
)

// Resuming: a spread transfer that stops part-way leaves its bytes for the
// next transfer of the same blob on this node, which asks its source for
// the rest only (a Range request) instead of starting again from byte 0.
//
// The blob stays the unit: the controller still orders whole blobs, and
// containerd still checks the whole blob's digest at commit, whichever
// sources its bytes came from. A blob's bytes are the same from every
// source, so the rest from one source follows the start from another.
//
// Three cases use it:
//
//   - A transfer fails (a peer or the registry drops the connection, a
//     stream is cut because its source stopped): its ingest stays, under
//     the job's lease, and the next transfer of the blob, from any source,
//     goes on from there.
//   - The slow-chain guard restarts a late transfer from a faster peer
//     (SpreadFetch.Replace): the new transfer takes the old one's ingest
//     over once the old one has stopped writing it, and fetches the rest.
//   - A peer copy of a blob still coming from the registry (or the other
//     way round, from a peer) should finish clearly sooner: the same hand-
//     off, instead of a race where both copies run and one is thrown away.
//
// Only peers hand off: a peer of this version always continues mid-blob,
// a registry may not (a 200 with the whole blob). A failed transfer's
// bytes are kept whatever its path, since a source that can't continue
// costs nothing more than starting over did before. Peers before 1.15.0
// send the whole blob too: during a rollout a hand-off to one starts over.
//
// A write containerd rejects (wrong size or digest) is aborted as before,
// so a bad byte from anywhere costs one more transfer of the blob from 0.
//
// What it holds: one partial entry per blob that stopped part-way, until a
// transfer takes it, the blob lands, or the job's lease ends. The bytes
// themselves stay in containerd's ingest, under the job's lease, as a
// failed transfer's did until containerd aborted it.

var (
	spreadResumedBytes = metrics.NewCounterVec(
		"angryduck_worker_spread_resumed_bytes_total",
		"Bytes of blobs this node committed for spreads without fetching them again: an earlier transfer of the blob on this node stopped part-way, and the transfer that committed it went on from its bytes. By the path of the transfer that committed it (registry, peer).",
		"node", "path",
	)
	spreadDiscardedBytes = metrics.NewCounterVec(
		"angryduck_worker_spread_discarded_bytes_total",
		"Bytes this node received for spreads and then threw away, by reason: lost_race (the blob landed over the other path first), dropped (the job was dropped or superseded), restarted (the next source could only send the blob from byte 0, so what was here went), failed (a failed transfer's bytes couldn't be kept: containerd rejected them, or the store can't resume), unused (kept for a next transfer of the blob that never came while the job ran). Also counted in spread_bytes_total when received.",
		"node", "reason",
	)
)

// Discard reasons, as spread_discarded_bytes_total labels them.
var discardReasons = []string{"lost_race", "dropped", "restarted", "failed", "unused"}

// EnableResume lets transfers go on from what a stopped one left
// (SPREAD_RESUME_ENABLED), where the store can. Call before Register.
// Without it a transfer that stops is aborted, a restart starts from 0,
// and a second copy of a blob races the first, as before 1.15.0; serving
// the rest of a blob to peers that ask for it stays on either way.
func (s *Spread) EnableResume() bool {
	rs, ok := s.store.(ResumingStore)
	if !ok {
		return false
	}
	s.resumer = rs
	for _, path := range []string{model.SpreadPathRegistry, model.SpreadPathPeer} {
		spreadResumedBytes.Add(0, s.nodeID, path)
	}
	return true
}

var (
	_ ResumingStore = (*ContainerdStore)(nil)
	_ RangeOpener   = (*registryclient.Client)(nil)
)

// Why a transfer was stopped on purpose (spreadXfer.why).
const (
	stopNone      int32 = iota
	stopOtherPath       // the blob landed over the other path
	stopDropped         // the job was dropped
	stopHandOff         // a faster peer goes on from its bytes
	stopRestart         // a faster peer starts over: the store can't resume
)

func stopReason(why int32) string {
	switch why {
	case stopOtherPath:
		return "it arrived another way first"
	case stopDropped:
		return "the job was dropped"
	case stopHandOff:
		return "a faster peer goes on from here"
	case stopRestart:
		return "a faster peer starts it over"
	}
	return "stopped"
}

// stop cancels x for why. The first reason given sticks.
func (x *spreadXfer) stop(why int32) {
	x.why.CompareAndSwap(stopNone, why)
	x.cancelled.Store(true)
	x.cancel()
}

// keep tells the store whether to leave x's ingest when its write fails:
// yes when it failed on its own (the next transfer of the blob goes on from
// there) or was stopped for a hand-off, no when stopped for anything else.
func (x *spreadXfer) keep(error) bool {
	return !x.cancelled.Load() || x.why.Load() == stopHandOff
}

// partial is an ingest a transfer left part-way, for the next one.
type partial struct {
	ref   string
	job   string
	bytes int64
	at    time.Time
}

// handsOff reports whether a transfer over path takes a running one's
// ingest over instead of starting its own: peers continue mid-blob.
func (s *Spread) handsOff(path string) bool {
	return s.resumer != nil && path == model.SpreadPathPeer
}

// keptFor is how many bytes of old a transfer over path wouldn't fetch
// again: all of them when it takes old's ingest over.
func (s *Spread) keptFor(old *spreadXfer, path string) int64 {
	if !s.handsOff(path) {
		return 0
	}
	return old.progress.have()
}

// takePartialLocked gives x the ingest an earlier transfer of its blob
// left part-way, if any. Caller holds s.mu.
func (s *Spread) takePartialLocked(x *spreadXfer) {
	if s.resumer == nil {
		return
	}
	if p, ok := s.partials[x.digest]; ok {
		delete(s.partials, x.digest)
		x.ref = p.ref
	}
}

// settle accounts for what x left behind: an ingest kept for the next
// transfer of its blob, or bytes thrown away.
func (s *Spread) settle(x *spreadXfer, o model.SpreadFetch, w WriteResult, err error) {
	spreadDiscardedBytes.Add(w.Dropped, s.nodeID, "restarted")
	if err == nil {
		return
	}
	held := w.Resumed + w.Received
	why := x.why.Load()
	switch {
	case w.Kept && !x.cancelled.Load():
		s.mu.Lock()
		s.keepPartialLocked(o.Digest, partial{ref: x.ref, job: o.Job, bytes: held, at: time.Now()})
		s.mu.Unlock()
	case why == stopHandOff:
		// The transfer that took it over goes on from these bytes.
	case why == stopOtherPath:
		spreadDiscardedBytes.Add(held, s.nodeID, "lost_race")
	case why == stopDropped:
		spreadDiscardedBytes.Add(held, s.nodeID, "dropped")
	case why == stopRestart:
		spreadDiscardedBytes.Add(held, s.nodeID, "restarted")
	default:
		spreadDiscardedBytes.Add(held, s.nodeID, "failed")
	}
}

// keepPartialLocked records p as dgst's partial ingest. Of two, the one
// holding more bytes stays; the other's go with its lease. Caller holds
// s.mu.
func (s *Spread) keepPartialLocked(dgst string, p partial) {
	if old, ok := s.partials[dgst]; ok && old.ref != p.ref {
		if old.bytes >= p.bytes {
			spreadDiscardedBytes.Add(p.bytes, s.nodeID, "unused")
			return
		}
		spreadDiscardedBytes.Add(old.bytes, s.nodeID, "unused")
	}
	s.partials[dgst] = p
}

// forgetPartialsLocked drops the partial ingests job left (its lease is
// going, and with it their bytes), or, with job "", those older than a
// lease lives. Caller holds s.mu.
func (s *Spread) forgetPartialsLocked(job string, now time.Time) {
	for d, p := range s.partials {
		if job != "" && p.job == job || job == "" && now.Sub(p.at) > spreadLeaseTTL {
			delete(s.partials, d)
			spreadDiscardedBytes.Add(p.bytes, s.nodeID, "unused")
		}
	}
}

// RangeOpener opens a blob from a byte on (registryclient.Client has it).
// start is where the body really starts: from, or 0 for the whole blob.
type RangeOpener interface {
	OpenBlobFrom(ctx context.Context, image, digest string, from int64) (body io.ReadCloser, start, size int64, err error)
}

// rangeFrom reads a Range request for the rest of a blob of size bytes
// ("bytes=N-", the only form receivers send). 0 serves the whole blob, as
// for any other form: a server may ignore a range it doesn't take.
func rangeFrom(r *http.Request, size int64) int64 {
	h := r.Header.Get("Range")
	if h == "" {
		return 0
	}
	spec, ok := strings.CutPrefix(h, "bytes=")
	if !ok {
		return 0
	}
	first, last, ok := strings.Cut(spec, "-")
	if !ok || last != "" {
		return 0
	}
	n, err := strconv.ParseInt(first, 10, 64)
	if err != nil || n <= 0 || n >= size {
		return 0
	}
	return n
}

// partialHeaders announces the bytes [from, size) of a blob of size bytes.
func partialHeaders(w http.ResponseWriter, from, size int64) {
	w.Header().Set("Content-Range", "bytes "+strconv.FormatInt(from, 10)+"-"+strconv.FormatInt(size-1, 10)+"/"+strconv.FormatInt(size, 10))
	w.Header().Set("Content-Length", strconv.FormatInt(size-from, 10))
	w.WriteHeader(http.StatusPartialContent)
}

// skipTo moves body to byte from: a seek for a file, else reading past.
func skipTo(body io.Reader, from int64) error {
	if sk, ok := body.(io.Seeker); ok {
		_, err := sk.Seek(from, io.SeekStart)
		return err
	}
	n, err := io.CopyN(io.Discard, body, from)
	if err == nil && n < from {
		err = errors.New("blob shorter than the range asked for")
	}
	return err
}
