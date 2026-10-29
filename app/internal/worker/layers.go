package worker

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"sort"
	"strings"
	"sync"
	"time"

	"angryduck/internal/logging"
	"angryduck/internal/model"
)

// LayerLister is what the tracker needs from containerd. ContainerdStore has it.
type LayerLister interface {
	Digests(ctx context.Context) (map[string]bool, error)
	Snapshots(ctx context.Context) (map[string]bool, error)
}

// fullRetry spaces out full syncs a controller never acknowledges (one
// that predates layer syncs), so an old controller doesn't get the whole
// inventory on every report.
const fullRetry = 10 * time.Minute

// LayerTracker keeps this node's layer inventory in step with the
// controller: every blob digest in containerd's content store and every
// committed snapshot, sent as deltas against what the controller last
// acknowledged.
//
// Scans run at most every interval, plus right after a pull or rescue
// lands (force). Two containerd listings per scan; nothing is sent when
// nothing changed.
type LayerTracker struct {
	lister   LayerLister
	nodeID   string
	interval time.Duration
	epoch    string

	mu       sync.Mutex
	lastScan time.Time
	scanned  bool
	curBlobs digestSet // latest scan; replaced, never mutated
	curSnaps digestSet

	ackedSeq   uint64 // 0: the controller holds nothing we know of
	ackBlobs   digestSet
	ackSnaps   digestSet
	seq        uint64 // last seq handed out
	pending    *pendingSync
	wantFull   bool // send a full sync at the next chance, unthrottled
	lastFullAt time.Time
}

type pendingSync struct {
	seq          uint64
	blobs, snaps digestSet
}

// NewLayerTracker builds a tracker; interval is the minimum time between
// scans that nothing forced.
func NewLayerTracker(lister LayerLister, nodeID string, interval time.Duration) *LayerTracker {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return &LayerTracker{
		lister: lister, nodeID: nodeID, interval: interval,
		epoch: hex.EncodeToString(b), wantFull: true,
	}
}

// SetInterval changes the time between periodic scans: a slow resync while
// containerd's events drive the reports, the regular interval while they
// don't.
func (t *LayerTracker) SetInterval(d time.Duration) {
	t.mu.Lock()
	t.interval = d
	t.mu.Unlock()
}

// scan refreshes the current inventory if it's due. Caller holds mu.
func (t *LayerTracker) scan(ctx context.Context, force bool) {
	if !force && t.scanned && time.Since(t.lastScan) < t.interval {
		return
	}
	if force {
		// Something landed outside the store's knowledge (a pull):
		// don't answer from its short-lived cache.
		if inv, ok := t.lister.(interface{ Invalidate() }); ok {
			inv.Invalidate()
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	blobs, err := t.lister.Digests(ctx)
	if err != nil {
		logging.Warnf("angryduck-worker[%s]: layer inventory: listing content: %v", t.nodeID, err)
		return
	}
	snaps, err := t.lister.Snapshots(ctx)
	if err != nil {
		logging.Warnf("angryduck-worker[%s]: layer inventory: listing snapshots: %v", t.nodeID, err)
		return
	}
	// Only committed image snapshots are named by chainID; container
	// snapshots and our own temporary keys are not layers.
	isLayer := func(k string) bool { return strings.HasPrefix(k, "sha256:") }
	// Kept compact (see digestSet), and the previous set itself when
	// nothing changed, so an unchanged inventory is held once, not per scan.
	t.curBlobs = newDigestSet(blobs, nil).sameOr(t.curBlobs)
	t.curSnaps = newDigestSet(snaps, isLayer).sameOr(t.curSnaps)
	t.lastScan, t.scanned = time.Now(), true
}

// Next returns the sync to put in the next report, or nil when there is
// nothing to send. force rescans now (a pull just landed).
func (t *LayerTracker) Next(ctx context.Context, force bool) *model.LayerSync {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.scan(ctx, force)
	t.pending = nil
	if !t.scanned {
		return nil // never scanned successfully
	}
	if t.ackedSeq == 0 {
		if !t.wantFull && time.Since(t.lastFullAt) < fullRetry {
			return nil
		}
		t.seq++
		t.wantFull, t.lastFullAt = false, time.Now()
		t.pending = &pendingSync{seq: t.seq, blobs: t.curBlobs, snaps: t.curSnaps}
		return &model.LayerSync{
			Epoch: t.epoch, Seq: t.seq, Full: true,
			AddBlobs: t.curBlobs.strings(), AddSnaps: t.curSnaps.strings(),
		}
	}
	addB, delB := diff(t.ackBlobs, t.curBlobs)
	addS, delS := diff(t.ackSnaps, t.curSnaps)
	if len(addB)+len(delB)+len(addS)+len(delS) == 0 {
		return nil
	}
	t.seq++
	t.pending = &pendingSync{seq: t.seq, blobs: t.curBlobs, snaps: t.curSnaps}
	return &model.LayerSync{
		Epoch: t.epoch, Seq: t.seq, Base: t.ackedSeq,
		AddBlobs: addB, DelBlobs: delB, AddSnaps: addS, DelSnaps: delS,
	}
}

// Ack records the controller's answer to a report. sent is what that
// report carried (nil if it carried no layers).
func (t *LayerTracker) Ack(ack model.ReportAck, sent *model.LayerSync) {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch {
	case ack.LayersResync:
		t.ackedSeq, t.wantFull = 0, true
	case sent != nil && t.pending != nil && ack.LayersSeq == sent.Seq && sent.Seq == t.pending.seq:
		t.ackedSeq = sent.Seq
		t.ackBlobs, t.ackSnaps = t.pending.blobs, t.pending.snaps
	case sent == nil && t.ackedSeq != 0 && ack.LayersSeq != t.ackedSeq:
		// The controller no longer holds what it acknowledged (it
		// restarted): start over with a full sync.
		t.ackedSeq, t.wantFull = 0, true
	}
	t.pending = nil
}

// digestSet is a set of digests held compactly: each sha256 digest as its
// 32 raw bytes in a sorted slice, a third of a map entry keyed by its
// 71-character string; anything else (another algorithm) as a sorted
// string. A large node holds thousands of blobs and snapshots, twice
// (scanned and acknowledged).
type digestSet struct {
	sha  []digestKey
	odd  []string
	full bool // built from a listing (a zero digestSet is empty)
}

type digestKey [32]byte

// newDigestSet builds the set of keys of m that keep allows (nil: all).
func newDigestSet(m map[string]bool, keep func(string) bool) digestSet {
	s := digestSet{sha: make([]digestKey, 0, len(m)), full: true}
	for k := range m {
		if keep != nil && !keep(k) {
			continue
		}
		if dk, ok := parseDigestKey(k); ok {
			s.sha = append(s.sha, dk)
		} else {
			s.odd = append(s.odd, k)
		}
	}
	sort.Slice(s.sha, func(i, j int) bool { return lessKey(s.sha[i], s.sha[j]) })
	sort.Strings(s.odd)
	if cap(s.sha)-len(s.sha) > len(s.sha)/8 {
		s.sha = append([]digestKey(nil), s.sha...) // drop the filtered-out room
	}
	return s
}

func parseDigestKey(d string) (digestKey, bool) {
	var k digestKey
	h, ok := strings.CutPrefix(d, "sha256:")
	if !ok || len(h) != 64 {
		return k, false
	}
	for i := 0; i < len(h); i++ {
		if c := h[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return k, false // only the canonical lowercase form round-trips
		}
	}
	if _, err := hex.Decode(k[:], []byte(h)); err != nil {
		return k, false
	}
	return k, true
}

func lessKey(a, b digestKey) bool { return bytes.Compare(a[:], b[:]) < 0 }

func keyString(k digestKey) string { return "sha256:" + hex.EncodeToString(k[:]) }

// sameOr returns prev if it holds exactly what s does, else s.
func (s digestSet) sameOr(prev digestSet) digestSet {
	if !prev.full || len(prev.sha) != len(s.sha) || len(prev.odd) != len(s.odd) {
		return s
	}
	for i := range s.sha {
		if s.sha[i] != prev.sha[i] {
			return s
		}
	}
	for i := range s.odd {
		if s.odd[i] != prev.odd[i] {
			return s
		}
	}
	return prev
}

// strings returns every digest, sorted.
func (s digestSet) strings() []string {
	out := make([]string, 0, len(s.sha)+len(s.odd))
	for _, k := range s.sha {
		out = append(out, keyString(k))
	}
	out = append(out, s.odd...)
	sort.Strings(out)
	return out
}

// diff returns what to add and remove to turn old into cur, sorted.
func diff(old, cur digestSet) (add, del []string) {
	i, j := 0, 0
	for i < len(old.sha) || j < len(cur.sha) {
		switch {
		case i == len(old.sha):
			add = append(add, keyString(cur.sha[j]))
			j++
		case j == len(cur.sha):
			del = append(del, keyString(old.sha[i]))
			i++
		case old.sha[i] == cur.sha[j]:
			i++
			j++
		case lessKey(old.sha[i], cur.sha[j]):
			del = append(del, keyString(old.sha[i]))
			i++
		default:
			add = append(add, keyString(cur.sha[j]))
			j++
		}
	}
	oldOdd := make(map[string]bool, len(old.odd))
	for _, k := range old.odd {
		oldOdd[k] = true
	}
	curOdd := make(map[string]bool, len(cur.odd))
	for _, k := range cur.odd {
		curOdd[k] = true
		if !oldOdd[k] {
			add = append(add, k)
		}
	}
	for _, k := range old.odd {
		if !curOdd[k] {
			del = append(del, k)
		}
	}
	sort.Strings(add)
	sort.Strings(del)
	return add, del
}
