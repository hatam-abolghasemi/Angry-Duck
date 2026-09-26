package worker

import (
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

// LayerLister is what the tracker needs from containerd. CtrStore has it.
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
// lands (force). Two `ctr` listings per scan; nothing is sent when nothing
// changed.
type LayerTracker struct {
	lister   LayerLister
	nodeID   string
	interval time.Duration
	epoch    string

	mu       sync.Mutex
	lastScan time.Time
	curBlobs map[string]bool // latest scan; replaced, never mutated
	curSnaps map[string]bool

	ackedSeq   uint64 // 0: the controller holds nothing we know of
	ackBlobs   map[string]bool
	ackSnaps   map[string]bool
	seq        uint64 // last seq handed out
	pending    *pendingSync
	wantFull   bool // send a full sync at the next chance, unthrottled
	lastFullAt time.Time
}

type pendingSync struct {
	seq          uint64
	blobs, snaps map[string]bool
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

// scan refreshes the current inventory if it's due. Caller holds mu.
func (t *LayerTracker) scan(ctx context.Context, force bool) {
	if !force && t.curBlobs != nil && time.Since(t.lastScan) < t.interval {
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
	// snapshots and our own temporary keys are not layers. The listing is
	// shared with other callers, so filter into a new map.
	layers := make(map[string]bool, len(snaps))
	for k := range snaps {
		if strings.HasPrefix(k, "sha256:") {
			layers[k] = true
		}
	}
	t.curBlobs, t.curSnaps, t.lastScan = blobs, layers, time.Now()
}

// Next returns the sync to put in the next report, or nil when there is
// nothing to send. force rescans now (a pull just landed).
func (t *LayerTracker) Next(ctx context.Context, force bool) *model.LayerSync {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.scan(ctx, force)
	t.pending = nil
	if t.curBlobs == nil {
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
			AddBlobs: keys(t.curBlobs), AddSnaps: keys(t.curSnaps),
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

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// diff returns what to add and remove to turn old into cur.
func diff(old, cur map[string]bool) (add, del []string) {
	for k := range cur {
		if !old[k] {
			add = append(add, k)
		}
	}
	for k := range old {
		if !cur[k] {
			del = append(del, k)
		}
	}
	sort.Strings(add)
	sort.Strings(del)
	return add, del
}
