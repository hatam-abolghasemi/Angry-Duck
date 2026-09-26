package worker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"angryduck/internal/blobship"
	"angryduck/internal/logging"
	"angryduck/internal/metrics"
)

var (
	rescuePinnedSnapshots = metrics.NewGaugeVec(
		"angryduck_worker_rescue_pinned_snapshots",
		"Snapshots shipped by a rescue that are still pinned against garbage collection (kept for a retry, until RESCUE_PIN_TTL_S).",
		"node",
	)
	rescueCleanupsTotal = metrics.NewCounterVec(
		"angryduck_worker_rescue_cleanups_total",
		"Leftovers removed: \"temp_snapshot\" or \"temp_image\" (from an interrupted rescue) or \"expired_pin\".",
		"node", "kind",
	)
)

// Temporary snapshot keys created by CtrStore; anything with these
// prefixes at startup was left by a worker that died mid-rescue.
var tempSnapshotPrefixes = []string{"angryduck-rescue-", "angryduck-view-"}

// Pins remembers which shipped snapshots are still pinned against GC, and
// until when.
//
// A rescue pins every snapshot it commits. On success the imported image
// references them, so the pins are dropped right away. On failure they are
// kept for a while, so the next attempt finds them and ships only what is
// still missing instead of starting over. After the TTL they are released
// and containerd's GC removes them if nothing uses them.
//
// The list is also written to a small file on the node, so a restarted
// worker still knows about pins from before and can release them.
type Pins struct {
	store  blobship.Store
	nodeID string
	ttl    time.Duration
	path   string // on the node's filesystem (via HOST_ROOT); "" = memory only

	mu   sync.Mutex
	pins map[string]time.Time // chainID -> expiry
}

// NewPins loads any saved pins from path.
func NewPins(store blobship.Store, nodeID string, ttl time.Duration, path string) *Pins {
	p := &Pins{store: store, nodeID: nodeID, ttl: ttl, path: path, pins: map[string]time.Time{}}
	if path != "" {
		if b, err := os.ReadFile(path); err == nil {
			if err := json.Unmarshal(b, &p.pins); err != nil {
				logging.Warnf("angryduck-worker[%s]: ignoring unreadable pin file %s: %v", nodeID, path, err)
				p.pins = map[string]time.Time{}
			}
		}
	}
	p.publish()
	return p
}

// Add records chainID as pinned until now+TTL.
func (p *Pins) Add(chainID string) {
	p.mu.Lock()
	p.pins[chainID] = time.Now().Add(p.ttl)
	p.saveLocked()
	p.mu.Unlock()
}

// Has reports whether chainID is currently pinned by a rescue.
func (p *Pins) Has(chainID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.pins[chainID]
	return ok
}

// Release unpins every given chainID that is pinned here.
func (p *Pins) Release(ctx context.Context, chainIDs []string) {
	p.mu.Lock()
	var mine []string
	for _, c := range chainIDs {
		if _, ok := p.pins[c]; ok {
			mine = append(mine, c)
		}
	}
	p.mu.Unlock()
	p.unpin(ctx, mine, "")
}

// Sweep releases pins past their TTL.
func (p *Pins) Sweep(ctx context.Context) {
	now := time.Now()
	p.mu.Lock()
	var expired []string
	for c, until := range p.pins {
		if now.After(until) {
			expired = append(expired, c)
		}
	}
	p.mu.Unlock()
	p.unpin(ctx, expired, "expired_pin")
}

func (p *Pins) unpin(ctx context.Context, chainIDs []string, metricKind string) {
	for _, c := range chainIDs {
		// A snapshot that no longer exists has no pin left to remove.
		if err := p.store.Unpin(ctx, c); err != nil && !strings.Contains(err.Error(), "not found") {
			logging.Warnf("angryduck-worker[%s]: unpinning snapshot %s: %v (will retry)", p.nodeID, c, err)
			continue
		}
		p.mu.Lock()
		delete(p.pins, c)
		p.saveLocked()
		p.mu.Unlock()
		if metricKind != "" {
			rescueCleanupsTotal.Inc(p.nodeID, metricKind)
		}
	}
	p.publish()
}

// Run sweeps expired pins every interval until ctx is done.
func (p *Pins) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.Sweep(ctx)
		}
	}
}

func (p *Pins) publish() {
	p.mu.Lock()
	n := len(p.pins)
	p.mu.Unlock()
	rescuePinnedSnapshots.Set(float64(n), p.nodeID)
}

func (p *Pins) saveLocked() {
	if p.path == "" {
		return
	}
	b, _ := json.Marshal(p.pins)
	tmp := p.path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(p.path), 0o700); err == nil {
		if err = os.WriteFile(tmp, b, 0o600); err == nil {
			err = os.Rename(tmp, p.path)
		}
		if err == nil {
			return
		}
		logging.Warnf("angryduck-worker[%s]: saving pin file %s: %v (pins still tracked in memory)", p.nodeID, p.path, err)
		return
	}
	logging.Warnf("angryduck-worker[%s]: cannot create %s (pins still tracked in memory)", p.nodeID, filepath.Dir(p.path))
}

// CleanupLeftovers removes temporary snapshots a previous worker left
// behind, and releases pins that expired while it was down. Run once at
// startup, before this worker starts any rescue of its own.
func CleanupLeftovers(ctx context.Context, store blobship.Store, pins *Pins, nodeID string) {
	keys, err := store.Snapshots(ctx)
	if err != nil {
		logging.Warnf("angryduck-worker[%s]: startup cleanup: listing snapshots: %v", nodeID, err)
	}
	for key := range keys {
		for _, prefix := range tempSnapshotPrefixes {
			if strings.HasPrefix(key, prefix) {
				if err := store.RemoveSnapshot(ctx, key); err != nil {
					logging.Warnf("angryduck-worker[%s]: startup cleanup: removing %s: %v", nodeID, key, err)
					continue
				}
				logging.Infof("angryduck-worker[%s]: startup cleanup: removed leftover snapshot %s", nodeID, key)
				rescueCleanupsTotal.Inc(nodeID, "temp_snapshot")
			}
		}
	}
	if lister, ok := store.(interface {
		ImageNames(ctx context.Context) ([]string, error)
	}); ok {
		names, err := lister.ImageNames(ctx)
		if err != nil {
			logging.Warnf("angryduck-worker[%s]: startup cleanup: listing images: %v", nodeID, err)
		}
		var temps []string
		for _, n := range names {
			if strings.HasPrefix(n, baseImagePrefix) {
				temps = append(temps, n)
			}
		}
		if len(temps) > 0 {
			if err := store.DeleteImages(ctx, temps...); err != nil {
				logging.Warnf("angryduck-worker[%s]: startup cleanup: removing base images %v: %v", nodeID, temps, err)
			} else {
				logging.Infof("angryduck-worker[%s]: startup cleanup: removed %d leftover base image(s)", nodeID, len(temps))
				rescueCleanupsTotal.Add(int64(len(temps)), nodeID, "temp_image")
			}
		}
	}
	pins.Sweep(ctx)
}
