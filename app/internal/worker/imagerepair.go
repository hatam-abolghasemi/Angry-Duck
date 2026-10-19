package worker

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"angryduck/internal/blobship"
	"angryduck/internal/logging"
	"angryduck/internal/metrics"
)

var (
	brokenImagesGauge = metrics.NewGaugeVec(
		"angryduck_worker_broken_images",
		"Images registered on this node that can't be unpacked: a layer is neither unpacked nor has its blob here, so every container create fails (\"failed to get reader from content store\") while kubelet calls the image present and never pulls it. The repair removes them on its next check.",
		"node",
	)
	brokenImagesRemovedTotal = metrics.NewCounterVec(
		"angryduck_worker_broken_images_removed_total",
		"Images the repair removed because they couldn't be unpacked, so kubelet (or Angry Duck) fetches them again. Counted once per image, whatever its number of names.",
		"node",
	)
	tempImagesRemovedTotal = metrics.NewCounterVec(
		"angryduck_worker_temp_images_removed_total",
		"Temporary image names an earlier import left behind (a worker killed mid-import), removed by the repair.",
		"node",
	)
)

// tempImageHost is the registry host of every image name Angry Duck makes
// up for itself: rescue base images and the names imports unpack under.
// They are never reported, cleaned up by the image cleanup, or repaired.
const tempImageHost = "angryduck.local/"

// RepairStore is what the repair needs from containerd. ContainerdStore
// has it.
type RepairStore interface {
	blobship.Store
	ImageTargets(ctx context.Context) (map[string]string, error)
}

// ImageRepair finds images that are registered here but can't be
// unpacked, and removes them, so they can be fetched again.
//
// Such an image is the worst kind of missing: kubelet sees it as present
// ("already present on machine"), never pulls it, and every container
// create fails trying to unpack it ("failed to get reader from content
// store ... not found"); Angry Duck's controller counts the node as
// holding it, so it doesn't send it either. Before 1.8.12 a spread or
// rescue import whose unpack failed left exactly that behind; imports now
// publish a name only once its image is unpacked, but nodes may still carry
// images broken then, and anything else (a manual `ctr` import, a disk
// error) can still make one.
//
// An image counts as broken when its top layer isn't unpacked and some
// layer has neither its snapshot nor its blob here: nothing can unpack it.
// It is removed only when it is still broken one interval later, checked
// against fresh listings, and nothing on this node is pulling or receiving
// it. Every name pointing at it goes, like the cleanup does. Once gone,
// the next container create makes kubelet pull it, and the report that
// follows tells the controller the node no longer has it.
type ImageRepair struct {
	store    RepairStore
	platform string
	nodeID   string
	interval time.Duration
	busy     func(names []string) bool
	onChange func()

	mu       sync.Mutex
	suspects map[string]time.Time // target digest -> first found broken
}

// NewImageRepair builds the repair. busy reports images being pulled or
// received right now (never touched); onChange runs after it removed
// something (the reporter's Kick).
func NewImageRepair(store RepairStore, platform, nodeID string, interval time.Duration, busy func([]string) bool, onChange func()) *ImageRepair {
	brokenImagesGauge.Set(0, nodeID)
	brokenImagesRemovedTotal.Add(0, nodeID)
	tempImagesRemovedTotal.Add(0, nodeID)
	if busy == nil {
		busy = func([]string) bool { return false }
	}
	return &ImageRepair{store: store, platform: platform, nodeID: nodeID, interval: interval,
		busy: busy, onChange: onChange, suspects: map[string]time.Time{}}
}

// Run checks every interval until ctx is done, the first time shortly
// after startup.
func (r *ImageRepair) Run(ctx context.Context) {
	logging.Infof("angryduck-worker[%s]: image repair started: interval=%s platform=%s", r.nodeID, r.interval, r.platform)
	first := 30 * time.Second
	if r.interval < first {
		first = r.interval
	}
	t := time.NewTimer(first)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.Pass(ctx, time.Now())
			t.Reset(r.interval)
		}
	}
}

// Pass is one check. now is when it runs (tests pass their own).
func (r *ImageRepair) Pass(ctx context.Context, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	targets, err := r.store.ImageTargets(ctx)
	if err != nil {
		logging.Warnf("angryduck-worker[%s]: image repair: listing images: %v", r.nodeID, err)
		return
	}
	changed := r.removeStaleTemps(ctx, targets)

	byTarget := map[string][]string{}
	for name, d := range targets {
		if strings.HasPrefix(name, tempImageHost) {
			continue
		}
		byTarget[d] = append(byTarget[d], name)
	}
	digests := make([]string, 0, len(byTarget))
	for d := range byTarget {
		digests = append(digests, d)
	}
	sort.Strings(digests)

	broken := map[string]bool{}
	for _, d := range digests {
		if ctx.Err() != nil {
			return
		}
		names := byTarget[d]
		sort.Strings(names)
		h, err := blobship.Check(ctx, r.store, names[0], r.platform)
		if err != nil || !h.Broken() {
			continue // fine, or another platform's, or can't tell: leave it
		}
		broken[d] = true
		first, seen := r.suspects[d]
		if !seen {
			r.suspects[d] = now
			logging.Warnf("angryduck-worker[%s]: image repair: image=%s can't be unpacked: %d layer(s) neither unpacked nor here, first %s; removing it in %s unless that changes",
				r.nodeID, strings.Join(names, ","), len(h.Missing), h.Missing[0].Blob.Digest, r.interval)
			continue
		}
		if now.Sub(first) < r.interval {
			continue
		}
		if r.repair(ctx, d, names) {
			delete(broken, d)
			delete(r.suspects, d)
			changed = true
		}
	}
	for d := range r.suspects {
		if !broken[d] {
			delete(r.suspects, d) // fixed, or gone some other way
		}
	}
	brokenImagesGauge.Set(float64(len(broken)), r.nodeID)
	if changed && r.onChange != nil {
		r.onChange()
	}
}

// repair removes the image digest points at, after checking once more,
// against fresh listings, that it is still broken. It reports whether the
// image is gone.
func (r *ImageRepair) repair(ctx context.Context, digest string, names []string) bool {
	if inv, ok := r.store.(interface{ Invalidate() }); ok {
		inv.Invalidate()
	}
	h, err := blobship.Check(ctx, r.store, names[0], r.platform)
	if err != nil || !h.Broken() {
		return false
	}
	// Every name pointing at it now, not only those seen an interval ago.
	targets, err := r.store.ImageTargets(ctx)
	if err != nil {
		return false
	}
	names = names[:0:0]
	for n, d := range targets {
		if d == digest && !strings.HasPrefix(n, tempImageHost) {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		return true
	}
	if r.busy(names) {
		logging.Infof("angryduck-worker[%s]: image repair: image=%s is being pulled or received; checking again later", r.nodeID, strings.Join(names, ","))
		return false
	}
	if err := r.store.DeleteImages(ctx, names...); err != nil {
		logging.Warnf("angryduck-worker[%s]: image repair: removing image=%s: %v", r.nodeID, strings.Join(names, ","), err)
		return false
	}
	brokenImagesRemovedTotal.Inc(r.nodeID)
	logging.Warnf("angryduck-worker[%s]: image repair: removed image=%s: it couldn't be unpacked (%d layer(s) gone, first %s); kubelet pulls it again on the next container start",
		r.nodeID, strings.Join(names, ","), len(h.Missing), h.Missing[0].Blob.Digest)
	return true
}

// removeStaleTemps removes temporary import names no import in this
// process is using: a previous worker was killed mid-import.
func (r *ImageRepair) removeStaleTemps(ctx context.Context, targets map[string]string) bool {
	inflight, ok := r.store.(interface{ ImportInFlight(name string) bool })
	if !ok {
		return false
	}
	var stale []string
	for n := range targets {
		if strings.HasPrefix(n, importTempPrefix) && !inflight.ImportInFlight(n) {
			stale = append(stale, n)
		}
	}
	if len(stale) == 0 {
		return false
	}
	sort.Strings(stale)
	if err := r.store.DeleteImages(ctx, stale...); err != nil {
		logging.Warnf("angryduck-worker[%s]: image repair: removing leftover temporary images %v: %v", r.nodeID, stale, err)
		return false
	}
	tempImagesRemovedTotal.Add(int64(len(stale)), r.nodeID)
	logging.Infof("angryduck-worker[%s]: image repair: removed %d leftover temporary image name(s) of an interrupted import", r.nodeID, len(stale))
	return true
}
