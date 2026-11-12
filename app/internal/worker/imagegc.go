package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"angryduck/internal/imageref"
	"angryduck/internal/logging"
	"angryduck/internal/memlimit"
	"angryduck/internal/metrics"
	"angryduck/internal/model"
	"angryduck/internal/sharedtoken"
)

var (
	imagesDeletedTotal = metrics.NewCounterVec(
		"angryduck_worker_gc_images_deleted_total",
		"Images this node's cleanup removed, by tier: old (unused for GC_UNUSED_FOR_S, removed whatever the disk), pressure (unused for less, removed only because the disk went above GC_HIGH), companion (a companion image, removed under disk pressure after every other unused image), retention (the cluster's retention copy, removed when its GC_RETENTION_TTL_S ran out), or error.",
		"node", "tier",
	)
	gcImageReturns = metrics.NewGaugeVec(
		"angryduck_worker_gc_image_returns",
		"Images that came back to this node within an hour of this cleanup removing them, and how many times: something on the node still uses them, so each removal costs a re-pull. Only images that came back in the last hour are listed. image is the tag, else repo@digest, else the image ID.",
		"node", "image",
	)
	gcCleaning = metrics.NewGaugeVec(
		"angryduck_worker_gc_cleaning",
		"1 while this node's disk-pressure cleanup is active: disk went above GC_HIGH and hasn't come down to GC_LOW yet.",
		"node",
	)
	gcStalled = metrics.NewGaugeVec(
		"angryduck_worker_gc_stalled",
		"1 while this node's disk-pressure cleanup has stopped because removing images didn't free space (the disk is filled by something else: running images, volumes, logs). The GC_UNUSED_FOR_S rule and expiring retention copies still go; pressure removals resume after an hour, or once the disk grows by 1%.",
		"node",
	)
	gcImageRemoved = metrics.NewRecentGaugeVec(
		"angryduck_worker_gc_image_removed_timestamp_seconds",
		"When this node's cleanup removed the image, as Unix time, by tier. An event, shown for METRICS_EVENT_TTL_S. image is the tag, else repo@digest, else the image ID.",
		"node", "image", "tier",
	)
	gcImageRemoval = metrics.NewGaugeVec(
		"angryduck_worker_gc_image_removal_seconds",
		"Images unused on this node that the cleanup will remove, and in how many seconds (0: at the next check), by tier: old or pressure (the age rule; disk pressure removes them sooner, oldest first), or retention (this node holds the cluster's retention copy; removed when it expires, never for disk pressure).",
		"node", "image", "tier",
	)
	gcCandidates = metrics.NewGaugeVec(
		"angryduck_worker_gc_candidates",
		"Images the cleanup could remove, by tier (old, pressure, companion). old goes on its own; pressure only while cleaning; companion only while cleaning, after every pressure one.",
		"node", "tier",
	)
	gcRetained = metrics.NewGaugeVec(
		"angryduck_worker_gc_retained_images",
		"Images this node keeps as the cluster's retention copy (labeled "+RetainLabel+"), not yet expired. The cleanup leaves them alone, disk pressure included, until they expire.",
		"node",
	)
)

// RetainLabel marks an image as the cluster's retention copy, on the one
// node the controller picked. Its value is when the copy expires, in Unix
// seconds. The cleanup never removes a marked image before then, whatever
// the disk, and removes it right after.
const RetainLabel = "angryduck.io/last-survivor"

// ImageStore is what the cleanup needs from containerd. ContainerdStore has it.
type ImageStore interface {
	// ImageTargetsLabeled maps every image name to its target digest and,
	// from the same listing, every target carrying label to its value.
	ImageTargetsLabeled(ctx context.Context, label string) (targets, labeled map[string]string, err error)
	DeleteImages(ctx context.Context, names ...string) error
	// SetImageLabel sets label on names, or removes it with value "".
	SetImageLabel(ctx context.Context, label, value string, names ...string) error
}

// GCConfig holds the cleanup's tunables.
type GCConfig struct {
	Interval          time.Duration // how often usage is sampled and the disk checked
	High, Low         float64       // start above High, stop at Low (0-1)
	UnusedFor         time.Duration // an image is removable once no container used it for this long
	Retention         bool          // honor RetainLabel and report image state (GC_RETENTION_ENABLED)
	Batch             int           // images removed before re-measuring the disk
	Settle            time.Duration // wait after a batch for containerd to free the space
	ProtectSubstrings []string      // never remove images whose name contains one of these
	StatePath         string        // where usage times survive restarts ("" = memory only)
}

// ImageGC is this node's image cleanup. It replaces kubelet's
// disk-pressure GC with a gentler one that knows more. An image in use on
// this node is never touched: one with a running container, or with a
// container of a pod that is still up (a crashlooping container is exited
// most of the time, yet kubelet restarts it from that image); neither is a protected
// image or one being pulled or received. Any other image is removed when
// EITHER of these holds:
//
//   - it has been unused for UnusedFor (1h by default): no container
//     on this node used it for that long, counting from when
//     this node got it if it never ran here. This happens whatever the
//     disk usage;
//   - the disk is above High (70%): unused images go oldest first, however
//     recently they ran, until the disk is back down to Low (40%).
//
// Removal happens in small batches, re-measuring the disk after each
// while under pressure.
//
// Retention (with Retention on): an image carrying RetainLabel is the
// cluster's one retention copy of an image nothing runs anymore, put here
// by the controller. Neither rule above touches it, disk pressure
// included. It goes as soon as the label's time is up, and loses the
// label if a container here starts using it again.
//
// Every name of an image (tag, repo@digest, image ID) goes together, so
// containerd's own garbage collector really frees its layers; layers
// shared with images that stay are kept. Usage times persist in
// StatePath, so a worker restart doesn't make old images look new.
type ImageGC struct {
	store  ImageStore
	inUse  func() ([]string, error)  // images in use on this node (any alias form)
	util   func() (float64, error)   // root filesystem utilization, 0-1
	busy   func(names []string) bool // an image being pulled or received right now
	nodeID string
	cfg    GCConfig
	onDone func()

	mu        sync.Mutex
	firstSeen map[string]time.Time // target digest -> first seen here
	lastUsed  map[string]time.Time // target digest -> last seen in use here
	cleaning  bool
	dirty     bool
	savedAt   time.Time

	// Disk-pressure cleanup gives up while removing images frees nothing:
	// until stallUntil, unless the disk grows past stallAt+gcStallRegrow.
	stallUntil time.Time
	stallAt    float64

	// Re-pull loop detection, bounded by what was removed in the last
	// returnWindow: removed holds images this cleanup removed recently,
	// returned those that came back.
	removed  map[string]removal // target digest -> when, under which name
	returned map[string]*churn  // target digest -> how often it came back
	churnPub bool               // returned changed since last published

	// Retention: retained is every target carrying RetainLabel and when
	// its copy expires, from the last listing plus the controller's orders
	// since. inUseNow is the last sample's in-use targets. Both under mu.
	retained map[string]time.Time
	inUseNow map[string]bool
	// orders is the controller's retain orders not yet sure to be in a
	// listing: a listing started before an order must not undo it.
	orders map[string]retainOrder
	// What was last handed to onState, to hand it again only on a change.
	pubInUse    map[string]string // target digest -> name
	pubRetained map[string]int64
	onState     func(inUse []model.ImageRef, retained map[string]int64)
	pubMu       sync.Mutex // one publish at a time, so they arrive in order

	comp *gcCompanions // nil: companions off (see EnableCompanions)
}

// A batch that frees less than gcMinFree of the disk means the disk isn't
// full of removable images: cleanup stops for gcStallFor, or until the
// disk grows by gcStallRegrow (new images arrived, maybe removable ones).
const (
	gcMinFree     = 0.002
	gcStallFor    = time.Hour
	gcStallRegrow = 0.01
)

// returnWindow is how long a removed image is watched for coming back,
// and how long one that came back stays in gcImageReturns.
const returnWindow = time.Hour

type removal struct {
	name string
	at   time.Time
}

type churn struct {
	name  string
	count int
	last  time.Time
}

// NewImageGC builds the cleanup and loads saved usage times.
func NewImageGC(store ImageStore, inUse func() ([]string, error), util func() (float64, error), busy func([]string) bool, nodeID string, cfg GCConfig) *ImageGC {
	for _, tier := range []string{"old", "pressure", "error"} {
		imagesDeletedTotal.Add(0, nodeID, tier)
	}
	for _, tier := range []string{"old", "pressure"} {
		gcCandidates.Set(0, nodeID, tier)
	}
	if cfg.Retention {
		imagesDeletedTotal.Add(0, nodeID, "retention")
		gcRetained.Set(0, nodeID)
	}
	gcCleaning.Set(0, nodeID)
	gcStalled.Set(0, nodeID)
	if cfg.Batch < 1 {
		cfg.Batch = 5
	}
	g := &ImageGC{store: store, inUse: inUse, util: util, busy: busy, nodeID: nodeID, cfg: cfg,
		firstSeen: map[string]time.Time{}, lastUsed: map[string]time.Time{},
		removed: map[string]removal{}, returned: map[string]*churn{}}
	g.load()
	return g
}

// OnDone registers fn to run after images were removed (the reporter's
// Kick, so the controller sees the freed space right away).
func (g *ImageGC) OnDone(fn func()) { g.onDone = fn }

// OnState registers fn to receive this node's in-use images and retention
// copies whenever either changes (the reporter's SetImageState). Only
// with Retention on. Call before Run.
func (g *ImageGC) OnState(fn func(inUse []model.ImageRef, retained map[string]int64)) {
	g.onState = fn
}

// Run blocks, ticking until ctx is done.
func (g *ImageGC) Run(ctx context.Context) {
	logging.Infof("angryduck-worker[%s]: image cleanup started: interval=%s high=%.2f low=%.2f unused_for=%s retention=%t batch=%d protect=%v state=%s",
		g.nodeID, g.cfg.Interval, g.cfg.High, g.cfg.Low, g.cfg.UnusedFor, g.cfg.Retention, g.cfg.Batch, g.cfg.ProtectSubstrings, g.cfg.StatePath)
	t := time.NewTicker(g.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			g.save(true)
			return
		case <-t.C:
			g.Tick(ctx)
		}
	}
}

// candidate is one removable image: every name pointing at one digest.
type candidate struct {
	digest string
	names  []string
	age    time.Duration // unused for this long; for retention, expired for this long
	tier   string        // old, pressure or retention
}

// sample updates usage times and returns the names per digest, and which
// digests are in use. With Retention on it also refreshes g.retained from
// the same listing.
func (g *ImageGC) sample(ctx context.Context, now time.Time) (groups map[string][]string, inUse map[string]bool, err error) {
	label := ""
	if g.cfg.Retention {
		label = RetainLabel
	}
	listedAt := time.Now()
	targets, labeled, err := g.store.ImageTargetsLabeled(ctx, label)
	if err != nil {
		return nil, nil, err
	}
	used, pods, err := g.listInUse()
	if err != nil {
		return nil, nil, err // without it nothing is safe to judge
	}
	groups = namesByTarget(targets, false)
	inUse = make(map[string]bool, len(used))
	for _, r := range used {
		if d, ok := targets[r]; ok {
			inUse[d] = true
		} else if d, ok := targets[imageref.Normalize(r)]; ok {
			inUse[d] = true
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for d := range groups {
		if _, ok := g.firstSeen[d]; !ok {
			g.firstSeen[d], g.dirty = now, true
			if r, ok := g.removed[d]; ok {
				g.cameBackLocked(d, r, now)
			}
		}
		if inUse[d] {
			g.lastUsed[d] = now
		}
	}
	for d := range g.firstSeen {
		if _, ok := groups[d]; !ok {
			delete(g.firstSeen, d)
			delete(g.lastUsed, d)
			g.dirty = true
		}
	}
	g.noteCompanionsLocked(pods, targets, groups)
	if g.cfg.Retention {
		g.inUseNow = inUse
		var retained map[string]time.Time
		for d, v := range labeled {
			sec, err := strconv.ParseInt(v, 10, 64)
			if err != nil || sec <= 0 {
				continue // not ours to judge: an ordinary image
			}
			if retained == nil {
				retained = make(map[string]time.Time, len(labeled))
			}
			retained[d] = time.Unix(sec, 0)
		}
		// Orders that came in while listing win over it; older ones are
		// in it.
		for d, o := range g.orders {
			if !o.at.After(listedAt) {
				delete(g.orders, d)
				continue
			}
			if o.until.IsZero() {
				delete(retained, d)
				continue
			}
			if retained == nil {
				retained = map[string]time.Time{}
			}
			retained[d] = o.until
		}
		g.retained = retained
	}
	return groups, inUse, nil
}

// namesByTarget returns the names of each target digest, sorted, without
// Angry Duck's own temporary ones if skipTemp. Most images have two or
// three names (tag, repo@digest, ID): every name goes into one backing
// array, cut per digest, rather than a slice each.
func namesByTarget(targets map[string]string, skipTemp bool) map[string][]string {
	type pair struct{ d, n string }
	pairs := make([]pair, 0, len(targets))
	for n, d := range targets {
		if skipTemp && strings.HasPrefix(n, tempImageHost) {
			continue
		}
		pairs = append(pairs, pair{d, n})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].d != pairs[j].d {
			return pairs[i].d < pairs[j].d
		}
		return pairs[i].n < pairs[j].n
	})
	names := make([]string, len(pairs))
	groups := make(map[string][]string, len(pairs)/2+1)
	for i := 0; i < len(pairs); {
		j := i
		for j < len(pairs) && pairs[j].d == pairs[i].d {
			names[j] = pairs[j].n
			j++
		}
		groups[pairs[i].d] = names[i:j:j] // full slice: an append can't spill into the next digest's names
		i = j
	}
	return groups
}

// Tick samples usage, removes expired retention copies and images unused
// for UnusedFor, and, if the disk is too full, removes more, oldest first.
func (g *ImageGC) Tick(ctx context.Context) {
	defer memlimit.Soon()
	now := time.Now()
	groups, inUse, err := g.sample(ctx, now)
	if err != nil {
		logging.Warnf("angryduck-worker[%s]: image cleanup: %v", g.nodeID, err)
		return
	}
	defer g.save(false)
	defer g.publishChurn(now)

	if g.cfg.Retention {
		g.unretainInUse(ctx, groups, inUse)
		g.publishState(groups, inUse)
	}
	g.publishCompanions(groups)

	// Disk pressure is judged only with a fresh reading; the age rule
	// doesn't need one.
	u, uerr := g.util()
	if uerr != nil {
		logging.Warnf("angryduck-worker[%s]: image cleanup: reading disk usage: %v", g.nodeID, uerr)
	}
	g.mu.Lock()
	switch {
	case uerr != nil:
	case u >= g.cfg.High && !g.cleaning:
		g.cleaning = true
		logging.Infof("angryduck-worker[%s]: image cleanup: disk at %.1f%%, above %.0f%%: removing unused images, oldest first, until %.0f%%",
			g.nodeID, u*100, g.cfg.High*100, g.cfg.Low*100)
	case u <= g.cfg.Low && g.cleaning:
		g.cleaning = false
		g.stallUntil = time.Time{}
		logging.Infof("angryduck-worker[%s]: image cleanup: disk at %.1f%%, done", g.nodeID, u*100)
	}
	cleaning := g.cleaning && uerr == nil
	stalled := cleaning && now.Before(g.stallUntil) && u < g.stallAt+gcStallRegrow
	if cleaning && !stalled && !g.stallUntil.IsZero() {
		logging.Infof("angryduck-worker[%s]: image cleanup: disk at %.1f%%, trying removals again", g.nodeID, u*100)
		g.stallUntil = time.Time{}
	}
	g.mu.Unlock()
	if stalled {
		gcStalled.Set(1, g.nodeID)
	} else {
		gcStalled.Set(0, g.nodeID)
	}
	if cleaning {
		gcCleaning.Set(1, g.nodeID)
	} else {
		gcCleaning.Set(0, g.nodeID)
	}

	unused, expired, kept := g.plan(groups, inUse, now)
	var old []candidate
	companions := 0
	for _, c := range unused {
		switch c.tier {
		case "old":
			old = append(old, c)
		case "companion":
			companions++
		}
	}
	gcImageRemoval.Reset()
	for _, c := range unused {
		if c.tier == "companion" {
			continue // no deadline: only disk pressure removes it
		}
		left := (g.cfg.UnusedFor - c.age).Seconds()
		if left < 0 {
			left = 0
		}
		gcImageRemoval.Set(left, g.nodeID, displayName(c.names), c.tier)
	}
	for _, k := range kept {
		gcImageRemoval.Set(k.left.Seconds(), g.nodeID, displayName(k.names), "retention")
	}
	gcCandidates.Set(float64(len(old)), g.nodeID, "old")
	gcCandidates.Set(float64(len(unused)-len(old)-companions), g.nodeID, "pressure")
	if g.comp != nil {
		gcCandidates.Set(float64(companions), g.nodeID, "companion")
	}
	if g.cfg.Retention {
		gcRetained.Set(float64(len(kept)), g.nodeID)
	}

	// An expired retention copy goes whatever the disk: it was kept only
	// for its time.
	removed, _, _ := g.remove(ctx, expired, nil, nil)
	if !cleaning || stalled {
		// Age rule only: everything unused for UnusedFor goes. A fresh
		// worker first waits for the cluster's companion images.
		if g.holdAgeRule(now) {
			old = nil
		}
		if len(old) > 0 {
			logging.Infof("angryduck-worker[%s]: image cleanup: removing %d image(s) unused for %s", g.nodeID, len(old), g.cfg.UnusedFor)
		}
		n, _, _ := g.remove(ctx, old, nil, nil)
		removed += n
	} else {
		// Disk pressure: unused images oldest first (the age-rule ones are
		// the oldest, so they go first), companion images last, down to
		// Low. It stops as soon as a batch frees (almost) nothing.
		// Retention copies are never part of it.
		start := u
		n, ok, futile := g.remove(ctx, unused, &u, func(u float64) bool { return u <= g.cfg.Low })
		removed += n
		switch {
		case futile:
			g.stall(now, u)
			logging.Warnf("angryduck-worker[%s]: image cleanup: removed %d image(s) but the disk only went from %.1f%% to %.1f%%: it's filled by something else (running images, volumes, logs). Leaving the rest; trying again in %s or once the disk grows by %.0f%%",
				g.nodeID, n, start*100, u*100, gcStallFor, gcStallRegrow*100)
		case ok && n == 0 && u >= g.cfg.High:
			g.stall(now, u)
			logging.Warnf("angryduck-worker[%s]: image cleanup: disk at %.1f%% but nothing is removable (every image is in use, protected, being pulled or a retention copy); looking again in %s or once the disk grows by %.0f%%",
				g.nodeID, u*100, gcStallFor, gcStallRegrow*100)
		}
	}
	if removed > 0 && g.onDone != nil {
		g.onDone()
	}
}

// stall stops disk-pressure removals (see gcStallFor).
func (g *ImageGC) stall(now time.Time, u float64) {
	g.mu.Lock()
	g.stallUntil, g.stallAt = now.Add(gcStallFor), u
	g.mu.Unlock()
	gcStalled.Set(1, g.nodeID)
}

// remove deletes list in batches, oldest first. With u set, it stops once
// stop(*u) holds, re-measuring the disk into *u after each batch, and
// stops with futile=true when a batch freed less than gcMinFree. It
// returns how many images went and false if it had to give up (a failed
// delete or disk reading, or ctx done).
func (g *ImageGC) remove(ctx context.Context, list []candidate, u *float64, stop func(float64) bool) (removed int, ok, futile bool) {
	for len(list) > 0 {
		if u != nil && stop(*u) {
			break
		}
		n := g.cfg.Batch
		if n > len(list) {
			n = len(list)
		}
		batch := g.notRetained(list[:n])
		list = list[n:]
		if len(batch) == 0 {
			continue
		}
		var names []string
		for _, c := range batch {
			names = append(names, c.names...)
		}
		if err := g.store.DeleteImages(ctx, names...); err != nil {
			logging.Warnf("angryduck-worker[%s]: image cleanup: removing %v: %v", g.nodeID, names, err)
			imagesDeletedTotal.Add(int64(len(batch)), g.nodeID, "error")
			return removed, false, false
		}
		removed += len(batch)
		now := time.Now()
		g.mu.Lock()
		for _, c := range batch {
			g.removed[c.digest] = removal{name: displayName(c.names), at: now}
			if c.tier == "retention" {
				delete(g.retained, c.digest)
			}
		}
		g.mu.Unlock()
		for _, c := range batch {
			imagesDeletedTotal.Add(1, g.nodeID, c.tier)
			gcImageRemoved.Set(float64(now.Unix()), g.nodeID, displayName(c.names), c.tier)
			gcImageRemoval.Delete(g.nodeID, displayName(c.names), c.tier)
			if c.tier == "retention" {
				logging.Infof("angryduck-worker[%s]: image cleanup: removed %s (retention copy, expired %s ago)", g.nodeID, strings.Join(c.names, " "), c.age.Round(time.Second))
			} else {
				logging.Infof("angryduck-worker[%s]: image cleanup: removed %s (%s tier, unused for %s)", g.nodeID, strings.Join(c.names, " "), c.tier, c.age.Round(time.Minute))
			}
		}
		if u == nil && len(list) == 0 {
			break // nothing to measure and nothing left to pace
		}
		select { // let containerd's GC free the space before measuring
		case <-ctx.Done():
			return removed, false, false
		case <-time.After(g.cfg.Settle):
		}
		if u != nil {
			v, err := g.util()
			if err != nil {
				return removed, false, false
			}
			before := *u
			*u = v
			if before-v < gcMinFree && !stop(v) {
				return removed, true, true
			}
		}
	}
	return removed, true, false
}

// notRetained drops from batch every image that became a retention copy
// after the plan was made (an order from the controller in between):
// the last look before anything is removed.
func (g *ImageGC) notRetained(batch []candidate) []candidate {
	if !g.cfg.Retention {
		return batch
	}
	now := time.Now()
	g.mu.Lock()
	defer g.mu.Unlock()
	out := batch[:0:0]
	for _, c := range batch {
		if until, ok := g.retained[c.digest]; ok && now.Before(until) {
			continue
		}
		out = append(out, c)
	}
	return out
}

// retainOrder is one order from the controller, as HandleRetain applied it.
type retainOrder struct {
	at    time.Time
	until time.Time // zero: unmarked
}

// keptCopy is a retention copy not yet expired, for the metrics.
type keptCopy struct {
	names []string
	left  time.Duration
}

// plan returns every image that isn't in use, protected or busy, oldest
// first: unused (old tier if unused for UnusedFor, else pressure tier) and
// expired retention copies. Retention copies not yet expired are in
// neither; kept lists them.
func (g *ImageGC) plan(groups map[string][]string, inUse map[string]bool, now time.Time) (unused, expired []candidate, kept []keptCopy) {
	g.mu.Lock()
	defer g.mu.Unlock()
	unused = make([]candidate, 0, len(groups)-len(inUse))
	var companions []candidate
	for d, names := range groups {
		if inUse[d] || g.protected(names) || (g.busy != nil && g.busy(names)) {
			continue
		}
		if until, ok := g.retained[d]; ok {
			if now.Before(until) {
				kept = append(kept, keptCopy{names: names, left: until.Sub(now)})
			} else {
				expired = append(expired, candidate{digest: d, names: names, age: now.Sub(until), tier: "retention"})
			}
			continue
		}
		last := g.firstSeen[d]
		if u := g.lastUsed[d]; u.After(last) {
			last = u
		}
		age := now.Sub(last)
		if g.isCompanionLocked(d) {
			companions = append(companions, candidate{digest: d, names: names, age: age, tier: "companion"})
			continue
		}
		tier := "pressure"
		if age >= g.cfg.UnusedFor {
			tier = "old"
		}
		unused = append(unused, candidate{digest: d, names: names, age: age, tier: tier})
	}
	oldestFirst := func(cs []candidate) {
		sort.Slice(cs, func(i, j int) bool {
			if cs[i].age != cs[j].age {
				return cs[i].age > cs[j].age
			}
			return cs[i].digest < cs[j].digest
		})
	}
	oldestFirst(unused)
	oldestFirst(expired)
	oldestFirst(companions)
	unused = append(unused, companions...) // disk pressure takes them last
	return unused, expired, kept
}

// unretainInUse takes RetainLabel off every image a container here uses
// again: it is no longer a copy kept for nothing.
func (g *ImageGC) unretainInUse(ctx context.Context, groups map[string][]string, inUse map[string]bool) {
	g.mu.Lock()
	var back []string
	for d := range g.retained {
		if inUse[d] {
			back = append(back, d)
		}
	}
	g.mu.Unlock()
	for _, d := range back {
		if err := g.store.SetImageLabel(ctx, RetainLabel, "", groups[d]...); err != nil {
			logging.Warnf("angryduck-worker[%s]: image cleanup: unmarking retention copy %s: %v", g.nodeID, d, err)
			continue
		}
		g.mu.Lock()
		delete(g.retained, d)
		g.mu.Unlock()
		logging.Infof("angryduck-worker[%s]: image cleanup: %s is in use here again: no longer a retention copy", g.nodeID, displayName(groups[d]))
	}
}

// publishState hands onState this node's in-use images and retention
// copies, only when either changed since the last time.
func (g *ImageGC) publishState(groups map[string][]string, inUse map[string]bool) {
	if g.onState == nil {
		return
	}
	g.pubMu.Lock()
	defer g.pubMu.Unlock()
	g.mu.Lock()
	same := len(inUse) == len(g.pubInUse) && len(g.retained) == len(g.pubRetained)
	if same {
		for d := range inUse {
			if _, ok := g.pubInUse[d]; !ok {
				same = false
				break
			}
		}
	}
	if same {
		for d, t := range g.retained {
			if g.pubRetained[d] != t.Unix() {
				same = false
				break
			}
		}
	}
	if same {
		g.mu.Unlock()
		return
	}
	pubInUse := make(map[string]string, len(inUse))
	refs := make([]model.ImageRef, 0, len(inUse))
	for d := range inUse {
		name := g.pubInUse[d] // an unchanged image keeps its name
		if name == "" {
			name = displayName(groups[d]) // sorted by namesByTarget
		}
		pubInUse[d] = name
		refs = append(refs, model.ImageRef{Digest: d, Name: name})
	}
	var retained map[string]int64
	if len(g.retained) > 0 {
		retained = make(map[string]int64, len(g.retained))
		for d, t := range g.retained {
			retained[d] = t.Unix()
		}
	}
	g.pubInUse, g.pubRetained = pubInUse, retained
	fn := g.onState
	g.mu.Unlock()
	sort.Slice(refs, func(i, j int) bool { return refs[i].Digest < refs[j].Digest })
	fn(refs, retained)
}

// Register mounts /images/retain, the controller's retention orders,
// behind the shared token. Only with Retention on.
func (g *ImageGC) Register(token string) func(*http.ServeMux) {
	return func(mux *http.ServeMux) {
		mux.HandleFunc("/images/retain", sharedtoken.Require(token, g.HandleRetain))
	}
}

// HandleRetain marks an image as the cluster's retention copy until the
// order's time, or unmarks it (Until 0). Every name of the digest is
// marked, so the mark goes wherever the image goes.
func (g *ImageGC) HandleRetain(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !g.cfg.Retention {
		http.Error(w, "retention is off on this node", http.StatusConflict)
		return
	}
	var o model.RetainOrder
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10)).Decode(&o); err != nil || !strings.HasPrefix(o.Digest, "sha256:") || o.Until < 0 {
		http.Error(w, "invalid order", http.StatusBadRequest)
		return
	}
	targets, _, err := g.store.ImageTargetsLabeled(r.Context(), "")
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	var names []string
	for n, d := range targets {
		if d == o.Digest {
			names = append(names, n)
		}
	}
	if len(names) == 0 {
		if o.Until == 0 {
			w.WriteHeader(http.StatusOK) // nothing to unmark
			return
		}
		http.Error(w, "no such image here", http.StatusNotFound)
		return
	}
	sort.Strings(names)
	value := ""
	if o.Until > 0 {
		value = strconv.FormatInt(o.Until, 10)
		// Recorded before the label lands: a cleanup running meanwhile
		// must not remove what is about to be kept.
		g.mu.Lock()
		if g.retained == nil {
			g.retained = map[string]time.Time{}
		}
		g.retained[o.Digest] = time.Unix(o.Until, 0)
		g.recordOrderLocked(o.Digest, time.Unix(o.Until, 0))
		g.mu.Unlock()
	}
	if err := g.store.SetImageLabel(r.Context(), RetainLabel, value, names...); err != nil {
		if o.Until > 0 {
			g.mu.Lock()
			delete(g.retained, o.Digest)
			delete(g.orders, o.Digest)
			g.mu.Unlock()
		}
		logging.Warnf("angryduck-worker[%s]: retention: marking %s: %v", g.nodeID, displayName(names), err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	g.mu.Lock()
	if o.Until == 0 {
		delete(g.retained, o.Digest)
		g.recordOrderLocked(o.Digest, time.Time{})
	}
	inUse := g.inUseNow
	g.mu.Unlock()
	if o.Until > 0 {
		logging.Infof("angryduck-worker[%s]: retention: keeping %s as the cluster's copy until %s", g.nodeID, displayName(names), time.Unix(o.Until, 0).UTC().Format(time.RFC3339))
	} else {
		logging.Infof("angryduck-worker[%s]: retention: %s is no longer the cluster's copy", g.nodeID, displayName(names))
	}
	// Tell the controller now rather than at the next cleanup tick.
	groups := map[string][]string{o.Digest: names}
	for d := range inUse {
		if d != o.Digest {
			groups[d] = nil // names only matter for images not published yet
		}
	}
	g.publishState(groups, inUse)
	w.WriteHeader(http.StatusOK)
}

// recordOrderLocked notes an order, so a listing that started before it
// can't undo it (see sample). Holds g.mu.
func (g *ImageGC) recordOrderLocked(digest string, until time.Time) {
	if g.orders == nil {
		g.orders = map[string]retainOrder{}
	}
	g.orders[digest] = retainOrder{at: time.Now(), until: until}
}

// Unretain removes RetainLabel from every image carrying it. The worker
// calls it at startup with retention off (or the cleanup off), so no
// mark outlives the feature.
func Unretain(ctx context.Context, store ImageStore, nodeID string) {
	targets, labeled, err := store.ImageTargetsLabeled(ctx, RetainLabel)
	if err != nil || len(labeled) == 0 {
		return
	}
	var names []string
	for n, d := range targets {
		if _, ok := labeled[d]; ok {
			names = append(names, n)
		}
	}
	if err := store.SetImageLabel(ctx, RetainLabel, "", names...); err != nil {
		logging.Warnf("angryduck-worker[%s]: retention off: unmarking %d image name(s): %v", nodeID, len(names), err)
		return
	}
	logging.Infof("angryduck-worker[%s]: retention off: unmarked %d retention cop(y/ies); the cleanup treats them as any other image", nodeID, len(labeled))
}

func (g *ImageGC) protected(names []string) bool {
	for _, n := range names {
		if strings.HasPrefix(n, tempImageHost) {
			return true // a rescue's base image or an import's temporary name
		}
		for _, sub := range g.cfg.ProtectSubstrings {
			if sub != "" && strings.Contains(n, sub) {
				return true
			}
		}
	}
	return false
}

// cameBackLocked records that an image this cleanup removed is back on
// the node. Holds g.mu.
func (g *ImageGC) cameBackLocked(d string, r removal, now time.Time) {
	delete(g.removed, d)
	c := g.returned[d]
	if c == nil {
		c = &churn{}
		g.returned[d] = c
	}
	c.name, c.last, g.churnPub = r.name, now, true
	c.count++
	logging.Warnf("angryduck-worker[%s]: image cleanup: %s came back %s after being removed (%d time(s) within %s): something on this node still uses it",
		g.nodeID, r.name, now.Sub(r.at).Round(time.Second), c.count, returnWindow)
}

// publishChurn drops removals and returns older than returnWindow and,
// only when the set changed, rewrites gcImageReturns. Both maps hold just
// the last hour, so neither grows with the worker's uptime.
func (g *ImageGC) publishChurn(now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for d, r := range g.removed {
		if now.Sub(r.at) > returnWindow {
			delete(g.removed, d)
		}
	}
	for d, c := range g.returned {
		if now.Sub(c.last) > returnWindow {
			delete(g.returned, d)
			g.churnPub = true
		}
	}
	if !g.churnPub {
		return
	}
	g.churnPub = false
	gcImageReturns.Reset()
	for _, c := range g.returned {
		gcImageReturns.Set(float64(c.count), g.nodeID, c.name)
	}
}

// displayName picks the name a person knows an image by, for the image
// label: a tag, else repo@digest, else the ID. names is sorted.
func displayName(names []string) string {
	var digestRef string
	for _, n := range names {
		switch {
		case strings.HasPrefix(n, "sha256:"):
		case strings.Contains(n, "@"):
			if digestRef == "" {
				digestRef = n
			}
		default:
			return n
		}
	}
	if digestRef != "" {
		return digestRef
	}
	if len(names) > 0 {
		return names[0]
	}
	return ""
}

type gcState struct {
	FirstSeen map[string]int64 `json:"first_seen"`
	LastUsed  map[string]int64 `json:"last_used"`
}

func (g *ImageGC) load() {
	if g.cfg.StatePath == "" {
		return
	}
	b, err := os.ReadFile(g.cfg.StatePath)
	if err != nil {
		return
	}
	var st gcState
	if json.Unmarshal(b, &st) != nil {
		return
	}
	for d, t := range st.FirstSeen {
		g.firstSeen[d] = time.Unix(t, 0)
	}
	for d, t := range st.LastUsed {
		g.lastUsed[d] = time.Unix(t, 0)
	}
	logging.Infof("angryduck-worker[%s]: image cleanup: loaded usage times of %d image(s) from %s", g.nodeID, len(st.FirstSeen), g.cfg.StatePath)
}

// save writes usage times when the image set changed, and otherwise at
// most every 5 minutes (last-used times only move forward; losing a few
// minutes of them errs on the side of keeping images).
func (g *ImageGC) save(force bool) {
	if g.cfg.StatePath == "" {
		return
	}
	g.mu.Lock()
	if !force && !g.dirty && time.Since(g.savedAt) < 5*time.Minute {
		g.mu.Unlock()
		return
	}
	st := gcState{FirstSeen: make(map[string]int64, len(g.firstSeen)), LastUsed: make(map[string]int64, len(g.lastUsed))}
	for d, t := range g.firstSeen {
		st.FirstSeen[d] = t.Unix()
	}
	for d, t := range g.lastUsed {
		st.LastUsed[d] = t.Unix()
	}
	g.dirty, g.savedAt = false, time.Now()
	g.mu.Unlock()
	b, _ := json.Marshal(st)
	tmp := g.cfg.StatePath + ".tmp"
	if err := os.MkdirAll(filepath.Dir(g.cfg.StatePath), 0o755); err == nil {
		if err := os.WriteFile(tmp, b, 0o644); err == nil {
			_ = os.Rename(tmp, g.cfg.StatePath)
		}
	}
}
