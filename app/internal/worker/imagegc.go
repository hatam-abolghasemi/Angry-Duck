package worker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"angryduck/internal/imageref"
	"angryduck/internal/logging"
	"angryduck/internal/metrics"
)

var (
	imagesDeletedTotal = metrics.NewCounterVec(
		"angryduck_worker_gc_images_deleted_total",
		"Images this node's cleanup removed, by tier: old (unused for GC_UNUSED_FOR_S, removed whatever the disk), pressure (unused for less, removed only because the disk went above GC_HIGH), rollback (one of the newest GC_ROLLBACK_KEEP of its repo, removed only while the disk stays above GC_HIGH), or error.",
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
	gcCandidates = metrics.NewGaugeVec(
		"angryduck_worker_gc_candidates",
		"Images the cleanup could remove, by tier (old, pressure, rollback). old goes on its own; pressure and rollback only while cleaning.",
		"node", "tier",
	)
)

// ImageStore is what the cleanup needs from containerd. ContainerdStore has it.
type ImageStore interface {
	ImageTargets(ctx context.Context) (map[string]string, error)
	DeleteImages(ctx context.Context, names ...string) error
}

// GCConfig holds the cleanup's tunables.
type GCConfig struct {
	Interval          time.Duration // how often usage is sampled and the disk checked
	High, Low         float64       // start above High, stop at Low (0-1)
	UnusedFor         time.Duration // an image is removable once no container used it for this long
	RollbackKeep      int           // newest removable images per repo kept until nothing else is left
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
//   - it has been unused for UnusedFor (6h by default): no container
//     on this node used it for that long, counting from when
//     this node got it if it never ran here. This happens whatever the
//     disk usage;
//   - the disk is above High (70%): unused images go oldest first, however
//     recently they ran, until the disk is back down to Low (60%).
//
// Per repo, the newest RollbackKeep (3) unused images are kept for a quick
// rollback: the age rule never removes them, and disk pressure removes
// them only after every other candidate is gone and the disk is still
// above High. Removal happens in small batches, re-measuring the disk
// after each while under pressure.
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

	// Re-pull loop detection, bounded by what was removed in the last
	// returnWindow: removed holds images this cleanup removed recently,
	// returned those that came back.
	removed  map[string]removal // target digest -> when, under which name
	returned map[string]*churn  // target digest -> how often it came back
	churnPub bool               // returned changed since last published
}

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

// Run blocks, ticking until ctx is done.
func (g *ImageGC) Run(ctx context.Context) {
	logging.Infof("angryduck-worker[%s]: image cleanup started: interval=%s high=%.2f low=%.2f unused_for=%s rollback_keep=%d batch=%d protect=%v state=%s",
		g.nodeID, g.cfg.Interval, g.cfg.High, g.cfg.Low, g.cfg.UnusedFor, g.cfg.RollbackKeep, g.cfg.Batch, g.cfg.ProtectSubstrings, g.cfg.StatePath)
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
	repo   string
	age    time.Duration
	tier   string // old, pressure or rollback
}

// sample updates usage times and returns the names per digest, and which
// digests are in use.
func (g *ImageGC) sample(ctx context.Context, now time.Time) (groups map[string][]string, inUse map[string]bool, err error) {
	targets, err := g.store.ImageTargets(ctx)
	if err != nil {
		return nil, nil, err
	}
	used, err := g.inUse()
	if err != nil {
		return nil, nil, err // without it nothing is safe to judge
	}
	groups = map[string][]string{}
	for name, d := range targets {
		groups[d] = append(groups[d], name)
	}
	inUse = map[string]bool{}
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
	return groups, inUse, nil
}

// Tick samples usage, removes images unused for UnusedFor, and, if the
// disk is too full, removes more, oldest first.
func (g *ImageGC) Tick(ctx context.Context) {
	now := time.Now()
	groups, inUse, err := g.sample(ctx, now)
	if err != nil {
		logging.Warnf("angryduck-worker[%s]: image cleanup: %v", g.nodeID, err)
		return
	}
	defer g.save(false)
	defer g.publishChurn(now)

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
		logging.Infof("angryduck-worker[%s]: image cleanup: disk at %.1f%%, done", g.nodeID, u*100)
	}
	cleaning := g.cleaning && uerr == nil
	g.mu.Unlock()
	if cleaning {
		gcCleaning.Set(1, g.nodeID)
	} else {
		gcCleaning.Set(0, g.nodeID)
	}

	unused, rollback := g.plan(groups, inUse, now)
	var old []candidate
	for _, c := range unused {
		if c.tier == "old" {
			old = append(old, c)
		}
	}
	gcCandidates.Set(float64(len(old)), g.nodeID, "old")
	gcCandidates.Set(float64(len(unused)-len(old)), g.nodeID, "pressure")
	gcCandidates.Set(float64(len(rollback)), g.nodeID, "rollback")

	var removed int
	if !cleaning {
		// Age rule only: everything unused for UnusedFor goes.
		if len(old) > 0 {
			logging.Infof("angryduck-worker[%s]: image cleanup: removing %d image(s) unused for %s", g.nodeID, len(old), g.cfg.UnusedFor)
		}
		removed, _ = g.remove(ctx, old, nil, nil)
	} else {
		// Disk pressure: unused images oldest first (the age-rule ones are
		// the oldest, so they go first), down to Low; then rollback images,
		// only while still above High.
		removed, ok := g.remove(ctx, unused, &u, func(u float64) bool { return u <= g.cfg.Low })
		if ok {
			var n int
			n, ok = g.remove(ctx, rollback, &u, func(u float64) bool { return u < g.cfg.High })
			removed += n
		}
		if ok && removed == 0 && u >= g.cfg.High {
			logging.Warnf("angryduck-worker[%s]: image cleanup: disk at %.1f%% but nothing is removable (every image is in use, protected or being pulled)", g.nodeID, u*100)
		}
	}
	if removed > 0 && g.onDone != nil {
		g.onDone()
	}
}

// remove deletes list in batches, oldest first. With u set, it stops once
// stop(*u) holds, re-measuring the disk into *u after each batch. It
// returns how many images went and false if it had to give up (a failed
// delete or disk reading, or ctx done).
func (g *ImageGC) remove(ctx context.Context, list []candidate, u *float64, stop func(float64) bool) (int, bool) {
	removed := 0
	for len(list) > 0 {
		if u != nil && stop(*u) {
			break
		}
		n := g.cfg.Batch
		if n > len(list) {
			n = len(list)
		}
		batch := list[:n]
		list = list[n:]
		var names []string
		for _, c := range batch {
			names = append(names, c.names...)
		}
		if err := g.store.DeleteImages(ctx, names...); err != nil {
			logging.Warnf("angryduck-worker[%s]: image cleanup: removing %v: %v", g.nodeID, names, err)
			imagesDeletedTotal.Add(int64(len(batch)), g.nodeID, "error")
			return removed, false
		}
		removed += len(batch)
		now := time.Now()
		g.mu.Lock()
		for _, c := range batch {
			g.removed[c.digest] = removal{name: displayName(c.names), at: now}
		}
		g.mu.Unlock()
		for _, c := range batch {
			imagesDeletedTotal.Add(1, g.nodeID, c.tier)
			logging.Infof("angryduck-worker[%s]: image cleanup: removed %s (%s tier, unused for %s)", g.nodeID, strings.Join(c.names, " "), c.tier, c.age.Round(time.Minute))
		}
		if u == nil && len(list) == 0 {
			break // nothing to measure and nothing left to pace
		}
		select { // let containerd's GC free the space before measuring
		case <-ctx.Done():
			return removed, false
		case <-time.After(g.cfg.Settle):
		}
		if u != nil {
			v, err := g.util()
			if err != nil {
				return removed, false
			}
			*u = v
		}
	}
	return removed, true
}

// plan returns every image that isn't in use, protected or busy, oldest
// first, split in two: the newest RollbackKeep per repo (rollback tier)
// and the rest (old tier if unused for UnusedFor, else pressure tier).
func (g *ImageGC) plan(groups map[string][]string, inUse map[string]bool, now time.Time) (unused, rollback []candidate) {
	g.mu.Lock()
	defer g.mu.Unlock()
	byRepo := map[string][]candidate{}
	for d, names := range groups {
		if inUse[d] || g.protected(names) || (g.busy != nil && g.busy(names)) {
			continue
		}
		last := g.firstSeen[d]
		if u := g.lastUsed[d]; u.After(last) {
			last = u
		}
		sort.Strings(names)
		c := candidate{digest: d, names: names, repo: repoOf(names, d), age: now.Sub(last)}
		byRepo[c.repo] = append(byRepo[c.repo], c)
	}
	for _, cs := range byRepo {
		sort.Slice(cs, func(i, j int) bool { return cs[i].age < cs[j].age }) // newest first
		k := g.cfg.RollbackKeep
		if k < 0 {
			k = 0
		}
		if k > len(cs) {
			k = len(cs)
		}
		for i := range cs {
			switch {
			case i < k:
				cs[i].tier = "rollback"
				rollback = append(rollback, cs[i])
			case cs[i].age >= g.cfg.UnusedFor:
				cs[i].tier = "old"
				unused = append(unused, cs[i])
			default:
				cs[i].tier = "pressure"
				unused = append(unused, cs[i])
			}
		}
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
	oldestFirst(rollback)
	return unused, rollback
}

func (g *ImageGC) protected(names []string) bool {
	for _, n := range names {
		if strings.HasPrefix(n, baseImagePrefix) {
			return true // a rescue's temporary base image
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

// repoOf names the repo an image belongs to, for the rollback tier.
func repoOf(names []string, digest string) string {
	for _, n := range names {
		if !strings.HasPrefix(n, "sha256:") {
			if r := imageref.Repo(n); r != "" {
				return r
			}
		}
	}
	return digest
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
