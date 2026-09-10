package controller

import (
	"math/rand"
	"sort"
	"sync"
	"time"

	"angryduck/internal/model"
)

// workerEntry is the controller's internal record of a single worker.
type workerEntry struct {
	NodeID      string
	Address     string
	Utilization float64
	// Repos is the set of bare repo identities (imageref.Repo) this node
	// last reported having locally, any tag. A map (rather than a slice)
	// because the ranker's only use of it is an O(1) "does this node
	// already have repo X" membership check per candidate, per preheat —
	// never iteration over the full list.
	Repos map[string]struct{}
	// Digests is the set of manifest digests this node can export to a
	// peer. Replaced wholesale by each report; extended between reports
	// by /announce as peer transfers land.
	Digests  map[string]struct{}
	LastSeen time.Time
}

// HasRepo reports whether this worker last reported having repo present
// locally under any tag.
func (w *workerEntry) HasRepo(repo string) bool {
	if repo == "" {
		return false
	}
	_, ok := w.Repos[repo]
	return ok
}

// Registry tracks all workers the controller has heard from, and the
// currently active "preheat" target image (if any).
type Registry struct {
	mu          sync.RWMutex
	workers     map[string]*workerEntry
	staleAfter  time.Duration
	targetImage string
	targetSetAt time.Time
	targetTTL   time.Duration
}

// NewRegistry builds an empty worker registry.
func NewRegistry(staleAfter, targetTTL time.Duration) *Registry {
	return &Registry{
		workers:    make(map[string]*workerEntry),
		staleAfter: staleAfter,
		targetTTL:  targetTTL,
	}
}

// Update records (or refreshes) a worker's latest report.
func (r *Registry) Update(rep model.WorkerReport) {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.workers[rep.NodeID]
	if !ok {
		w = &workerEntry{NodeID: rep.NodeID}
		r.workers[rep.NodeID] = w
	}
	w.Address = rep.Address
	w.Utilization = rep.Utilization
	w.LastSeen = rep.Timestamp

	// Rebuild rather than mutate the existing map in place: a worker that
	// removed a repo since its last report (GC'd it) must stop showing up
	// as having it, and a fresh map per report is the simplest way to
	// guarantee that without diffing old vs new. A previously-taken
	// FreshWorkers() snapshot still holds a reference to the OLD map, so
	// replacing it here is safe and never mutates data a caller is
	// concurrently reading.
	repos := make(map[string]struct{}, len(rep.Repos))
	for _, repo := range rep.Repos {
		if repo != "" {
			repos[repo] = struct{}{}
		}
	}
	w.Repos = repos

	digests := make(map[string]struct{}, len(rep.Digests))
	for _, d := range rep.Digests {
		if d != "" {
			digests[d] = struct{}{}
		}
	}
	w.Digests = digests
}

// Announce adds one digest to a known worker between reports. Unknown
// workers are ignored — they'll show up with their first report.
func (r *Registry) Announce(nodeID, digest string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.workers[nodeID]
	if !ok || digest == "" {
		return false
	}
	// Copy-on-write, same reasoning as Update: never mutate a map a
	// FreshWorkers() snapshot may be reading.
	next := make(map[string]struct{}, len(w.Digests)+1)
	for d := range w.Digests {
		next[d] = struct{}{}
	}
	next[digest] = struct{}{}
	w.Digests = next
	return true
}

// PeersFor returns up to limit addresses of fresh workers holding digest,
// excluding the asking node, in random order. Random rather than ranked:
// during a rollout dozens of nodes ask at once, and a shuffle spreads
// them over every source without the controller tracking who is busy —
// sources that are busy say so themselves (503) and requesters move on.
func (r *Registry) PeersFor(digest, excludeNode string, limit int) []string {
	r.mu.RLock()
	now := time.Now()
	var out []string
	for id, w := range r.workers {
		if id == excludeNode || !r.isFresh(w, now) {
			continue
		}
		if _, ok := w.Digests[digest]; ok {
			out = append(out, w.Address)
		}
	}
	r.mu.RUnlock()
	rand.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// isFresh reports whether a worker has reported within staleAfter of now.
// Caller must hold at least a read lock.
func (r *Registry) isFresh(w *workerEntry, now time.Time) bool {
	return now.Sub(w.LastSeen) <= r.staleAfter
}

// FreshWorkers returns all workers that have reported recently, sorted by
// ascending utilization (least-full node first).
func (r *Registry) FreshWorkers() []*workerEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	now := time.Now()
	var fresh []*workerEntry
	for _, w := range r.workers {
		if r.isFresh(w, now) {
			// copy to avoid races after unlocking
			cp := *w
			fresh = append(fresh, &cp)
		}
	}
	sort.Slice(fresh, func(i, j int) bool {
		return fresh[i].Utilization < fresh[j].Utilization
	})
	return fresh
}

// FreshCount is a cheap check used by the webhook handler to decide whether
// to accept a preheat request at all ("no guessing at disk state").
func (r *Registry) FreshCount() int {
	return len(r.FreshWorkers())
}

// SetTarget records the image the pipeline just pushed, which the ranker
// loop will keep ordering fresh workers to pull until it expires (targetTTL)
// or a newer target replaces it.
func (r *Registry) SetTarget(image string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.targetImage = image
	r.targetSetAt = time.Now()
}

// CurrentTarget returns the active target image and whether it's still
// within its TTL window. An empty image means "nothing to preheat".
func (r *Registry) CurrentTarget() (image string, setAt time.Time, active bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.targetImage == "" {
		return "", time.Time{}, false
	}
	if time.Since(r.targetSetAt) > r.targetTTL {
		return r.targetImage, r.targetSetAt, false
	}
	return r.targetImage, r.targetSetAt, true
}

// Snapshot builds the full debug view served at /status.
func (r *Registry) Snapshot() model.ControllerStatus {
	r.mu.RLock()
	now := time.Now()
	var out []model.WorkerStatus
	for _, w := range r.workers {
		var repos []string
		if len(w.Repos) > 0 {
			repos = make([]string, 0, len(w.Repos))
			for repo := range w.Repos {
				repos = append(repos, repo)
			}
			sort.Strings(repos)
		}
		out = append(out, model.WorkerStatus{
			NodeID:      w.NodeID,
			Address:     w.Address,
			Utilization: w.Utilization,
			Repos:       repos,
			DigestCount: len(w.Digests),
			LastSeen:    w.LastSeen,
			Fresh:       r.isFresh(w, now),
		})
	}
	target := r.targetImage
	setAt := r.targetSetAt
	r.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })

	freshCount := 0
	for _, w := range out {
		if w.Fresh {
			freshCount++
		}
	}

	status := model.ControllerStatus{
		Workers:     out,
		FreshCount:  freshCount,
		TargetImage: target,
	}
	if target != "" {
		t := setAt
		status.TargetSetAt = &t
	}
	return status
}
