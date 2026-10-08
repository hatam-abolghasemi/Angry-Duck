package controller

import (
	"sort"
	"sync"
	"time"

	"angryduck/internal/layerindex"
	"angryduck/internal/metrics"
	"angryduck/internal/model"
)

var layerIndexDigests = metrics.NewGaugeVec(
	"angryduck_controller_layer_index_digests",
	"Distinct blob digests and snapshot chainIDs held in the controller's layer index, across all nodes.",
)

// workerEntry is the controller's internal record of a single worker.
type workerEntry struct {
	NodeID      string
	Address     string
	Utilization float64
	// Repos is the sorted set of bare repo identities (imageref.Repo)
	// this node last reported having locally, any tag. Images is every
	// full image reference it reported (tag and repo@digest forms), also
	// sorted; the rescuer uses it to find nodes holding the exact image a
	// stuck pod needs. Both hold interned strings shared across nodes,
	// since most nodes run the same images, and are replaced, never
	// modified, so a copy taken by FreshWorkers stays valid.
	Repos    []string
	Images   []string
	invHash  string // InventoryHash of the inventory held
	LastSeen time.Time
}

// hasSorted reports whether sorted list contains s.
func hasSorted(list []string, s string) bool {
	i := sort.SearchStrings(list, s)
	return i < len(list) && list[i] == s
}

// HasRepo reports whether this worker last reported having repo present
// locally under any tag.
func (w *workerEntry) HasRepo(repo string) bool {
	if repo == "" {
		return false
	}
	return hasSorted(w.Repos, repo)
}

// HasImage reports whether this worker last reported having exactly image
// (a normalized reference) locally.
func (w *workerEntry) HasImage(image string) bool {
	if image == "" {
		return false
	}
	return hasSorted(w.Images, image)
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
	// layers is which image layers each node holds. It has its own lock
	// and lives outside workerEntry, which FreshWorkers copies.
	layers *layerindex.Index
	// names interns repo and image strings across workers. Guarded by mu.
	names interner
}

// interner keeps one copy of each string held by any worker's inventory,
// with a count of the lists holding it.
type interner struct{ m map[string]internEntry }

type internEntry struct {
	s string // the canonical copy
	n int32
}

// intern returns the canonical copy of s and takes a reference.
func (in *interner) intern(s string) string {
	if in.m == nil {
		in.m = make(map[string]internEntry)
	}
	e, ok := in.m[s]
	if !ok {
		e.s = s
	}
	e.n++
	in.m[s] = e
	return e.s
}

// release drops one reference to s.
func (in *interner) release(s string) {
	e, ok := in.m[s]
	if !ok {
		return
	}
	if e.n--; e.n <= 0 {
		delete(in.m, s)
		return
	}
	in.m[s] = e
}

// internList returns list sorted, deduplicated, without empty entries and
// interned. If that equals old, old itself is returned and no references
// change; otherwise the new list's references are taken and old's dropped.
func (in *interner) internList(old, list []string) []string {
	clean := make([]string, 0, len(list))
	for _, s := range list {
		if s != "" {
			clean = append(clean, s)
		}
	}
	sort.Strings(clean)
	uniq := clean[:0]
	for i, s := range clean {
		if i == 0 || s != clean[i-1] {
			uniq = append(uniq, s)
		}
	}
	if equalStrings(uniq, old) {
		return old
	}
	out := make([]string, len(uniq)) // exact size: this is what's kept
	for i, s := range uniq {
		out[i] = in.intern(s)
	}
	for _, s := range old {
		in.release(s)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// NewRegistry builds an empty worker registry.
func NewRegistry(staleAfter, targetTTL time.Duration) *Registry {
	return &Registry{
		workers:    make(map[string]*workerEntry),
		staleAfter: staleAfter,
		targetTTL:  targetTTL,
		layers:     layerindex.New(),
	}
}

// Layers returns the layer index.
func (r *Registry) Layers() *layerindex.Index { return r.layers }

// Update records (or refreshes) a worker's latest report and returns the
// ack to send back, which tells the worker where its layer sync stands.
func (r *Registry) Update(rep model.WorkerReport) model.ReportAck {
	ack := model.ReportAck{Accepted: true}
	if rep.Layers != nil {
		ack.LayersSeq, ack.LayersResync = r.layers.Apply(rep.NodeID, rep.Layers)
		layerIndexDigests.Set(float64(r.layers.Unique()))
	} else if _, _, known := r.layers.Counts(rep.NodeID); known {
		// Unchanged since the last sync: confirm what we hold, so the
		// worker can tell a controller that restarted (holds nothing,
		// resync) from one that simply had nothing new.
		ack.LayersSeq = r.layers.Seq(rep.NodeID)
	}
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

	// A worker omits an unchanged inventory once we've echoed its hash.
	// Workers that predate this never omit it.
	if rep.InventoryOmitted {
		if rep.InventoryHash == "" || rep.InventoryHash != w.invHash {
			// Not what we hold (we restarted, say): keep what we have
			// and don't echo, so the worker sends the full lists next.
			return ack
		}
	} else {
		w.Repos = r.names.internList(w.Repos, rep.Repos)
		w.Images = r.names.internList(w.Images, rep.Images)
		w.invHash = rep.InventoryHash
	}
	ack.InventoryHash = w.invHash
	return ack
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
			repos = append([]string(nil), w.Repos...) // already sorted
		}
		blobs, snaps, known := r.layers.Counts(w.NodeID)
		out = append(out, model.WorkerStatus{
			NodeID:      w.NodeID,
			Address:     w.Address,
			Utilization: w.Utilization,
			Repos:       repos,
			LastSeen:    w.LastSeen,
			Fresh:       r.isFresh(w, now),
			LayersKnown: known,
			Blobs:       blobs,
			Snapshots:   snaps,
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

// Holders returns every fresh worker (least full first) whose layer
// inventory holds blob digest, then those in also that it doesn't list
// yet, leaving out exclude.
func (r *Registry) Holders(digest, exclude string, also []string) []model.Holder {
	fresh := r.FreshWorkers()
	ids := make([]string, 0, len(fresh))
	addr := make(map[string]string, len(fresh))
	for _, w := range fresh {
		if w.NodeID != exclude {
			ids = append(ids, w.NodeID)
			addr[w.NodeID] = w.Address
		}
	}
	var out []model.Holder
	seen := map[string]bool{}
	for _, n := range r.layers.Holders(digest, ids) {
		out = append(out, model.Holder{NodeID: n, Address: addr[n]})
		seen[n] = true
	}
	// Nodes known to hold it ahead of their next inventory (a blob that
	// just landed in a spread), if they are fresh and not excluded.
	for _, n := range also {
		if a, ok := addr[n]; ok && !seen[n] {
			out = append(out, model.Holder{NodeID: n, Address: a})
			seen[n] = true
		}
	}
	return out
}

// worker returns a copy of node's entry, or nil.
func (r *Registry) worker(node string) *workerEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	w := r.workers[node]
	if w == nil {
		return nil
	}
	cp := *w
	return &cp
}
