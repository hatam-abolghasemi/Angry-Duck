package worker

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"angryduck/internal/imageref"
	"angryduck/internal/logging"
	"angryduck/internal/metrics"
	"angryduck/internal/model"
)

// pullsTotal counts pulls this worker has actually executed, by result.
// This is the worker-side half of the pull story; the controller-side half
// (orders sent, whether the worker accepted them) is
// angryduck_controller_pull_orders_total in controller/ranker.go. A pull
// order being accepted and a pull actually succeeding are different
// events — this counter is the only one that tells you the latter.
//
// registry is only populated when METRICS_LABEL_REGISTRY is enabled — see
// imagesDeletedTotal in gc.go for why this defaults to off.
var pullsTotal = metrics.NewCounterVec(
	"angryduck_worker_pulls_total",
	"Total image pulls executed by this worker, by result. registry is only populated when METRICS_LABEL_REGISTRY=true.",
	"node", "result", "registry",
)

// pullDurationSeconds times how long a preheat pull actually took,
// success or failure — the baseline stg benchmark (kubelet Pulled events,
// 0.25s-45.6s across workloads) sized these bucket boundaries. This times
// exactly the same event pullsTotal counts (this is the ONLY path Angry
// Duck pulls images through — rescue uses a separate export/import
// mechanism, not PullImage), so the two are always consistent with each
// other.
//
// spegel is only populated when SPEGEL_IMAGE_SUBSTRING is set — empty
// otherwise, at zero extra cost (see spegelPresence). Values are
// "true"/"false"/"unknown" (a runtime error checking presence is
// reported as unknown, never silently folded into "false" — an error
// isn't evidence Spegel was absent).
var pullDurationSeconds = metrics.NewHistogramVec(
	"angryduck_worker_pull_duration_seconds",
	"How long a preheat pull took, by result. registry is only populated when METRICS_LABEL_REGISTRY=true; spegel only when SPEGEL_IMAGE_SUBSTRING is set.",
	[]float64{0.5, 1, 2, 5, 10, 20, 30, 60, 120},
	"node", "result", "registry", "spegel",
)

// Puller receives pull orders from the controller, executes them
// asynchronously, and remembers when each image was ordered so the GC loop
// can grant it a grace period even if nothing has actually run it yet
// (Argo may not have synced the new pod onto this node the moment the pull
// lands).
type Puller struct {
	runtime              Runtime
	gracePeriod          time.Duration
	nodeID               string
	labelRegistry        bool
	spegelImageSubstring string

	mu        sync.Mutex
	orderedAt map[string]time.Time
	// preheatedAt tracks, per REPO (not full ref — see PreheatedRepos),
	// the last time a preheat pull for that repo succeeded on this node.
	// Deliberately separate from orderedAt: orderedAt exists to protect
	// an image from GC for one short gracePeriod, while this backs the
	// preheat-attribution sample (see PreheatMonitor) over a much longer
	// window — the two have unrelated lifetimes and shouldn't share one
	// prune policy.
	preheatedAt map[string]time.Time
	onSuccess   func()
}

// OnSuccess registers a callback run after every successful pull (the
// reporter's Kick, so the controller's locality view of this node is
// fresh immediately rather than waiting for the next report interval).
func (p *Puller) OnSuccess(fn func()) { p.onSuccess = fn }

// NewPuller builds a Puller. nodeID is only used to label metrics.
// labelRegistry controls whether pulls are labeled by registry host in
// pullsTotal/pullDurationSeconds (METRICS_LABEL_REGISTRY).
// spegelImageSubstring enables Spegel-presence detection on
// pullDurationSeconds when non-empty — see spegelPresence.
func NewPuller(runtime Runtime, gracePeriod time.Duration, nodeID string, labelRegistry bool, spegelImageSubstring string) *Puller {
	return &Puller{
		runtime:              runtime,
		gracePeriod:          gracePeriod,
		nodeID:               nodeID,
		labelRegistry:        labelRegistry,
		spegelImageSubstring: spegelImageSubstring,
		orderedAt:            make(map[string]time.Time),
		preheatedAt:          make(map[string]time.Time),
	}
}

// spegelPresence reports whether a container whose image reference
// contains spegelImageSubstring is currently running on this node — using
// only the local container runtime (the same source GC already trusts
// for "what's running here"), no Kubernetes API call and no extra RBAC.
// Checked once per pull rather than on a timer: a pull is already a much
// rarer event than any periodic sample would be (bounded by RANK_TOP_N
// per push), so this stays cheap even on the costlier ctr backend.
//
// Returns "" if detection is disabled (spegelImageSubstring unset — the
// default, and the only case with zero added runtime calls), "unknown" if
// the runtime call itself failed (never silently reported as "false": an
// error is not evidence Spegel is absent), otherwise "true" or "false".
func (p *Puller) spegelPresence() string {
	if p.spegelImageSubstring == "" {
		return ""
	}
	running, err := p.runtime.ListRunningImages()
	if err != nil {
		return "unknown"
	}
	for _, img := range running {
		if strings.Contains(img, p.spegelImageSubstring) {
			return "true"
		}
	}
	return "false"
}

// HandlePull is the HTTP handler mounted at /pull.
func (p *Puller) HandlePull(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var order model.PullOrder
	if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}
	if order.Image == "" {
		writeJSON(w, http.StatusBadRequest, model.PullAck{Accepted: false, Reason: "missing image"})
		return
	}
	// Defense-in-depth: the controller already normalizes at the webhook,
	// but normalize here too in case /pull is ever hit directly, so `ctr`
	// never sees an ambiguous short reference regardless of caller.
	normalized := imageref.Normalize(order.Image)
	if normalized != order.Image {
		logging.Infof("angryduck-worker: normalized image reference %q to %q", order.Image, normalized)
	}
	order.Image = normalized

	p.mu.Lock()
	p.orderedAt[order.Image] = time.Now()
	p.mu.Unlock()

	logging.Infof("angryduck-worker: received pull order for image=%s", order.Image)

	go func() {
		start := time.Now()
		logging.Infof("angryduck-worker: pulling image=%s", order.Image)
		registry := imageref.RegistryLabel(order.Image, p.labelRegistry)
		if err := p.runtime.PullImage(order.Image); err != nil {
			elapsed := time.Since(start)
			logging.Errorf("angryduck-worker: pull failed for image=%s after %s: %v", order.Image, elapsed.Round(time.Millisecond), err)
			pullsTotal.Inc(p.nodeID, "failure", registry)
			pullDurationSeconds.Observe(elapsed.Seconds(), p.nodeID, "failure", registry, p.spegelPresence())
			return
		}
		elapsed := time.Since(start)
		logging.Infof("angryduck-worker: pull succeeded for image=%s in %s", order.Image, elapsed.Round(time.Millisecond))
		pullsTotal.Inc(p.nodeID, "success", registry)
		pullDurationSeconds.Observe(elapsed.Seconds(), p.nodeID, "success", registry, p.spegelPresence())

		if repo := imageref.Repo(order.Image); repo != "" {
			p.mu.Lock()
			p.preheatedAt[repo] = time.Now()
			p.mu.Unlock()
		}

		if p.onSuccess != nil {
			p.onSuccess()
		}
	}()

	writeJSON(w, http.StatusAccepted, model.PullAck{Accepted: true})
}

// InGracePeriod reports whether an image was ordered pulled recently enough
// that the GC loop should not remove it yet, even if nothing is currently
// running it.
func (p *Puller) InGracePeriod(image string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	t, ok := p.orderedAt[image]
	if !ok {
		return false
	}
	return time.Since(t) <= p.gracePeriod
}

// PruneExpired drops orderedAt entries whose grace period has fully
// elapsed. Without this, orderedAt grows by one entry per unique image
// reference ever ordered, for the lifetime of the process — harmless at
// small scale, but unbounded on a long-lived pod in a repo with a steady
// stream of new tags. Safe to call on a timer (the GC loop already ticks
// on one); an entry past its grace period has nothing left to protect, so
// dropping it changes no GC decision.
func (p *Puller) PruneExpired() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	pruned := 0
	for img, t := range p.orderedAt {
		if now.Sub(t) > p.gracePeriod {
			delete(p.orderedAt, img)
			pruned++
		}
	}
	return pruned
}

// PreheatedRepos returns the set of repos this worker has successfully
// preheated within the last `within`, pruning anything older while it's
// at it. Used only by PreheatMonitor's periodic sample — a much coarser,
// longer-lived read than InGracePeriod's per-pull check, which is why
// it's backed by its own map instead of reusing orderedAt.
func (p *Puller) PreheatedRepos(within time.Duration) map[string]bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	out := make(map[string]bool, len(p.preheatedAt))
	for repo, t := range p.preheatedAt {
		if now.Sub(t) > within {
			delete(p.preheatedAt, repo)
			continue
		}
		out[repo] = true
	}
	return out
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
