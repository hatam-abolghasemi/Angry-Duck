package worker

import (
	"context"
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
// registry is only populated when METRICS_LABEL_REGISTRY is enabled —
// registry host is an unbounded-ish label (unlike node), so it's opt-in.
var pullsTotal = metrics.NewCounterVec(
	"angryduck_worker_pulls_total",
	"Total image pulls executed by this worker, by result. registry is only populated when METRICS_LABEL_REGISTRY=true.",
	"node", "result", "registry",
)

// pullDurationSeconds is the most recent preheat pull duration, per
// node/result/registry/image/spegel combination — a gauge, not a
// histogram: this times exactly the same event pullsTotal counts (this
// is the ONLY path Angry Duck pulls images through), so the two are
// always consistent with each other.
//
// image is the bare repo (imageref.Repo — no tag, no digest), added
// specifically so different repos pulled to the same node don't
// overwrite each other's value. It does NOT fully eliminate gauge
// overwrite: if the SAME repo is pulled to the SAME node twice before a
// scrape catches the first value (a rapid hotfix-then-redeploy, say),
// only the later duration survives to be scraped.
//
// Puller.Run periodically clears this (PULL_DURATION_RESET_INTERVAL_S)
// so a value is visible for roughly one scrape rather than sitting as a
// stale "last known duration" indefinitely between pulls — see Run's own
// doc comment for the timing tradeoff that involves.
//
// registry is only populated when METRICS_LABEL_REGISTRY is enabled;
// spegel only when SPEGEL_IMAGE_SUBSTRING is set — see spegelPresence.
var pullDurationSeconds = metrics.NewGaugeVec(
	"angryduck_worker_pull_duration_seconds",
	"Most recent preheat pull duration in seconds (a gauge, not a histogram — last value per label combination, periodically reset — see PULL_DURATION_RESET_INTERVAL_S). registry only when METRICS_LABEL_REGISTRY=true; spegel only when SPEGEL_IMAGE_SUBSTRING is set.",
	"node", "result", "registry", "image", "spegel",
)

// Puller receives pull orders from the controller and executes them
// asynchronously.
type Puller struct {
	runtime                   Runtime
	nodeID                    string
	labelRegistry             bool
	spegelImageSubstring      string
	pullDurationResetInterval time.Duration

	mu sync.Mutex
	// preheatedAt tracks, per REPO (not full ref — see PreheatedRepos),
	// the last time a preheat pull for that repo succeeded on this node.
	// Backs the preheat-attribution sample (see PreheatMonitor).
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
// pullDurationResetInterval controls how often Run clears
// pullDurationSeconds (0 disables resetting — see Run).
func NewPuller(runtime Runtime, nodeID string, labelRegistry bool, spegelImageSubstring string, pullDurationResetInterval time.Duration) *Puller {
	return &Puller{
		runtime:                   runtime,
		nodeID:                    nodeID,
		labelRegistry:             labelRegistry,
		spegelImageSubstring:      spegelImageSubstring,
		pullDurationResetInterval: pullDurationResetInterval,
		preheatedAt:               make(map[string]time.Time),
	}
}

// ResetPullDuration clears every label combination pullDurationSeconds
// currently holds. Split out from Run so the reset action itself is
// directly unit-testable without waiting on a real ticker.
func (p *Puller) ResetPullDuration() {
	pullDurationSeconds.Reset()
}

// Run periodically clears pullDurationSeconds (see ResetPullDuration) so
// a pull's duration is visible for roughly one scrape interval instead of
// lingering indefinitely as a "last known value" until the next pull
// happens — which could be minutes or hours later, long after the number
// stopped meaning anything current. Match pullDurationResetInterval to
// your actual Prometheus scrape_interval; Angry Duck has no way to
// observe that value itself.
//
// This has one unavoidable, narrow race: resetting on a fixed timer that
// isn't coordinated with the real scrape means a Reset could occasionally
// fire in the small window between a pull's Set and a delayed scrape,
// erasing a value just before it would have been read. Tightening this
// further would mean synchronizing with Prometheus's own scrape timing,
// which this process has no visibility into — the timer is an
// approximation of scrape cadence, not a guarantee every value survives
// to be scraped at least once.
//
// Does nothing (never starts a ticker) if pullDurationResetInterval <= 0.
func (p *Puller) Run(ctx context.Context) {
	if p.pullDurationResetInterval <= 0 {
		logging.Infof("angryduck-worker-puller: pull-duration reset disabled (PULL_DURATION_RESET_INTERVAL_S<=0) — pullDurationSeconds will hold its last value indefinitely")
		return
	}
	ticker := time.NewTicker(p.pullDurationResetInterval)
	defer ticker.Stop()
	logging.Infof("angryduck-worker-puller: resetting pull-duration gauge every %s", p.pullDurationResetInterval)
	for {
		select {
		case <-ctx.Done():
			logging.Infof("angryduck-worker-puller: stopped")
			return
		case <-ticker.C:
			p.ResetPullDuration()
		}
	}
}

// spegelPresence reports whether a container whose image reference
// contains spegelImageSubstring is currently running on this node — using
// only the local container runtime, no Kubernetes API call and no extra RBAC.
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

	logging.Infof("angryduck-worker: received pull order for image=%s", order.Image)

	go func() {
		start := time.Now()
		logging.Infof("angryduck-worker: pulling image=%s", order.Image)
		registry := imageref.RegistryLabel(order.Image, p.labelRegistry)
		repo := imageref.Repo(order.Image)
		if err := p.runtime.PullImage(order.Image); err != nil {
			elapsed := time.Since(start)
			logging.Errorf("angryduck-worker: pull failed for image=%s after %s: %v", order.Image, elapsed.Round(time.Millisecond), err)
			pullsTotal.Inc(p.nodeID, "failure", registry)
			pullDurationSeconds.Set(elapsed.Seconds(), p.nodeID, "failure", registry, repo, p.spegelPresence())
			return
		}
		elapsed := time.Since(start)
		logging.Infof("angryduck-worker: pull succeeded for image=%s in %s", order.Image, elapsed.Round(time.Millisecond))
		pullsTotal.Inc(p.nodeID, "success", registry)
		pullDurationSeconds.Set(elapsed.Seconds(), p.nodeID, "success", registry, repo, p.spegelPresence())

		if repo != "" {
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

// PreheatedRepos returns the set of repos this worker has successfully
// preheated within the last `within`, pruning anything older while it's
// at it. Used only by PreheatMonitor's periodic sample.
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
