package worker

import (
	"encoding/json"
	"net/http"
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
var pullsTotal = metrics.NewCounterVec(
	"angryduck_worker_pulls_total",
	"Total image pulls executed by this worker, by result.",
	"node", "result",
)

// Puller receives pull orders from the controller, executes them
// asynchronously, and remembers when each image was ordered so the GC loop
// can grant it a grace period even if nothing has actually run it yet
// (Argo may not have synced the new pod onto this node the moment the pull
// lands).
type Puller struct {
	runtime     Runtime
	gracePeriod time.Duration
	nodeID      string
	mu          sync.Mutex
	orderedAt   map[string]time.Time
	onSuccess   func()
}

// OnSuccess registers a callback run after every successful pull (the
// reporter's Kick, so the controller's locality view of this node is
// fresh immediately rather than waiting for the next report interval).
func (p *Puller) OnSuccess(fn func()) { p.onSuccess = fn }

// NewPuller builds a Puller. nodeID is only used to label metrics.
func NewPuller(runtime Runtime, gracePeriod time.Duration, nodeID string) *Puller {
	return &Puller{
		runtime:     runtime,
		gracePeriod: gracePeriod,
		nodeID:      nodeID,
		orderedAt:   make(map[string]time.Time),
	}
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
		if err := p.runtime.PullImage(order.Image); err != nil {
			logging.Errorf("angryduck-worker: pull failed for image=%s after %s: %v", order.Image, time.Since(start).Round(time.Millisecond), err)
			pullsTotal.Inc(p.nodeID, "failure")
			return
		}
		logging.Infof("angryduck-worker: pull succeeded for image=%s in %s", order.Image, time.Since(start).Round(time.Millisecond))
		pullsTotal.Inc(p.nodeID, "success")
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

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
