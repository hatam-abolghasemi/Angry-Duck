package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"angryduck/internal/imageref"
	"angryduck/internal/logging"
	"angryduck/internal/metrics"
	"angryduck/internal/model"
)

// pullsTotal counts preheat/seed pulls by result: success, failure, or
// cancelled (the controller gave the node the image from a peer instead).
var pullsTotal = metrics.NewCounterVec(
	"angryduck_worker_pulls_total",
	"Seed pulls from the registry executed by this worker, by result (success, failure, cancelled). registry is only populated when METRICS_LABEL_REGISTRY=true.",
	"node", "result", "registry",
)

// pullSecondsTotal replaces the old last-value duration gauge (which also
// carried image and spegel labels, and needed a reset loop): average pull
// time is rate(pull_seconds_total) / rate(pulls_total), with bounded
// cardinality and no goroutine.
var pullSecondsTotal = metrics.NewCounterVec(
	"angryduck_worker_pull_seconds_total",
	"Seconds spent in seed pulls from the registry, by result. Divide by angryduck_worker_pulls_total for the average.",
	"node", "result", "registry",
)

// pullsInFlight is the number of seed pulls running on this node.
var pullsInFlight = metrics.NewGaugeVec(
	"angryduck_worker_pulls_in_flight",
	"Seed pulls from the registry running on this node right now.",
	"node",
)

// Puller runs the pull orders the controller sends to seed nodes.
type Puller struct {
	runtime       Runtime
	nodeID        string
	labelRegistry bool

	mu          sync.Mutex
	preheatedAt map[string]time.Time // repo -> last successful pull
	running     map[string]context.CancelFunc
	onSuccess   func()
}

// OnSuccess registers fn to run after a pull lands (the reporter's Kick).
func (p *Puller) OnSuccess(fn func()) { p.onSuccess = fn }

// NewPuller builds a Puller.
func NewPuller(runtime Runtime, nodeID string, labelRegistry bool) *Puller {
	return &Puller{
		runtime:       runtime,
		nodeID:        nodeID,
		labelRegistry: labelRegistry,
		preheatedAt:   make(map[string]time.Time),
		running:       make(map[string]context.CancelFunc),
	}
}

// HandlePull accepts a pull order and runs it in the background. A second
// order for an image already being pulled is accepted and joins it.
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
	order.Image = imageref.Normalize(order.Image)

	p.mu.Lock()
	if _, busy := p.running[order.Image]; busy {
		p.mu.Unlock()
		writeJSON(w, http.StatusAccepted, model.PullAck{Accepted: true, Reason: "already pulling"})
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.running[order.Image] = cancel
	n := len(p.running)
	p.mu.Unlock()
	pullsInFlight.Set(float64(n), p.nodeID)

	logging.Infof("angryduck-worker: pulling image=%s (seed order)", order.Image)
	go p.pull(ctx, order.Image)
	writeJSON(w, http.StatusAccepted, model.PullAck{Accepted: true})
}

func (p *Puller) pull(ctx context.Context, image string) {
	start := time.Now()
	registry := imageref.RegistryLabel(image, p.labelRegistry)
	err := p.runtime.PullImage(ctx, image)
	elapsed := time.Since(start)
	cancelled := ctx.Err() != nil

	p.mu.Lock()
	if cancel, ok := p.running[image]; ok {
		cancel()
		delete(p.running, image)
	}
	n := len(p.running)
	if err == nil {
		if repo := imageref.Repo(image); repo != "" {
			p.preheatedAt[repo] = time.Now()
		}
	}
	p.mu.Unlock()
	pullsInFlight.Set(float64(n), p.nodeID)

	result := "success"
	switch {
	case err != nil && cancelled:
		result = "cancelled"
		logging.Infof("angryduck-worker: pull of image=%s cancelled after %s (a peer will ship it instead)", image, elapsed.Round(time.Millisecond))
	case err != nil:
		result = "failure"
		logging.Errorf("angryduck-worker: pull failed for image=%s after %s: %v", image, elapsed.Round(time.Millisecond), err)
	default:
		logging.Infof("angryduck-worker: pull succeeded for image=%s in %s", image, elapsed.Round(time.Millisecond))
	}
	pullsTotal.Inc(p.nodeID, result, registry)
	pullSecondsTotal.Add(int64(elapsed.Seconds()+0.5), p.nodeID, result, registry)
	if err == nil && p.onSuccess != nil {
		p.onSuccess()
	}
}

// HandleCancel stops a running pull of the image in the body
// ({"image": ...}). The controller calls it on a seed that is taking too
// long, right before ordering a peer to ship the image there, so two
// writers never fill the same content store at once. Answers whether a
// pull was running.
func (p *Puller) HandleCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var order model.PullOrder
	if err := json.NewDecoder(r.Body).Decode(&order); err != nil || order.Image == "" {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}
	image := imageref.Normalize(order.Image)
	p.mu.Lock()
	cancel, ok := p.running[image]
	p.mu.Unlock()
	if ok {
		cancel()
	}
	writeJSON(w, http.StatusOK, model.PullAck{Accepted: ok})
}

// Pulling reports whether image is being pulled right now.
func (p *Puller) Pulling(image string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.running[imageref.Normalize(image)]
	return ok
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
