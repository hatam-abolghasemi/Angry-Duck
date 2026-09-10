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

var pullsTotal = metrics.NewCounterVec(
	"angryduck_worker_pulls_total",
	"Total image pulls executed by this worker, by result.",
	"node", "result",
)

// Puller receives preheat orders from the controller. The actual image pull
// is intentionally just a normal CRI pull, so containerd's configured
// registry mirror (AngryDuck) handles local/peer/origin routing transparently.
type Puller struct {
	runtime     Runtime
	gracePeriod time.Duration
	nodeID      string
	mu          sync.Mutex
	orderedAt   map[string]time.Time
	inFlight    map[string]bool
}

func NewPuller(runtime Runtime, gracePeriod time.Duration, nodeID string, _ ...interface{}) *Puller {
	return &Puller{
		runtime:     runtime,
		gracePeriod: gracePeriod,
		nodeID:      nodeID,
		orderedAt:   make(map[string]time.Time),
		inFlight:    make(map[string]bool),
	}
}

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
	p.orderedAt[order.Image] = time.Now()
	if p.inFlight[order.Image] {
		p.mu.Unlock()
		writeJSON(w, http.StatusAccepted, model.PullAck{Accepted: true})
		return
	}
	p.inFlight[order.Image] = true
	p.mu.Unlock()

	logging.Infof("angryduck-worker[%s]: received preheat order image=%s", p.nodeID, order.Image)
	go p.executePull(order.Image)
	writeJSON(w, http.StatusAccepted, model.PullAck{Accepted: true})
}

func (p *Puller) executePull(image string) {
	defer func() {
		p.mu.Lock()
		delete(p.inFlight, image)
		p.mu.Unlock()
	}()

	start := time.Now()
	if err := p.runtime.PullImage(image); err != nil {
		logging.Errorf("angryduck-worker[%s]: preheat pull failed image=%s after %s: %v", p.nodeID, image, time.Since(start).Round(time.Millisecond), err)
		pullsTotal.Inc(p.nodeID, "failure")
		return
	}
	logging.Infof("angryduck-worker[%s]: preheat pull succeeded image=%s in %s", p.nodeID, image, time.Since(start).Round(time.Millisecond))
	pullsTotal.Inc(p.nodeID, "success")
}

func (p *Puller) InGracePeriod(image string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	t, ok := p.orderedAt[image]
	if !ok {
		return false
	}
	return time.Since(t) <= p.gracePeriod
}

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
