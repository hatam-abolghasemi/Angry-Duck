package worker

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"angryduck/internal/model"
)

// Puller receives pull orders from the controller, executes them
// asynchronously, and remembers when each image was ordered so the GC loop
// can grant it a grace period even if nothing has actually run it yet
// (Argo may not have synced the new pod onto this node the moment the pull
// lands).
type Puller struct {
	runtime     Runtime
	gracePeriod time.Duration
	mu          sync.Mutex
	orderedAt   map[string]time.Time
}

// NewPuller builds a Puller.
func NewPuller(runtime Runtime, gracePeriod time.Duration) *Puller {
	return &Puller{
		runtime:     runtime,
		gracePeriod: gracePeriod,
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

	p.mu.Lock()
	p.orderedAt[order.Image] = time.Now()
	p.mu.Unlock()

	go func() {
		log.Printf("angryduck-worker: pulling image=%s", order.Image)
		if err := p.runtime.PullImage(order.Image); err != nil {
			log.Printf("angryduck-worker: pull failed for image=%s: %v", order.Image, err)
			return
		}
		log.Printf("angryduck-worker: pull succeeded for image=%s", order.Image)
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

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
