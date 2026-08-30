package worker

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"angryduck/internal/imageref"
	"angryduck/internal/logging"
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
			return
		}
		logging.Infof("angryduck-worker: pull succeeded for image=%s in %s", order.Image, time.Since(start).Round(time.Millisecond))
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
