package controller

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"angryduck/internal/imageref"
	"angryduck/internal/logging"
	"angryduck/internal/metrics"
	"angryduck/internal/model"
	"angryduck/internal/sharedtoken"
)

// Server wires the registry + ranker up to HTTP handlers.
type Server struct {
	registry   *Registry
	ranker     *Ranker
	rescuer    *Rescuer    // nil when rescue is off
	propagator *Propagator // nil when propagation is off
	spreader   *Spreader   // nil when spreading is off
	retainer   *Retainer   // nil when retention is off
	companions *Companions // nil when companions are off
	tags       *tagLookup  // nil when mirror resolve is off
	auth       *sharedtoken.Auth
	webhook    *sharedtoken.Auth // CI's token for /webhook/preheat
	mux        *http.ServeMux
}

// SetPropagator makes accepted preheats also start a propagation, and adds
// propagation state to /status.
func (s *Server) SetPropagator(p *Propagator) { s.propagator = p }

// SetSpreader makes accepted preheats spread blob by blob, mounts
// /spread/done (workers' transfer reports) behind the shared token, and
// adds spreads to /status. Call after SetToken.
func (s *Server) SetSpreader(sp *Spreader) {
	s.spreader = sp
	s.mux.HandleFunc("/spread/done", sharedtoken.Require(s.auth.Token, sp.HandleDone))
}

// SetToken mounts /layers/holders, which workers' mirrors call, behind
// the shared token, and makes /report require it too (subject to the
// mode set with SetAuthMode).
func (s *Server) SetToken(token string) {
	s.auth.Token = token
	s.mux.HandleFunc("/layers/holders", sharedtoken.Require(token, s.handleHolders))
}

// SetWebhookToken makes /webhook/preheat require token. It is CI's own
// token, separate from the shared one, so CI can't talk to workers.
func (s *Server) SetWebhookToken(token string) { s.webhook.Token = token }

// SetAuthMode sets what /report does with a request without the token.
func (s *Server) SetAuthMode(m sharedtoken.Mode) { s.auth.Mode = m }

func (s *Server) handleHolders(w http.ResponseWriter, r *http.Request) {
	digest := r.URL.Query().Get("digest")
	if !strings.HasPrefix(digest, "sha256:") {
		http.Error(w, "digest required", http.StatusBadRequest)
		return
	}
	var spreading, arriving []string
	if s.spreader != nil {
		spreading = s.spreader.Holders(digest)
		arriving = s.spreader.Arriving(digest)
	}
	holders := s.registry.Holders(digest, r.URL.Query().Get("exclude"), spreading, arriving)
	if holders == nil {
		holders = []model.Holder{}
	}
	writeJSON(w, http.StatusOK, holders)
}

// SetRescuer adds the rescuer's state to /status.
func (s *Server) SetRescuer(rs *Rescuer) { s.rescuer = rs }

// SetCompanions adds the companion images to /status and tells the
// tracker which registries CI preheats from.
func (s *Server) SetCompanions(c *Companions) { s.companions = c }

// SetRetainer adds the retention copies to /status.
func (s *Server) SetRetainer(rt *Retainer) { s.retainer = rt }

// NewServer builds a Server with routes registered.
func NewServer(registry *Registry, ranker *Ranker) *Server {
	s := &Server{registry: registry, ranker: ranker, auth: &sharedtoken.Auth{}, webhook: &sharedtoken.Auth{}, mux: http.NewServeMux()}
	// A preheat makes every node pull with the registry credentials, so
	// whoever can reach the ingress could otherwise trigger it at will.
	s.mux.HandleFunc("/webhook/preheat", s.webhook.Guard("/webhook/preheat", s.handlePreheat))
	// /report decides which addresses rescue and propagation send the
	// token to, so an unauthenticated report could register an attacker's
	// address and collect it.
	s.mux.HandleFunc("/report", s.auth.Guard("/report", s.handleReport))
	s.mux.HandleFunc("/status", s.handleStatus)
	s.mux.Handle("/metrics", metrics.Handler())
	s.mux.HandleFunc("/healthz", s.handleHealth)
	return s
}

// Handler returns the underlying http.Handler for use with http.ListenAndServe.
func (s *Server) Handler() http.Handler {
	return s.mux
}

// handlePreheat is called by the CI/CD pipeline right after `docker push`.
// If no worker has reported recently, the controller has no reliable view
// of disk state on any node, so it refuses the request outright rather than
// guessing.
func (s *Server) handlePreheat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req model.PreheatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}
	req.Image = strings.TrimSpace(req.Image)
	if req.Image == "" {
		writeJSON(w, http.StatusBadRequest, model.PreheatResponse{
			Accepted: false,
			Reason:   "missing image",
		})
		return
	}
	// Normalize short Docker-style references ("nginx", "nginx:latest")
	// into fully-qualified ones ("docker.io/library/nginx:latest") here,
	// once, at the entry point, so every downstream consumer (ranking,
	// the pull order sent to workers) sees the same canonical reference
	// rather than inconsistent forms of the same image.
	normalized := imageref.Normalize(req.Image)
	if normalized != req.Image {
		logging.Infof("angryduck-controller: normalized image reference %q to %q", req.Image, normalized)
	}
	req.Image = normalized
	if s.companions != nil {
		s.companions.NoteRegistry(req.Image)
	}

	if s.registry.FreshCount() == 0 {
		logging.Warnf("angryduck-controller: rejecting preheat for image=%s: zero fresh workers reporting", req.Image)
		writeJSON(w, http.StatusServiceUnavailable, model.PreheatResponse{
			Accepted: false,
			Reason:   "no fresh workers reporting; refusing to guess at disk state",
		})
		return
	}

	if s.spreader != nil {
		// CI doesn't wait on the registry: the manifest is read in the
		// background, retried while the registry is slow, and the image
		// is seeded whole only if it never can be.
		s.spreader.Begin(req.Image, func(image string, _ error) { s.seedWhole(image) })
		writeJSON(w, http.StatusAccepted, model.PreheatResponse{Accepted: true, Status: "spreading", TargetImage: req.Image})
		return
	}
	ordered := s.seedWhole(req.Image)
	writeJSON(w, http.StatusAccepted, model.PreheatResponse{
		Accepted:     true,
		TargetImage:  req.Image,
		OrderedNodes: ordered,
	})
}

// seedWhole orders seeds to pull image whole and propagation to take it
// from there, and returns the seeds.
func (s *Server) seedWhole(image string) []string {
	s.registry.SetTarget(image)
	ordered := s.ranker.OrderNow(image)
	if s.propagator != nil {
		s.propagator.Start(image)
	}
	logging.Infof("angryduck-controller: accepted preheat for image=%s, ordered %d node(s): %v", image, len(ordered), ordered)
	return ordered
}

// handleReport ingests periodic disk-utilization reports from workers.
func (s *Server) handleReport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var rep model.WorkerReport
	if err := json.NewDecoder(r.Body).Decode(&rep); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}
	if rep.NodeID == "" || rep.Address == "" {
		http.Error(w, "node_id and address are required", http.StatusBadRequest)
		return
	}
	if rep.Timestamp.IsZero() {
		rep.Timestamp = time.Now()
	}
	ack := s.registry.Update(rep)
	if rep.Layers != nil {
		l := rep.Layers
		logging.Debugf("angryduck-controller: layer sync from node=%s full=%v seq=%d base=%d blobs=+%d/-%d snaps=+%d/-%d -> held=%d resync=%v",
			rep.NodeID, l.Full, l.Seq, l.Base, len(l.AddBlobs), len(l.DelBlobs), len(l.AddSnaps), len(l.DelSnaps), ack.LayersSeq, ack.LayersResync)
	}
	logging.Debugf("angryduck-controller: report from node=%s address=%s utilization=%.1f%%", rep.NodeID, rep.Address, rep.Utilization*100)
	writeJSON(w, http.StatusOK, ack)
}

// handleStatus is a debug endpoint showing the full worker registry and
// current preheat target.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	status := s.registry.Snapshot()
	if s.rescuer != nil {
		status.Rescues = s.rescuer.Status()
	}
	if s.propagator != nil {
		status.Propagations = s.propagator.Status()
	}
	if s.retainer != nil {
		status.Retention = s.retainer.Status()
	}
	if s.spreader != nil {
		status.Spreads = s.spreader.Status()
	}
	if s.companions != nil {
		status.Companions = s.companions.Status()
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
