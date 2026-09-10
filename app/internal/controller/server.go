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
)

// peerLookupsTotal counts /peers answers: found means at least one node
// could serve the digest, none means the requester went to origin.
var peerLookupsTotal = metrics.NewCounterVec(
	"angryduck_controller_peer_lookups_total",
	"Peer lookups answered for worker mirrors, by result.",
	"result",
)

// Server wires the registry + ranker up to HTTP handlers.
type Server struct {
	registry       *Registry
	ranker         *Ranker
	peerCandidates int
	mux            *http.ServeMux
}

// NewServer builds a Server with routes registered. peerCandidates caps
// how many sources one /peers answer lists.
func NewServer(registry *Registry, ranker *Ranker, peerCandidates int) *Server {
	s := &Server{registry: registry, ranker: ranker, peerCandidates: peerCandidates, mux: http.NewServeMux()}
	s.mux.HandleFunc("/webhook/preheat", s.handlePreheat)
	s.mux.HandleFunc("/report", s.handleReport)
	s.mux.HandleFunc("/peers", s.handlePeers)
	s.mux.HandleFunc("/announce", s.handleAnnounce)
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
	// once, at the entry point. `ctr` (the worker's default runtime) does
	// not do this expansion itself and fails with a confusing
	// "invalid port after host" error on short references — normalizing
	// centrally means every downstream consumer (ranking, the pull order
	// sent to workers, GC's tracking of what it pulled) sees the same
	// canonical reference throughout, rather than each having to repeat
	// this logic or risk seeing inconsistent forms of the same image.
	normalized := imageref.Normalize(req.Image)
	if normalized != req.Image {
		logging.Infof("angryduck-controller: normalized image reference %q to %q", req.Image, normalized)
	}
	req.Image = normalized

	if s.registry.FreshCount() == 0 {
		logging.Warnf("angryduck-controller: rejecting preheat for image=%s: zero fresh workers reporting", req.Image)
		writeJSON(w, http.StatusServiceUnavailable, model.PreheatResponse{
			Accepted: false,
			Reason:   "no fresh workers reporting; refusing to guess at disk state",
		})
		return
	}

	s.registry.SetTarget(req.Image)
	ordered := s.ranker.OrderNow(req.Image)

	logging.Infof("angryduck-controller: accepted preheat for image=%s, ordered %d node(s): %v", req.Image, len(ordered), ordered)
	writeJSON(w, http.StatusAccepted, model.PreheatResponse{
		Accepted:     true,
		TargetImage:  req.Image,
		OrderedNodes: ordered,
	})
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
	s.registry.Update(rep)
	logging.Debugf("angryduck-controller: report from node=%s address=%s utilization=%.1f%%", rep.NodeID, rep.Address, rep.Utilization*100)
	writeJSON(w, http.StatusOK, model.ReportAck{Accepted: true})
}

// handlePeers tells a worker's mirror which nodes can export a digest.
// Answered entirely from memory: no fan-out, no calls to workers.
func (s *Server) handlePeers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	digest := r.URL.Query().Get("digest")
	if !strings.HasPrefix(digest, "sha256:") {
		http.Error(w, "digest required", http.StatusBadRequest)
		return
	}
	peers := s.registry.PeersFor(digest, r.URL.Query().Get("node"), s.peerCandidates)
	if len(peers) == 0 {
		peerLookupsTotal.Inc("none")
	} else {
		peerLookupsTotal.Inc("found")
	}
	logging.Debugf("angryduck-controller: peers for %s (asked by %s): %v", digest, r.URL.Query().Get("node"), peers)
	writeJSON(w, http.StatusOK, model.PeersResponse{Peers: peers})
}

// handleAnnounce records a digest a worker just imported from a peer.
func (s *Server) handleAnnounce(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var a model.Announce
	if err := json.NewDecoder(r.Body).Decode(&a); err != nil || a.NodeID == "" || a.Digest == "" {
		http.Error(w, "node_id and digest required", http.StatusBadRequest)
		return
	}
	s.registry.Announce(a.NodeID, a.Digest)
	w.WriteHeader(http.StatusNoContent)
}

// handleStatus is a debug endpoint showing the full worker registry and
// current preheat target.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.registry.Snapshot())
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
