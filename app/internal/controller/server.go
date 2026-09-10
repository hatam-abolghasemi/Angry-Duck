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

// resolveLookupsTotal counts /resolve answers: found means the fleet has
// ever observed a digest for the asked tag (regardless of how stale),
// none means the worker has nothing to fall back to at all.
var resolveLookupsTotal = metrics.NewCounterVec(
	"angryduck_controller_resolve_lookups_total",
	"Tag-fallback resolve lookups answered, by result.",
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
	s.mux.HandleFunc("/resolve", s.handleResolve)
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
	s.registry.UpdateTags(rep.Tags)
	logging.Debugf("angryduck-controller: report from node=%s address=%s utilization=%.1f%% tags=%d", rep.NodeID, rep.Address, rep.Utilization*100, len(rep.Tags))
	writeJSON(w, http.StatusOK, model.ReportAck{Accepted: true})
}

// handleResolve answers "what digest does this tag currently mean,
// fleet-wide" — the tag-fallback lookup a worker makes only when origin
// itself couldn't resolve a tag (see internal/worker's
// Mirror.FallbackImport). Deliberately does not filter by freshness the
// way /peers does: a mapping observed by a now-stale worker is still a
// real fact worth returning, and PeersFor already re-checks freshness
// the moment the caller asks who can actually serve the resulting
// digest — staleness there fails safely (empty peer list), so filtering
// here too would only hide information for no added safety.
func (s *Server) handleResolve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	tag := r.URL.Query().Get("tag")
	if tag == "" {
		http.Error(w, "tag required", http.StatusBadRequest)
		return
	}
	digest, observedAt, ok := s.registry.ResolveTag(tag)
	if !ok {
		resolveLookupsTotal.Inc("none")
		logging.Debugf("angryduck-controller: node=%s asked to resolve tag=%s, nobody has ever reported it", r.URL.Query().Get("node"), tag)
		http.NotFound(w, r)
		return
	}
	resolveLookupsTotal.Inc("found")
	logging.Infof("angryduck-controller: node=%s resolved tag=%s to digest=%s (age=%s, fleet-observed, not origin-confirmed)",
		r.URL.Query().Get("node"), tag, digest, time.Since(observedAt).Round(time.Second))
	writeJSON(w, http.StatusOK, model.ResolveResponse{Digest: digest, ObservedAt: observedAt})
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
	asker := r.URL.Query().Get("node")
	peers := s.registry.PeersFor(digest, asker, s.peerCandidates)
	if len(peers) == 0 {
		peerLookupsTotal.Inc("none")
		logging.Infof("angryduck-controller: node=%s asked for peers of %s, found none — it'll go to origin", asker, digest)
	} else {
		peerLookupsTotal.Inc("found")
		logging.Infof("angryduck-controller: node=%s asked for peers of %s, offered %d: %v", asker, digest, len(peers), peers)
	}
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
	if ok := s.registry.Announce(a.NodeID, a.Digest); ok {
		logging.Infof("angryduck-controller: node=%s announced it now holds %s", a.NodeID, a.Digest)
	} else {
		// Node isn't in the registry yet (no /report has landed for it
		// yet) — its own next report will cover this digest anyway, so
		// this is informational, not an error.
		logging.Debugf("angryduck-controller: ignored announce from unknown node=%s for %s (no report from it yet)", a.NodeID, a.Digest)
	}
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
