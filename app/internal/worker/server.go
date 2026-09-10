package worker

import (
	"net/http"

	"angryduck/internal/metrics"
)

// NewServer builds the worker's HTTP handler: /pull to receive orders from
// the controller, /metrics for pull/GC counters, /healthz for
// liveness/readiness probes. With a non-nil mirror it also serves /v2/
// (containerd's registry mirror, loopback only) and /export (peer
// transfer source, token-protected).
func NewServer(puller *Puller, mirror *Mirror) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/pull", puller.HandlePull)
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	if mirror != nil {
		mux.HandleFunc("/v2/", mirror.ServeRegistry)
		mux.HandleFunc("/export", mirror.ServeExport)
	}
	return mux
}
