package worker

import (
	"net/http"

	"angryduck/internal/metrics"
)

// NewServer builds the worker's HTTP handler: /pull to receive orders from
// the controller, /metrics for pull metrics, /healthz for
// liveness/readiness probes. When rescue is non-nil it also mounts
// /rescue, /blobs/plan and /blobs/export, all behind the shared token.
func NewServer(puller *Puller, rescue *Rescue) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/pull", puller.HandlePull)
	if rescue != nil {
		rescue.Register(mux)
	}
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}
