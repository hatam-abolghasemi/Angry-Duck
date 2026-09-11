package worker

import (
	"net/http"

	"angryduck/internal/metrics"
)

// NewServer builds the worker's HTTP handler: /pull to receive orders from
// the controller, /metrics for pull/GC/rescue counters, /healthz for
// liveness/readiness probes. With a non-nil exporter it also serves
// /rescue-export, the token-protected one-shot image handoff used to
// rescue a pod stuck in ImagePullBackOff elsewhere in the fleet.
func NewServer(puller *Puller, exporter *RescueExporter) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/pull", puller.HandlePull)
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	if exporter != nil {
		mux.HandleFunc("/rescue-export", exporter.ServeExport)
	}
	return mux
}
