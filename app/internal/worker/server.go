package worker

import (
	"net/http"

	"angryduck/internal/metrics"
)

// NewServer builds the worker's HTTP handler: /pull and /pull/cancel for
// seed orders from the controller, /metrics, /healthz. When rescue is
// non-nil it also mounts /rescue, /blobs/plan, /blobs/export and
// /snapshots/export, all behind the shared token. extra mounts more
// (image cleanup, the mirror's peer endpoint).
func NewServer(puller *Puller, rescue *Rescue, extra ...func(*http.ServeMux)) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/pull", puller.HandlePull)
	mux.HandleFunc("/pull/cancel", puller.HandleCancel)
	if rescue != nil {
		rescue.Register(mux)
	}
	for _, register := range extra {
		register(mux)
	}
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}
