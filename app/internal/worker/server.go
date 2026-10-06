package worker

import (
	"net/http"

	"angryduck/internal/metrics"
	"angryduck/internal/sharedtoken"
)

// NewServer builds the worker's HTTP handler: /pull and /pull/cancel for
// seed orders from the controller, /metrics, /healthz. When rescue is
// non-nil it also mounts /rescue, /blobs/plan, /blobs/export and
// /snapshots/export, all behind the shared token. extra mounts more
// (image cleanup, the mirror's peer endpoint).
//
// auth guards /pull and /pull/cancel: the worker runs on the host network,
// where a NetworkPolicy doesn't apply, so without it anything that can
// reach the node could make it pull arbitrary images with the registry
// credentials.
func NewServer(auth *sharedtoken.Auth, puller *Puller, rescue *Rescue, extra ...func(*http.ServeMux)) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/pull", auth.Guard("/pull", puller.HandlePull))
	mux.HandleFunc("/pull/cancel", auth.Guard("/pull/cancel", puller.HandleCancel))
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
