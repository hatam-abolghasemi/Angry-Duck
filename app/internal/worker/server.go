package worker

import "net/http"

// NewServer builds the worker's HTTP handler: /pull to receive orders from
// the controller, /healthz for liveness/readiness probes.
func NewServer(puller *Puller) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/pull", puller.HandlePull)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}
