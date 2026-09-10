package worker

import (
	"net/http"

	"angryduck/internal/metrics"
)

// NewServer exposes the controller order endpoint plus the local OCI mirror.
// The mirror is intentionally on its own listener in production but is also
// mounted here for tests and simple local runs.
func NewServer(puller *Puller, registry *RegistryMirror) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/pull", puller.HandlePull)
	if registry != nil {
		mux.HandleFunc("/v2/", registry.Handle)
	}
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

// NewRegistryServer exposes only the OCI mirror surface on the port used by
// containerd. Control APIs stay on the worker's management port.
func NewRegistryServer(registry *RegistryMirror) http.Handler {
	mux := http.NewServeMux()
	if registry != nil {
		mux.HandleFunc("/v2/", registry.Handle)
	}
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}
