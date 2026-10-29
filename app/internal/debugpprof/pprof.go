//go:build pprof

// Package debugpprof serves Go's profiling endpoints when the binary is
// built with -tags pprof and DEBUG_PPROF_ADDR is set. Release images are
// built without the tag: the endpoints and the code behind them are not in
// them at all.
package debugpprof

import (
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"strconv"
)

// Start serves /debug/pprof/ on DEBUG_PPROF_ADDR (e.g. 127.0.0.1:6060)
// when it is set.
func Start(component string) {
	addr := os.Getenv("DEBUG_PPROF_ADDR")
	if addr == "" {
		return
	}
	if r, err := strconv.Atoi(os.Getenv("DEBUG_MEMPROFILERATE")); err == nil && r > 0 {
		runtime.MemProfileRate = r
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	go func() {
		log.Printf("angryduck-%s: pprof listening on %s", component, addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("angryduck-%s: pprof stopped: %v", component, err)
		}
	}()
}
