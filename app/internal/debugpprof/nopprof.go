//go:build !pprof

// Package debugpprof serves Go's profiling endpoints in binaries built
// with -tags pprof; in every other build Start does nothing.
package debugpprof

// Start does nothing: this binary was built without -tags pprof.
func Start(string) {}
