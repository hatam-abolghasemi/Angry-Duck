// Package memlimit makes the Go runtime actually respect the container's
// cgroup memory limit, instead of growing the heap using Go's own
// GOGC=100 heuristic (target heap ~= 2x live set) and never handing freed
// pages back to the OS absent real memory pressure. Go does not do this
// on its own — GOMEMLIMIT (the mechanism that makes it happen,
// runtime/debug.SetMemoryLimit under the hood, added in Go 1.19) only
// takes effect if something sets it, and by default nothing does. Once
// it's set, the runtime's scavenger also becomes meaningfully more
// aggressive about returning idle memory to the OS, which is the actual
// behavior we want ("keep only what's live, give the rest back") rather
// than something a diagnostic can show but not change.
package memlimit

import (
	"os"
	"runtime/debug"
	"strconv"
	"strings"

	"angryduck/internal/logging"
)

// headroomFraction is how much of the container's memory limit Go is
// allowed to target. GOMEMLIMIT is a SOFT limit — the GC tries to stay
// under it but can briefly exceed it during a collection, especially
// under an allocation-heavy burst (several concurrent preheat pulls, a
// big crictl listing). Targeting the full cgroup limit risks the kernel
// OOM-killing the container on that kind of transient overshoot; this
// leaves headroom for GC pause behavior, goroutine stacks, and anything
// else that isn't Go heap.
const headroomFraction = 0.9

// cgroup memory-limit file locations. v2 is checked first (the default on
// any reasonably current Kubernetes/containerd setup); v1 is a fallback
// for older nodes.
const (
	cgroupV2MaxPath   = "/sys/fs/cgroup/memory.max"
	cgroupV1LimitPath = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
)

// defaultGCPercent replaces Go's GOGC=100 unless GOGC is set. Both
// binaries keep a small live heap that allocates in bursts (a controller
// restart makes every worker send its full layer inventory at once; a
// transfer plan on a worker), and at 100 the heap is allowed to grow to
// twice the live set after each burst and stay there until the next GC,
// which in their quiet steady state can be minutes away. 50 keeps that
// high-water mark at 1.5x for a few extra GC cycles during bursts.
const defaultGCPercent = 50

// Apply reads this container's OWN cgroup memory limit — the one this
// process's PID 1 actually runs in, unrelated to HOST_ROOT/the host-root
// chroot the worker uses for crictl/ctr, which is about the host's
// containerd, not this process's own resource limits — and, if a real
// limit is set (not "unlimited"), applies it to the Go runtime via
// debug.SetMemoryLimit.
//
// Safe to call unconditionally: if no limit is found (no memory limit set
// on the container, a cgroup path missing for any reason), it logs that
// and leaves Go's default GC behavior untouched rather than guessing at a
// number. component is just for the log line ("worker" or "controller").
func Apply(component string) {
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(defaultGCPercent)
	}
	limit, source, ok := readLimit()
	if !ok {
		logging.Infof("angryduck-%s: no cgroup memory limit found — leaving Go's default GC behavior untouched", component)
		return
	}
	target := int64(float64(limit) * headroomFraction)
	debug.SetMemoryLimit(target)
	logging.Infof("angryduck-%s: cgroup memory limit=%d bytes (%s) — set GOMEMLIMIT=%d bytes (%.0f%% headroom)",
		component, limit, source, target, (1-headroomFraction)*100)
}

func readLimit() (limitBytes int64, source string, ok bool) {
	if v, ok := readCgroupV2(cgroupV2MaxPath); ok {
		return v, "cgroup v2", true
	}
	if v, ok := readCgroupV1(cgroupV1LimitPath); ok {
		return v, "cgroup v1", true
	}
	return 0, "", false
}

func readCgroupV2(path string) (int64, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	s := strings.TrimSpace(string(data))
	if s == "max" {
		return 0, false // explicitly unlimited
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v <= 0 {
		return 0, false
	}
	return v, true
}

func readCgroupV1(path string) (int64, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	v, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil || v <= 0 {
		return 0, false
	}
	// cgroup v1's "no limit" sentinel is an enormous number (typically
	// 9223372036854771712 — close to but not exactly math.MaxInt64, due
	// to page-size rounding) rather than a human-readable "max" string.
	// Treat anything absurdly large as effectively unlimited rather than
	// applying a GOMEMLIMIT so large it does nothing anyway.
	const absurdlyLarge = int64(1) << 50 // 1 PiB
	if v > absurdlyLarge {
		return 0, false
	}
	return v, true
}
