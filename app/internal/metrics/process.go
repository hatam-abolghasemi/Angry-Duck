package metrics

import (
	"context"
	"os"
	"runtime/metrics"
	"strconv"
	"strings"
	"time"
)

var processMemory = NewGaugeVec(
	"angryduck_process_memory_bytes",
	"This process's memory, refreshed every 15s. rss_anon is what it really "+
		"holds; rss_file is its own code, shared and reclaimable by the kernel. "+
		"heap_live is what the Go heap needs right now; heap_retained is freed "+
		"heap kept for reuse (returned to the OS when idle); heap_released was "+
		"already returned. From its container's cgroup: working_set is what "+
		"kubectl top and container_memory_working_set_bytes show, page_cache "+
		"the file cache charged to the container (files it read, its own "+
		"binary).",
	"component", "kind")

// TrackProcessMemory publishes angryduck_process_memory_bytes until ctx
// ends, so how much memory is actually needed is measurable, separately
// from what the kernel or Go's allocator happens to keep around.
func TrackProcessMemory(ctx context.Context, component string) {
	samples := []metrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/memory/classes/heap/unused:bytes"},
		{Name: "/memory/classes/heap/free:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
		{Name: "/memory/classes/heap/stacks:bytes"},
	}
	update := func() {
		metrics.Read(samples)
		v := func(i int) float64 { return float64(samples[i].Value.Uint64()) }
		processMemory.Set(v(0), component, "heap_live")
		processMemory.Set(v(1)+v(2), component, "heap_retained")
		processMemory.Set(v(3), component, "heap_released")
		processMemory.Set(v(4), component, "stacks")
		if anon, file, ok := rss(); ok {
			processMemory.Set(anon, component, "rss_anon")
			processMemory.Set(file, component, "rss_file")
		}
		if ws, cache, ok := cgroupMemory(); ok {
			processMemory.Set(ws, component, "working_set")
			processMemory.Set(cache, component, "page_cache")
		}
	}
	update()
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			update()
		}
	}
}

func rss() (anon, file float64, ok bool) {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, 0, false
	}
	for _, line := range strings.Split(string(b), "\n") {
		k, v, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		f := strings.Fields(v)
		if len(f) == 0 {
			continue
		}
		kb, _ := strconv.ParseFloat(f[0], 64)
		switch k {
		case "RssAnon":
			anon, ok = kb*1024, true
		case "RssFile":
			file = kb * 1024
		}
	}
	return anon, file, ok
}

// cgroupMemory reads this container's memory from its cgroup, the way
// cAdvisor does: the working set is usage minus inactive file pages, and
// page cache is every file page charged to it.
func cgroupMemory() (workingSet, pageCache float64, ok bool) {
	v2, v1 := ownCgroups()
	// cgroup v2, then v1; this process's own cgroup where it is visible
	// (no cgroup namespace), else the mount's root, which a container
	// sees as its own.
	for _, c := range []struct{ dir, usage, stat, inactive, file string }{
		{"/sys/fs/cgroup" + v2, "memory.current", "memory.stat", "inactive_file", "file"},
		{"/sys/fs/cgroup", "memory.current", "memory.stat", "inactive_file", "file"},
		{"/sys/fs/cgroup/memory" + v1, "memory.usage_in_bytes", "memory.stat", "total_inactive_file", "total_cache"},
		{"/sys/fs/cgroup/memory", "memory.usage_in_bytes", "memory.stat", "total_inactive_file", "total_cache"},
	} {
		u, err := os.ReadFile(c.dir + "/" + c.usage)
		if err != nil {
			continue
		}
		usage, err := strconv.ParseFloat(strings.TrimSpace(string(u)), 64)
		if err != nil {
			continue
		}
		st, err := os.ReadFile(c.dir + "/" + c.stat)
		if err != nil {
			continue
		}
		var inactive, file float64
		for _, line := range strings.Split(string(st), "\n") {
			k, v, found := strings.Cut(line, " ")
			if !found {
				continue
			}
			switch k {
			case c.inactive:
				inactive, _ = strconv.ParseFloat(v, 64)
			case c.file:
				file, _ = strconv.ParseFloat(v, 64)
			}
		}
		if inactive > usage {
			inactive = usage
		}
		return usage - inactive, file, true
	}
	return 0, 0, false
}

// ownCgroups returns this process's cgroup v2 path and cgroup v1 memory
// controller path, from /proc/self/cgroup ("" when not listed).
func ownCgroups() (v2, v1 string) {
	b, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return "", ""
	}
	return parseCgroups(string(b))
}

func parseCgroups(s string) (v2, v1 string) {
	for _, line := range strings.Split(s, "\n") {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 || parts[2] == "/" {
			continue
		}
		switch {
		case parts[0] == "0" && parts[1] == "":
			v2 = parts[2]
		case strings.Contains(","+parts[1]+",", ",memory,"):
			v1 = parts[2]
		}
	}
	return v2, v1
}

var buildInfo = NewGaugeVec(
	"angryduck_build_info",
	"One series per running Angry Duck process: its version, and as the value the Unix time it started. A new version shows up when a release rolls out; a new value means the process restarted.",
	"component", "version", "node")

// PublishBuildInfo sets angryduck_build_info for this process. node is
// the node it runs on ("" when unknown).
func PublishBuildInfo(component, version, node string) {
	buildInfo.Set(float64(time.Now().Unix()), component, version, node)
}
