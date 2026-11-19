package metrics

import (
	"context"
	"os"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"time"
)

var processMemory = NewGaugeVec(
	"angryduck_process_memory_bytes",
	"This process's memory, read at each scrape. rss_anon is what it really "+
		"holds; rss_file is its own code, shared and reclaimable by the kernel. "+
		"heap_live is what the Go heap needs right now; heap_retained is freed "+
		"heap kept for reuse (returned to the OS when idle); heap_released was "+
		"already returned. From its container's cgroup: working_set is what "+
		"kubectl top and container_memory_working_set_bytes show, page_cache "+
		"the file cache charged to the container (files it read, its own "+
		"binary), split into page_cache_active (counted in working_set) and "+
		"page_cache_inactive (not counted, first to go); page_cache_dirty is "+
		"written but not yet on disk, shmem is tmpfs and shared memory (kept "+
		"until freed, never dropped like file cache). slab_reclaimable is the "+
		"kernel's records of files the container created or opened (inodes, "+
		"directory entries): counted in working_set, freed once the files are "+
		"deleted or the node needs memory, and no process can hand it back "+
		"itself (cgroup v2 only).",
	"component", "kind")

// TrackProcessMemory publishes angryduck_process_memory_bytes, read at each
// scrape (nothing runs between scrapes), so how much memory is actually
// needed is measurable, separately from what the kernel or Go's allocator
// happens to keep around. ctx is unused, kept for callers.
func TrackProcessMemory(ctx context.Context, component string) {
	_ = ctx
	var mu sync.Mutex
	samples := []metrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/memory/classes/heap/unused:bytes"},
		{Name: "/memory/classes/heap/free:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
		{Name: "/memory/classes/heap/stacks:bytes"},
	}
	update := func() {
		mu.Lock()
		defer mu.Unlock()
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
		if m, ok := cgroupMemory(); ok {
			processMemory.Set(m.workingSet(), component, "working_set")
			processMemory.Set(m.file, component, "page_cache")
			processMemory.Set(m.activeFile, component, "page_cache_active")
			processMemory.Set(m.inactiveFile, component, "page_cache_inactive")
			processMemory.Set(m.dirty, component, "page_cache_dirty")
			processMemory.Set(m.shmem, component, "shmem")
			if m.hasSlab {
				processMemory.Set(m.slabReclaimable, component, "slab_reclaimable")
			}
		}
	}
	update()
	OnScrape(update)
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

// cgroupMem is a container's memory, from its cgroup.
type cgroupMem struct {
	usage, file, activeFile, inactiveFile, dirty, shmem float64
	slabReclaimable                                     float64
	hasSlab                                             bool // memory.stat has it (cgroup v2)
}

// workingSet is usage minus inactive file pages, the way cAdvisor (and so
// kubectl top and the OOM killer's view) counts it.
func (m cgroupMem) workingSet() float64 {
	return m.usage - min(m.inactiveFile, m.usage)
}

// memStatKeys names memory.stat's fields: cgroup v2's, and cgroup v1's
// hierarchical ones.
// slab is "" where memory.stat doesn't have it (cgroup v1).
type memStatKeys struct{ file, active, inactive, dirty, shmem, slab string }

var (
	memStatV2 = memStatKeys{"file", "active_file", "inactive_file", "file_dirty", "shmem", "slab_reclaimable"}
	memStatV1 = memStatKeys{"total_cache", "total_active_file", "total_inactive_file", "total_dirty", "total_shmem", ""}
)

// parseMemStat reads the fields of a memory.stat file.
func parseMemStat(s string, k memStatKeys) cgroupMem {
	var m cgroupMem
	for _, line := range strings.Split(s, "\n") {
		key, v, found := strings.Cut(line, " ")
		if !found {
			continue
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			continue
		}
		switch key {
		case k.file:
			m.file = f
		case k.active:
			m.activeFile = f
		case k.inactive:
			m.inactiveFile = f
		case k.dirty:
			m.dirty = f
		case k.shmem:
			m.shmem = f
		case k.slab:
			m.slabReclaimable, m.hasSlab = f, true
		}
	}
	return m
}

// cgroupMemory reads this container's memory from its cgroup.
func cgroupMemory() (cgroupMem, bool) {
	v2, v1 := ownCgroups()
	// cgroup v2, then v1; this process's own cgroup where it is visible
	// (no cgroup namespace), else the mount's root, which a container
	// sees as its own.
	for _, c := range []struct {
		dir, usage string
		keys       memStatKeys
	}{
		{"/sys/fs/cgroup" + v2, "memory.current", memStatV2},
		{"/sys/fs/cgroup", "memory.current", memStatV2},
		{"/sys/fs/cgroup/memory" + v1, "memory.usage_in_bytes", memStatV1},
		{"/sys/fs/cgroup/memory", "memory.usage_in_bytes", memStatV1},
	} {
		u, err := os.ReadFile(c.dir + "/" + c.usage)
		if err != nil {
			continue
		}
		usage, err := strconv.ParseFloat(strings.TrimSpace(string(u)), 64)
		if err != nil {
			continue
		}
		st, err := os.ReadFile(c.dir + "/memory.stat")
		if err != nil {
			continue
		}
		m := parseMemStat(string(st), c.keys)
		m.usage = usage
		return m, true
	}
	return cgroupMem{}, false
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
