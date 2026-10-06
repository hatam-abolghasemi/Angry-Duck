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
		"already returned.",
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
