package metrics

import (
	"net/http/httptest"
	"testing"
)

func TestParseCgroups(t *testing.T) {
	for _, c := range []struct{ in, v2, v1 string }{
		{"0::/\n", "", ""}, // a container's own namespace: the mount's root
		{"0::/kubepods/pod1/abc\n", "/kubepods/pod1/abc", ""},
		{"12:cpu,cpuacct:/x\n4:memory:/kubepods/pod1/abc\n0::/\n", "", "/kubepods/pod1/abc"},
		{"5:memory:/\n", "", ""},
	} {
		v2, v1 := parseCgroups(c.in)
		if v2 != c.v2 || v1 != c.v1 {
			t.Errorf("%q: got %q %q, want %q %q", c.in, v2, v1, c.v2, c.v1)
		}
	}
}

func TestParseMemStat(t *testing.T) {
	v2 := "anon 9437184\nfile 104857600\nkernel 1048576\nshmem 4096\nfile_dirty 8192\nactive_file 73400320\ninactive_file 31457280\nslab_reclaimable 2097152\n"
	m := parseMemStat(v2, memStatV2)
	m.usage = 115343360
	if m.file != 104857600 || m.activeFile != 73400320 || m.inactiveFile != 31457280 || m.dirty != 8192 || m.shmem != 4096 || m.slabReclaimable != 2097152 || !m.hasSlab {
		t.Fatalf("v2: %+v", m)
	}
	if ws := m.workingSet(); ws != 115343360-31457280 {
		t.Fatalf("working set %v", ws)
	}
	v1 := "cache 1\nrss 2\ntotal_cache 300\ntotal_rss 200\ntotal_active_file 120\ntotal_inactive_file 180\ntotal_dirty 4\ntotal_shmem 5\n"
	if m := parseMemStat(v1, memStatV1); m.file != 300 || m.activeFile != 120 || m.inactiveFile != 180 || m.dirty != 4 || m.shmem != 5 || m.hasSlab {
		t.Fatalf("v1: %+v", m)
	}
	// More inactive than usage (stats read a moment apart) never goes negative.
	if ws := (cgroupMem{usage: 10, inactiveFile: 20}).workingSet(); ws != 0 {
		t.Fatalf("working set %v, want 0", ws)
	}
}

// Scrape hooks run at the start of every scrape, before anything is written.
func TestOnScrapeRunsBeforeWriting(t *testing.T) {
	calls := 0
	OnScrape(func() { calls++ })
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if calls != 2 {
		t.Fatalf("hook ran %d times over 2 scrapes", calls)
	}
}
