package worker

import (
	"strings"
	"testing"
	"time"
)

const sampleMetrics = `# HELP node_filesystem_size_bytes Filesystem size in bytes.
# TYPE node_filesystem_size_bytes gauge
node_filesystem_size_bytes{device="tmpfs",fstype="tmpfs",mountpoint="/dev/shm"} 1.0e+09
node_filesystem_size_bytes{device="/dev/sda1",fstype="ext4",mountpoint="/"} 1.0e+11
node_filesystem_size_bytes{device="overlay",fstype="overlay",mountpoint="/var/lib/docker"} 5.0e+10
# HELP node_filesystem_free_bytes Filesystem free space in bytes.
# TYPE node_filesystem_free_bytes gauge
node_filesystem_free_bytes{device="tmpfs",fstype="tmpfs",mountpoint="/dev/shm"} 9.0e+08
node_filesystem_free_bytes{device="/dev/sda1",fstype="ext4",mountpoint="/"} 2.5e+10
node_filesystem_free_bytes{device="overlay",fstype="overlay",mountpoint="/var/lib/docker"} 1.0e+10
`

func TestParseRootUtilization(t *testing.T) {
	result, err := parseRootUtilization(strings.NewReader(sampleMetrics))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// (1e11 - 2.5e10) / 1e11 = 0.75
	want := 0.75
	if diff := result.Utilization - want; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("got utilization %v, want %v", result.Utilization, want)
	}
	if result.SizeBytes != 1.0e+11 {
		t.Errorf("got SizeBytes %v, want %v", result.SizeBytes, 1.0e+11)
	}
	if result.FreeBytes != 2.5e+10 {
		t.Errorf("got FreeBytes %v, want %v", result.FreeBytes, 2.5e+10)
	}
	wantUsed := 1.0e+11 - 2.5e+10
	if result.UsedBytes != wantUsed {
		t.Errorf("got UsedBytes %v, want %v", result.UsedBytes, wantUsed)
	}
}

func TestParseRootUtilizationMissing(t *testing.T) {
	_, err := parseRootUtilization(strings.NewReader("# nothing here\n"))
	if err == nil {
		t.Fatalf("expected error when root filesystem metrics are absent")
	}
}

func TestHasExcludedFstype(t *testing.T) {
	cases := map[string]bool{
		`{device="x",fstype="tmpfs",mountpoint="/"}`:      true,
		`{device="x",fstype="overlay",mountpoint="/"}`:    true,
		`{device="x",fstype="fuse.lxcfs",mountpoint="/"}`: true,
		`{device="x",fstype="ext4",mountpoint="/"}`:       false,
	}
	for labels, want := range cases {
		if got := hasExcludedFstype(labels); got != want {
			t.Errorf("hasExcludedFstype(%q) = %v, want %v", labels, got, want)
		}
	}
}

func TestCountersStartAtZero(t *testing.T) {
	const node = "test-node-zero"
	NewSpread(nil, nil, "", node, "linux/amd64", "http://c", time.Minute)
	NewMirror(nil, node, "", "http://c")
	NewPuller(newFakeRuntime(), node, false)
	out := scrapeMetrics(t)
	for _, want := range []string{
		`angryduck_worker_spread_transfers_total{node="` + node + `",path="registry",result="ok"} 0`,
		`angryduck_worker_spread_transfer_milliseconds_total{node="` + node + `",path="peer"} 0`,
		`angryduck_worker_mirror_requests_total{node="` + node + `",kind="blob",result="miss"} 0`,
		`angryduck_worker_pulls_total{node="` + node + `",result="failure",registry=""} 0`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s", want)
		}
	}
}
