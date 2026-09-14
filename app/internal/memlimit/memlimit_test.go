package memlimit

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "memlimit")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	return path
}

func TestReadCgroupV2(t *testing.T) {
	cases := []struct {
		name    string
		content string
		want    int64
		wantOk  bool
	}{
		{"numeric limit", "134217728\n", 134217728, true},
		{"numeric limit no trailing newline", "67108864", 67108864, true},
		{"unlimited", "max\n", 0, false},
		{"zero is not a real limit", "0\n", 0, false},
		{"garbage", "not-a-number\n", 0, false},
		{"empty", "", 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := writeTempFile(t, c.content)
			got, ok := readCgroupV2(path)
			if ok != c.wantOk {
				t.Fatalf("readCgroupV2(%q) ok = %v, want %v", c.content, ok, c.wantOk)
			}
			if ok && got != c.want {
				t.Errorf("readCgroupV2(%q) = %d, want %d", c.content, got, c.want)
			}
		})
	}

	if _, ok := readCgroupV2(filepath.Join(t.TempDir(), "does-not-exist")); ok {
		t.Error("expected ok=false for a missing file")
	}
}

func TestReadCgroupV1(t *testing.T) {
	cases := []struct {
		name    string
		content string
		want    int64
		wantOk  bool
	}{
		{"numeric limit", "134217728\n", 134217728, true},
		{"unlimited sentinel (huge number)", "9223372036854771712\n", 0, false},
		{"zero is not a real limit", "0\n", 0, false},
		{"garbage", "nope\n", 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := writeTempFile(t, c.content)
			got, ok := readCgroupV1(path)
			if ok != c.wantOk {
				t.Fatalf("readCgroupV1(%q) ok = %v, want %v", c.content, ok, c.wantOk)
			}
			if ok && got != c.want {
				t.Errorf("readCgroupV1(%q) = %d, want %d", c.content, got, c.want)
			}
		})
	}
}

// TestApplyDoesNotPanicWithoutCgroupFiles confirms Apply degrades
// gracefully (just logs and returns) in an environment with no cgroup
// memory files at all — e.g. running go test outside any container.
func TestApplyDoesNotPanicWithoutCgroupFiles(t *testing.T) {
	// Real /sys/fs/cgroup paths may or may not exist in the test sandbox
	// itself; either way Apply must not panic, and its readLimit helper
	// must not error out ungracefully when the real system happens to
	// have no accessible memory limit for this test process.
	Apply("test")
}
