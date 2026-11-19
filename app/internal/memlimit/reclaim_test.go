package memlimit

import (
	"os"
	"path/filepath"
	"testing"
)

// A fake cgroup directory: reclaim writes file + slab_reclaimable to
// memory.reclaim only after ReclaimSoon, and only once per burst.
func TestReclaimAfterWork(t *testing.T) {
	dir := t.TempDir()
	stat := "anon 100\nfile 3145728\nslab_reclaimable 2097152\nslab_unreclaimable 4096\n"
	if err := os.WriteFile(filepath.Join(dir, "memory.stat"), []byte(stat), 0o600); err != nil {
		t.Fatal(err)
	}
	f := filepath.Join(dir, "memory.reclaim")
	if err := os.WriteFile(f, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reclaimFile.Store(nil); reclaimDue.Store(false) })

	reclaim() // not enabled: nothing
	reclaimFile.Store(&f)
	reclaim() // enabled but no work: nothing
	if b, _ := os.ReadFile(f); len(b) != 0 {
		t.Fatalf("reclaimed without work: %q", b)
	}
	reclaimDue.Store(true) // what ReclaimSoon sets
	reclaim()
	if b, _ := os.ReadFile(f); string(b) != "5242880" {
		t.Fatalf("memory.reclaim = %q, want 5242880", b)
	}
	os.WriteFile(f, nil, 0o600)
	reclaim() // once per burst
	if b, _ := os.ReadFile(f); len(b) != 0 {
		t.Fatalf("reclaimed twice: %q", b)
	}
}

func TestReclaimSoonOff(t *testing.T) {
	reclaimFile.Store(nil)
	ReclaimSoon()
	if reclaimDue.Load() {
		t.Fatal("ReclaimSoon armed reclaim while it is off")
	}
}
