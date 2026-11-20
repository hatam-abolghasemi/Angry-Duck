package memlimit

import (
	"os"
	"path/filepath"
	"testing"
	"time"
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

// Traffic that never pauses long enough for Soon must not hold the memory:
// the deadline reclaims anyway, once.
func TestReclaimDeadlineUnderSteadyTraffic(t *testing.T) {
	dir := t.TempDir()
	stat := "file 3145728\nslab_reclaimable 2097152\n"
	if err := os.WriteFile(filepath.Join(dir, "memory.stat"), []byte(stat), 0o600); err != nil {
		t.Fatal(err)
	}
	f := filepath.Join(dir, "memory.reclaim")
	if err := os.WriteFile(f, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	oldQ, oldD := quietAfter, reclaimDeadline
	quietAfter, reclaimDeadline = 100*time.Millisecond, 150*time.Millisecond
	t.Cleanup(func() {
		quietAfter, reclaimDeadline = oldQ, oldD
		reclaimFile.Store(nil)
		reclaimDue.Store(false)
	})
	reclaimFile.Store(&f)

	ReclaimSoon()
	stop := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(stop) { // a request every 20ms: never quiet
		Soon()
		time.Sleep(20 * time.Millisecond)
	}
	b, _ := os.ReadFile(f)
	if string(b) != "5242880" {
		t.Fatalf("memory.reclaim = %q under steady traffic, want the deadline to have written 5242880", b)
	}
	if reclaimDue.Load() {
		t.Fatal("reclaim still due after the deadline")
	}
	deadline.mu.Lock()
	armed := deadline.t != nil
	deadline.mu.Unlock()
	if armed {
		t.Fatal("deadline left armed after reclaiming")
	}
}

// Reclaim never runs while work moves data: a quiet or deadline moment
// that comes during a transfer waits for its end, then reclaims at once.
func TestReclaimWaitsForWorkInFlight(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "memory.stat"), []byte("file 3145728\nslab_reclaimable 2097152\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	f := filepath.Join(dir, "memory.reclaim")
	if err := os.WriteFile(f, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	oldQ, oldD := quietAfter, reclaimDeadline
	quietAfter, reclaimDeadline = 20*time.Millisecond, 50*time.Millisecond
	t.Cleanup(func() {
		quietAfter, reclaimDeadline = oldQ, oldD
		reclaimFile.Store(nil)
		reclaimDue.Store(false)
		reclaimWaiting.Store(false)
	})
	reclaimFile.Store(&f)

	transfer := Working() // a blob still being served
	ReclaimSoon()         // an unpack just ended
	time.Sleep(150 * time.Millisecond)
	if b, _ := os.ReadFile(f); len(b) != 0 {
		t.Fatalf("reclaimed during a transfer: %q", b)
	}
	if !reclaimDue.Load() {
		t.Fatal("reclaim dropped instead of waiting")
	}
	transfer()
	deadline := time.Now().Add(time.Second)
	for {
		if b, _ := os.ReadFile(f); string(b) == "5242880" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no reclaim after the transfer ended")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if inFlight.Load() != 0 || reclaimWaiting.Load() {
		t.Fatalf("left inFlight=%d waiting=%t", inFlight.Load(), reclaimWaiting.Load())
	}
}

// Records of files whose metadata isn't written out yet can't be freed:
// a hand-back that leaves 1 MiB or more tries once more after the
// kernel's writeback window, and only once per burst.
func TestReclaimRetriesOnceAfterWriteback(t *testing.T) {
	dir := t.TempDir()
	// memory.stat doesn't change here: the kernel "freed nothing".
	if err := os.WriteFile(filepath.Join(dir, "memory.stat"), []byte("file 0\nslab_reclaimable 26214400\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	f := filepath.Join(dir, "memory.reclaim")
	if err := os.WriteFile(f, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	oldQ, oldD, oldR := quietAfter, reclaimDeadline, retryAfter.Load()
	quietAfter, reclaimDeadline = 10*time.Millisecond, time.Hour
	retryAfter.Store(int64(60 * time.Millisecond))
	t.Cleanup(func() {
		quietAfter, reclaimDeadline = oldQ, oldD
		retryAfter.Store(oldR)
		reclaimFile.Store(nil)
		reclaimDue.Store(false)
	})
	reclaimFile.Store(&f)
	written := func() bool { b, _ := os.ReadFile(f); return len(b) > 0 }
	wait := func(what string, cond func() bool) {
		t.Helper()
		for end := time.Now().Add(time.Second); !cond(); time.Sleep(5 * time.Millisecond) {
			if time.Now().After(end) {
				t.Fatal(what)
			}
		}
	}

	ReclaimSoon()
	wait("no first hand-back", written)
	os.Truncate(f, 0)
	time.Sleep(30 * time.Millisecond)
	if written() {
		t.Fatal("retried before the writeback window")
	}
	wait("no retry after the writeback window", written)
	os.Truncate(f, 0)
	time.Sleep(200 * time.Millisecond)
	if written() {
		t.Fatal("retried more than once")
	}
}

func TestWritebackWindowFromKernel(t *testing.T) {
	d, err := writebackWindow()
	if err != nil {
		t.Skipf("no /proc/sys/vm here: %v", err)
	}
	if d <= 0 || d > time.Hour {
		t.Fatalf("writeback window %s", d)
	}
}

// Without the kernel's window there is no retry: nothing is guessed.
func TestNoRetryWithoutTheKernelsWindow(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "memory.stat"), []byte("file 0\nslab_reclaimable 26214400\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	f := filepath.Join(dir, "memory.reclaim")
	if err := os.WriteFile(f, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	oldQ, oldD, oldR := quietAfter, reclaimDeadline, retryAfter.Load()
	quietAfter, reclaimDeadline = 10*time.Millisecond, time.Hour
	retryAfter.Store(0)
	t.Cleanup(func() {
		quietAfter, reclaimDeadline = oldQ, oldD
		retryAfter.Store(oldR)
		reclaimFile.Store(nil)
		reclaimDue.Store(false)
	})
	reclaimFile.Store(&f)
	ReclaimSoon()
	for end := time.Now().Add(time.Second); ; time.Sleep(5 * time.Millisecond) {
		if b, _ := os.ReadFile(f); len(b) > 0 {
			break
		}
		if time.Now().After(end) {
			t.Fatal("no hand-back")
		}
	}
	if retryUsed.Load() {
		t.Fatal("a retry was scheduled without the kernel's writeback window")
	}
}
