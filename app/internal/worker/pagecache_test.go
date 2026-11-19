package worker

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// coldFile writes size bytes to a file and evicts them from the page
// cache, so reading it brings the pages in, as serving an old blob does.
func coldFile(t *testing.T, size int) (*os.File, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "blob")
	if err := os.WriteFile(path, make([]byte, size), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	_ = f.Sync()
	_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
	if _, all := residentPages(f, int64(size)); all {
		t.Skip("can't evict the test file from the page cache here (tmpfs?)")
	}
	return f, path
}

func cachedPages(f *os.File, size int64) int {
	bits, all := residentPages(f, size)
	if all {
		return int((size + 4095) / 4096)
	}
	n := 0
	for i := int64(0); i < (size+4095)/4096; i++ {
		if bitSet(bits, i) {
			n++
		}
	}
	return n
}

func TestPageCacheGivesBackWhatReadsBroughtIn(t *testing.T) {
	const size = 1 << 20
	f, path := coldFile(t, size)
	pc := newPageCache(50*time.Millisecond, "n")
	r1 := pc.open(f, path, size)
	r2 := pc.open(f, path, size) // a second reader of the same file
	if _, err := io.Copy(io.Discard, io.NewSectionReader(f, 0, size)); err != nil {
		t.Fatal(err)
	}
	if cachedPages(f, size) == 0 {
		t.Skip("reads didn't populate the page cache here")
	}
	r1()
	r1() // releasing twice counts once
	time.Sleep(150 * time.Millisecond)
	if cachedPages(f, size) == 0 {
		t.Fatal("dropped while a reader was still reading")
	}
	r2()
	deadline := time.Now().Add(2 * time.Second)
	for cachedPages(f, size) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d pages still cached after the last reader and the idle time", cachedPages(f, size))
		}
		time.Sleep(10 * time.Millisecond)
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if len(pc.files) != 0 {
		t.Fatal("entry left behind")
	}
}

func TestPageCacheKeepsWhatWasCachedBefore(t *testing.T) {
	const size = 1 << 20
	f, path := coldFile(t, size)
	// The first half is cached before we start (someone else read it).
	if _, err := io.Copy(io.Discard, io.NewSectionReader(f, 0, size/2)); err != nil {
		t.Fatal(err)
	}
	before := cachedPages(f, size)
	if before == 0 {
		t.Skip("reads didn't populate the page cache here")
	}
	pc := newPageCache(20*time.Millisecond, "n")
	release := pc.open(f, path, size)
	if _, err := io.Copy(io.Discard, io.NewSectionReader(f, 0, size)); err != nil {
		t.Fatal(err)
	}
	release()
	time.Sleep(200 * time.Millisecond)
	if got := cachedPages(f, size); got < before {
		t.Fatalf("%d pages cached, fewer than the %d cached before our reads", got, before)
	}
}

func TestPageCacheOff(t *testing.T) {
	var pc *pageCache
	pc.open(nil, "", 10)() // no tracker: nothing to do
	newPageCache(0, "n").open(nil, "", 10)()
}

// The tracked gauges follow a file from reading to idle to handed back,
// and fall back to zero: a level that holds is a reader that never let go.
func TestPageCacheTrackedGauges(t *testing.T) {
	const size = 256 << 10
	f, path := coldFile(t, size)
	pc := newPageCache(50*time.Millisecond, "gauges")
	state := func() (reading, idle int64, files int) {
		pc.mu.Lock()
		defer pc.mu.Unlock()
		return pc.bytes[pcReading], pc.bytes[pcIdle], pc.count[pcReading] + pc.count[pcIdle]
	}
	r1 := pc.open(f, path, size)
	r2 := pc.open(f, path, size)
	if reading, idle, files := state(); reading != size || idle != 0 || files != 1 {
		t.Fatalf("while read: reading %d idle %d files %d, want %d 0 1", reading, idle, files, size)
	}
	r1()
	if reading, _, _ := state(); reading != size {
		t.Fatalf("one reader left: reading %d, want %d", reading, size)
	}
	r2()
	if reading, idle, _ := state(); reading != 0 || idle != size {
		t.Fatalf("all done: reading %d idle %d, want 0 %d", reading, idle, size)
	}
	r3 := pc.open(f, path, size) // read again before the drop: back to reading
	if reading, idle, files := state(); reading != size || idle != 0 || files != 1 {
		t.Fatalf("read again: reading %d idle %d files %d", reading, idle, files)
	}
	r3()
	deadline := time.Now().Add(2 * time.Second)
	for {
		reading, idle, files := state()
		if reading == 0 && idle == 0 && files == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after the drop: reading %d idle %d files %d, want all 0", reading, idle, files)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(scrapeMetrics(t), `angryduck_worker_page_cache_tracked_bytes{node="gauges",state="idle"} 0`) {
		t.Fatal("gauge not exported at 0")
	}
}
