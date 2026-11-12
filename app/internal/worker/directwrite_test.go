package worker

import (
	"bytes"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

// resident counts the bytes of f's first size bytes in page cache.
func resident(t *testing.T, f *os.File, size int64) int64 {
	return residentRange(t, f, 0, size)
}

// residentRange counts the bytes of [off, off+size) of f in page cache;
// off is page aligned.
func residentRange(t *testing.T, f *os.File, off, size int64) int64 {
	t.Helper()
	m, err := unix.Mmap(int(f.Fd()), off, int(size), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Munmap(m)
	page := int64(os.Getpagesize())
	vec := make([]byte, (size+page-1)/page)
	if _, _, e := unix.Syscall(unix.SYS_MINCORE, uintptr(unsafe.Pointer(&m[0])), uintptr(len(m)), uintptr(unsafe.Pointer(&vec[0]))); e != 0 {
		t.Fatal(e)
	}
	var n int64
	for _, v := range vec {
		n += int64(v & 1)
	}
	return n * page
}

func pageCacheDir(t *testing.T) string {
	dir := t.TempDir()
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil || st.Type == unix.TMPFS_MAGIC {
		t.Skip("the temporary directory is tmpfs: its pages are its storage")
	}
	return dir
}

// holdAt is a write observer with a reader stuck at an offset.
type holdAt struct{ from, written int64 }

func (h *holdAt) opened()           {}
func (h *holdAt) wrote(total int64) { h.written = total }
func (h *holdAt) holdFrom() int64   { return h.from }

// The window keeps little of a file in page cache while writing it, and
// nothing once done; the bytes are all there.
func TestPageWindowKeepsLittleCached(t *testing.T) {
	dir := pageCacheDir(t)
	data := make([]byte, 48<<20+777)
	_, _ = rand.Read(data)
	for name, hold := range map[string]int64{"no reader": -1, "reader at 0": 0, "reader at 30MiB": 30 << 20} {
		f, err := os.Create(filepath.Join(dir, "data"))
		if err != nil {
			t.Fatal(err)
		}
		obs := &holdAt{from: hold}
		pw := &pageWindow{f: f, obs: obs}
		var peak int64
		r := &chunkingReader{b: data, n: 64 << 10, every: func() {
			if c := resident(t, f, max(pw.off, 1)); c > peak {
				peak = c
			}
		}}
		n, err := pw.copyFrom(r)
		if err != nil || n != int64(len(data)) || obs.written != n {
			t.Fatalf("%s: wrote %d (observed %d), %v", name, n, obs.written, err)
		}
		if peak > directCap+directFlushStep+(512<<10) {
			t.Fatalf("%s: %d MiB cached while writing, cap %d MiB", name, peak>>20, directCap>>20)
		}
		pw.release()
		if c := resident(t, f, n); c > 64<<10 {
			t.Fatalf("%s: %d KiB still cached after release", name, c>>10)
		}
		back, _ := os.ReadFile(f.Name())
		if !bytes.Equal(back, data) {
			t.Fatalf("%s: file differs", name)
		}
		f.Close()
		os.Remove(f.Name())
	}
}

// A reader lets go of what it read once the writer no longer holds it.
func TestDropReadLeavesTheWritersWindow(t *testing.T) {
	dir := pageCacheDir(t)
	data := make([]byte, 32<<20)
	_, _ = rand.Read(data)
	path := filepath.Join(dir, "blob")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	f, _ := os.Open(path)
	defer f.Close()
	_ = unix.Fsync(int(f.Fd()))
	size := int64(len(data))
	_ = unix.Fadvise(int(f.Fd()), 0, size, unix.FADV_DONTNEED)
	// Writer at 24 MiB: of [0, 24 MiB) read, [16, 24) is its window.
	_, _ = f.ReadAt(make([]byte, 24<<20), 0)
	dropRead(f, 0, 24<<20, 24<<20, size)
	if c := residentRange(t, f, 0, 16<<20); c > 64<<10 {
		t.Fatalf("%d KiB of what the writer let go still cached", c>>10)
	}
	if c := residentRange(t, f, 16<<20, directWindow); c != directWindow {
		t.Fatalf("%d MiB of the writer's %d MiB window cached", c>>20, directWindow>>20)
	}
	// Writer done: everything read goes.
	_, _ = f.ReadAt(make([]byte, size), 0)
	dropRead(f, 0, size, size, size)
	if c := resident(t, f, size); c > 64<<10 {
		t.Fatalf("%d KiB cached after the writer finished", c>>10)
	}
}

// chunkingReader hands out b n bytes at a time, calling every before each.
type chunkingReader struct {
	b     []byte
	n     int
	every func()
}

func (r *chunkingReader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, io.EOF
	}
	r.every()
	k := copy(p[:min(len(p), r.n)], r.b)
	r.b = r.b[k:]
	return k, nil
}
