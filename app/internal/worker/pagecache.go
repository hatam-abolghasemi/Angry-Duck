package worker

import (
	"os"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	"angryduck/internal/logging"
	"angryduck/internal/metrics"
)

var pageCacheReleasedBytes = metrics.NewCounterVec(
	"angryduck_worker_page_cache_released_bytes_total",
	"Page cache this worker handed back after serving blob files: pages its own reads brought in, dropped once the file was idle. Without this they stay charged to the worker's pod as file cache, and count in its container memory.",
	"node")

// pageCache gives back the page cache that reading blob files brings in.
//
// The kernel charges a file page to the cgroup that first reads it. A
// worker serving a blob nobody read lately (a spread source, the mirror, a
// rescue) pulls the whole file into the page cache, charged to the worker's
// pod; read twice, it counts as active and stays in the pod's working set
// until the node runs short of memory. One 120 MB layer served twice is
// 120 MiB of "worker memory" that the worker doesn't use.
//
// So the first reader of a file records which of its pages were already
// cached (mincore), and once nothing has read the file for `after`, the
// pages that weren't are dropped (fadvise DONTNEED). Pages that were there
// before, whoever brought them in, stay. While a file keeps being read (a
// spread serving the same blob to peer after peer), nothing is dropped, so
// transfers read from memory exactly as before.
type pageCache struct {
	after  time.Duration // 0: off
	nodeID string

	mu    sync.Mutex
	files map[fileID]*cachedFile
}

type fileID struct{ dev, ino uint64 }

type cachedFile struct {
	path   string
	size   int64    // bytes covered by before
	before []uint64 // bitset: page i was cached before the first read
	refs   int
	timer  *time.Timer
}

// newPageCache returns a tracker dropping a file's pages after it has been
// idle for after; 0 turns it off.
func newPageCache(after time.Duration, nodeID string) *pageCache {
	return &pageCache{after: after, nodeID: nodeID, files: map[fileID]*cachedFile{}}
}

func noRelease() {}

// open registers a read of f, a blob file of size bytes at path, starting
// now. Call the returned func once done reading.
func (pc *pageCache) open(f *os.File, path string, size int64) func() {
	if pc == nil || pc.after <= 0 || size <= 0 {
		return noRelease
	}
	var st syscall.Stat_t
	if err := syscall.Fstat(int(f.Fd()), &st); err != nil {
		return noRelease
	}
	id := fileID{dev: uint64(st.Dev), ino: st.Ino}
	pc.mu.Lock()
	e := pc.files[id]
	if e == nil {
		// First reader: note what is cached already. Done under the lock
		// so a second reader can't start reading (and caching) first; it
		// is one mmap and mincore, microseconds even for a large file.
		before, all := residentPages(f, size)
		if all {
			// Everything is cached already, by someone else: nothing of
			// ours to give back.
			pc.mu.Unlock()
			return noRelease
		}
		e = &cachedFile{path: path, size: size, before: before}
		pc.files[id] = e
	} else if e.timer != nil {
		e.timer.Stop()
		e.timer = nil
	}
	e.refs++
	pc.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { pc.release(id, e) }) }
}

func (pc *pageCache) release(id fileID, e *cachedFile) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if e.refs--; e.refs > 0 || pc.files[id] != e {
		return
	}
	e.timer = time.AfterFunc(pc.after, func() { pc.drop(id, e) })
}

// drop gives back e's pages that weren't cached before, if nobody started
// reading the file again meanwhile.
func (pc *pageCache) drop(id fileID, e *cachedFile) {
	// Under the lock, so a reader opening the file meanwhile waits and then
	// sees what is really cached, not pages about to go.
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if e.refs > 0 || pc.files[id] != e {
		return
	}
	delete(pc.files, id)

	f, err := os.Open(e.path)
	if err != nil {
		return // removed meanwhile: its pages went with it
	}
	defer f.Close()
	var st syscall.Stat_t
	if err := syscall.Fstat(int(f.Fd()), &st); err != nil || uint64(st.Dev) != id.dev || st.Ino != id.ino {
		return // not the same file any more
	}
	page := int64(os.Getpagesize())
	pages := (e.size + page - 1) / page
	var dropped int64
	for i := int64(0); i < pages; {
		if bitSet(e.before, i) {
			i++
			continue
		}
		j := i
		for j < pages && !bitSet(e.before, j) {
			j++
		}
		off, n := i*page, (j-i)*page
		if off+n > e.size {
			n = e.size - off
		}
		if unix.Fadvise(int(f.Fd()), off, n, unix.FADV_DONTNEED) == nil {
			dropped += n
		}
		i = j
	}
	if dropped > 0 {
		pageCacheReleasedBytes.Add(dropped, pc.nodeID)
		logging.Debugf("angryduck-worker[%s]: gave back up to %d KiB of page cache from %s", pc.nodeID, dropped/1024, e.path)
	}
}

// residentPages reports which of f's first size bytes' pages are in the
// page cache, as a bitset, and whether all of them are. If it can't tell,
// it reports all, so nothing is ever dropped on a guess.
func residentPages(f *os.File, size int64) (bits []uint64, all bool) {
	if int64(int(size)) != size {
		return nil, true
	}
	data, err := unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, true
	}
	defer unix.Munmap(data)
	page := os.Getpagesize()
	vec := make([]byte, (len(data)+page-1)/page)
	if len(vec) == 0 {
		return nil, true
	}
	if _, _, errno := unix.Syscall(unix.SYS_MINCORE, uintptr(unsafe.Pointer(&data[0])), uintptr(len(data)), uintptr(unsafe.Pointer(&vec[0]))); errno != 0 {
		return nil, true
	}
	bits = make([]uint64, (len(vec)+63)/64)
	all = true
	for i, v := range vec {
		if v&1 != 0 {
			bits[i/64] |= 1 << (i % 64)
		} else {
			all = false
		}
	}
	return bits, all
}

func bitSet(bits []uint64, i int64) bool {
	return bits[i/64]&(1<<(i%64)) != 0
}
