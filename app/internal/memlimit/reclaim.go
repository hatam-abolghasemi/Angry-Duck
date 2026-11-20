package memlimit

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"

	"angryduck/internal/logging"
)

// Reclaim after file-writing work.
//
// Files the worker writes itself (layers unpacked while they arrive) are
// charged to its cgroup twice over: their page cache, and the kernel's
// inode and dentry records (slab_reclaimable, about 1.6 KiB a file). The
// page cache the unpack drops as it goes; the records it cannot: the
// kernel keeps them until memory runs short, so after a 20,000-file layer
// the worker shows ~40 MiB it will never use again and looks like it
// leaks. Writing to the cgroup's memory.reclaim (kernel 5.19+) hands it
// all back at once, measured: slab_reclaimable 40.9 MB -> 2.2 MB, file
// 110 MB -> 0 MB, in one write.
//
// It runs only after such work (ReclaimSoon), once things are quiet (the
// same few seconds as Soon) or reclaimDeadline after the work ended,
// whichever comes first, and only where EnableReclaim found a
// memory.reclaim it can write. Nothing runs between bursts. Each run logs
// what it handed back, at info level: one line per burst.

var reclaimFile atomic.Pointer[string] // memory.reclaim to write; nil: off
var reclaimDue atomic.Bool

// reclaimDeadline bounds the wait for quiet: on a node whose mirror or
// transfers never pause for a few seconds, Soon's wait could go on for
// as long as the traffic does, and the memory with it.
var reclaimDeadline = 30 * time.Second

var deadline struct {
	mu sync.Mutex
	t  *time.Timer // armed while a reclaim is due; nil otherwise
}

// EnableReclaim finds the worker's own cgroup's memory.reclaim and turns
// reclaim-after-work on. /sys/fs/cgroup is read-only in a container, so
// failing that it mounts the cgroup2 filesystem at mnt: with a private
// cgroup namespace (the default with cgroup v2) the mount's root is the
// container's own cgroup. That mount needs CAP_SYS_ADMIN and an AppArmor
// profile that allows it (the chart sets both with stream unpack on).
// On failure reclaim stays off; it returns why.
func EnableReclaim(mnt string) error {
	f := "/sys/fs/cgroup/memory.reclaim"
	if !writable(f) {
		if mnt == "" {
			return errors.New("/sys/fs/cgroup is read-only and no mount directory was given")
		}
		if err := mountCgroup(mnt); err != nil {
			return err
		}
		f = filepath.Join(mnt, "memory.reclaim")
		if !writable(f) {
			return fmt.Errorf("%s is missing or read-only (cgroup v1, or a kernel before 5.19)", f)
		}
	}
	reclaimFile.Store(&f)
	return nil
}

var mountOnce sync.Once
var mountErr error

func mountCgroup(mnt string) error {
	mountOnce.Do(func() {
		if err := os.MkdirAll(mnt, 0o700); err != nil {
			mountErr = err
			return
		}
		if _, err := os.Stat(filepath.Join(mnt, "memory.reclaim")); err == nil {
			return // already mounted
		}
		if err := unix.Mount("cgroup2", mnt, "cgroup2", unix.MS_NOSUID|unix.MS_NODEV|unix.MS_NOEXEC, ""); err != nil {
			mountErr = fmt.Errorf("mount cgroup2 at %s: %w", mnt, err)
		}
	})
	return mountErr
}

func writable(f string) bool {
	return unix.Access(f, unix.W_OK) == nil
}

// ReclaimSoon says work that wrote files just ended: once things are quiet,
// the cgroup's reclaimable memory goes back to the kernel. A no-op unless
// EnableReclaim succeeded.
func ReclaimSoon() {
	if reclaimFile.Load() == nil {
		return
	}
	reclaimDue.Store(true)
	Soon()
	deadline.mu.Lock()
	if deadline.t == nil {
		deadline.t = time.AfterFunc(reclaimDeadline, func() {
			deadline.mu.Lock()
			deadline.t = nil
			deadline.mu.Unlock()
			reclaim()
		})
	}
	deadline.mu.Unlock()
}

func reclaim() {
	f := reclaimFile.Load()
	if f == nil || !reclaimDue.Swap(false) {
		return
	}
	deadline.mu.Lock()
	if deadline.t != nil {
		deadline.t.Stop() // quiet came first
		deadline.t = nil
	}
	deadline.mu.Unlock()
	dir := filepath.Dir(*f)
	before := statOf(dir)
	want := before["file"] + before["slab_reclaimable"]
	if want < 1<<20 {
		return // under 1 MiB: not worth the call
	}
	err := os.WriteFile(*f, []byte(strconv.FormatUint(want, 10)), 0)
	// EAGAIN: the kernel freed less than asked, which is expected (some
	// of it is in use): not an error.
	if err != nil && !errors.Is(err, unix.EAGAIN) {
		logging.Warnf("memlimit: memory.reclaim: %v", err)
		return
	}
	after := statOf(dir)
	logging.Infof("memlimit: reclaimed page cache %d -> %d KiB, slab_reclaimable %d -> %d KiB",
		before["file"]/1024, after["file"]/1024, before["slab_reclaimable"]/1024, after["slab_reclaimable"]/1024)
}

func statOf(dir string) map[string]uint64 {
	m := map[string]uint64{}
	b, err := os.ReadFile(filepath.Join(dir, "memory.stat"))
	if err != nil {
		return m
	}
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		k, v, ok := bytes.Cut(sc.Bytes(), []byte{' '})
		if !ok {
			continue
		}
		if n, err := strconv.ParseUint(string(v), 10, 64); err == nil {
			m[string(k)] = n
		}
	}
	return m
}
