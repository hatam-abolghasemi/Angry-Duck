package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/leases"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sys/unix"

	"angryduck/internal/metrics"
)

// Direct writes: a blob's bytes go straight into containerd's ingest file.
//
// Through the content API, every byte is copied into a gRPC message,
// written to containerd's socket, read and hashed by containerd, and only
// then written to the ingest file: on a spread, most of the CPU of both
// the worker and containerd. Instead:
//
//  1. the write is opened through containerd as before (its ingest record,
//     under the transfer's lease and ref), and closed again at once;
//  2. the worker writes the bytes into that write's own data file;
//  3. the write is opened again: containerd resumes it as it resumes any
//     interrupted write, by reading the file and hashing it;
//  4. it is committed through containerd, which checks size and digest.
//
// containerd stays the only one to commit anything, and checks every
// byte: a broken or partial file fails the commit like a bad download, and
// is aborted. The worker only ever writes the data file of a write it
// opened itself, under a ref no one else uses. If the worker dies
// half-way, the write is left as an interrupted one under an expiring
// lease, which containerd's garbage collection removes, as before.
//
// Page cache: the bytes written land in page cache charged to the
// worker's pod (through the content API they were containerd's). The
// writer starts writeback as it goes and drops pages once they are on
// disk, keeping at most directWindow of a blob, more only while a peer
// streaming from the file still needs them, never more than directCap.
// Once the blob is written, all of its pages are dropped: containerd's
// own read at step 3 caches it again, charged to containerd as before.
//
// It needs the ingest directory mounted writable; the worker tries it once
// at startup (EnableDirectWrites) and otherwise keeps using the API.

const (
	// directMin: smaller blobs (manifests, configs) go through the API,
	// one round trip, cheaper than opening the write twice.
	directMin = rpcChunk
	// directWindow is how much of a blob being written stays in page
	// cache behind the writer; directCap is the most it ever keeps, when
	// a peer reading the file lags.
	directWindow = 8 << 20
	directCap    = 16 << 20
	// directFlushStep is how far the writer goes between writeback calls.
	directFlushStep = 1 << 20
)

var directWritesTotal = metrics.NewCounterVec(
	"angryduck_worker_direct_writes_total",
	"Blobs written straight into containerd's ingest file, by result: ok, or failed (the write was aborted, as a failed transfer through the API would be).",
	"node", "result",
)

// readHolder is what a write observer may also tell: the lowest offset a
// reader of the ingest file still needs, or -1 when none does.
type readHolder interface {
	holdFrom() int64
}

// EnableDirectWrites turns direct writes on if a probe works end to end:
// a small random blob written straight into its ingest file and committed
// through containerd. Any failure leaves them off; the error says why.
func (s *ContainerdStore) EnableDirectWrites(ctx context.Context, nodeID string) error {
	s.nodeID = nodeID
	if s.ingestDir == "" {
		return errors.New("containerd's content store isn't mounted")
	}
	b := make([]byte, 64<<10)
	for i := range b {
		b[i] = byte(i)
	}
	copy(b, randomSuffix()+randomSuffix())
	d := digest.FromBytes(b)
	lease := "angryduck-probe-" + randomSuffix()
	if err := s.CreateLease(ctx, lease, time.Minute); err != nil {
		return fmt.Errorf("creating the probe's lease: %w", err)
	}
	defer func() { _ = s.DeleteLease(context.WithoutCancel(ctx), lease) }()
	if _, err := s.writeDirect(ctx, lease, lease, d, int64(len(b)), bytes.NewReader(b), nil); err != nil {
		return err
	}
	if have, _ := s.HasBlob(d.String()); !have {
		return errors.New("the probe blob isn't in the content store after its commit")
	}
	s.directWrites = true
	return nil
}

// DirectWrites reports whether blobs are written straight into ingest
// files.
func (s *ContainerdStore) DirectWrites() bool { return s.directWrites }

// writeDirect is WriteBlob's direct path (see the top of this file).
func (s *ContainerdStore) writeDirect(ctx context.Context, lease, ref string, d digest.Digest, size int64, r io.Reader, obs writeObserver) (n int64, err error) {
	ctx = leases.WithLease(s.ns(ctx), lease)
	cs := s.client.ContentStore()
	desc := ocispec.Descriptor{Digest: d, Size: size}
	cw, err := content.OpenWriter(ctx, cs, content.WithRef(ref), content.WithDescriptor(desc))
	if errdefs.IsAlreadyExists(err) {
		return 0, s.holdContent(ctx, lease, d.String())
	}
	if err != nil {
		return 0, err
	}
	defer s.invalidate()
	abort := func() {
		actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		_ = cs.Abort(actx, ref)
		cancel()
	}
	if st, err := cw.Status(); err == nil && st.Offset > 0 {
		if err := cw.Truncate(0); err != nil {
			cw.Close()
			abort()
			return 0, err
		}
	}
	// Close it (the ingest stays, under the lease) so containerd holds no
	// writer of its own while ours fills the file.
	if err := cw.Close(); err != nil {
		abort()
		return 0, err
	}
	defer func() {
		if err != nil {
			abort()
			directWritesTotal.Inc(s.nodeID, "failed")
		} else {
			directWritesTotal.Inc(s.nodeID, "ok")
		}
	}()
	path, err := s.ingestDataPath(ref)
	if err != nil {
		return 0, err
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return 0, err
	}
	if st, err := f.Stat(); err != nil || st.Size() != 0 {
		f.Close()
		if err == nil {
			err = fmt.Errorf("ingest file of %s isn't empty (%d bytes)", ref, st.Size())
		}
		return 0, err
	}
	if obs != nil {
		obs.opened()
	}
	pw := &pageWindow{f: f, obs: obs}
	n, err = pw.copyFrom(io.LimitReader(r, size+1))
	pw.release()
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil && n != size {
		err = fmt.Errorf("got %d bytes, want %d", n, size)
	}
	if err != nil {
		return n, err
	}
	// Reopen: containerd reads the file and hashes it, then commits.
	cw, err = content.OpenWriter(ctx, cs, content.WithRef(ref), content.WithDescriptor(desc))
	if errdefs.IsAlreadyExists(err) {
		// Committed meanwhile by another write of the same blob (the
		// other path of a spread, a pull): ours is spare. As through the
		// API, that is success: drop our write, hold the blob.
		abort()
		return n, s.holdContent(ctx, lease, d.String())
	}
	if err != nil {
		return n, err
	}
	if st, serr := cw.Status(); serr == nil && st.Offset != size {
		cw.Close()
		return n, fmt.Errorf("containerd resumed %s at %d bytes, want %d", ref, st.Offset, size)
	}
	if err := cw.Commit(ctx, size, d); err != nil && !errdefs.IsAlreadyExists(err) {
		cw.Close()
		return n, err
	}
	cw.Close()
	return n, s.holdContent(ctx, lease, d.String())
}

// holdContent adds blob dgst to lease (already there is fine).
func (s *ContainerdStore) holdContent(ctx context.Context, lease, dgst string) error {
	err := s.client.LeasesService().AddResource(ctx, leases.Lease{ID: lease}, leases.Resource{ID: dgst, Type: "content"})
	if err == nil || errdefs.IsAlreadyExists(err) {
		return nil
	}
	return err
}

// ingestDataPath finds the data file of the write running under ref, as
// OpenIngest does, and checks it is a plain file inside the ingest
// directory.
func (s *ContainerdStore) ingestDataPath(ref string) (string, error) {
	f, err := s.OpenIngest(ref)
	if err != nil {
		return "", fmt.Errorf("finding the ingest file of %s: %w", ref, err)
	}
	path := f.Name()
	st, err := f.Stat()
	f.Close()
	if err != nil {
		return "", err
	}
	if !st.Mode().IsRegular() || filepath.Dir(filepath.Dir(path)) != filepath.Clean(s.ingestDir) || !strings.HasSuffix(path, "/data") {
		return "", fmt.Errorf("unexpected ingest file %s", path)
	}
	return path, nil
}

// pageWindow writes a file front to back, keeping little of it in page
// cache (see the top of this file).
type pageWindow struct {
	f       *os.File
	obs     writeObserver
	off     int64 // written up to
	flushed int64 // writeback started up to
	dropped int64 // pages dropped up to
}

func (w *pageWindow) copyFrom(r io.Reader) (int64, error) {
	bp := relayBuffers.Get().(*[]byte)
	defer relayBuffers.Put(bp)
	buf := *bp
	for {
		k, rerr := r.Read(buf)
		if k > 0 {
			if _, err := w.f.WriteAt(buf[:k], w.off); err != nil {
				return w.off, err
			}
			w.off += int64(k)
			if w.obs != nil {
				w.obs.wrote(w.off)
			}
			w.trim()
		}
		if rerr == io.EOF {
			return w.off, nil
		}
		if rerr != nil {
			return w.off, rerr
		}
	}
}

// trim starts writeback of what was written since the last call (once a
// step's worth), and drops the pages the window no longer covers.
func (w *pageWindow) trim() {
	fd := int(w.f.Fd())
	if w.off-w.flushed >= directFlushStep {
		_ = unix.SyncFileRange(fd, w.flushed, w.off-w.flushed, unix.SYNC_FILE_RANGE_WRITE)
		w.flushed = w.off
	}
	end := w.flushed - directWindow
	if h, ok := w.obs.(readHolder); ok {
		if from := h.holdFrom(); from >= 0 && from < end {
			end = max(from, w.flushed-directCap)
		}
	}
	if end-w.dropped >= directFlushStep {
		w.drop(end)
	}
}

// drop waits for [dropped, end) to be on disk and lets its pages go.
func (w *pageWindow) drop(end int64) {
	fd := int(w.f.Fd())
	n := end - w.dropped
	_ = unix.SyncFileRange(fd, w.dropped, n, unix.SYNC_FILE_RANGE_WAIT_BEFORE|unix.SYNC_FILE_RANGE_WRITE|unix.SYNC_FILE_RANGE_WAIT_AFTER)
	_ = unix.Fadvise(fd, w.dropped, n, unix.FADV_DONTNEED)
	w.dropped = end
}

// release lets go of every page still held, once the file is written:
// containerd's read before the commit brings it back, charged to it.
func (w *pageWindow) release() {
	if w.off > w.dropped {
		w.drop(w.off)
	}
}

// dropRead lets go of the pages a reader of an ingest file just read,
// [from, to), of a blob of size bytes written up to written. While the
// writer goes on, it holds the last directWindow and drops them itself;
// once it is done it has let go of everything. A reader lagging the
// writer reads pages the writer already let go: without this they would
// stay cached, charged to the worker, after the transfer.
func dropRead(f *os.File, from, to, written, size int64) {
	end := to
	if written < size {
		end = min(to, written-directWindow)
	}
	if end > from {
		_ = unix.Fadvise(int(f.Fd()), from, end-from, unix.FADV_DONTNEED)
	}
}
