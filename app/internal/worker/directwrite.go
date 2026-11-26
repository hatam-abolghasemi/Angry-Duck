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
	"Blobs written straight into containerd's ingest file, by result: ok, failed (the write was aborted, as a failed transfer through the API would be), or kept (a spread transfer stopped part-way, and its bytes stay in the ingest for the next transfer of that blob to continue).",
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
	if _, err := s.writeDirect(ctx, lease, lease, d, int64(len(b)), fromStart(bytes.NewReader(b)), nil, nil); err != nil {
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

// writeDirect is WriteBlob's direct path (see the top of this file), and
// WriteBlobFrom's: open and keep as there.
func (s *ContainerdStore) writeDirect(ctx context.Context, lease, ref string, d digest.Digest, size int64, open BlobSource, keep func(error) bool, obs writeObserver) (res WriteResult, err error) {
	ctx = leases.WithLease(s.ns(ctx), lease)
	cs := s.client.ContentStore()
	desc := ocispec.Descriptor{Digest: d, Size: size}
	cw, err := content.OpenWriter(ctx, cs, content.WithRef(ref), content.WithDescriptor(desc))
	if errdefs.IsAlreadyExists(err) {
		return res, s.holdContent(ctx, lease, d.String())
	}
	if err != nil {
		return res, err
	}
	defer s.invalidate()
	abort := func() {
		actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		_ = cs.Abort(actx, ref)
		cancel()
	}
	// How far an earlier, interrupted write under ref got: containerd
	// tells, having hashed what is there.
	var off int64
	if st, err := cw.Status(); err == nil && st.Offset > 0 && st.Offset <= size {
		off = st.Offset
	}
	if off == size {
		return res, s.commitPresent(ctx, cw, lease, d, size, "direct", abort, &res)
	}
	body, start, err := open(off)
	if err != nil {
		cw.Close()
		if off == 0 || keep == nil || !keep(err) {
			abort()
		} else {
			res.Kept = true
		}
		return res, err
	}
	defer body.Close()
	if start != off {
		// The source starts at 0: what is here goes.
		res.Dropped = off
		off = 0
	}
	res.Resumed = off
	// Close it (the ingest stays, under the lease) so containerd holds no
	// writer of its own while ours fills the file.
	if err := cw.Close(); err != nil {
		abort()
		return res, err
	}
	keepIt := false // a failure leaves the write for a resume
	defer func() {
		switch {
		case err == nil:
			directWritesTotal.Inc(s.nodeID, "ok")
		case keepIt:
			res.Kept = true
			directWritesTotal.Inc(s.nodeID, "kept")
		default:
			abort()
			directWritesTotal.Inc(s.nodeID, "failed")
		}
	}()
	path, err := s.ingestDataPath(ref)
	if err != nil {
		return res, err
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return res, err
	}
	// The file holds what containerd counted, or more (bytes it hadn't
	// hashed, or a write starting over): cut it to where this one goes on.
	// containerd's own Truncate through the API takes effect only with a
	// write through it, which a direct write never makes. It holds no
	// writer now, and hashes the file again when reopened.
	if err := f.Truncate(off); err != nil {
		f.Close()
		return res, err
	}
	if st, err := f.Stat(); err != nil || st.Size() != off {
		f.Close()
		if err == nil {
			err = fmt.Errorf("ingest file of %s holds %d bytes, want %d", ref, st.Size(), off)
		}
		return res, err
	}
	if obs != nil {
		obs.opened(off)
	}
	pw := &pageWindow{f: f, obs: obs, off: off, flushed: off, dropped: off}
	total, err := pw.copyFrom(io.LimitReader(body, size-off+1))
	pw.release()
	res.Received = total - off
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	switch {
	case err == nil && total > size:
		err = fmt.Errorf("got %d bytes, want %d", total, size)
	case err == nil && total < size:
		err = fmt.Errorf("got %d bytes, want %d", total, size)
		keepIt = keep != nil && total > 0 && keep(err)
	case err != nil:
		keepIt = keep != nil && total > 0 && keep(err)
	}
	if err != nil {
		return res, err
	}
	// Reopen: containerd reads the file and hashes it, then commits.
	cw, err = content.OpenWriter(ctx, cs, content.WithRef(ref), content.WithDescriptor(desc))
	if errdefs.IsAlreadyExists(err) {
		// Committed meanwhile by another write of the same blob (the
		// other path of a spread, a pull): ours is spare. As through the
		// API, that is success: drop our write, hold the blob.
		abort()
		return res, s.holdContent(ctx, lease, d.String())
	}
	if err != nil {
		return res, err
	}
	if st, serr := cw.Status(); serr == nil && st.Offset != size {
		cw.Close()
		return res, fmt.Errorf("containerd resumed %s at %d bytes, want %d", ref, st.Offset, size)
	}
	committing := time.Now()
	if err := cw.Commit(ctx, size, d); err != nil && !errdefs.IsAlreadyExists(err) {
		cw.Close()
		return res, err
	}
	observeBlobCommit("direct", size, committing)
	cw.Close()
	return res, s.holdContent(ctx, lease, d.String())
}

// commitPresent commits a write whose every byte containerd already has,
// without reading any source: the blob is in containerd's store under
// another namespace (containerd shares it), or an earlier write finished
// without its commit. containerd still checks the digest; a wrong one is
// aborted, and the next write starts from 0.
func (s *ContainerdStore) commitPresent(ctx context.Context, cw content.Writer, lease string, d digest.Digest, size int64, mode string, abort func(), res *WriteResult) error {
	res.Present = true
	committing := time.Now()
	if err := cw.Commit(ctx, size, d); err != nil && !errdefs.IsAlreadyExists(err) {
		cw.Close()
		abort()
		return err
	}
	observeBlobCommit(mode, size, committing)
	cw.Close()
	return s.holdContent(ctx, lease, d.String())
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
