package worker

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/containerd/containerd/archive"
	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/mount"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/opencontainers/go-digest"
	"golang.org/x/sys/unix"
)

// Stream unpacking (SPREAD_UNPACK_STREAM_ENABLED, off by default): a layer
// is unpacked while its blob is still arriving, instead of once it has
// landed, so a large layer is ready about when its last byte is.
//
// containerd can only unpack a blob that is committed, so here the worker
// writes the files itself, with the same code containerd's own unpack runs
// for overlayfs (its archive package, the version this binary links): the
// layer's tar goes straight into the upper directory of a snapshot
// prepared, through containerd, on the snapshot of the layer below. The
// layer below must be unpacked first, as in containerd's own unpack: a
// directory the layer only implies takes its owner, mode and xattrs from
// the layers below.
//
// Nothing is trusted until it is checked: the snapshot is committed under
// its chainID, through containerd, only when the bytes read hash to the
// blob's digest and size and the unpacked tar hashes to the diffID the
// chainID names. Otherwise it is removed, and the layer is left to the
// unpack through containerd once its blob has landed. Until the commit,
// the snapshot is an active one under the job's lease: a worker that dies
// half way leaves nothing containerd or kubelet would ever use, and the
// lease takes it away.
//
// The price is privileges. Writing image files with their owners, modes,
// whiteouts (character devices) and opaque directories (trusted.* xattrs)
// takes CAP_CHOWN, CAP_DAC_OVERRIDE, CAP_FOWNER, CAP_FSETID, CAP_MKNOD,
// CAP_SETFCAP and CAP_SYS_ADMIN, and containerd's overlayfs snapshots
// mounted writable at the same path: a worker with those can change any
// image on its node. See docs/stream-unpack.md.

// errStreamUnpackUnsupported: the snapshot isn't one this can write into
// (another snapshotter, overlayfs with userxattr); the layer is left to
// the unpack through containerd.
var errStreamUnpackUnsupported = errors.New("stream unpacking doesn't support this snapshot")

// streamUnpackSource marks errors reading the blob (its transfer stopped
// and nothing else has it): the layer waits for its blob instead.
type streamUnpackSource struct{ err error }

func (e streamUnpackSource) Error() string { return "reading the blob: " + e.err.Error() }
func (e streamUnpackSource) Unwrap() error { return e.err }

// sourceReader marks read errors as the source's.
type sourceReader struct{ r io.Reader }

func (s sourceReader) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	if err != nil && err != io.EOF {
		err = streamUnpackSource{err}
	}
	return n, err
}

// EnableStreamUnpack checks this worker can unpack into containerd's
// snapshots: it prepares a throwaway snapshot through containerd, finds
// its directory here, and creates in it what a layer can hold (an owned
// file, a whiteout device, an opaque directory). The snapshot is removed
// whatever happens. Errors leave stream unpacking off.
func (s *ContainerdStore) EnableStreamUnpack(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(s.ns(ctx), 30*time.Second)
	defer cancel()
	lease := "angryduck-probe-" + randomSuffix()
	if err := s.CreateLease(ctx, lease, time.Minute); err != nil {
		return err
	}
	defer func() { _ = s.DeleteLease(context.WithoutCancel(ctx), lease) }()
	lctx := leases.WithLease(ctx, lease)
	sn := s.snapshotterSvc()
	key := "angryduck-probe-" + randomSuffix()
	mounts, err := sn.Prepare(lctx, key, "")
	if err != nil {
		return fmt.Errorf("preparing a snapshot: %w", err)
	}
	defer removeQuietly(lctx, sn, key)
	root, _, err := writableRoot(mounts)
	if err != nil {
		return err
	}
	// A fresh snapshot's directory: here, and empty, or it isn't the one
	// containerd just made (the snapshotter mounted elsewhere, or not at
	// all).
	ents, err := os.ReadDir(root)
	if err != nil {
		return fmt.Errorf("containerd's %s snapshots aren't mounted at the same path here: %w", s.snapshotter, err)
	}
	if len(ents) != 0 {
		return fmt.Errorf("%s isn't the snapshot containerd just prepared", root)
	}
	f := filepath.Join(root, "owned")
	if err := os.WriteFile(f, nil, 0o4755); err != nil {
		return fmt.Errorf("writing into the snapshot: %w", err)
	}
	if err := os.Lchown(f, 1000, 1000); err != nil {
		return fmt.Errorf("setting owners: %w", err)
	}
	if err := os.Chmod(f, 0o4755); err != nil {
		return fmt.Errorf("setting modes: %w", err)
	}
	if err := unix.Mknod(filepath.Join(root, "whiteout"), unix.S_IFCHR, 0); err != nil {
		return fmt.Errorf("creating whiteouts: %w", err)
	}
	d := filepath.Join(root, "opaque")
	if err := os.Mkdir(d, 0o755); err != nil {
		return err
	}
	if err := unix.Setxattr(d, "trusted.overlay.opaque", []byte{'y'}, 0); err != nil {
		return fmt.Errorf("marking opaque directories: %w", err)
	}
	s.streamUnpack = true
	return nil
}

// StreamUnpacks reports whether EnableStreamUnpack succeeded.
func (s *ContainerdStore) StreamUnpacks() bool { return s.streamUnpack }

// writableRoot is where a prepared snapshot's files go, and the layers
// below it (top first), as containerd's own applier reads its mounts.
func writableRoot(ms []mount.Mount) (root string, parents []string, err error) {
	if len(ms) != 1 {
		return "", nil, errStreamUnpackUnsupported
	}
	m := ms[0]
	switch m.Type {
	case "bind":
		return m.Source, nil, nil
	case "overlay":
		for _, o := range m.Options {
			switch {
			case o == "userxattr":
				return "", nil, errStreamUnpackUnsupported
			case strings.HasPrefix(o, "upperdir="):
				root = strings.TrimPrefix(o, "upperdir=")
			case strings.HasPrefix(o, "lowerdir="):
				parents = strings.Split(strings.TrimPrefix(o, "lowerdir="), ":")
			}
		}
		if root == "" {
			return "", nil, errStreamUnpackUnsupported
		}
		return root, parents, nil
	}
	return "", nil, errStreamUnpackUnsupported
}

// ApplyLayerStream is ApplyLayer reading the blob from r as it arrives (see
// above). r is read only once the layer below is known to be here:
// errParentMissing (nothing read) when it isn't. applied is false (nothing
// read) when the layer is already unpacked. The caller closes r after it
// returns: a read of r may still be waiting when it fails.
func (s *ContainerdStore) ApplyLayerStream(ctx context.Context, lease string, l LayerSpec, r io.Reader) (applied bool, err error) {
	if !s.streamUnpack {
		return false, errStreamUnpackUnsupported
	}
	ctx = leases.WithLease(s.ns(ctx), lease)
	sn := s.snapshotterSvc()
	if _, err := sn.Stat(ctx, l.ChainID); err == nil {
		return false, nil
	} else if !errdefs.IsNotFound(err) {
		return false, err
	}
	key := "angryduck-unpack-" + randomSuffix()
	mounts, err := sn.Prepare(ctx, key, l.Parent)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return false, errParentMissing
		}
		return false, fmt.Errorf("preparing a snapshot on %q: %w", l.Parent, err)
	}
	defer s.invalidate()
	committed := false
	defer func() {
		if !committed {
			removeQuietly(ctx, sn, key)
		}
	}()
	root, parents, err := writableRoot(mounts)
	if err != nil {
		return false, err
	}
	var opts []archive.ApplyOpt
	if len(mounts) == 1 && mounts[0].Type == "overlay" {
		opts = append(opts, archive.WithConvertWhiteout(archive.OverlayConvertWhiteout))
		if len(parents) > 0 {
			opts = append(opts, archive.WithParents(parents))
		}
	}
	diffID, err := streamInto(ctx, root, opts, nil, l, r)
	if err != nil {
		return false, err
	}
	if err := sn.Commit(ctx, l.ChainID, key); err != nil {
		if errdefs.IsAlreadyExists(err) {
			return false, nil // unpacked meanwhile: theirs is as good
		}
		return false, fmt.Errorf("committing %s: %w", l.ChainID, err)
	}
	committed = true
	s.labelUncompressed(ctx, l, diffID)
	return true, nil
}

// ExtractLayerStream unpacks a layer from its blob as it arrives into a
// snapshot of its own, with no layer below, so it doesn't wait for the
// layer below to be unpacked first: every layer of an image unpacks at the
// same time as its bytes arrive. Nothing is committed: CommitExtracted
// attaches it onto the layer below once that one is, bottom up.
//
// Whiteouts are written as overlay whiteouts (as onto a parent), and the
// directories the layer only implies (an entry inside them, none for
// themselves) are noted: unpacked alone they get the default owner and
// mode, and CommitExtracted gives them those of the layers below, as an
// unpack onto them would have.
func (s *ContainerdStore) ExtractLayerStream(ctx context.Context, lease string, l LayerSpec, r io.Reader) (ExtractedLayer, error) {
	if !s.streamUnpack {
		return ExtractedLayer{}, errStreamUnpackUnsupported
	}
	ctx = leases.WithLease(s.ns(ctx), lease)
	sn := s.snapshotterSvc()
	if _, err := sn.Stat(ctx, l.ChainID); err == nil {
		return ExtractedLayer{Present: true}, nil
	} else if !errdefs.IsNotFound(err) {
		return ExtractedLayer{}, err
	}
	key := "angryduck-unpack-" + randomSuffix()
	mounts, err := sn.Prepare(ctx, key, "")
	if err != nil {
		return ExtractedLayer{}, fmt.Errorf("preparing a snapshot: %w", err)
	}
	defer s.invalidate()
	root, _, err := writableRoot(mounts)
	if err != nil {
		removeQuietly(ctx, sn, key)
		return ExtractedLayer{}, err
	}
	dirs := &impliedDirs{listed: map[string]bool{}, implied: map[string]bool{}}
	opts := []archive.ApplyOpt{archive.WithConvertWhiteout(archive.OverlayConvertWhiteout)}
	diffID, err := streamInto(ctx, root, opts, dirs, l, r)
	if err != nil {
		removeQuietly(ctx, sn, key)
		return ExtractedLayer{}, err
	}
	return ExtractedLayer{Key: key, DiffID: diffID, ByWorker: true, Implied: dirs.list(), RootEntry: dirs.root}, nil
}

// labelUncompressed records the layer's diffID on its blob, as containerd's
// unpack does; the blob may not be committed yet (it is read while it
// arrives), then this is skipped.
func (s *ContainerdStore) labelUncompressed(ctx context.Context, l LayerSpec, diffID string) {
	_, _ = s.client.ContentStore().Update(ctx, content.Info{Digest: digest.Digest(l.Digest),
		Labels: map[string]string{uncompressedLabel: diffID}}, "labels."+uncompressedLabel)
}

// streamInto unpacks l's blob, read from r as it arrives, into root, and
// checks it: the bytes read must be the blob's size, and the unpacked tar
// must hash to the diffID l's chainID names onto its parent. The blob's
// own digest isn't computed here: containerd checks it when it commits the
// blob, and a diffID that matches already proves the content is the
// layer's. dirs, if set, notes the directories the layer only implies.
func streamInto(ctx context.Context, root string, opts []archive.ApplyOpt, dirs *impliedDirs, l LayerSpec, r io.Reader) (string, error) {
	// Stages on goroutines of their own, so decompressing (most of the
	// CPU) overlaps the rest: one reads the blob as it arrives, one
	// decompresses it, and the archive code hashes the tar and writes the
	// files. Bounded pipes between them hold 1.25 MiB in all. The tar's
	// trailing bytes are part of the diffID, as in containerd's applier.
	diffHash := digest.Canonical.Digester()
	counted := &countingReader{r: sourceReader{r}}
	blob := newChunkPipe(streamUnpackChunks, streamUnpackBlobChunk)
	pipe := newChunkPipe(streamUnpackChunks, streamUnpackChunk)
	read, produced := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := io.Copy(blob, counted)
		blob.closeWrite(err)
		read <- err
	}()
	go func() {
		err := func() error {
			dec, err := decompressor(blob)
			if err != nil {
				return fmt.Errorf("decompressing %s: %w", l.Digest, err)
			}
			defer dec.Close()
			if _, err := io.Copy(pipe, dec); err != nil {
				return err
			}
			_, err = io.Copy(io.Discard, blob)
			return err
		}()
		blob.closeRead()
		pipe.closeWrite(err)
		produced <- err
	}()
	tarStream := io.TeeReader(pipe, diffHash.Hash())
	pages := &pageDropper{root: root}
	filter := pages.next
	if dirs != nil {
		filter = func(h *tar.Header) (bool, error) {
			dirs.note(h)
			return pages.next(h)
		}
	}
	_, err := archive.Apply(ctx, root, tarStream, append([]archive.ApplyOpt{archive.WithFilter(filter)}, opts...)...)
	pages.done()
	if err == nil {
		_, err = io.Copy(io.Discard, tarStream) // the tar's trailing bytes
	}
	pipe.closeRead()
	if perr := <-produced; err == nil && perr != nil {
		err = perr
	}
	if err == nil {
		// The decompressor read the blob to its end: the reader is done.
		err = <-read
	}
	// Otherwise the reader may still wait on r: the caller closes r.
	if err != nil {
		return "", fmt.Errorf("unpacking %s: %w", l.Digest, err)
	}
	if counted.n != l.Size {
		return "", fmt.Errorf("read %d bytes of blob %s, it has %d", counted.n, l.Digest, l.Size)
	}
	diffID := diffHash.Digest().String()
	if got := chainOf(l.Parent, diffID); got != l.ChainID {
		return "", fmt.Errorf("%s unpacks to chainID %s, the image says %s", l.Digest, got, l.ChainID)
	}
	return diffID, nil
}

// impliedDirs notes, as a layer's tar entries go by, the directories it
// lists and the ones it only implies (some entry is inside them, none is
// for them): those are the ones containerd's unpack creates on the way,
// taking owner, mode and xattrs from the layers below. Their number is
// small; the listed set lives only while the layer unpacks.
type impliedDirs struct {
	listed  map[string]bool
	implied map[string]bool
	root    bool // the tar has an entry for its root directory
}

// note sees one entry; its name is already clean (archive.Apply cleans it
// before the filter).
func (d *impliedDirs) note(h *tar.Header) {
	name := strings.TrimPrefix(filepath.Clean("/"+h.Name), "/")
	if name == "" {
		d.root = true
		return
	}
	if h.Typeflag == tar.TypeDir {
		d.listed[name] = true
		delete(d.implied, name)
	}
	for dir := filepath.Dir(name); dir != "." && dir != "/"; dir = filepath.Dir(dir) {
		if d.listed[dir] {
			break // listed, and so were (or will be judged) its own parents
		}
		d.implied[dir] = true
	}
}

// list returns the implied directories, parents before children.
func (d *impliedDirs) list() []string {
	out := make([]string, 0, len(d.implied))
	for dir := range d.implied {
		if !d.listed[dir] {
			out = append(out, dir)
		}
	}
	sort.Slice(out, func(a, b int) bool {
		da, db := strings.Count(out[a], "/"), strings.Count(out[b], "/")
		if da != db {
			return da < db
		}
		return out[a] < out[b]
	})
	return out
}

// The bounded pipes between the stages of a stream unpack: the blob in
// streamUnpackChunks chunks of streamUnpackBlobChunk bytes, the tar in
// chunks of streamUnpackChunk.
const (
	streamUnpackChunks    = 4
	streamUnpackBlobChunk = 64 << 10
	streamUnpackChunk     = 256 << 10
)

// decompressor reads a layer blob as its tar: gzip (klauspost's decoder,
// about a third faster than Go's), zstd or plain, told apart by their
// first bytes, as containerd does.
func decompressor(r io.Reader) (io.ReadCloser, error) {
	br := bufio.NewReaderSize(r, 32<<10)
	head, err := br.Peek(4)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	switch {
	case bytes.HasPrefix(head, []byte{0x1f, 0x8b, 0x08}):
		return gzip.NewReader(br)
	case bytes.HasPrefix(head, []byte{0x28, 0xb5, 0x2f, 0xfd}):
		// Not ordered (see streamUnpackable), but read right if it is.
		zr, err := zstd.NewReader(br, zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true))
		if err != nil {
			return nil, err
		}
		return zr.IOReadCloser(), nil
	default:
		return io.NopCloser(br), nil
	}
}

// chunkPipe is an io.Pipe with a little room: the writer runs up to n
// chunks ahead of the reader, so neither waits on the other's every step.
type chunkPipe struct {
	full, free chan []byte
	chunk      []byte // the chunk being read, whole
	cur        []byte // what is left of it
	mu         sync.Mutex
	werr       error         // why the writer stopped (io.EOF: the end)
	stop       chan struct{} // closed when the reader is gone
	stopOnce   sync.Once
}

func newChunkPipe(n, size int) *chunkPipe {
	p := &chunkPipe{full: make(chan []byte, n), free: make(chan []byte, n), stop: make(chan struct{})}
	for i := 0; i < n; i++ {
		p.free <- make([]byte, 0, size)
	}
	return p
}

var errChunkPipeClosed = errors.New("the reader is gone")

func (p *chunkPipe) Write(b []byte) (int, error) {
	n := 0
	for len(b) > 0 {
		var c []byte
		select {
		case c = <-p.free:
		case <-p.stop:
			return n, errChunkPipeClosed
		}
		k := copy(c[:cap(c)], b)
		c = c[:k]
		b = b[k:]
		n += k
		select {
		case p.full <- c:
		case <-p.stop:
			return n, errChunkPipeClosed
		}
	}
	return n, nil
}

// closeWrite ends the stream: err nil reads as io.EOF.
func (p *chunkPipe) closeWrite(err error) {
	if err == nil {
		err = io.EOF
	}
	p.mu.Lock()
	p.werr = err
	p.mu.Unlock()
	close(p.full)
}

func (p *chunkPipe) Read(b []byte) (int, error) {
	if len(p.cur) == 0 {
		c, ok := <-p.full
		if !ok {
			p.mu.Lock()
			defer p.mu.Unlock()
			return 0, p.werr
		}
		p.chunk, p.cur = c, c
	}
	n := copy(b, p.cur)
	p.cur = p.cur[n:]
	if len(p.cur) == 0 {
		p.free <- p.chunk[:0]
		p.chunk = nil
	}
	return n, nil
}

// closeRead stops the writer if it is still going.
func (p *chunkPipe) closeRead() { p.stopOnce.Do(func() { close(p.stop) }) }

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// pageDropper keeps the files a layer writes from piling up in page cache
// charged to the worker (containerd's own unpack charges them to
// containerd). Left alone, they stay charged to the worker's container
// until the node runs short of memory or the image is removed: a layer of
// many small files (a vendor/ or node_modules/ tree) is tens of MiB of
// "worker memory" per node, for as long as the image stays. Page cache
// can't be handed to another cgroup afterwards (cgroup v2 has no way to
// move it), so it is dropped instead.
//
// The archive code writes one file per tar entry; when it moves on to the
// next entry, the file before is complete. Each complete regular file is
// opened and its writeback started (sync_file_range, no waiting). A few
// files later it is handed to a drainer goroutine, which waits for that
// writeback to finish and only then drops the file's cached pages (fadvise
// DONTNEED) and closes it: the kernel skips pages still dirty or being
// written, so dropping without waiting left part of every layer cached.
// The unpack itself never waits on the disk: the hand-off doesn't block,
// and when the drainer is behind, the file is dropped at once, as before.
// The drainer lives only while a layer unpacks and drains.
type pageDropper struct {
	root     string
	rootFD   int    // root, opened on first use; -1 when it can't be
	pending  string // complete, writeback not started yet
	pendSize int64
	flushing []flushingFile // writeback started, oldest first
	inFlight int64          // bytes in flushing
	drain    chan *os.File  // to the drainer; nil until the first hand-off
}

type flushingFile struct {
	f    *os.File
	size int64
}

const (
	// pageDropFiles and pageDropBytes bound the files whose writeback was
	// started and that aren't handed to the drainer yet: the oldest is
	// handed over once either is passed. pageDropFiles is also how many
	// files may wait for the drainer.
	pageDropFiles = 64
	pageDropBytes = 8 << 20
)

// next is the archive filter: it sees each entry before it is written.
func (p *pageDropper) next(h *tar.Header) (bool, error) {
	p.rotate()
	if h.Typeflag == tar.TypeReg && h.Size > 0 {
		p.pending, p.pendSize = h.Name, h.Size
	}
	return true, nil
}

// rotate starts the writeback of the file just completed, and hands the
// oldest files in flight to the drainer once there are too many.
func (p *pageDropper) rotate() {
	if p.pending != "" {
		if f := p.open(p.pending); f != nil {
			_ = unix.SyncFileRange(int(f.Fd()), 0, 0, unix.SYNC_FILE_RANGE_WRITE)
			p.flushing = append(p.flushing, flushingFile{f, p.pendSize})
			p.inFlight += p.pendSize
		}
		p.pending, p.pendSize = "", 0
	}
	for len(p.flushing) > 0 && (len(p.flushing) > pageDropFiles || p.inFlight > pageDropBytes) {
		p.handOff(p.flushing[0].f)
		p.inFlight -= p.flushing[0].size
		p.flushing[0] = flushingFile{}
		p.flushing = p.flushing[1:]
	}
}

// handOff gives f to the drainer, starting it on first use; never blocks.
func (p *pageDropper) handOff(f *os.File) {
	if p.drain == nil {
		p.drain = make(chan *os.File, pageDropFiles)
		go drainPages(p.drain)
	}
	select {
	case p.drain <- f:
	default: // the drainer is behind (a slow disk): don't make the unpack wait
		dropPages(f)
	}
}

// drainPages drops each file's pages once its writeback is done, until ch
// is closed.
func drainPages(ch <-chan *os.File) {
	for f := range ch {
		_ = unix.SyncFileRange(int(f.Fd()), 0, 0,
			unix.SYNC_FILE_RANGE_WAIT_BEFORE|unix.SYNC_FILE_RANGE_WRITE|unix.SYNC_FILE_RANGE_WAIT_AFTER)
		dropPages(f)
	}
}

// open opens name, as the archive wrote it, resolved inside root.
func (p *pageDropper) open(name string) *os.File {
	if p.rootFD == 0 {
		p.rootFD = -1
		if fd, err := unix.Open(p.root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0); err == nil {
			p.rootFD = fd
		}
	}
	if p.rootFD < 0 {
		return nil
	}
	fd, err := unix.Openat2(p.rootFD, name, &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_IN_ROOT | unix.RESOLVE_NO_MAGICLINKS,
	})
	if err != nil {
		return nil
	}
	return os.NewFile(uintptr(fd), name)
}

// dropPages drops f's cached pages and closes it.
func dropPages(f *os.File) {
	_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
	f.Close()
}

// done starts the last file's writeback, hands every file still in flight
// to the drainer (which ends once it has dropped them), and lets go of
// root. It never waits on the disk.
func (p *pageDropper) done() {
	p.rotate()
	for _, ff := range p.flushing {
		p.handOff(ff.f)
	}
	p.flushing, p.inFlight = nil, 0
	if p.drain != nil {
		close(p.drain)
		p.drain = nil
	}
	if p.rootFD > 0 {
		unix.Close(p.rootFD)
		p.rootFD = -1
	}
}
