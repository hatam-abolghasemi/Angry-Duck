package worker

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/content"
	ctrdiff "github.com/containerd/containerd/diff"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/images/archive"
	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/platforms"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// ContainerdStore is blobship.Store (and the mirror's, cleanup's and layer
// tracker's store) backed by containerd's own gRPC API, over its socket.
//
// It replaces running the node's ctr and tar chrooted into /proc/1/root:
// everything that needs privileges on the node (unpacking layers, mounting
// snapshots, writing files with their owners and whiteouts) now happens
// inside the containerd daemon, which already has them. The worker only
// needs the socket.
type ContainerdStore struct {
	client      *containerd.Client
	namespace   string // kubelet's images live in k8s.io
	snapshotter string // what kubelet's images are unpacked with; overlayfs by default

	// Short-lived caches of the two full listings, shared by the mirror
	// (asked on every request containerd makes), rescue plans and the
	// layer inventory. Concurrent callers share one walk, and a result is
	// handed out as a shared, read-only map. Anything this store changes
	// itself drops the cache.
	digests   listing
	snapshots listing

	// blobDir is containerd's blob directory as mounted into this
	// container ("" when it isn't). Committed blobs are immutable files
	// named by digest, so they are read directly; anything not found there
	// is read through containerd's content API.
	blobDir string
}

// NewContainerdStore wraps a connected client.
func NewContainerdStore(client *containerd.Client, namespace, snapshotter string) *ContainerdStore {
	if snapshotter == "" {
		snapshotter = "overlayfs"
	}
	return &ContainerdStore{client: client, namespace: namespace, snapshotter: snapshotter}
}

func (s *ContainerdStore) ns(ctx context.Context) context.Context {
	return namespaces.WithNamespace(ctx, s.namespace)
}

// Invalidate drops cached listings. Call it after something outside this
// store (a pull, say) changed containerd's content.
func (s *ContainerdStore) Invalidate() { s.invalidate() }

func (s *ContainerdStore) invalidate() {
	s.digests.invalidate()
	s.snapshots.invalidate()
}

// UseBlobDir enables direct blob reads. root is containerd's root
// directory as mounted into this container ("" = read it from
// configPath, else /var/lib/containerd). It returns the blob directory
// used, or "" if it isn't there.
func (s *ContainerdStore) UseBlobDir(root, configPath string) string {
	if root == "" {
		root = containerdRoot(configPath)
	}
	dir := strings.TrimRight(root, "/") + "/io.containerd.content.v1.content/blobs/sha256"
	if st, err := os.Stat(dir); err != nil || !st.IsDir() {
		return ""
	}
	s.blobDir = dir
	return dir
}

// containerdRoot reads the top-level root setting from containerd's
// config file, defaulting to /var/lib/containerd.
func containerdRoot(configPath string) string {
	const def = "/var/lib/containerd"
	b, err := os.ReadFile(configPath)
	if err != nil {
		return def
	}
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "[") {
			break // top-level keys come before the first table
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok || strings.TrimSpace(k) != "root" {
			continue
		}
		v = strings.TrimSpace(v)
		if i := strings.Index(v, "#"); i >= 0 {
			v = strings.TrimSpace(v[:i])
		}
		if v = strings.Trim(v, `"'`); strings.HasPrefix(v, "/") {
			return v
		}
	}
	return def
}

// --- blobs ---

// openBlob opens a committed blob straight from the content store. ok is
// false when direct reads are off or the file isn't there; callers then
// go through the content API.
func (s *ContainerdStore) openBlob(dgst string) (f *os.File, size int64, ok bool) {
	if s.blobDir == "" || !validDigest(dgst) {
		return nil, 0, false
	}
	f, err := os.Open(s.blobDir + "/" + strings.TrimPrefix(dgst, "sha256:"))
	if err != nil {
		return nil, 0, false
	}
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() {
		f.Close()
		return nil, 0, false
	}
	return f, st.Size(), true
}

// HasBlob reports whether a committed blob is on disk. known is false when
// direct reads are off, and the caller must ask Digests instead.
func (s *ContainerdStore) HasBlob(dgst string) (have, known bool) {
	if s.blobDir == "" {
		return false, false
	}
	if !validDigest(dgst) {
		return false, true
	}
	st, err := os.Stat(s.blobDir + "/" + strings.TrimPrefix(dgst, "sha256:"))
	return err == nil && st.Mode().IsRegular(), true
}

// copyContext copies a blob file to w. Local file reads don't block the
// way a network read does; cancellation reaches us through w, whose
// consumer (an HTTP client, an import pipe) goes away when ctx ends.
func copyContext(ctx context.Context, w io.Writer, f io.Reader) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return io.Copy(w, f)
}

// rpcChunk is the read and write size for streams through containerd's
// content API, where every call is a gRPC round trip.
const rpcChunk = 1 << 20

// copyBlob writes a committed blob to w: straight from the content store
// directory when it is mounted, otherwise through the content API in
// rpcChunk reads.
func (s *ContainerdStore) copyBlob(ctx context.Context, w io.Writer, desc ocispec.Descriptor) (int64, error) {
	if f, _, ok := s.openBlob(desc.Digest.String()); ok {
		defer f.Close()
		return copyContext(ctx, w, f)
	}
	ra, err := s.client.ContentStore().ReaderAt(ctx, desc)
	if err != nil {
		return 0, err
	}
	defer ra.Close()
	return copyContext(ctx, w, contentReader(ra))
}

// contentReader reads a blob through the content API in rpcChunk calls
// instead of the 32 KiB io.Copy would ask for, each a new gRPC stream.
func contentReader(ra content.ReaderAt) io.Reader {
	return bufio.NewReaderSize(content.NewReader(ra), rpcChunk)
}

// openContent opens a blob through containerd's content API.
func (s *ContainerdStore) openContent(ctx context.Context, dgst string) (content.ReaderAt, error) {
	d, err := digest.Parse(dgst)
	if err != nil {
		return nil, err
	}
	cs := s.client.ContentStore()
	info, err := cs.Info(ctx, d)
	if err != nil {
		return nil, err
	}
	return cs.ReaderAt(ctx, ocispec.Descriptor{Digest: d, Size: info.Size})
}

// ReadBlob reads a whole blob into memory, refusing anything above max.
func (s *ContainerdStore) ReadBlob(ctx context.Context, dgst string, max int64) ([]byte, error) {
	if f, size, ok := s.openBlob(dgst); ok {
		defer f.Close()
		if size > max {
			return nil, fmt.Errorf("blob %s is larger than %d bytes", dgst, max)
		}
		b := make([]byte, size)
		if _, err := io.ReadFull(f, b); err != nil {
			return nil, fmt.Errorf("reading blob %s: %w", dgst, err)
		}
		return b, nil
	}
	ctx = s.ns(ctx)
	ra, err := s.openContent(ctx, dgst)
	if err != nil {
		return nil, err
	}
	defer ra.Close()
	if ra.Size() > max {
		return nil, fmt.Errorf("blob %s is larger than %d bytes", dgst, max)
	}
	b := make([]byte, ra.Size())
	if _, err := ra.ReadAt(b, 0); err != nil && !(errors.Is(err, io.EOF) && int64(len(b)) == ra.Size()) {
		return nil, fmt.Errorf("reading blob %s: %w", dgst, err)
	}
	return b, nil
}

// StreamBlob writes a blob to w and checks the byte count, so a blob that
// doesn't match its descriptor never passes silently.
func (s *ContainerdStore) StreamBlob(ctx context.Context, dgst string, size int64, w io.Writer) error {
	var r io.Reader
	var have int64
	if f, n, ok := s.openBlob(dgst); ok {
		defer f.Close()
		r, have = f, n
	} else {
		ra, err := s.openContent(s.ns(ctx), dgst)
		if err != nil {
			return err
		}
		defer ra.Close()
		r, have = contentReader(ra), ra.Size()
	}
	if have != size {
		return fmt.Errorf("blob %s: have %d bytes, expected %d", dgst, have, size)
	}
	n, err := copyContext(ctx, w, r)
	if err == nil && n != size {
		err = fmt.Errorf("blob %s: got %d bytes, expected %d", dgst, n, size)
	}
	return err
}

// OpenContent opens a blob to serve it, with its size: the file in the
// content store when it is mounted, so an HTTP response can hand it to
// sendfile, otherwise a reader over containerd's content API. The reader
// on the other end (containerd for the mirror, WriteBlob for a peer)
// checks the digest.
func (s *ContainerdStore) OpenContent(ctx context.Context, dgst string) (io.ReadCloser, int64, error) {
	if f, n, ok := s.openBlob(dgst); ok {
		return f, n, nil
	}
	ra, err := s.openContent(s.ns(ctx), dgst)
	if err != nil {
		return nil, 0, err
	}
	return readCloser{contentReader(ra), ra}, ra.Size(), nil
}

type readCloser struct {
	io.Reader
	io.Closer
}

// Digests lists every blob digest in the namespace's content store. The
// returned map is shared: callers must not modify it.
func (s *ContainerdStore) Digests(ctx context.Context) (map[string]bool, error) {
	return s.digests.get(ctx, s.listDigests)
}

func (s *ContainerdStore) listDigests(ctx context.Context) (map[string]bool, error) {
	release, err := acquireListing(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	have := make(map[string]bool)
	err = s.client.ContentStore().Walk(s.ns(ctx), func(info content.Info) error {
		have[info.Digest.String()] = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	return have, nil
}

// --- images ---

// Resolve returns the media type and digest image points at.
func (s *ContainerdStore) Resolve(ctx context.Context, image string) (string, string, error) {
	img, err := s.client.ImageService().Get(s.ns(ctx), image)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return "", "", fmt.Errorf("image %s not found in containerd namespace %s", image, s.namespace)
		}
		return "", "", err
	}
	return img.Target.MediaType, img.Target.Digest.String(), nil
}

// Import imports an OCI archive the way `ctr images import --platform`
// does: names come from index.json, then each image is unpacked for the
// platform. Unpacking skips every layer whose chainID snapshot already
// exists, so the archive needs no blob for those layers.
func (s *ContainerdStore) Import(ctx context.Context, r io.Reader, platform string) error {
	defer s.invalidate()
	spec, err := platforms.Parse(platform)
	if err != nil {
		return err
	}
	match := platforms.OnlyStrict(spec)
	ctx, done, err := s.client.WithLease(s.ns(ctx), leases.WithRandomID(), leases.WithExpiration(time.Hour))
	if err != nil {
		return err
	}
	defer done(context.WithoutCancel(ctx))

	imgs, err := s.client.Import(ctx, r,
		containerd.WithImageRefTranslator(archive.AddRefPrefix("import-"+time.Now().Format("2006-01-02"))),
		containerd.WithImportPlatform(match),
		containerd.WithAllPlatforms(false),
	)
	if err != nil {
		return fmt.Errorf("import: %w", err)
	}
	for _, img := range imgs {
		if err := containerd.NewImageWithPlatform(s.client, img, match).Unpack(ctx, s.snapshotter); err != nil {
			return fmt.Errorf("unpacking %s: %w", img.Name, err)
		}
	}
	return nil
}

// ImageNames lists every image name in the namespace.
func (s *ContainerdStore) ImageNames(ctx context.Context) ([]string, error) {
	targets, err := s.ImageTargets(ctx)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(targets))
	for n := range targets {
		names = append(names, n)
	}
	return names, nil
}

// ImageTargets maps every image name to the digest it points at.
func (s *ContainerdStore) ImageTargets(ctx context.Context) (map[string]string, error) {
	release, err := acquireListing(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	imgs, err := s.client.ImageService().List(s.ns(ctx))
	if err != nil {
		return nil, err
	}
	targets := make(map[string]string, len(imgs))
	for _, img := range imgs {
		targets[img.Name] = img.Target.Digest.String()
	}
	return targets, nil
}

// DeleteImages removes image names. containerd's garbage collector then
// frees whatever content and snapshots no remaining image references.
// Names already gone are not an error.
func (s *ContainerdStore) DeleteImages(ctx context.Context, names ...string) error {
	if len(names) == 0 {
		return nil
	}
	defer s.invalidate()
	ctx = s.ns(ctx)
	var errs []error
	for _, n := range names {
		if err := s.client.ImageService().Delete(ctx, n); err != nil && !errdefs.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("%s: %w", n, err))
		}
	}
	return errors.Join(errs...)
}

// --- snapshots ---
//
// Snapshot shipping moves a layer between nodes when no node has its
// compressed blob (containerd's discard_unpacked_layers). Both ends go
// through containerd's diff service: the source asks it for the layer as
// an OCI tar of what changed between the snapshot and its parent, and the
// receiver asks it to apply that tar onto a fresh snapshot of the parent.
// containerd does the mounting and writes owners, modes, xattrs and
// whiteouts itself, so the worker needs no capabilities for it.

// gcRootLabel pins a snapshot against containerd's garbage collector while
// nothing references it yet: between its commit and the image import that
// makes the image point at it.
const gcRootLabel = "containerd.io/gc.root"

// tempLeaseTTL bounds how long a crashed worker's temporary views, staged
// layers and work-in-progress snapshots survive: they are all held by a
// lease, and containerd's GC frees them once it expires.
const tempLeaseTTL = time.Hour

func (s *ContainerdStore) snapshotterSvc() snapshots.Snapshotter {
	return s.client.SnapshotService(s.snapshotter)
}

// Snapshots lists every snapshot key. The returned map is shared: callers
// must not modify it.
func (s *ContainerdStore) Snapshots(ctx context.Context) (map[string]bool, error) {
	return s.snapshots.get(ctx, s.listSnapshots)
}

func (s *ContainerdStore) listSnapshots(ctx context.Context) (map[string]bool, error) {
	release, err := acquireListing(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	keys := make(map[string]bool)
	err = s.snapshotterSvc().Walk(s.ns(ctx), func(_ context.Context, info snapshots.Info) error {
		keys[info.Name] = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// ExportSnapshot writes committed snapshot chainID as an uncompressed OCI
// layer tar: containerd's diff of it against its parent.
func (s *ContainerdStore) ExportSnapshot(ctx context.Context, chainID, parent string, w io.Writer) error {
	ctx, done, err := s.client.WithLease(s.ns(ctx), leases.WithRandomID(), leases.WithExpiration(tempLeaseTTL))
	if err != nil {
		return err
	}
	// The diff blob is only held by this lease; dropping it lets GC free
	// it. It is never deleted directly: its digest could, in principle,
	// equal a real uncompressed layer some image uses.
	defer done(context.WithoutCancel(ctx))

	sn := s.snapshotterSvc()
	info, err := sn.Stat(ctx, chainID)
	if err != nil {
		return err
	}
	if info.Kind != snapshots.KindCommitted {
		return fmt.Errorf("snapshot %s is not committed", chainID)
	}
	if info.Parent != parent {
		return fmt.Errorf("snapshot %s has parent %q, expected %q", chainID, info.Parent, parent)
	}

	// Views named with a temp prefix, so the startup cleanup also finds
	// any a crash left behind.
	lowerKey := "angryduck-view-" + randomSuffix()
	lower, err := sn.View(ctx, lowerKey, parent)
	if err != nil {
		return fmt.Errorf("view of parent %q: %w", parent, err)
	}
	defer removeQuietly(ctx, sn, lowerKey)
	upperKey := "angryduck-view-" + randomSuffix()
	upper, err := sn.View(ctx, upperKey, chainID)
	if err != nil {
		return fmt.Errorf("view of %s: %w", chainID, err)
	}
	defer removeQuietly(ctx, sn, upperKey)

	desc, err := s.client.DiffService().Compare(ctx, lower, upper,
		ctrdiff.WithMediaType(ocispec.MediaTypeImageLayer),
		ctrdiff.WithReference("angryduck-export-"+randomSuffix()))
	if err != nil {
		return fmt.Errorf("diff of %s: %w", chainID, err)
	}
	n, err := s.copyBlob(ctx, w, desc)
	if err == nil && n != desc.Size {
		err = fmt.Errorf("layer of %s: wrote %d bytes, expected %d", chainID, n, desc.Size)
	}
	return err
}

// ApplySnapshot stages the OCI layer tar from r in the content store, runs
// verify, then has containerd apply it onto a new snapshot of parent and
// commits that as chainID, pinned against GC.
func (s *ContainerdStore) ApplySnapshot(ctx context.Context, chainID, parent string, r io.Reader, verify func() error) error {
	defer s.invalidate()
	ctx, done, err := s.client.WithLease(s.ns(ctx), leases.WithRandomID(), leases.WithExpiration(tempLeaseTTL))
	if err != nil {
		return err
	}
	defer done(context.WithoutCancel(ctx))

	cs := s.client.ContentStore()
	cw, err := content.OpenWriter(ctx, cs, content.WithRef("angryduck-apply-"+randomSuffix()))
	if err != nil {
		return err
	}
	// Every Write to a content writer is one synchronous round trip to
	// containerd. Batch them: 1 MiB per call instead of io.Copy's 32 KiB.
	bw := bufio.NewWriterSize(cw, rpcChunk)
	n, err := io.Copy(bw, r)
	if err == nil {
		err = bw.Flush()
	}
	if err != nil {
		cw.Close()
		return fmt.Errorf("staging layer: %w", err)
	}
	if err := verify(); err != nil {
		cw.Close()
		return err
	}
	dgst := cw.Digest()
	if err := cw.Commit(ctx, n, dgst); err != nil && !errdefs.IsAlreadyExists(err) {
		cw.Close()
		return fmt.Errorf("staging layer: %w", err)
	}
	cw.Close()
	desc := ocispec.Descriptor{MediaType: ocispec.MediaTypeImageLayer, Digest: dgst, Size: n}

	sn := s.snapshotterSvc()
	pin := snapshots.WithLabels(map[string]string{gcRootLabel: "angryduck-rescue"})
	key := "angryduck-rescue-" + randomSuffix()
	mounts, err := sn.Prepare(ctx, key, parent, pin)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			removeQuietly(ctx, sn, key)
		}
	}()
	if _, err := s.client.DiffService().Apply(ctx, desc, mounts); err != nil {
		return fmt.Errorf("applying layer: %w", err)
	}
	if err := sn.Commit(ctx, chainID, key, pin); err != nil {
		if errdefs.IsAlreadyExists(err) {
			// Another rescue or pull created it meanwhile. Theirs is as
			// good as ours; drop ours (the deferred remove) and use it.
			return nil
		}
		return err
	}
	committed = true
	return nil
}

// Unpin removes the GC pin ApplySnapshot set.
func (s *ContainerdStore) Unpin(ctx context.Context, chainID string) error {
	_, err := s.snapshotterSvc().Update(s.ns(ctx), snapshots.Info{Name: chainID, Labels: map[string]string{gcRootLabel: ""}}, "labels."+gcRootLabel)
	return err
}

// RemoveSnapshot deletes a snapshot by key.
func (s *ContainerdStore) RemoveSnapshot(ctx context.Context, key string) error {
	defer s.invalidate()
	return s.snapshotterSvc().Remove(s.ns(ctx), key)
}

// removeQuietly is best-effort cleanup, on its own context so it still
// runs when the caller's was cancelled.
func removeQuietly(ctx context.Context, sn snapshots.Snapshotter, key string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	_ = sn.Remove(ctx, key)
}

func randomSuffix() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// --- spread: single blobs held by a lease ---
//
// A spread lands an image blob by blob, long before any image references
// them, so each job's blobs are held by a containerd lease. The lease
// expires on its own: a job abandoned by a crash or a restart frees its
// blobs without anyone cleaning up. A finished job deletes its lease once
// the imported image references the blobs.

// CreateLease makes sure lease id exists, expiring ttl from now.
func (s *ContainerdStore) CreateLease(ctx context.Context, id string, ttl time.Duration) error {
	_, err := s.client.LeasesService().Create(s.ns(ctx), leases.WithID(id), leases.WithExpiration(ttl))
	if errdefs.IsAlreadyExists(err) {
		return nil
	}
	return err
}

// MoveLease creates lease to (expiring ttl from now), moves every resource
// of lease from to it, and deletes from. It keeps a long job's blobs held
// past the first lease's expiry.
func (s *ContainerdStore) MoveLease(ctx context.Context, from, to string, ttl time.Duration) error {
	ctx = s.ns(ctx)
	lm := s.client.LeasesService()
	if _, err := lm.Create(ctx, leases.WithID(to), leases.WithExpiration(ttl)); err != nil && !errdefs.IsAlreadyExists(err) {
		return err
	}
	res, err := lm.ListResources(ctx, leases.Lease{ID: from})
	if err != nil && !errdefs.IsNotFound(err) {
		return err
	}
	for _, r := range res {
		if err := lm.AddResource(ctx, leases.Lease{ID: to}, r); err != nil && !errdefs.IsNotFound(err) {
			return err
		}
	}
	return s.DeleteLease(ctx, from)
}

// DeleteLease deletes lease id; containerd's GC then frees whatever
// nothing else references. A lease already gone is not an error.
func (s *ContainerdStore) DeleteLease(ctx context.Context, id string) error {
	err := s.client.LeasesService().Delete(s.ns(ctx), leases.Lease{ID: id})
	if errdefs.IsNotFound(err) {
		return nil
	}
	return err
}

// WriteBlob streams exactly size bytes from r into the content store as
// dgst, held by lease, and returns the bytes read. containerd checks the
// digest and size at commit, so a wrong or truncated stream never lands.
// ref names the ingest; a leftover ingest under the same ref (a crash
// mid-transfer) is restarted from zero, and a failed write is aborted
// right away, so partial data never stays on disk.
func (s *ContainerdStore) WriteBlob(ctx context.Context, lease, ref, dgst string, size int64, r io.Reader) (int64, error) {
	d, err := digest.Parse(dgst)
	if err != nil {
		return 0, err
	}
	ctx = leases.WithLease(s.ns(ctx), lease)
	cs := s.client.ContentStore()
	hold := func() error {
		err := s.client.LeasesService().AddResource(ctx, leases.Lease{ID: lease}, leases.Resource{ID: dgst, Type: "content"})
		if err == nil || errdefs.IsAlreadyExists(err) {
			return nil
		}
		return err
	}
	cw, err := content.OpenWriter(ctx, cs, content.WithRef(ref), content.WithDescriptor(ocispec.Descriptor{Digest: d, Size: size}))
	if errdefs.IsAlreadyExists(err) {
		return 0, hold() // committed meanwhile, by us or anyone: just hold it
	}
	if err != nil {
		return 0, err
	}
	defer s.invalidate()
	abort := func() {
		cw.Close()
		actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		_ = cs.Abort(actx, ref)
		cancel()
	}
	if st, err := cw.Status(); err == nil && st.Offset > 0 {
		if err := cw.Truncate(0); err != nil {
			abort()
			return 0, err
		}
	}
	// Every Write is one synchronous round trip to containerd: batch them
	// at 1 MiB, the same as snapshot staging.
	bw := bufio.NewWriterSize(cw, rpcChunk)
	n, err := io.Copy(bw, io.LimitReader(r, size+1))
	if err == nil {
		err = bw.Flush()
	}
	if err == nil && n != size {
		err = fmt.Errorf("got %d bytes, want %d", n, size)
	}
	if err != nil {
		abort()
		return n, err
	}
	if err := cw.Commit(ctx, size, d); err != nil && !errdefs.IsAlreadyExists(err) {
		abort()
		return n, err
	}
	cw.Close()
	return n, hold()
}
