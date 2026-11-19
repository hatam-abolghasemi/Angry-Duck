package worker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/mount"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"google.golang.org/protobuf/encoding/protowire"
)

// Parallel unpacking (SPREAD_UNPACK_PARALLEL_ENABLED), where containerd
// supports it: its snapshotter can commit a snapshot prepared without a
// parent onto a parent ("rebase", overlayfs from containerd 2.2). Then
// layers are unpacked side by side, each into a snapshot of its own with
// no parent, as soon as its blob lands, and committed onto the layer below
// once that one is: the commits stay bottom up, the unpacking doesn't
// wait. This is how containerd 2.2 itself unpacks in parallel
// (max_concurrent_unpacks above 1), through the same API; containerd
// still does all the unpacking, so the worker needs nothing new.
//
// Each layer is checked as in the sequential unpack: its unpacked content
// must chain to its chainID onto the layer below. After the commit, the
// snapshot's parent is read back: a containerd that took the commit
// without its parent would leave a snapshot that isn't the layer it is
// named after, so it is removed at once and parallel unpacking turned off.
//
// One difference, shared with containerd's own parallel unpack: a layer
// is unpacked without the layers below, so a directory it only implies
// (a file in it, no entry for it) is created with default owner and mode
// instead of taking those of the layers below. Image builders (BuildKit,
// Docker) always write the entry; see docs/propagation.md.

// commitParentField is CommitSnapshotRequest.parent, added in containerd's
// API 1.10 (containerd 2.2). The API module this binary links predates it,
// so it travels as a field of its own on the wire.
const commitParentField protowire.Number = 5

// errRebaseIgnored: containerd committed a snapshot without the parent it
// was asked for.
var errRebaseIgnored = errors.New("containerd ignored the parent of a commit: parallel unpacking is not supported here")

// ExtractedLayer is a layer unpacked into a snapshot with no parent, not
// committed yet. Present: the layer was already unpacked, nothing was.
// ByWorker: the worker unpacked it while its blob arrived
// (ExtractLayerStream); it is attached by moving its files into a snapshot
// prepared on the layer below, which works on any containerd, and Implied
// and RootEntry say what that attach must fix.
type ExtractedLayer struct {
	Key       string
	DiffID    string
	Present   bool
	ByWorker  bool
	Implied   []string // directories the layer only implies, parents first
	RootEntry bool     // the layer's tar has an entry for its root directory
}

// EnableParallelUnpack checks that containerd can commit a snapshot onto a
// parent: the snapshotter says so ("rebase"), and doing it with two empty
// throwaway snapshots works. Errors leave parallel unpacking off.
func (s *ContainerdStore) EnableParallelUnpack(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(s.ns(ctx), 30*time.Second)
	defer cancel()
	resp, err := s.client.IntrospectionService().Plugins(ctx, []string{
		fmt.Sprintf(`type=="io.containerd.snapshotter.v1",id==%q`, s.snapshotter)})
	if err != nil {
		return fmt.Errorf("listing containerd's plugins: %w", err)
	}
	if len(resp.Plugins) != 1 || !slices.Contains(resp.Plugins[0].Capabilities, "rebase") {
		return fmt.Errorf("containerd's %s snapshotter can't commit onto a parent (no \"rebase\" capability: containerd 2.2 or later)", s.snapshotter)
	}
	lease := "angryduck-probe-" + randomSuffix()
	if err := s.CreateLease(ctx, lease, time.Minute); err != nil {
		return err
	}
	defer func() { _ = s.DeleteLease(context.WithoutCancel(ctx), lease) }()
	lctx := leases.WithLease(ctx, lease)
	sn := s.snapshotterSvc()
	suffix := randomSuffix()
	below, above := "angryduck-probe-below-"+suffix, "angryduck-probe-above-"+suffix
	for _, name := range []string{below, above} {
		if _, err := sn.Prepare(lctx, name+"-key", ""); err != nil {
			return fmt.Errorf("preparing a snapshot: %w", err)
		}
	}
	defer func() {
		removeQuietly(lctx, sn, above)
		removeQuietly(lctx, sn, above+"-key")
		removeQuietly(lctx, sn, below)
		removeQuietly(lctx, sn, below+"-key")
	}()
	if err := s.commitOnto(lctx, below, below+"-key", ""); err != nil {
		return fmt.Errorf("committing a snapshot: %w", err)
	}
	if err := s.commitOnto(lctx, above, above+"-key", below); err != nil {
		return fmt.Errorf("committing a snapshot onto another: %w", err)
	}
	info, err := sn.Stat(lctx, above)
	if err != nil {
		return err
	}
	if info.Parent != below {
		return errRebaseIgnored
	}
	s.parallelUnpack.Store(true)
	return nil
}

// ParallelUnpacks reports whether EnableParallelUnpack succeeded.
func (s *ContainerdStore) ParallelUnpacks() bool { return s.parallelUnpack.Load() }

// commitOnto commits active snapshot key as name, onto parent ("": none).
func (s *ContainerdStore) commitOnto(ctx context.Context, name, key, parent string) error {
	req := &snapshotsapi.CommitSnapshotRequest{Snapshotter: s.snapshotter, Name: name, Key: key}
	if parent != "" {
		var b []byte
		b = protowire.AppendTag(b, commitParentField, protowire.BytesType)
		b = protowire.AppendString(b, parent)
		req.ProtoReflect().SetUnknown(b)
	}
	_, err := snapshotsapi.NewSnapshotsClient(s.client.Conn()).Commit(ctx, req)
	return errdefs.FromGRPC(err)
}

// ExtractLayer has containerd unpack a layer blob that is in the content
// store into a new snapshot with no parent, under lease, and checks it
// unpacked to what its chainID says. Nothing is committed: see
// CommitExtracted.
func (s *ContainerdStore) ExtractLayer(ctx context.Context, lease string, l LayerSpec) (ExtractedLayer, error) {
	if !s.parallelUnpack.Load() {
		return ExtractedLayer{}, errors.New("parallel unpacking is off")
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
	if l.Parent != "" {
		// Unpacked as a bind mount, a layer's whiteouts would delete files
		// from its own empty directory instead of being written as
		// whiteouts, and the files they remove would show through once it
		// is committed onto its parent (containerd issue 13030, fixed
		// where containerd unpacks in parallel itself in 2.2.3 and 2.3).
		mounts = bindToOverlay(mounts)
	}
	desc := ocispec.Descriptor{MediaType: l.MediaType, Digest: digest.Digest(l.Digest), Size: l.Size}
	diff, err := s.client.DiffService().Apply(ctx, desc, mounts)
	if err == nil && chainOf(l.Parent, diff.Digest.String()) != l.ChainID {
		err = fmt.Errorf("%s unpacks to chainID %s, the image says %s", l.Digest, chainOf(l.Parent, diff.Digest.String()), l.ChainID)
	}
	if err != nil {
		removeQuietly(ctx, sn, key)
		return ExtractedLayer{}, fmt.Errorf("unpacking %s: %w", l.Digest, err)
	}
	return ExtractedLayer{Key: key, DiffID: diff.Digest.String()}, nil
}

// CommitExtracted commits an extracted layer under its chainID onto the
// layer below. errParentMissing (nothing changed) when that isn't there
// yet; applied false when the layer was unpacked meanwhile (the extracted
// snapshot is then removed).
func (s *ContainerdStore) CommitExtracted(ctx context.Context, lease string, l LayerSpec, x ExtractedLayer) (applied bool, err error) {
	if x.ByWorker {
		return s.attachExtracted(ctx, lease, l, x)
	}
	ctx = leases.WithLease(s.ns(ctx), lease)
	sn := s.snapshotterSvc()
	defer s.invalidate()
	if l.Parent != "" {
		if _, err := sn.Stat(ctx, l.Parent); errdefs.IsNotFound(err) {
			return false, errParentMissing
		} else if err != nil {
			return false, err
		}
	}
	if err := s.commitOnto(ctx, l.ChainID, x.Key, l.Parent); err != nil {
		if errdefs.IsAlreadyExists(err) {
			removeQuietly(ctx, sn, x.Key)
			return false, nil
		}
		return false, fmt.Errorf("committing %s onto %q: %w", l.ChainID, l.Parent, err)
	}
	// A wrong parent would make every image using this layer wrong: read
	// it back, and take the snapshot away at once if containerd dropped it.
	info, err := sn.Stat(ctx, l.ChainID)
	if err == nil && info.Parent != l.Parent {
		removeQuietly(ctx, sn, l.ChainID)
		s.parallelUnpack.Store(false)
		return false, errRebaseIgnored
	}
	if err != nil {
		return false, err
	}
	_, _ = s.client.ContentStore().Update(ctx, content.Info{Digest: digest.Digest(l.Digest),
		Labels: map[string]string{uncompressedLabel: x.DiffID}}, "labels."+uncompressedLabel)
	return true, nil
}

// bindToOverlay turns a parentless snapshot's bind mount into an overlay
// mount with only an upper directory: containerd's applier then writes the
// layer straight into that directory, converting whiteouts as for any
// overlay layer, without mounting anything. As containerd's own unpacker
// does since 2.2.3.
func bindToOverlay(ms []mount.Mount) []mount.Mount {
	if len(ms) != 1 || ms[0].Type != "bind" {
		return ms
	}
	m := mount.Mount{Type: "overlay", Source: "overlay"}
	for _, o := range ms[0].Options {
		if o != "rbind" {
			m.Options = append(m.Options, o)
		}
	}
	m.Options = append(m.Options, "upperdir="+ms[0].Source)
	return []mount.Mount{m}
}

// DiscardExtracted removes an extracted layer that won't be committed.
func (s *ContainerdStore) DiscardExtracted(ctx context.Context, lease string, x ExtractedLayer) {
	if x.Key == "" {
		return
	}
	defer s.invalidate()
	ctx = s.ns(ctx)
	if lease != "" {
		ctx = leases.WithLease(ctx, lease)
	}
	removeQuietly(ctx, s.snapshotterSvc(), x.Key)
}
