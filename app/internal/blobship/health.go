package blobship

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/containerd/containerd/errdefs"
)

// Health is whether a registered image can actually be run here.
//
// containerd keeps an image's name, its metadata blobs (index, manifest,
// config), its layer blobs and its unpacked snapshots apart, and a name
// alone is enough for kubelet to call the image "present". Creating a
// container needs the metadata, then the top layer's snapshot (chainID);
// if that isn't there, containerd unpacks the image on the spot, which
// needs the compressed blob of every layer whose snapshot is missing. When
// a metadata blob or one of those layer blobs is gone, every container
// create fails, and kubelet never pulls again because the image is
// "already present on machine".
//
// Nothing here hashes anything: a metadata blob counts as corrupted only
// when it doesn't parse or its size isn't the one its parent names.
type Health struct {
	// Known is false when nothing can be said: another platform's image,
	// an unsupported format, foreign layers. Callers treat it as fine.
	Known bool
	// Meta is the first metadata blob that is gone or unusable. The walk
	// stops there, so Missing is empty and Unpacked false when it is set.
	Meta *MetaProblem
	// Unpacked: the top layer's snapshot exists, containers can start.
	Unpacked bool
	// Missing lists the layers that are neither unpacked nor have their
	// blob here, bottom first. Non-empty means the image can't be run
	// until those blobs are here again.
	Missing []Layer
}

// Broken reports whether the image is registered but can never start a
// container as it stands.
func (h Health) Broken() bool {
	return h.Known && (h.Meta != nil || (!h.Unpacked && len(h.Missing) > 0))
}

// String says what is wrong, for logs.
func (h Health) String() string {
	switch {
	case !h.Broken():
		return "fine"
	case h.Meta != nil:
		return h.Meta.Error()
	default:
		return fmt.Sprintf("%d layer(s) neither unpacked nor here, first %s", len(h.Missing), h.Missing[0].Blob.Digest)
	}
}

// MetaProblem is a metadata blob (index, manifest or config) that is gone
// or unusable.
type MetaProblem struct {
	Blob    Descriptor
	Kind    string // "index", "manifest" or "config"
	Corrupt bool   // here but unusable; false: gone
	Err     error
}

func (p *MetaProblem) Error() string {
	if p.Corrupt {
		return fmt.Sprintf("%s %s is unusable: %v", p.Kind, p.Blob.Digest, p.Err)
	}
	return fmt.Sprintf("%s %s is gone: %v", p.Kind, p.Blob.Digest, p.Err)
}

func (p *MetaProblem) Unwrap() error { return p.Err }

// IsNotFound reports whether err means a blob, snapshot or image doesn't
// exist (containerd's NotFound, or a test store's "not found").
func IsNotFound(err error) bool {
	return err != nil && (errdefs.IsNotFound(err) || strings.Contains(err.Error(), "not found"))
}

// MetaWalk is what WalkMetadata found.
type MetaWalk struct {
	Top Descriptor
	// Metadata holds every metadata blob read, by digest.
	Metadata map[string][]byte
	// Layers is bottom first; nil when they can't be paired with
	// snapshots (foreign layers).
	Layers []Layer
}

// ErrOtherPlatform: the image has nothing for this node's platform.
var ErrOtherPlatform = errors.New("no manifest for this platform")

// WalkMetadata walks an image's metadata for platform the way containerd
// does: top-level object, the platform's manifest when top is an index,
// then the config. read returns one blob; its error, or a blob that
// doesn't parse or has the wrong size, stops the walk with a *MetaProblem
// naming that blob. top.Size 0 means unknown (not checked).
func WalkMetadata(top Descriptor, platform string, read func(Descriptor) ([]byte, error)) (MetaWalk, error) {
	want, err := ParsePlatform(platform)
	if err != nil {
		return MetaWalk{}, err
	}
	w := MetaWalk{Top: top, Metadata: map[string][]byte{}}
	get := func(d Descriptor, kind string) ([]byte, error) {
		b, err := read(d)
		if err != nil {
			return nil, &MetaProblem{Blob: stripDescriptor(d), Kind: kind, Err: err}
		}
		if d.Size > 0 && int64(len(b)) != d.Size {
			return nil, &MetaProblem{Blob: stripDescriptor(d), Kind: kind, Corrupt: true, Err: fmt.Errorf("%d bytes, want %d", len(b), d.Size)}
		}
		w.Metadata[d.Digest] = b
		return b, nil
	}
	corrupt := func(d Descriptor, kind string, err error) error {
		delete(w.Metadata, d.Digest)
		return &MetaProblem{Blob: stripDescriptor(d), Kind: kind, Corrupt: true, Err: err}
	}

	topKind := "manifest"
	if top.MediaType == MediaTypeOCIIndex || top.MediaType == MediaTypeDockerList {
		topKind = "index"
	}
	topBytes, err := get(top, topKind)
	if err != nil {
		return w, err
	}
	manifest, manifestBytes := top, topBytes
	switch top.MediaType {
	case MediaTypeOCIIndex, MediaTypeDockerList:
		var idx indexJSON
		if err := json.Unmarshal(topBytes, &idx); err != nil || idx.Manifests == nil {
			if err == nil {
				err = errors.New("no manifests")
			}
			return w, corrupt(top, topKind, err)
		}
		chosen, err := pickManifest(idx.Manifests, want)
		if err != nil {
			return w, fmt.Errorf("%w: %v", ErrOtherPlatform, err)
		}
		manifest = chosen
		if manifestBytes, err = get(chosen, "manifest"); err != nil {
			return w, err
		}
	case MediaTypeOCIManifest, MediaTypeDockerManifest:
	default:
		return w, fmt.Errorf("unsupported media type %q", top.MediaType)
	}
	var m manifestJSON
	if err := json.Unmarshal(manifestBytes, &m); err != nil {
		return w, corrupt(manifest, "manifest", err)
	}
	if m.SchemaVersion != 2 {
		return w, fmt.Errorf("manifest schemaVersion %d is not supported", m.SchemaVersion)
	}
	if m.Config.Digest == "" || len(m.Layers) == 0 {
		return w, corrupt(manifest, "manifest", errors.New("no config or no layers"))
	}
	cfg, err := get(m.Config, "config")
	if err != nil {
		return w, err
	}
	for _, l := range m.Layers {
		if len(l.URLs) > 0 {
			return w, nil // foreign layers: can't be paired with snapshots
		}
	}
	if w.Layers = pairLayers(m, cfg); w.Layers == nil {
		return w, corrupt(m.Config, "config", errors.New("diff_ids don't match the manifest's layers"))
	}
	return w, nil
}

// Check works out image's Health in store for platform, the way
// containerd's container create does. The store's listings may be cached:
// a caller about to act on a broken verdict should check again with fresh
// listings first.
func Check(ctx context.Context, store Store, image, platform string) (Health, error) {
	h, _, err := CheckWalk(ctx, store, image, platform)
	return h, err
}

// CheckWalk is Check, also returning the metadata walk it made, so a
// caller can keep the layers (immutable for a given top digest) and check
// them again later with CheckLayers alone.
func CheckWalk(ctx context.Context, store Store, image, platform string) (Health, MetaWalk, error) {
	mediaType, digest, err := store.Resolve(ctx, image)
	if err != nil {
		return Health{}, MetaWalk{}, err
	}
	walk, err := WalkMetadata(Descriptor{MediaType: mediaType, Digest: digest}, platform, func(d Descriptor) ([]byte, error) {
		return store.ReadBlob(ctx, d.Digest, maxMetadataBlob)
	})
	var mp *MetaProblem
	switch {
	case errors.As(err, &mp) && (mp.Corrupt || IsNotFound(mp.Err)):
		return Health{Known: true, Meta: mp}, walk, nil
	case err != nil:
		return Health{}, walk, err
	case len(walk.Layers) == 0:
		return Health{}, walk, nil
	}
	snaps, err := store.Snapshots(ctx)
	if err != nil {
		return Health{}, walk, err
	}
	h, err := checkLayers(walk.Layers, snaps, nil, func() (map[string]bool, error) { return store.Digests(ctx) })
	return h, walk, err
}

// CheckLayers is Check for an image whose layers are already known, from
// listings of snapshots and blobs: no containerd call of its own.
func CheckLayers(layers []Layer, snaps, blobs map[string]bool) Health {
	if len(layers) == 0 {
		return Health{}
	}
	h, _ := checkLayers(layers, snaps, blobs, nil)
	return h
}

// checkLayers is Check once the layers are known. blobs is listed lazily:
// an unpacked image doesn't need the (larger) content listing at all.
func checkLayers(layers []Layer, snaps, blobs map[string]bool, listBlobs func() (map[string]bool, error)) (Health, error) {
	h := Health{Known: true}
	if snaps[layers[len(layers)-1].ChainID] {
		h.Unpacked = true
		return h, nil
	}
	if blobs == nil {
		var err error
		if blobs, err = listBlobs(); err != nil {
			return Health{}, err
		}
	}
	// Unpacking skips every layer whose snapshot exists and applies the
	// blob of every other one.
	for _, l := range layers {
		if !snaps[l.ChainID] && !blobs[l.Blob.Digest] {
			h.Missing = append(h.Missing, l)
		}
	}
	return h, nil
}

// LayersFromMetadata works out an image's layers for platform from its
// metadata blobs alone (top-level object, manifest, config, by digest), as
// a spread finalize carries them. It returns nil, nil when layers can't be
// paired with snapshots (see layersOf).
func LayersFromMetadata(top Descriptor, metadata map[string][]byte, platform string) ([]Layer, error) {
	walk, err := WalkMetadata(Descriptor{MediaType: top.MediaType, Digest: top.Digest}, platform, func(d Descriptor) ([]byte, error) {
		b, ok := metadata[d.Digest]
		if !ok {
			return nil, fmt.Errorf("%s not in metadata", d.Digest)
		}
		return b, nil
	})
	var mp *MetaProblem
	if errors.As(err, &mp) && !mp.Corrupt && mp.Kind == "config" {
		return nil, nil // no config: nothing to pair
	}
	if err != nil {
		return nil, err
	}
	return walk.Layers, nil
}
