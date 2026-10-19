package blobship

import (
	"context"
	"encoding/json"
	"fmt"
)

// Health is whether a registered image can actually be run here.
//
// containerd keeps an image's name, its metadata blobs, its layer blobs
// and its unpacked snapshots apart, and a name alone is enough for kubelet
// to call the image "present". Creating a container then needs the top
// layer's snapshot (chainID); if it isn't there, containerd unpacks the
// image on the spot, which needs the compressed blob of every layer whose
// snapshot is missing. When one of those blobs is gone too, every container
// create fails with "failed to get reader from content store", and kubelet
// never pulls again because the image is "already present on machine".
type Health struct {
	// Known is false when the image's layers couldn't be worked out (no
	// manifest for the platform, an unreadable config, foreign layers):
	// nothing can be said, and callers must treat the image as fine.
	Known bool
	// Unpacked: the top layer's snapshot exists, containers can start.
	Unpacked bool
	// Missing lists the layers that are neither unpacked nor have their
	// blob here, bottom first. Non-empty means the image can't be run
	// until it is fetched again.
	Missing []Layer
}

// Broken reports whether the image is registered but can never start a
// container as it stands.
func (h Health) Broken() bool { return h.Known && !h.Unpacked && len(h.Missing) > 0 }

// Check works out image's Health in store for platform, the way
// containerd's container create does. The store's listings may be cached:
// a caller about to act on a broken verdict should check again with fresh
// listings first.
func Check(ctx context.Context, store Store, image, platform string) (Health, error) {
	plan, err := BuildPlan(ctx, store, image, platform)
	if err != nil {
		return Health{}, err
	}
	if len(plan.Layers) == 0 {
		return Health{}, nil
	}
	snaps, err := store.Snapshots(ctx)
	if err != nil {
		return Health{}, err
	}
	return checkLayers(plan.Layers, snaps, nil, func() (map[string]bool, error) { return store.Digests(ctx) })
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
	want, err := ParsePlatform(platform)
	if err != nil {
		return nil, err
	}
	topBytes, ok := metadata[top.Digest]
	if !ok {
		return nil, fmt.Errorf("top-level object %s not in metadata", top.Digest)
	}
	manifestBytes := topBytes
	switch top.MediaType {
	case MediaTypeOCIIndex, MediaTypeDockerList:
		var idx indexJSON
		if err := json.Unmarshal(topBytes, &idx); err != nil {
			return nil, fmt.Errorf("parsing index %s: %w", top.Digest, err)
		}
		chosen, err := pickManifest(idx.Manifests, want)
		if err != nil {
			return nil, err
		}
		if manifestBytes, ok = metadata[chosen.Digest]; !ok {
			return nil, fmt.Errorf("manifest %s not in metadata", chosen.Digest)
		}
	case MediaTypeOCIManifest, MediaTypeDockerManifest:
	default:
		return nil, fmt.Errorf("unsupported media type %q", top.MediaType)
	}
	var m manifestJSON
	if err := json.Unmarshal(manifestBytes, &m); err != nil {
		return nil, fmt.Errorf("parsing manifest: %w", err)
	}
	cfg, ok := metadata[m.Config.Digest]
	if !ok {
		return nil, nil
	}
	return pairLayers(m, cfg), nil
}
