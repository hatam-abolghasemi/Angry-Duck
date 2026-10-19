package blobship

import (
	"context"
	"io"
	"testing"
)

func TestCheck(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()
	ti := m.AddTestImage("r.io/app:1", "base", "libs", "app")

	h, err := Check(ctx, m, "r.io/app:1", "linux/amd64")
	if err != nil || !h.Known || !h.Unpacked || h.Broken() {
		t.Fatalf("fresh image: %+v %v", h, err)
	}
	// discard_unpacked_layers: blobs gone, snapshots there. Fine.
	for _, l := range ti.Layers {
		m.Delete(l)
	}
	if h, _ = Check(ctx, m, "r.io/app:1", "linux/amd64"); h.Broken() {
		t.Fatalf("unpacked image without blobs: %+v", h)
	}
	// Top snapshot gone, its blob gone too: broken, and only that layer
	// is missing (the ones below are unpacked).
	m.DeleteSnapshot(ti.Chains[2])
	h, _ = Check(ctx, m, "r.io/app:1", "linux/amd64")
	if !h.Broken() || len(h.Missing) != 1 || h.Missing[0].Blob.Digest != ti.Layers[2] {
		t.Fatalf("broken image: %+v", h)
	}
	// Its blob back: not unpacked, but containerd can unpack it.
	m.Put([]byte("app"))
	if h, _ = Check(ctx, m, "r.io/app:1", "linux/amd64"); h.Unpacked || h.Broken() {
		t.Fatalf("unpackable image: %+v", h)
	}
	// Another platform: nothing can be said.
	if _, err := Check(ctx, m, "r.io/app:1", "linux/s390x"); err == nil {
		t.Fatal("no error for a platform the image lacks")
	}
}

func TestLayersFromMetadataMatchesBuildPlan(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()
	ti := m.AddTestImage("r.io/app:1", "base", "app")
	plan, err := BuildPlan(ctx, m, "r.io/app:1", "linux/amd64")
	if err != nil {
		t.Fatal(err)
	}
	meta := map[string][]byte{}
	for _, d := range []string{ti.Index, ti.AMD64Manifest, ti.Config} {
		b, err := m.ReadBlob(ctx, d, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		meta[d] = b
	}
	layers, err := LayersFromMetadata(plan.Top, meta, "linux/amd64")
	if err != nil || len(layers) != len(plan.Layers) {
		t.Fatalf("layers %v err %v", layers, err)
	}
	for i := range layers {
		if layers[i].Blob.Digest != plan.Layers[i].Blob.Digest || layers[i].DiffID != plan.Layers[i].DiffID || layers[i].ChainID != plan.Layers[i].ChainID {
			t.Fatalf("layer %d: %+v, want %+v", i, layers[i], plan.Layers[i])
		}
	}
	delete(meta, ti.Config)
	if layers, err := LayersFromMetadata(plan.Top, meta, "linux/amd64"); err != nil || layers != nil {
		t.Fatalf("without config: %v %v, want nil, nil", layers, err)
	}
}

// A failed import leaves the name as it was: never pointing at an image
// that can't be unpacked.
func TestMemStoreFailedImportKeepsThePreviousImage(t *testing.T) {
	ctx := context.Background()
	src := NewMemStore()
	ti := src.AddTestImage("r.io/app:1", "base", "app2")
	plan, _ := BuildPlan(ctx, src, "r.io/app:1", "linux/amd64")

	dst := NewMemStore()
	old := dst.AddTestImage("r.io/app:1", "base", "app1")
	// Ship only metadata: the new top layer is neither here nor in the
	// archive, so its unpack fails.
	var meta []Descriptor
	for _, b := range plan.Blobs {
		if b.Digest != ti.Layers[1] && b.Digest != ti.Layers[0] {
			meta = append(meta, b)
		}
	}
	pr, pw := io.Pipe()
	go func() { pw.CloseWithError(WriteArchive(ctx, pw, src, plan, meta)) }()
	if err := dst.Import(ctx, pr, "linux/amd64"); err == nil {
		t.Fatal("import of an image that can't be unpacked succeeded")
	}
	if _, d, err := dst.Resolve(ctx, "r.io/app:1"); err != nil || d != old.Index {
		t.Fatalf("name now points at %s (%v), want the previous image %s", d, err, old.Index)
	}
}
