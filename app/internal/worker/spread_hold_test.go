package worker

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"angryduck/internal/blobship"
	"angryduck/internal/model"
)

// spreadImage is a two-layer single-platform image's metadata, as a
// finalize carries it.
type spreadImage struct {
	layers   [][]byte
	digests  []string // compressed layer digests
	chains   []string
	top      []byte
	topD     string
	metadata map[string][]byte
}

func newSpreadImage(layers ...string) spreadImage {
	var im spreadImage
	var diffIDs []string
	var descs []blobship.Descriptor
	for _, l := range layers {
		b := []byte(l)
		d := digestOf(b)
		im.layers = append(im.layers, b)
		im.digests = append(im.digests, d)
		diffIDs = append(diffIDs, digestOf(append([]byte("uncompressed:"), b...)))
		descs = append(descs, blobship.Descriptor{MediaType: "application/vnd.oci.image.layer.v1.tar+gzip", Digest: d, Size: int64(len(b))})
	}
	im.chains = blobship.ChainIDs(diffIDs)
	cfg, _ := json.Marshal(map[string]any{"architecture": "amd64", "os": "linux", "rootfs": map[string]any{"type": "layers", "diff_ids": diffIDs}})
	cfgD := digestOf(cfg)
	im.top, _ = json.Marshal(map[string]any{
		"schemaVersion": 2, "mediaType": blobship.MediaTypeOCIManifest,
		"config": blobship.Descriptor{MediaType: "application/vnd.oci.image.config.v1+json", Digest: cfgD, Size: int64(len(cfg))},
		"layers": descs,
	})
	im.topD = digestOf(im.top)
	im.metadata = map[string][]byte{im.topD: im.top, cfgD: cfg}
	return im
}

func (im spreadImage) finalize(job, image string) model.SpreadFinalize {
	return model.SpreadFinalize{Job: job, Image: image, TopType: blobship.MediaTypeOCIManifest, TopDig: im.topD, TopSize: int64(len(im.top)), Metadata: im.metadata}
}

// A blob that is already here, because another image brought it, is only
// "present" for a spread once the job's lease holds it: otherwise
// removing that other image lets containerd's GC free it before the
// finalize, and the image is registered without it.
func TestSpreadPresentBlobIsHeldByTheJobLease(t *testing.T) {
	blob := []byte("layer of another image")
	d := digestOf(blob)
	store := newFakeSpreadStore()
	store.blobs[d] = blob
	sp, srv := newTestSpread(t, store, &fakeRegistry{}, nil, "n1")
	resp := postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Image: "r.io/app:1", Digest: d, Size: int64(len(blob)), Path: model.SpreadPathRegistry})
	var ack model.SpreadFetchAck
	_ = json.NewDecoder(resp.Body).Decode(&ack)
	if resp.StatusCode != http.StatusOK || !ack.Present {
		t.Fatalf("status %d ack %+v, want present", resp.StatusCode, ack)
	}
	if !store.leases["angryduck-spread-s1-"+sp.epoch+"-0"][d] {
		t.Fatalf("present blob not held by the job's lease: %v", store.leases)
	}
	store.collect() // the other image is removed
	if _, ok := store.blobs[d]; !ok {
		t.Fatal("garbage collection freed a blob the spread counted as present")
	}
}

// The incident: the controller finalizes from an inventory that still
// lists a layer the node lost meanwhile. The finalize must fail without
// importing anything, so no name points at an image that can't be
// unpacked, and ask for a fresh report.
func TestSpreadFinalizeRefusesWhenALayerIsGone(t *testing.T) {
	im := newSpreadImage("base", "app")
	store := newFakeSpreadStore()
	store.snaps[im.chains[0]] = true // base unpacked (discard_unpacked_layers: no blob)
	// app's blob was here at the last layer scan; it's gone now.
	store.topOf = func() (string, string) { return "r.io/app:1", im.topD }
	sp, srv := newTestSpread(t, store, &fakeRegistry{}, nil, "n1")
	stale := false
	sp.OnStale(func() { stale = true })

	resp := postSpread(t, srv, "/spread/finalize", im.finalize("s1", "r.io/app:1"))
	var res model.SpreadFinalizeResult
	_ = json.NewDecoder(resp.Body).Decode(&res)
	if resp.StatusCode != http.StatusBadGateway || res.OK {
		t.Fatalf("finalize: status %d %+v, want 502", resp.StatusCode, res)
	}
	if !strings.Contains(res.Error, im.digests[1]) {
		t.Fatalf("error %q doesn't name the missing layer %s", res.Error, im.digests[1])
	}
	if len(store.imported) != 0 || len(store.images) != 0 {
		t.Fatalf("imported %d archive(s), images %v: nothing may be registered", len(store.imported), store.images)
	}
	if !stale {
		t.Fatal("no fresh report asked for")
	}
}

// Layers counted as here are held from the check until the import: the
// highest unpacked one by its snapshot (which keeps those below), the
// rest by their blobs.
func TestSpreadFinalizeHoldsLayersThroughTheImport(t *testing.T) {
	im := newSpreadImage("base", "mid", "app")
	store := newFakeSpreadStore()
	store.snaps[im.chains[0]] = true
	store.snaps[im.chains[1]] = true
	store.blobs[im.digests[2]] = im.layers[2] // here, but no lease holds it
	store.topOf = func() (string, string) { return "r.io/app:1", im.topD }
	survived := false
	store.onImport = func() {
		store.collect() // kubelet removes the images these came with
		store.mu.Lock()
		_, blob := store.blobs[im.digests[2]]
		survived = blob && store.snaps[im.chains[1]]
		store.mu.Unlock()
	}
	_, srv := newTestSpread(t, store, &fakeRegistry{}, nil, "n1")
	resp := postSpread(t, srv, "/spread/finalize", im.finalize("s1", "r.io/app:1"))
	var res model.SpreadFinalizeResult
	_ = json.NewDecoder(resp.Body).Decode(&res)
	if resp.StatusCode != http.StatusOK || !res.OK {
		t.Fatalf("finalize: status %d %+v", resp.StatusCode, res)
	}
	if !survived {
		t.Fatal("a layer counted as here was freed before the import used it")
	}
	if store.leaseCount() != 0 {
		t.Fatal("the job's lease outlived the registered image")
	}
}
