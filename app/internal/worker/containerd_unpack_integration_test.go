//go:build integration

package worker

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"angryduck/internal/model"
)

// unpackFetch is the fetch order the controller sends for layer i of img.
func unpackFetch(img builtImage, job string, i int) model.SpreadFetch {
	u := &model.SpreadUnpack{ChainID: img.chains[i], MediaType: "application/vnd.oci.image.layer.v1.tar+gzip"}
	if i > 0 {
		u.Parent = img.chains[i-1]
	}
	d := img.layers[i]
	return model.SpreadFetch{Job: job, Image: img.name, Digest: d, Size: int64(len(img.blobs[d])), Path: model.SpreadPathRegistry, Unpack: u}
}

func spreadFinalizeOf(img builtImage, job string) model.SpreadFinalize {
	f := model.SpreadFinalize{Job: job, Image: img.name, TopType: "application/vnd.oci.image.manifest.v1+json",
		TopDig: img.manifest, TopSize: int64(len(img.blobs[img.manifest])), Metadata: map[string][]byte{}}
	isLayer := map[string]bool{}
	for _, l := range img.layers {
		isLayer[l] = true
	}
	for d, b := range img.blobs {
		if !isLayer[d] {
			f.Metadata[d] = b
		}
	}
	return f
}

// Layers arriving top first are unpacked bottom up as soon as each can be,
// before finalize, with the same files containerd's own unpack makes; the
// finalize then registers the image without unpacking them again.
func TestContainerd_SpreadUnpacksLayersAsTheyArrive(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	s := NewContainerdStore(c, testNS(t, "unpack"), "overlayfs")
	extra := []fileEntry{
		{name: "srv/", typ: tar.TypeDir, mode: 0o755},
		{name: "srv/index.html", typ: tar.TypeReg, mode: 0o644, body: "<h1>quack</h1>\n"},
	}
	img := buildImage(t, testImage, baseLayer, appLayer, extra)
	sp := NewSpread(s, builtRegistry{blobs: img.blobs}, spreadToken, "n1", "linux/amd64", "", time.Minute)
	sp.EnableUnpack(s)
	mux := http.NewServeMux()
	sp.Register(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	snap := func(chain string) bool {
		_, err := s.snapshotterSvc().Stat(s.ns(ctx), chain)
		return err == nil
	}
	for _, i := range []int{2, 1} {
		postSpread(t, srv, "/spread/fetch", unpackFetch(img, "s1", i))
		sp.wg.Wait()
	}
	if snap(img.chains[1]) || snap(img.chains[2]) {
		t.Fatal("upper layers unpacked before the bottom one arrived")
	}
	postSpread(t, srv, "/spread/fetch", unpackFetch(img, "s1", 0))
	sp.wg.Wait()
	for i, ch := range img.chains {
		if !snap(ch) {
			t.Fatalf("layer %d not unpacked on arrival", i)
		}
	}

	// The same files a full import makes elsewhere.
	ref := NewContainerdStore(c, testNS(t, "unpackref"), "overlayfs")
	if err := ref.Import(ctx, bytes.NewReader(img.archive), "linux/amd64"); err != nil {
		t.Fatal(err)
	}
	top := img.chains[len(img.chains)-1]
	sameTree(t, tree(t, ref, top), tree(t, s, top))

	resp := postSpread(t, srv, "/spread/finalize", spreadFinalizeOf(img, "s1"))
	var res model.SpreadFinalizeResult
	_ = json.NewDecoder(resp.Body).Decode(&res)
	if resp.StatusCode != http.StatusOK || !res.OK {
		t.Fatalf("finalize: %d %+v", resp.StatusCode, res)
	}
	if _, got, err := s.Resolve(ctx, img.name); err != nil || got != img.manifest {
		t.Fatalf("resolves to %q (%v)", got, err)
	}
}

// A layer whose unpacked content doesn't chain to the chainID it was
// ordered under is never committed under it.
func TestContainerd_ApplyLayerRefusesAWrongChainID(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	s := NewContainerdStore(c, testNS(t, "unpackbad"), "overlayfs")
	img := buildImage(t, testImage, baseLayer, appLayer)
	if err := s.CreateLease(ctx, "l1", time.Hour); err != nil {
		t.Fatal(err)
	}
	for _, d := range img.layers {
		if _, err := s.WriteBlob(ctx, "l1", "w-"+d, d, int64(len(img.blobs[d])), bytes.NewReader(img.blobs[d])); err != nil {
			t.Fatal(err)
		}
	}
	mt := "application/vnd.oci.image.layer.v1.tar+gzip"
	// The bottom layer, ordered as if it were the app layer.
	_, err := s.ApplyLayer(ctx, "l1", LayerSpec{Digest: img.layers[0], Size: int64(len(img.blobs[img.layers[0]])), MediaType: mt, ChainID: img.chains[1]})
	if err == nil {
		t.Fatal("committed a layer under a chainID it doesn't have")
	}
	if _, err := s.snapshotterSvc().Stat(s.ns(ctx), img.chains[1]); err == nil {
		t.Fatal("snapshot exists under the wrong chainID")
	}
	// The app layer before its parent: waits.
	_, err = s.ApplyLayer(ctx, "l1", LayerSpec{Digest: img.layers[1], Size: int64(len(img.blobs[img.layers[1]])), MediaType: mt, ChainID: img.chains[1], Parent: img.chains[0]})
	if !errors.Is(err, errParentMissing) {
		t.Fatalf("err = %v, want errParentMissing", err)
	}
	applied, err := s.ApplyLayer(ctx, "l1", LayerSpec{Digest: img.layers[0], Size: int64(len(img.blobs[img.layers[0]])), MediaType: mt, ChainID: img.chains[0]})
	if err != nil || !applied {
		t.Fatalf("bottom layer: applied=%t err=%v", applied, err)
	}
	applied, err = s.ApplyLayer(ctx, "l1", LayerSpec{Digest: img.layers[0], Size: int64(len(img.blobs[img.layers[0]])), MediaType: mt, ChainID: img.chains[0]})
	if err != nil || applied {
		t.Fatalf("again: applied=%t err=%v, want already there", applied, err)
	}
}
