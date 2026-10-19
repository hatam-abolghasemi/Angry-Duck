//go:build integration

package worker

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/platforms"

	"angryduck/internal/blobship"
	"angryduck/internal/model"
)

// metadataOnly is img's archive without its layer blobs: what a spread
// finalize imports, or a rescue that counted every layer as already here.
func metadataOnly(t *testing.T, img builtImage) []byte {
	t.Helper()
	isLayer := map[string]bool{}
	for _, l := range img.layers {
		isLayer[l] = true
	}
	man := img.blobs[img.manifest]
	index, _ := json.Marshal(map[string]any{
		"schemaVersion": 2,
		"manifests": []map[string]any{{"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": img.manifest, "size": len(man),
			"annotations": map[string]string{"io.containerd.image.name": img.name}}},
	})
	var arch bytes.Buffer
	tw := tar.NewWriter(&arch)
	put := func(n string, b []byte) {
		tw.WriteHeader(&tar.Header{Name: n, Mode: 0o644, Size: int64(len(b)), Typeflag: tar.TypeReg})
		tw.Write(b)
	}
	put("oci-layout", []byte(`{"imageLayoutVersion":"1.0.0"}`))
	put("index.json", index)
	var ds []string
	for d := range img.blobs {
		if !isLayer[d] {
			ds = append(ds, d)
		}
	}
	sort.Strings(ds)
	for _, d := range ds {
		put("blobs/sha256/"+strings.TrimPrefix(d, "sha256:"), img.blobs[d])
	}
	tw.Close()
	return arch.Bytes()
}

var otherAppLayer = []fileEntry{
	{name: "etc/", typ: tar.TypeDir, mode: 0o755},
	{name: "etc/os-release", typ: tar.TypeReg, mode: 0o644, body: "ID=app-v2\n"},
}

// The incident: an import whose archive leaves out a layer that turns out
// not to be here. containerd's import would register the name anyway and
// the unpack fail after; kubelet then calls the image present forever.
func TestContainerd_ImportThatCantUnpackRegistersNothing(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	s := NewContainerdStore(c, testNS(t, "atomic"), "overlayfs")
	base := buildImage(t, "registry.example.com/team/base:1", baseLayer)
	if err := s.Import(ctx, bytes.NewReader(base.archive), "linux/amd64"); err != nil {
		t.Fatal(err)
	}
	app := buildImage(t, testImage, baseLayer, appLayer)
	err := s.Import(ctx, bytes.NewReader(metadataOnly(t, app)), "linux/amd64")
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("import = %v, want an unpack failure on the missing layer", err)
	}
	if _, _, err := s.Resolve(ctx, testImage); err == nil {
		t.Fatal("the name of an image that can't be unpacked was registered")
	}
	names, _ := s.ImageNames(ctx)
	if len(names) != 1 || names[0] != base.name {
		t.Fatalf("images after the failed import: %v, want only %s (no temporary names)", names, base.name)
	}

	// The full archive then imports, registers and unpacks.
	if err := s.Import(ctx, bytes.NewReader(app.archive), "linux/amd64"); err != nil {
		t.Fatal(err)
	}
	if _, d, err := s.Resolve(ctx, testImage); err != nil || d != app.manifest {
		t.Fatalf("Resolve = %s %v", d, err)
	}
	img, _ := c.ImageService().Get(namespaces.WithNamespace(ctx, s.namespace), testImage)
	ok, err := containerd.NewImageWithPlatform(c, img, platforms.OnlyStrict(platforms.MustParse("linux/amd64"))).IsUnpacked(namespaces.WithNamespace(ctx, s.namespace), "overlayfs")
	if err != nil || !ok {
		t.Fatalf("published but not unpacked: %v %v", ok, err)
	}
	if names, _ := s.ImageNames(ctx); len(names) != 2 {
		t.Fatalf("images: %v, want base and app only", names)
	}
}

// A tag that moved: the new image fails to unpack, the name keeps the old,
// working one.
func TestContainerd_FailedImportKeepsWhatTheNameHad(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	s := NewContainerdStore(c, testNS(t, "keep"), "overlayfs")
	v1 := buildImage(t, testImage, baseLayer, appLayer)
	if err := s.Import(ctx, bytes.NewReader(v1.archive), "linux/amd64"); err != nil {
		t.Fatal(err)
	}
	v2 := buildImage(t, testImage, baseLayer, otherAppLayer)
	if err := s.Import(ctx, bytes.NewReader(metadataOnly(t, v2)), "linux/amd64"); err == nil {
		t.Fatal("import of v2 without its app layer succeeded")
	}
	if _, d, err := s.Resolve(ctx, testImage); err != nil || d != v1.manifest {
		t.Fatalf("name points at %s (%v), want v1 %s", d, err, v1.manifest)
	}
}

// What a lease holds survives garbage collection; what nothing holds
// doesn't.
func TestContainerd_HoldKeepsLayersThroughGC(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	s := NewContainerdStore(c, testNS(t, "hold"), "overlayfs")
	nctx := s.ns(ctx)
	lm := c.LeasesService()
	img := buildImage(t, testImage, baseLayer, appLayer)
	if err := s.Import(ctx, bytes.NewReader(img.archive), "linux/amd64"); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateLease(ctx, "job", time.Hour); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.HoldSnapshot(ctx, "job", img.chains[1]); err != nil || !ok {
		t.Fatalf("HoldSnapshot = %v %v", ok, err)
	}
	if ok, err := s.HoldContent(ctx, "job", img.layers[1]); err != nil || !ok {
		t.Fatalf("HoldContent = %v %v", ok, err)
	}
	// kubelet removes the image they came with.
	if err := c.ImageService().Delete(nctx, testImage, images.SynchronousDelete()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.snapshotterSvc().Stat(nctx, img.chains[0]); err != nil {
		t.Fatalf("parent of a held snapshot freed: %v", err)
	}
	if _, err := c.ContentStore().Info(nctx, mustDigest(img.layers[1])); err != nil {
		t.Fatalf("held blob freed: %v", err)
	}
	if _, err := c.ContentStore().Info(nctx, mustDigest(img.layers[0])); !errdefs.IsNotFound(err) {
		t.Fatalf("unheld blob still there: %v", err)
	}
	if ok, err := s.HoldContent(ctx, "job", img.layers[0]); err != nil || ok {
		t.Fatalf("HoldContent of a freed blob = %v %v, want false", ok, err)
	}
	if err := lm.Delete(nctx, leases.Lease{ID: "job"}, leases.SynchronousDelete); err != nil {
		t.Fatal(err)
	}
	if _, err := s.snapshotterSvc().Stat(nctx, img.chains[1]); !errdefs.IsNotFound(err) {
		t.Fatalf("snapshot still there after its lease went: %v", err)
	}
}

// A spread finalize planned from a stale inventory: the app layer's blob
// is gone. Nothing gets registered.
func TestContainerd_SpreadFinalizeWithALayerGone(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	s := NewContainerdStore(c, testNS(t, "spread"), "overlayfs")
	base := buildImage(t, "registry.example.com/team/base:1", baseLayer)
	if err := s.Import(ctx, bytes.NewReader(base.archive), "linux/amd64"); err != nil {
		t.Fatal(err)
	}
	app := buildImage(t, testImage, baseLayer, appLayer)
	sp := NewSpread(s, &fakeRegistry{}, spreadToken, "n1", "linux/amd64", "", time.Minute)
	mux := http.NewServeMux()
	sp.Register(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	f := model.SpreadFinalize{Job: "s1", Image: testImage, TopType: "application/vnd.oci.image.manifest.v1+json",
		TopDig: app.manifest, TopSize: int64(len(app.blobs[app.manifest])), Metadata: map[string][]byte{}}
	for d, b := range app.blobs {
		if d != app.layers[0] && d != app.layers[1] {
			f.Metadata[d] = b
		}
	}
	resp := postSpread(t, srv, "/spread/finalize", f)
	var res model.SpreadFinalizeResult
	_ = json.NewDecoder(resp.Body).Decode(&res)
	if resp.StatusCode != http.StatusBadGateway || !strings.Contains(res.Error, app.layers[1]) {
		t.Fatalf("finalize: %d %+v, want 502 naming %s", resp.StatusCode, res, app.layers[1])
	}
	if _, _, err := s.Resolve(ctx, testImage); err == nil {
		t.Fatal("registered an image whose layer is gone")
	}
}

// Images a pre-1.8.12 import left broken (registered by containerd's own
// import, never unpacked, a layer gone) are removed by the repair; healthy
// ones stay.
func TestContainerd_RepairRemovesAnImageThatCantBeUnpacked(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	s := NewContainerdStore(c, testNS(t, "repair"), "overlayfs")
	nctx := s.ns(ctx)
	base := buildImage(t, "registry.example.com/team/base:1", baseLayer)
	if err := s.Import(ctx, bytes.NewReader(base.archive), "linux/amd64"); err != nil {
		t.Fatal(err)
	}
	app := buildImage(t, testImage, baseLayer, appLayer)
	// The old bug, reproduced with containerd's own import.
	if _, err := c.Import(nctx, bytes.NewReader(metadataOnly(t, app)), containerd.WithAllPlatforms(false)); err != nil {
		t.Fatal(err)
	}
	s.Invalidate()
	h, err := blobship.Check(ctx, s, testImage, "linux/amd64")
	if err != nil || !h.Broken() || len(h.Missing) != 1 || h.Missing[0].Blob.Digest != app.layers[1] {
		t.Fatalf("Check = %+v %v, want broken on the app layer", h, err)
	}
	// A rescue of it is no longer answered "already present".
	if _, broken := brokenImage(ctx, s, testImage, "linux/amd64"); !broken {
		t.Fatal("brokenImage says fine")
	}

	r := NewImageRepair(s, "linux/amd64", "n1", time.Minute, nil, nil)
	now := time.Now()
	r.Pass(ctx, now)
	if _, _, err := s.Resolve(ctx, testImage); err != nil {
		t.Fatal("removed on the first look")
	}
	r.Pass(ctx, now.Add(2*time.Minute))
	if _, _, err := s.Resolve(ctx, testImage); err == nil {
		t.Fatal("broken image still registered after the second look")
	}
	if _, _, err := s.Resolve(ctx, base.name); err != nil {
		t.Fatalf("healthy image removed: %v", err)
	}
}
