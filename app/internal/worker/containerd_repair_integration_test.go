//go:build integration

package worker

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

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

// builtRegistry serves a built image's blobs and manifests by digest.
type builtRegistry struct{ blobs map[string][]byte }

func (b builtRegistry) open(d string) (io.ReadCloser, int64, error) {
	data, ok := b.blobs[d]
	if !ok {
		return nil, 0, errors.New("404")
	}
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
}
func (b builtRegistry) OpenBlob(_ context.Context, _, d string) (io.ReadCloser, int64, error) {
	return b.open(d)
}
func (b builtRegistry) OpenManifest(_ context.Context, _, d string) (io.ReadCloser, int64, error) {
	return b.open(d)
}

// collect runs containerd's garbage collector, by removing and re-adding a
// throwaway image synchronously.
func collect(t *testing.T, c *containerd.Client, nctx context.Context) {
	t.Helper()
	img := images.Image{Name: "angryduck.local/gc-trigger:1", Target: ocispec.Descriptor{MediaType: "application/vnd.oci.image.manifest.v1+json", Digest: digest.Digest("sha256:" + strings.Repeat("0", 64)), Size: 1}}
	if _, err := c.ImageService().Create(nctx, img); err != nil && !errdefs.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	if err := c.ImageService().Delete(nctx, img.Name, images.SynchronousDelete()); err != nil {
		t.Fatal(err)
	}
}

// stillRunnable checks image is unpacked and stays so after the image it
// shared a layer with is removed and the garbage collector runs: the
// repair left the image's content referenced by the image itself.
func stillRunnable(t *testing.T, c *containerd.Client, s *ContainerdStore, image, other string) {
	t.Helper()
	ctx := context.Background()
	nctx := s.ns(ctx)
	if other != "" {
		if err := c.ImageService().Delete(nctx, other, images.SynchronousDelete()); err != nil {
			t.Fatal(err)
		}
	}
	collect(t, c, nctx)
	s.Invalidate()
	h, err := blobship.Check(ctx, s, image, "linux/amd64")
	if err != nil || h.Broken() || !h.Unpacked {
		t.Fatalf("%s after the repair and a GC: %+v (%v)", image, h, err)
	}
}

// Images a pre-1.8.12 import left broken (registered by containerd's own
// import, never unpacked, a layer gone) are repaired in place: the one
// missing blob is fetched, the name stays, healthy images stay.
func TestContainerd_RepairFetchesOnlyTheMissingLayer(t *testing.T) {
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
	if _, broken := brokenImage(ctx, s, testImage, "linux/amd64"); !broken {
		t.Fatal("brokenImage says fine")
	}

	reg := &countingRegistry{builtRegistry: builtRegistry{app.blobs}}
	NewImageRepair(s, reg, "linux/amd64", "n1", time.Minute, nil, nil).Pass(ctx)
	if _, d, err := s.Resolve(ctx, testImage); err != nil || d != app.manifest {
		t.Fatalf("name now points at %s (%v), want %s", d, err, app.manifest)
	}
	if len(reg.got) != 1 || reg.got[0] != app.layers[1] {
		t.Fatalf("fetched %v, want only the app layer %s", reg.got, app.layers[1])
	}
	stillRunnable(t, c, s, testImage, base.name)
}

// The image's manifest is gone: fetched by digest, and the image's
// content is referenced again (a GC keeps it).
func TestContainerd_RepairFetchesMissingMetadata(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	s := NewContainerdStore(c, testNS(t, "repairmeta"), "overlayfs")
	app := buildImage(t, testImage, baseLayer, appLayer)
	if err := s.Import(ctx, bytes.NewReader(app.archive), "linux/amd64"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteContent(ctx, app.manifest); err != nil {
		t.Fatal(err)
	}
	h, err := blobship.Check(ctx, s, testImage, "linux/amd64")
	if err != nil || !h.Broken() || h.Meta == nil || h.Meta.Blob.Digest != app.manifest {
		t.Fatalf("Check = %+v %v, want broken on the manifest", h, err)
	}
	// With the manifest gone, nothing referenced the config and layers any
	// more, and the garbage collector freed them: the repair fetches each
	// of them back, manifest first.
	reg := &countingRegistry{builtRegistry: builtRegistry{app.blobs}}
	NewImageRepair(s, reg, "linux/amd64", "n1", time.Minute, nil, nil).Pass(ctx)
	if len(reg.got) == 0 || reg.got[0] != app.manifest {
		t.Fatalf("fetched %v, want the manifest first", reg.got)
	}
	stillRunnable(t, c, s, testImage, "")
}

// A layer blob that is here but corrupted on disk: unpacking it fails, the
// controller's wake carries that error, and the repair replaces just that
// blob.
func TestContainerd_RepairReplacesALayerThatFailedToUnpack(t *testing.T) {
	root := os.Getenv("ANGRYDUCK_CONTAINERD_ROOT")
	if root == "" {
		t.Skip("ANGRYDUCK_CONTAINERD_ROOT not set: the test corrupts a blob file directly")
	}
	c := testClient(t)
	ctx := context.Background()
	s := NewContainerdStore(c, testNS(t, "repaircorrupt"), "overlayfs")
	nctx := s.ns(ctx)
	// A layer no other test uses: containerd shares a blob file between
	// namespaces, and a corrupted shared file stays while any uses it.
	app := buildImage(t, testImage, baseLayer, []fileEntry{
		{name: "etc/", typ: tar.TypeDir, mode: 0o755},
		{name: "etc/only-here", typ: tar.TypeReg, mode: 0o644, body: "corruption test\n"},
	})
	// Registered, all blobs here, not unpacked (containerd's own import).
	if _, err := c.Import(nctx, bytes.NewReader(app.archive), containerd.WithAllPlatforms(false)); err != nil {
		t.Fatal(err)
	}
	// Whatever happens, take the corrupted file away with this namespace's
	// references, so it can't leak into a later run.
	t.Cleanup(func() {
		_ = s.DeleteImages(ctx, testImage)
		_ = s.DeleteContent(ctx, app.layers[1])
	})
	f := filepath.Join(root, "io.containerd.content.v1.content", "blobs", "sha256", strings.TrimPrefix(app.layers[1], "sha256:"))
	if err := os.WriteFile(f, make([]byte, len(app.blobs[app.layers[1]])), 0o444); err != nil {
		t.Fatal(err)
	}
	img, err := c.GetImage(nctx, testImage)
	if err != nil {
		t.Fatal(err)
	}
	unpackErr := img.Unpack(nctx, "overlayfs")
	if unpackErr == nil {
		t.Fatal("unpacking a zeroed layer worked")
	}
	t.Logf("containerd: %v", unpackErr)
	if len(corruptLayers(unpackErr.Error())) != 1 {
		t.Fatalf("corruptLayers doesn't recognize %q", unpackErr)
	}
	reg := &countingRegistry{builtRegistry: builtRegistry{app.blobs}}
	r := NewImageRepair(s, reg, "linux/amd64", "n1", time.Minute, nil, nil)
	r.Wake(testImage, "failed to create containerd container: error unpacking image: "+unpackErr.Error())
	r.handleWakes(ctx)
	if len(reg.got) != 1 || reg.got[0] != app.layers[1] {
		t.Fatalf("fetched %v, want only the corrupted layer", reg.got)
	}
	stillRunnable(t, c, s, testImage, "")
}

type countingRegistry struct {
	builtRegistry
	got []string
}

func (c *countingRegistry) OpenBlob(ctx context.Context, image, d string) (io.ReadCloser, int64, error) {
	c.got = append(c.got, d)
	return c.builtRegistry.OpenBlob(ctx, image, d)
}
func (c *countingRegistry) OpenManifest(ctx context.Context, image, d string) (io.ReadCloser, int64, error) {
	c.got = append(c.got, d)
	return c.builtRegistry.OpenManifest(ctx, image, d)
}
