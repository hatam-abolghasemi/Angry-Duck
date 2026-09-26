package blobship

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

const img = "registry.example.com/team/app:1.5.0"

func TestBuildPlan_PicksPlatformAndSkipsAttestation(t *testing.T) {
	m := NewMemStore()
	ti := m.AddTestImage(img, "base", "libs", "app")

	plan, err := BuildPlan(context.Background(), m, img, "linux/amd64")
	if err != nil {
		t.Fatal(err)
	}
	want := append([]string{ti.Index, ti.AMD64Manifest, ti.Config}, ti.Layers...)
	if got := digests(plan.Blobs); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("plan blobs:\n got %v\nwant %v", got, want)
	}
	if plan.Top.Digest != ti.Index || plan.Top.MediaType != MediaTypeOCIIndex {
		t.Fatalf("top = %+v", plan.Top)
	}
	for _, b := range plan.Blobs {
		if b.Digest == ti.Attestation || b.Digest == ti.ARM64Manifest {
			t.Fatalf("plan includes a manifest for another platform: %s", b.Digest)
		}
	}
}

func TestBuildPlan_NoMatchingPlatform(t *testing.T) {
	m := NewMemStore()
	m.AddTestImage(img, "a")
	if _, err := BuildPlan(context.Background(), m, img, "linux/s390x"); err == nil {
		t.Fatal("expected an error for a platform the index doesn't have")
	}
}

func TestBuildPlan_SingleManifestImage(t *testing.T) {
	m := NewMemStore()
	cfg := m.Put([]byte("{}"))
	layer := m.Put([]byte("layer"))
	manifest, _ := json.Marshal(manifestJSON{SchemaVersion: 2, MediaType: MediaTypeDockerManifest,
		Config: Descriptor{Digest: cfg, Size: 2},
		Layers: []Descriptor{{Digest: layer, Size: 5}, {Digest: layer, Size: 5}}}) // duplicate layer
	top := m.Put(manifest)
	m.Tag(img, MediaTypeDockerManifest, top)

	plan, err := BuildPlan(context.Background(), m, img, "linux/amd64")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(digests(plan.Blobs), ","); got != strings.Join([]string{top, cfg, layer}, ",") {
		t.Fatalf("got %s", got)
	}
}

func TestSelect_RefusesDigestsOutsideThePlan(t *testing.T) {
	m := NewMemStore()
	m.AddTestImage(img, "a")
	other := m.Put([]byte("some other image's secret layer"))
	plan, _ := BuildPlan(context.Background(), m, img, "linux/amd64")
	if _, err := Select(plan, []string{other}); err == nil {
		t.Fatal("Select must refuse a digest that isn't part of the image")
	}
}

func TestWriteArchive_OnlySelectedBlobs_ImportsIntoPartialStore(t *testing.T) {
	ctx := context.Background()
	src := NewMemStore()
	ti := src.AddTestImage(img, "base", "libs", "app")
	plan, _ := BuildPlan(ctx, src, img, "linux/amd64")

	// The receiver already has the base layer, like master1 had 3 of 15.
	dst := NewMemStore()
	dst.Put([]byte("base"))
	have, _ := dst.Digests(ctx)
	missing := plan.Missing(have)
	if len(missing) != len(plan.Blobs)-1 {
		t.Fatalf("missing %d of %d", len(missing), len(plan.Blobs))
	}

	var buf bytes.Buffer
	if err := WriteArchive(ctx, &buf, src, plan, missing); err != nil {
		t.Fatal(err)
	}
	names := tarNames(t, buf.Bytes())
	if names[0] != "oci-layout" || names[1] != "index.json" {
		t.Fatalf("archive starts with %v", names[:2])
	}
	for _, n := range names {
		if n == "blobs/sha256/"+strings.TrimPrefix(ti.Layers[0], "sha256:") {
			t.Fatal("archive carries a blob the receiver already has")
		}
	}

	if err := dst.Import(ctx, bytes.NewReader(buf.Bytes()), "linux/amd64"); err != nil {
		t.Fatalf("import: %v", err)
	}
	if _, d, err := dst.Resolve(ctx, img); err != nil || d != ti.Index {
		t.Fatalf("after import: %s %v", d, err)
	}
}

func TestWriteArchive_EmptySelectionStillNamesTheImage(t *testing.T) {
	ctx := context.Background()
	src := NewMemStore()
	src.AddTestImage(img, "a")
	plan, _ := BuildPlan(ctx, src, img, "linux/amd64")

	dst := NewMemStore()
	for _, b := range plan.Blobs { // dst has every blob but not the name
		data, _ := src.ReadBlob(ctx, b.Digest, 1<<20)
		dst.Put(data)
	}
	var buf bytes.Buffer
	if err := WriteArchive(ctx, &buf, src, plan, nil); err != nil {
		t.Fatal(err)
	}
	if err := dst.Import(ctx, &buf, "linux/amd64"); err != nil {
		t.Fatal(err)
	}
}

func TestWriteArchive_FailsWhenBlobDoesNotMatchPlan(t *testing.T) {
	ctx := context.Background()
	src := NewMemStore()
	src.AddTestImage(img, "a")
	plan, _ := BuildPlan(ctx, src, img, "linux/amd64")
	plan.Blobs[len(plan.Blobs)-1].Size++ // descriptor lies about the size
	if err := WriteArchive(ctx, io.Discard, src, plan, plan.Blobs); err == nil {
		t.Fatal("expected an error when a blob's bytes don't match its size")
	}
}

func TestParsePlatform(t *testing.T) {
	for in, ok := range map[string]bool{"linux/amd64": true, "linux/arm64/v8": true, "linux": false, "": false, "/amd64": false} {
		if _, err := ParsePlatform(in); (err == nil) != ok {
			t.Errorf("ParsePlatform(%q) err=%v", in, err)
		}
	}
}

func digests(ds []Descriptor) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = d.Digest
	}
	return out
}

func tarNames(t *testing.T, b []byte) []string {
	t.Helper()
	var names []string
	tr := tar.NewReader(bytes.NewReader(b))
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return names
		}
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, h.Name)
	}
}

func TestChainIDs_MatchesContainerd(t *testing.T) {
	// diff_ids and chainIDs of angry-duck-worker:1.5.0 as seen on a stg node.
	diffs := []string{
		"sha256:50abe06dfc0957e14cb8332ed242e8b884ef2563a9eb202e5172f243f62792f7",
		"sha256:621c35e751a51a9a9dc3e80aa0b7fe8be2a93402ea6ccd307d30852cd7776cda",
		"sha256:c8b007d0206e4b10ed4d3b3d99dfeab47c2648e82011989fd78a5731baf33fc3",
	}
	want := []string{
		"sha256:50abe06dfc0957e14cb8332ed242e8b884ef2563a9eb202e5172f243f62792f7",
		"sha256:3674d77cc37cf3092e09a587f9c5fa99c260138eff02ded551ca63711f9f92c3",
		"sha256:ec801d9a2009885e6da541b213d37af471f769957a7fc0ab80636c48bbae3bd1",
	}
	if got := ChainIDs(diffs); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("got %v", got)
	}
}

func TestDecide(t *testing.T) {
	ctx := context.Background()
	src := NewMemStore()
	ti := src.AddTestImage(img, "l0", "l1", "l2", "l3")
	plan, _ := BuildPlan(ctx, src, img, "linux/amd64")
	mark := func(p Plan, noBlob ...int) Plan {
		p.Layers = append([]Layer(nil), p.Layers...)
		for i := range p.Layers {
			p.Layers[i].BlobPresent, p.Layers[i].SnapshotPresent = true, true
		}
		for _, i := range noBlob {
			p.Layers[i].BlobPresent = false
		}
		return p
	}
	set := func(ds ...string) map[string]bool {
		m := map[string]bool{}
		for _, d := range ds {
			m[d] = true
		}
		return m
	}

	t.Run("layers already present need nothing, not even their blob", func(t *testing.T) {
		d, err := Decide(mark(plan), set(), set(ti.Chains[0], ti.Chains[1]))
		if err != nil || d.Present != 2 || len(d.Snapshots) != 0 || len(d.Blobs) != 5 { // 3 metadata + l2 + l3
			t.Fatalf("%+v %v", d, err)
		}
	})
	t.Run("a snapshot-only layer pulls every missing layer below it into snapshots", func(t *testing.T) {
		d, err := Decide(mark(plan, 2), set(), set(ti.Chains[0]))
		if err != nil || d.Present != 1 || len(d.Snapshots) != 2 || d.Snapshots[0].ChainID != ti.Chains[1] || len(d.Blobs) != 4 { // metadata + l3
			t.Fatalf("%+v %v", d, err)
		}
	})
	t.Run("a blob the receiver has counts even if the source lacks it", func(t *testing.T) {
		d, err := Decide(mark(plan, 3), set(ti.Layers[3]), set())
		if err != nil || len(d.Snapshots) != 0 || len(d.Blobs) != 6 { // metadata + l0..l2
			t.Fatalf("%+v %v", d, err)
		}
	})
	t.Run("neither blob nor snapshot on the source", func(t *testing.T) {
		p := mark(plan, 1)
		p.Layers[1].SnapshotPresent = false
		if _, err := Decide(p, set(), set()); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("old source without layer info", func(t *testing.T) {
		p := plan
		p.Layers = nil
		d, err := Decide(p, set(ti.Layers[0]), set())
		if err != nil || len(d.Blobs) != len(plan.Blobs)-1 {
			t.Fatalf("%+v %v", d, err)
		}
	})
}
