package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"angryduck/internal/blobship"
	"angryduck/internal/model"
)

const (
	testToken = "0123456789abcdef0123456789abcdef"
	testImage = "registry.example.com/team/app:1.5.0"
)

// peer is one worker's rescue endpoints over a MemStore.
func peer(t *testing.T, node string, store *blobship.MemStore) (*Rescue, *httptest.Server) {
	t.Helper()
	rs := NewRescue(store, nil, testToken, node, "linux/amd64", 2)
	mux := http.NewServeMux()
	rs.Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return rs, srv
}

func order(t *testing.T, srv *httptest.Server, o model.RescueOrder, token string) (int, model.RescueResult) {
	t.Helper()
	body, _ := json.Marshal(o)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/rescue", bytes.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var res model.RescueResult
	_ = json.NewDecoder(resp.Body).Decode(&res)
	return resp.StatusCode, res
}

func addr(srv *httptest.Server) string { return strings.TrimPrefix(srv.URL, "http://") }

func TestRescue_ShipsOnlyMissingBlobs(t *testing.T) {
	srcStore := blobship.NewMemStore()
	ti := srcStore.AddTestImage(testImage, "base", "libs", "app")
	_, src := peer(t, "worker14", srcStore)

	dstStore := blobship.NewMemStore()
	dstStore.Put([]byte("base")) // shared with an older version
	dstStore.Put([]byte("libs"))
	kicked := false
	dst, dstSrv := peer(t, "master1", dstStore)
	dst.OnSuccess(func() { kicked = true })

	code, res := order(t, dstSrv, model.RescueOrder{Image: testImage, Sources: []model.RescueSource{{NodeID: "worker14", Address: addr(src)}}}, testToken)
	if code != http.StatusOK || !res.OK {
		t.Fatalf("rescue failed: %d %+v", code, res)
	}
	// index + amd64 manifest + config + "app" layer
	if res.Blobs != 4 || res.Source != "worker14" {
		t.Fatalf("result = %+v", res)
	}
	shipped := dstStore.Imports[0]
	for _, d := range shipped {
		if d == ti.Layers[0] || d == ti.Layers[1] {
			t.Fatalf("shipped %s, which the receiver already had", d)
		}
	}
	if _, d, err := dstStore.Resolve(context.Background(), testImage); err != nil || d != ti.Index {
		t.Fatalf("image not registered: %s %v", d, err)
	}
	if !kicked {
		t.Fatal("OnSuccess not called")
	}
}

func TestRescue_ShipsSnapshotsWhenNoBlobExists(t *testing.T) {
	// The config-guard case: the source pulled with
	// discard_unpacked_layers=true, so it has the image only as snapshots.
	srcStore := blobship.NewMemStore()
	ti := srcStore.AddTestImage(testImage, "ubuntu base", "tools", "script")
	for _, l := range ti.Layers {
		srcStore.Delete(l)
	}
	_, src := peer(t, "worker14", srcStore)

	// The receiver already has the base layer's snapshot (another image
	// built on the same base), like worker20 did.
	dstStore := blobship.NewMemStore()
	base := dstStore.AddTestImage("registry.example.com/other:1", "ubuntu base")
	if base.Chains[0] != ti.Chains[0] {
		t.Fatal("test setup: base chainIDs differ")
	}
	_, dstSrv := peer(t, "worker20", dstStore)

	code, res := order(t, dstSrv, model.RescueOrder{Image: testImage, Sources: []model.RescueSource{{NodeID: "worker14", Address: addr(src)}}}, testToken)
	if code != http.StatusOK || !res.OK {
		t.Fatalf("got %d %+v", code, res)
	}
	if res.Snapshots != 2 || res.Blobs != 3 { // tools+script as snapshots; index, manifest, config as blobs
		t.Fatalf("result = %+v", res)
	}
	if got := strings.Join(dstStore.Applied, ","); got != ti.Chains[1]+","+ti.Chains[2] {
		t.Fatalf("applied %s, want layers 1 and 2 in order", got)
	}
	if len(dstStore.Pinned) != 0 {
		t.Fatalf("snapshots left pinned after the import: %v", dstStore.Pinned)
	}
	if _, d, err := dstStore.Resolve(context.Background(), testImage); err != nil || d != ti.Index {
		t.Fatalf("image not registered: %s %v", d, err)
	}
}

func TestRescue_BlobsAboveTheHighestSnapshotStillTravelAsBlobs(t *testing.T) {
	// Only the bottom layer's blob is gone on the source: that layer comes
	// as a snapshot, the upper ones as (compressed, verified) blobs.
	srcStore := blobship.NewMemStore()
	ti := srcStore.AddTestImage(testImage, "base", "mid", "app")
	srcStore.Delete(ti.Layers[0])
	_, src := peer(t, "worker7", srcStore)

	dstStore := blobship.NewMemStore()
	_, dstSrv := peer(t, "worker20", dstStore)
	code, res := order(t, dstSrv, model.RescueOrder{Image: testImage, Sources: []model.RescueSource{{NodeID: "worker7", Address: addr(src)}}}, testToken)
	if code != http.StatusOK || res.Snapshots != 1 || res.Blobs != 5 {
		t.Fatalf("got %d %+v", code, res)
	}
}

func TestRescue_FallsBackWhenSourceHasNeitherBlobNorSnapshot(t *testing.T) {
	brokenStore := blobship.NewMemStore()
	ti := brokenStore.AddTestImage(testImage, "base", "app")
	brokenStore.Delete(ti.Layers[1])
	brokenStore.DeleteSnapshot(ti.Chains[1])
	_, broken := peer(t, "worker3", brokenStore)

	goodStore := blobship.NewMemStore()
	goodStore.AddTestImage(testImage, "base", "app")
	_, good := peer(t, "worker14", goodStore)

	_, dstSrv := peer(t, "master1", blobship.NewMemStore())
	code, res := order(t, dstSrv, model.RescueOrder{Image: testImage, Sources: []model.RescueSource{
		{NodeID: "worker3", Address: addr(broken)},
		{NodeID: "worker14", Address: addr(good)},
	}}, testToken)
	if code != http.StatusOK || res.Source != "worker14" {
		t.Fatalf("expected fallback to worker14, got %d %+v", code, res)
	}
}

func TestRescue_FailedSnapshotLeavesNothingPinned(t *testing.T) {
	srcStore := blobship.NewMemStore()
	ti := srcStore.AddTestImage(testImage, "base", "app")
	for _, l := range ti.Layers {
		srcStore.Delete(l)
	}
	_, src := peer(t, "worker14", srcStore)

	dstStore := blobship.NewMemStore()
	dstStore.FailApply = true
	_, dstSrv := peer(t, "worker20", dstStore)
	code, _ := order(t, dstSrv, model.RescueOrder{Image: testImage, Sources: []model.RescueSource{{NodeID: "worker14", Address: addr(src)}}}, testToken)
	if code != http.StatusBadGateway || len(dstStore.Pinned) != 0 {
		t.Fatalf("status %d, pinned %v", code, dstStore.Pinned)
	}
}

func TestSnapshotExport_OnlyLayersOfTheNamedImage(t *testing.T) {
	store := blobship.NewMemStore()
	store.AddTestImage(testImage, "a")
	other := store.AddTestImage("registry.example.com/secret:1", "secret layer")
	_, srv := peer(t, "worker14", store)

	body, _ := json.Marshal(model.SnapshotExportRequest{Image: testImage, Platform: "linux/amd64", ChainID: other.Chains[0]})
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/snapshots/export", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", resp.StatusCode)
	}
}

func TestRescue_SourceLackingABlobTheReceiverHasStillServes(t *testing.T) {
	// The stg case: the source reused a base-layer snapshot and never
	// fetched that layer's blob, but the receiver already has the blob.
	srcStore := blobship.NewMemStore()
	ti := srcStore.AddTestImage(testImage, "base", "app")
	srcStore.Delete(ti.Layers[0])
	_, src := peer(t, "worker35", srcStore)

	dstStore := blobship.NewMemStore()
	dstStore.Put([]byte("base"))
	_, dstSrv := peer(t, "master1", dstStore)

	code, res := order(t, dstSrv, model.RescueOrder{Image: testImage, Sources: []model.RescueSource{{NodeID: "worker35", Address: addr(src)}}}, testToken)
	if code != http.StatusOK || !res.OK || res.Source != "worker35" {
		t.Fatalf("got %d %+v", code, res)
	}
}

func TestExport_RefusesBlobsThisNodeLacksBeforeStreaming(t *testing.T) {
	store := blobship.NewMemStore()
	ti := store.AddTestImage(testImage, "base", "app")
	store.Delete(ti.Layers[0])
	_, srv := peer(t, "worker35", store)

	body, _ := json.Marshal(model.BlobExportRequest{Image: testImage, Platform: "linux/amd64", Digests: []string{ti.Layers[0]}})
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/blobs/export", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status %d, want 409", resp.StatusCode)
	}
}

func TestRescue_AllSourcesFail(t *testing.T) {
	_, empty := peer(t, "worker3", blobship.NewMemStore())
	_, dstSrv := peer(t, "master1", blobship.NewMemStore())
	code, res := order(t, dstSrv, model.RescueOrder{Image: testImage, Sources: []model.RescueSource{{NodeID: "worker3", Address: addr(empty)}}}, testToken)
	if code != http.StatusBadGateway || res.OK || !strings.Contains(res.Error, "worker3") {
		t.Fatalf("got %d %+v", code, res)
	}
}

func TestRescue_AlreadyPresent(t *testing.T) {
	store := blobship.NewMemStore()
	store.AddTestImage(testImage, "a")
	_, dstSrv := peer(t, "master1", store)
	code, res := order(t, dstSrv, model.RescueOrder{Image: testImage}, testToken)
	if code != http.StatusOK || !res.OK || res.Blobs != 0 {
		t.Fatalf("got %d %+v", code, res)
	}
}

func TestRescue_EndpointsRequireToken(t *testing.T) {
	store := blobship.NewMemStore()
	store.AddTestImage(testImage, "secret layer")
	_, srv := peer(t, "worker14", store)
	for _, path := range []string{"/rescue", "/blobs/plan", "/blobs/export", "/snapshots/export"} {
		for _, tok := range []string{"", "wrong-token-wrong-token-wrong-token"} {
			req, _ := http.NewRequest(http.MethodPost, srv.URL+path, strings.NewReader(`{"image":"`+testImage+`","platform":"linux/amd64"}`))
			if tok != "" {
				req.Header.Set("Authorization", "Bearer "+tok)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Errorf("%s with token %q: status %d, want 401", path, tok, resp.StatusCode)
			}
		}
	}
}

func TestExport_RefusesDigestsOfOtherImages(t *testing.T) {
	store := blobship.NewMemStore()
	store.AddTestImage(testImage, "a")
	other := store.Put([]byte("another image's layer"))
	_, srv := peer(t, "worker14", store)

	body, _ := json.Marshal(model.BlobExportRequest{Image: testImage, Platform: "linux/amd64", Digests: []string{other}})
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/blobs/export", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", resp.StatusCode)
	}
}

func TestParseCtrImageRow(t *testing.T) {
	// Real `ctr -n k8s.io images ls` output from a stg node.
	out := `REF                                                                                                                                  TYPE                                                      DIGEST                                                                  SIZE      PLATFORMS                                                                              LABELS
registry.example.com/devops/generic/angry-duck-worker:1.4.10                                                                        application/vnd.oci.image.index.v1+json                   sha256:528765b3403fae29b50bd0428339d79d280777decaa1f4afc8455284120532db 3.8 MiB   linux/amd64                                                                            io.cri-containerd.image=managed
registry.example.com/devops/generic/angry-duck-worker:1.5.0                                                                         application/vnd.oci.image.index.v1+json                   sha256:4e24924f85f679ea6747cd558dda60bd66bf7acba4d2b54eb053de9cb5edd5c6 3.8 MiB   linux/amd64                                                                            io.cri-containerd.image=managed
`
	mt, d, ok := parseCtrImageRow(out, "registry.example.com/devops/generic/angry-duck-worker:1.5.0")
	if !ok || mt != blobship.MediaTypeOCIIndex || d != "sha256:4e24924f85f679ea6747cd558dda60bd66bf7acba4d2b54eb053de9cb5edd5c6" {
		t.Fatalf("got %q %q %v", mt, d, ok)
	}
	if _, _, ok := parseCtrImageRow(out, "registry.example.com/devops/generic/angry-duck-worker:1.5"); ok {
		t.Fatal("prefix of a ref must not match")
	}
}
