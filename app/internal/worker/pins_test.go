package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"angryduck/internal/blobship"
	"angryduck/internal/model"
)

func TestRescue_RetryAfterFailedImportDoesNotReshipSnapshots(t *testing.T) {
	srcStore := blobship.NewMemStore()
	ti := srcStore.AddTestImage(testImage, "base", "tools", "app")
	for _, l := range ti.Layers {
		srcStore.Delete(l) // snapshots only, like a discard node
	}
	_, src := peer(t, "worker14", srcStore)

	dstStore := blobship.NewMemStore()
	pins := NewPins(dstStore, "worker20", time.Hour, filepath.Join(t.TempDir(), "pins.json"))
	rs := NewRescue(dstStore, pins, testToken, "worker20", "linux/amd64", 1)
	mux := http.NewServeMux()
	rs.Register(mux)
	dst := httptest.NewServer(mux)
	defer dst.Close()
	o := model.RescueOrder{Image: testImage, Sources: []model.RescueSource{{NodeID: "worker14", Address: addr(src)}}}

	// 1st attempt: all 3 snapshots arrive, then the import fails.
	dstStore.FailImport = true
	if code, _ := order(t, dst, o, testToken); code != http.StatusBadGateway {
		t.Fatalf("status %d", code)
	}
	if len(dstStore.Applied) != 3 || !pins.Has(ti.Chains[2]) || !dstStore.Pinned[ti.Chains[2]] {
		t.Fatalf("after failed import: applied=%v pinned=%v", dstStore.Applied, dstStore.Pinned)
	}

	// 2nd attempt: nothing is shipped again, only the import is redone.
	dstStore.FailImport = false
	code, res := order(t, dst, o, testToken)
	if code != http.StatusOK || res.Snapshots != 0 {
		t.Fatalf("retry: %d %+v", code, res)
	}
	if len(dstStore.Applied) != 3 {
		t.Fatalf("snapshots re-shipped: %v", dstStore.Applied)
	}
	if len(dstStore.Pinned) != 0 || pins.Has(ti.Chains[0]) {
		t.Fatalf("pins left after success: %v", dstStore.Pinned)
	}
}

func TestPins_ExpireAndSurviveRestart(t *testing.T) {
	ctx := context.Background()
	store := blobship.NewMemStore()
	path := filepath.Join(t.TempDir(), "state", "pins.json")

	p := NewPins(store, "n", time.Hour, path)
	p.Add("sha256:keep")
	p.Add("sha256:old")
	p.mu.Lock()
	p.pins["sha256:old"] = time.Now().Add(-time.Minute)
	p.saveLocked()
	p.mu.Unlock()

	// A new worker process reads the same file.
	p2 := NewPins(store, "n", time.Hour, path)
	if !p2.Has("sha256:keep") || !p2.Has("sha256:old") {
		t.Fatal("pins not reloaded from the state file")
	}
	p2.Sweep(ctx)
	if p2.Has("sha256:old") || !p2.Has("sha256:keep") {
		t.Fatal("sweep released the wrong pins")
	}
	if p3 := NewPins(store, "n", time.Hour, path); p3.Has("sha256:old") {
		t.Fatal("released pin still in the state file")
	}
}

func TestCleanupLeftovers_RemovesOnlyOurTempSnapshots(t *testing.T) {
	store := blobship.NewMemStore()
	store.AddRawSnapshot("angryduck-rescue-abc123")
	store.AddRawSnapshot("angryduck-view-def456")
	store.AddRawSnapshot("sha256:real-layer")
	store.AddRawSnapshot("k8s-container-id")

	CleanupLeftovers(context.Background(), store, NewPins(store, "n", time.Hour, ""), "n")
	snaps, _ := store.Snapshots(context.Background())
	if snaps["angryduck-rescue-abc123"] || snaps["angryduck-view-def456"] {
		t.Fatalf("leftovers not removed: %v", snaps)
	}
	if !snaps["sha256:real-layer"] || !snaps["k8s-container-id"] {
		t.Fatalf("removed something that wasn't ours: %v", snaps)
	}
}
