package worker

import (
	"context"
	"net/http"
	"testing"
	"time"

	"angryduck/internal/blobship"
	"angryduck/internal/model"
)

// breakImage leaves name registered but impossible to unpack, the way a
// failed import before 1.8.12 did: its top layer isn't unpacked, and that
// layer's blob is gone too.
func breakImage(store *blobship.MemStore, ti blobship.TestImage) {
	top := len(ti.Layers) - 1
	store.DeleteSnapshot(ti.Chains[top])
	store.Delete(ti.Layers[top])
}

func TestRescue_ReceivesAgainAnImageThatCantBeUnpacked(t *testing.T) {
	srcStore := blobship.NewMemStore()
	srcStore.AddTestImage(testImage, "base", "app")
	_, src := peer(t, "worker14", srcStore)

	dstStore := blobship.NewMemStore()
	ti := dstStore.AddTestImage(testImage, "base", "app")
	breakImage(dstStore, ti)
	_, dstSrv := peer(t, "worker21", dstStore)

	code, res := order(t, dstSrv, model.RescueOrder{Image: testImage, Sources: []model.RescueSource{{NodeID: "worker14", Address: addr(src)}}}, testToken)
	if code != http.StatusOK || !res.OK {
		t.Fatalf("rescue: %d %+v", code, res)
	}
	if res.Source != "worker14" || len(dstStore.Imports) != 1 {
		t.Fatalf("answered %+v with %d import(s): a broken image was taken as present", res, len(dstStore.Imports))
	}
	h, err := blobship.Check(context.Background(), dstStore, testImage, "linux/amd64")
	if err != nil || !h.Unpacked {
		t.Fatalf("after the rescue: %+v %v, want unpacked", h, err)
	}
}

func TestImageRepair_RemovesABrokenImageOnTheSecondLook(t *testing.T) {
	store := blobship.NewMemStore()
	ti := store.AddTestImage(testImage, "base", "app")
	store.Tag("registry.example.com/team/app@"+ti.Index, blobship.MediaTypeOCIIndex, ti.Index) // a second name for it
	healthy := store.AddTestImage("registry.example.com/team/other:1", "base", "other")
	breakImage(store, ti)
	store.Delete(healthy.Layers[1]) // unpacked, blob discarded: fine

	changed := 0
	r := NewImageRepair(store, "linux/amd64", "n1", time.Minute, nil, func() { changed++ })
	now := time.Now()
	r.Pass(context.Background(), now)
	if !store.HasImage(testImage) {
		t.Fatal("removed on the first look")
	}
	r.Pass(context.Background(), now.Add(30*time.Second))
	if !store.HasImage(testImage) {
		t.Fatal("removed before a whole interval passed")
	}
	r.Pass(context.Background(), now.Add(61*time.Second))
	if store.HasImage(testImage) || store.HasImage("registry.example.com/team/app@"+ti.Index) {
		t.Fatalf("broken image still registered, deleted %v", store.Deleted)
	}
	if !store.HasImage("registry.example.com/team/other:1") {
		t.Fatal("removed a healthy image")
	}
	if changed != 1 {
		t.Fatalf("onChange ran %d times, want 1", changed)
	}
}

func TestImageRepair_LeavesAnImageThatRecoveredOrIsBusy(t *testing.T) {
	store := blobship.NewMemStore()
	ti := store.AddTestImage(testImage, "base", "app")
	breakImage(store, ti)
	busy := true
	r := NewImageRepair(store, "linux/amd64", "n1", time.Minute, func([]string) bool { return busy }, nil)
	now := time.Now()
	r.Pass(context.Background(), now)
	r.Pass(context.Background(), now.Add(2*time.Minute))
	if !store.HasImage(testImage) {
		t.Fatal("removed an image being pulled")
	}
	// Its blob comes back (the pull finished): no longer broken.
	busy = false
	store.Put([]byte("app"))
	r.Pass(context.Background(), now.Add(4*time.Minute))
	r.Pass(context.Background(), now.Add(6*time.Minute))
	if !store.HasImage(testImage) {
		t.Fatal("removed an image that can be unpacked again")
	}
	if len(r.suspects) != 0 {
		t.Fatalf("suspects left: %v", r.suspects)
	}
}

func TestImageRepair_IgnoresAngryDucksOwnNames(t *testing.T) {
	store := blobship.NewMemStore()
	ti := store.AddTestImage(baseImagePrefix+"x:layers-0-1", "base", "app")
	breakImage(store, ti)
	r := NewImageRepair(store, "linux/amd64", "n1", time.Minute, nil, nil)
	now := time.Now()
	r.Pass(context.Background(), now)
	r.Pass(context.Background(), now.Add(2*time.Minute))
	if len(store.Deleted) != 0 {
		t.Fatalf("deleted %v", store.Deleted)
	}
}
