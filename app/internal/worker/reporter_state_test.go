package worker

import (
	"testing"

	"angryduck/internal/model"
)

func TestInventoryHashUnchangedWithoutImageState(t *testing.T) {
	repos, images := []string{"r.io/a"}, []string{"r.io/a:1"}
	// Without image state the hash must not depend on it at all, so a
	// worker with retention off hashes exactly as before.
	if inventoryHash(repos, images, false, []model.ImageRef{{Digest: "sha256:x", Name: "n"}}, map[string]int64{"sha256:y": 1}) !=
		inventoryHash(repos, images, false, nil, nil) {
		t.Fatal("hash depends on image state while it is off")
	}
	base := inventoryHash(repos, images, true, nil, nil)
	if base == inventoryHash(repos, images, false, nil, nil) {
		t.Fatal("turning image state on must resend the inventory")
	}
	withUse := inventoryHash(repos, images, true, []model.ImageRef{{Digest: "sha256:x", Name: "r.io/a:1"}}, nil)
	withKeep := inventoryHash(repos, images, true, nil, map[string]int64{"sha256:x": 10})
	if base == withUse || base == withKeep || withUse == withKeep {
		t.Fatal("in-use images and retention copies must each change the hash")
	}
	a := inventoryHash(repos, images, true, nil, map[string]int64{"sha256:x": 1, "sha256:y": 2})
	b := inventoryHash(repos, images, true, nil, map[string]int64{"sha256:y": 2, "sha256:x": 1})
	if a != b {
		t.Fatal("map order must not change the hash")
	}
}

func TestReporterKicksOnImageState(t *testing.T) {
	rp := NewReporter("n1", "a:1", "", "", 0, nil)
	rp.SetImageState([]model.ImageRef{{Digest: "sha256:x", Name: "r.io/a:1"}}, nil)
	select {
	case <-rp.kick:
	default:
		t.Fatal("a state change must send a report right away")
	}
	on, inUse, _ := rp.imageState()
	if !on || len(inUse) != 1 {
		t.Fatalf("state %v %v", on, inUse)
	}
}
