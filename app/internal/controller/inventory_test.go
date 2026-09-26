package controller

import (
	"strings"
	"testing"
	"time"
	"unsafe"

	"angryduck/internal/model"
)

func invReport(node, hash string, omitted bool, images ...string) model.WorkerReport {
	r := model.WorkerReport{NodeID: node, Address: "x", Timestamp: time.Now(), InventoryHash: hash, InventoryOmitted: omitted}
	if !omitted {
		r.Images = images
		for _, img := range images {
			if i := strings.LastIndex(img, ":"); i > 0 {
				r.Repos = append(r.Repos, img[:i])
			}
		}
	}
	return r
}

func TestInventoryHashProtocol(t *testing.T) {
	reg := NewRegistry(time.Minute, time.Minute)
	get := func(n string) *workerEntry { reg.mu.RLock(); defer reg.mu.RUnlock(); return reg.workers[n] }

	// Old worker: no hash, full lists, nothing echoed.
	if ack := reg.Update(invReport("old", "", false, "r.io/a:1")); ack.InventoryHash != "" || !get("old").HasImage("r.io/a:1") {
		t.Fatalf("old worker: ack %+v", ack)
	}
	// New worker: stored and echoed.
	if ack := reg.Update(invReport("n1", "h1", false, "r.io/b:1", "r.io/a:1")); ack.InventoryHash != "h1" {
		t.Fatalf("full report not echoed: %+v", ack)
	}
	// Omitted with the held hash: kept and echoed.
	if ack := reg.Update(invReport("n1", "h1", true)); ack.InventoryHash != "h1" || !get("n1").HasImage("r.io/b:1") || !get("n1").HasRepo("r.io/b") {
		t.Fatalf("omitted report lost the inventory: %+v", ack)
	}
	// Omitted with another hash: kept, not echoed, so the worker resends.
	if ack := reg.Update(invReport("n1", "h2", true)); ack.InventoryHash != "" || !get("n1").HasImage("r.io/b:1") {
		t.Fatalf("mismatched omitted report: %+v", ack)
	}
	// A real empty inventory clears the lists.
	if ack := reg.Update(invReport("n1", "h3", false)); ack.InventoryHash != "h3" || get("n1").HasImage("r.io/b:1") || len(get("n1").Images) != 0 {
		t.Fatalf("empty inventory not applied: %+v %v", ack, get("n1").Images)
	}
	// A restarted controller holds nothing: an omitted report isn't echoed.
	fresh := NewRegistry(time.Minute, time.Minute)
	if ack := fresh.Update(invReport("n1", "h3", true)); ack.InventoryHash != "" {
		t.Fatalf("restarted controller echoed an inventory it doesn't hold: %+v", ack)
	}
}

func TestInventoryStringsInternedAndReleased(t *testing.T) {
	reg := NewRegistry(time.Minute, time.Minute)
	img := func() string { return string([]byte("registry.example.com/team/app:1.0.0")) } // distinct copies
	reg.Update(invReport("a", "1", false, img()))
	reg.Update(invReport("b", "1", false, img()))
	wa, wb := reg.workers["a"], reg.workers["b"]
	if unsafe.StringData(wa.Images[0]) != unsafe.StringData(wb.Images[0]) {
		t.Fatal("identical image names on two nodes are not shared")
	}
	if n := reg.names.m["registry.example.com/team/app:1.0.0"].n; n != 2 {
		t.Fatalf("refcount %d, want 2", n)
	}
	before := wa.Images
	reg.Update(invReport("a", "1", false, img())) // unchanged: same slice, same refs
	if &reg.workers["a"].Images[0] != &before[0] {
		t.Fatal("unchanged inventory was reallocated")
	}
	reg.Update(invReport("a", "2", false, "registry.example.com/team/app:2.0.0"))
	reg.Update(invReport("b", "2", false, "registry.example.com/team/app:2.0.0"))
	if _, ok := reg.names.m["registry.example.com/team/app:1.0.0"]; ok {
		t.Fatal("name no node holds any more was not released")
	}
	// Duplicates and empties are cleaned; lookups work on sorted lists.
	reg.Update(invReport("c", "3", false, "z.io/x:1", "", "a.io/y:1", "z.io/x:1"))
	if got := reg.workers["c"].Images; len(got) != 2 || got[0] != "a.io/y:1" || !reg.workers["c"].HasImage("z.io/x:1") {
		t.Fatalf("list not cleaned: %v", got)
	}
}
