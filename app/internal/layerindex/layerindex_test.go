package layerindex

import (
	"fmt"
	"testing"

	"angryduck/internal/model"
)

func d(n int) string { return fmt.Sprintf("sha256:%064x", n) }

func full(epoch string, seq uint64, blobs, snaps []string) *model.LayerSync {
	return &model.LayerSync{Epoch: epoch, Seq: seq, Full: true, AddBlobs: blobs, AddSnaps: snaps}
}

func TestMissingCountsBlobOrSnapshot(t *testing.T) {
	x := New()
	// node a keeps blobs (discard off), node b only snapshots (discard on).
	x.Apply("a", full("e", 1, []string{d(1), d(2)}, nil))
	x.Apply("b", full("e", 1, nil, []string{d(101), d(102)}))
	layers := []Layer{
		{Digest: d(1), ChainID: d(101), Size: 100},
		{Digest: d(2), ChainID: d(102), Size: 200},
		{Digest: d(3), ChainID: d(103), Size: 40},
	}
	for _, node := range []string{"a", "b"} {
		m, known := x.Missing(node, layers)
		if !known || m != 40 {
			t.Fatalf("%s: missing=%d known=%v, want 40 true", node, m, known)
		}
	}
	if _, known := x.Missing("c", layers); known {
		t.Fatal("node without inventory must be unknown, not empty")
	}
}

func TestDeltaAppliesOnlyOnMatchingBase(t *testing.T) {
	x := New()
	x.Apply("a", full("e1", 1, []string{d(1)}, nil))
	if seq, resync := x.Apply("a", &model.LayerSync{Epoch: "e1", Seq: 2, Base: 1, AddBlobs: []string{d(2)}, DelBlobs: []string{d(1)}}); resync || seq != 2 {
		t.Fatalf("delta on matching base: seq=%d resync=%v", seq, resync)
	}
	if b, _, _ := x.Counts("a"); b != 1 {
		t.Fatalf("blobs=%d, want 1", b)
	}
	// Wrong base (a lost ack), wrong epoch (worker restart), unknown node
	// (controller restart): all ask for a full sync and change nothing.
	cases := []*model.LayerSync{
		{Epoch: "e1", Seq: 5, Base: 4, AddBlobs: []string{d(9)}},
		{Epoch: "e2", Seq: 3, Base: 2, AddBlobs: []string{d(9)}},
	}
	for _, c := range cases {
		if _, resync := x.Apply("a", c); !resync {
			t.Fatalf("delta %+v should need a resync", c)
		}
	}
	if _, resync := x.Apply("zz", &model.LayerSync{Epoch: "e", Seq: 2, Base: 1}); !resync {
		t.Fatal("delta for unknown node should need a resync")
	}
	if m, _ := x.Missing("a", []Layer{{Digest: d(9), Size: 1}}); m != 1 {
		t.Fatal("rejected delta must not be applied")
	}
}

func TestInterningRefcountsAndFrees(t *testing.T) {
	x := New()
	shared := []string{d(1), d(2), d(3)}
	for i := 0; i < 50; i++ {
		x.Apply(fmt.Sprint("n", i), full("e", 1, shared, shared[:1]))
	}
	if u := x.Unique(); u != 3 {
		t.Fatalf("unique=%d, want 3 (digests shared by 50 nodes are stored once)", u)
	}
	// Duplicate adds must not take extra references.
	x.Apply("n0", &model.LayerSync{Epoch: "e", Seq: 2, Base: 1, AddBlobs: shared})
	for i := 0; i < 50; i++ {
		x.Forget(fmt.Sprint("n", i))
	}
	if u := x.Unique(); u != 0 {
		t.Fatalf("unique=%d after forgetting every node, want 0", u)
	}
	// Freed IDs are reused, and a full sync replaces rather than merges.
	x.Apply("a", full("e", 1, []string{d(7)}, nil))
	x.Apply("a", full("e", 2, []string{d(8)}, nil))
	if u := x.Unique(); u != 1 {
		t.Fatalf("unique=%d, want 1", u)
	}
	if m, _ := x.Missing("a", []Layer{{Digest: d(7), Size: 5}}); m != 5 {
		t.Fatal("full sync must drop digests it no longer lists")
	}
	if len(x.keys) > 3 {
		t.Fatalf("id table grew to %d, freed ids not reused", len(x.keys))
	}
}

func TestMalformedDigestsIgnored(t *testing.T) {
	x := New()
	x.Apply("a", full("e", 1, []string{"sha256:nothex", "md5:abc", "", d(1)}, []string{"angryduck-view-1"}))
	if b, s, _ := x.Counts("a"); b != 1 || s != 0 {
		t.Fatalf("blobs=%d snaps=%d, want 1 0", b, s)
	}
}
