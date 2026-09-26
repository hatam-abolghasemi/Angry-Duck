package layerindex

import (
	"angryduck/internal/model"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func TestIDSetMatchesMap(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	var s idSet
	ref := map[uint32]bool{}
	for i := 0; i < 200000; i++ {
		id := uint32(r.Intn(5000))
		switch r.Intn(3) {
		case 0, 1:
			if s.add(id) == ref[id] {
				t.Fatalf("add(%d) reported wrongly", id)
			}
			ref[id] = true
		case 2:
			if s.del(id) != ref[id] {
				t.Fatalf("del(%d) reported wrongly", id)
			}
			delete(ref, id)
		}
	}
	if s.len() != len(ref) {
		t.Fatalf("len %d, want %d", s.len(), len(ref))
	}
	var got []uint32
	s.each(func(id uint32) { got = append(got, id) })
	if len(got) != len(ref) || !sort.SliceIsSorted(got, func(i, j int) bool { return got[i] < got[j] }) {
		t.Fatalf("each returned %d ids", len(got))
	}
	for _, id := range got {
		if !ref[id] || !s.has(id) {
			t.Fatalf("id %d", id)
		}
	}
	if s.has(1 << 30) {
		t.Fatal("has beyond range")
	}
}

func TestIDsReusedAfterRelease(t *testing.T) {
	x := New()
	d := func(i int) string { return fmt.Sprintf("sha256:%064x", i) }
	var all []string
	for i := 0; i < 100; i++ {
		all = append(all, d(i))
	}
	x.Apply("a", &model.LayerSync{Epoch: "e", Seq: 1, Full: true, AddBlobs: all})
	x.Forget("a")
	if x.Unique() != 0 {
		t.Fatalf("unique %d after forget", x.Unique())
	}
	x.Apply("b", &model.LayerSync{Epoch: "e", Seq: 1, Full: true, AddBlobs: all[:10]})
	if len(x.keys) != 100 {
		t.Fatalf("ids not reused: %d keys allocated", len(x.keys))
	}
	if h := x.Holders(all[3], []string{"a", "b"}); len(h) != 1 || h[0] != "b" {
		t.Fatalf("holders %v", h)
	}
}
