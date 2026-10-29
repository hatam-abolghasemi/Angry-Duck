package worker

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"angryduck/internal/blobship"
)

// The compact knownImage must judge an image exactly as the walk's own
// lists would.
func TestKnownImageMatchesCheckLayers(t *testing.T) {
	dg := func(kind string, i int) string { return fmt.Sprintf("sha256:%064x", i*1000+len(kind)) }
	rnd := rand.New(rand.NewSource(1))
	for n := 0; n < 500; n++ {
		w := blobship.MetaWalk{Metadata: map[string][]byte{dg("m", n): nil, dg("c", n): nil}}
		for i := 0; i < rnd.Intn(5); i++ {
			w.Layers = append(w.Layers, blobship.Layer{Blob: blobship.Descriptor{Digest: dg("b", n*10+i)}, ChainID: dg("chain", n*10+i)})
		}
		blobs, snaps := map[string]bool{}, map[string]bool{}
		for d := range w.Metadata {
			blobs[d] = rnd.Intn(8) != 0
		}
		for _, l := range w.Layers {
			blobs[l.Blob.Digest] = rnd.Intn(2) == 0
			snaps[l.ChainID] = rnd.Intn(3) == 0
		}
		k := newKnownImage(w)
		if k.other != nil {
			t.Fatal("plain sha256 digests should be kept compact")
		}
		got, gotOK := k.check(blobs, snaps)
		wantOK := true
		for d := range w.Metadata {
			if !blobs[d] {
				wantOK = false
			}
		}
		if gotOK != wantOK {
			t.Fatalf("case %d: ok %v, want %v", n, gotOK, wantOK)
		}
		if !wantOK {
			continue
		}
		want := blobship.CheckLayers(w.Layers, snaps, blobs)
		var gotMissing, wantMissing [][2]string
		for _, l := range got.Missing {
			gotMissing = append(gotMissing, [2]string{l.Blob.Digest, l.ChainID})
		}
		for _, l := range want.Missing {
			wantMissing = append(wantMissing, [2]string{l.Blob.Digest, l.ChainID})
		}
		if got.Known != want.Known || got.Unpacked != want.Unpacked || got.Broken() != want.Broken() || !reflect.DeepEqual(gotMissing, wantMissing) {
			t.Fatalf("case %d: got %+v, want %+v", n, got, want)
		}
	}
	// A digest that doesn't fit keeps the walk's lists, judged the same way.
	w := blobship.MetaWalk{Metadata: map[string][]byte{"sha512:abc": nil}, Layers: []blobship.Layer{{Blob: blobship.Descriptor{Digest: "sha512:def"}, ChainID: dg("chain", 1)}}}
	k := newKnownImage(w)
	if k.other == nil {
		t.Fatal("expected the walk's own lists")
	}
	h, ok := k.check(map[string]bool{"sha512:abc": true}, map[string]bool{dg("chain", 1): true})
	if !ok || !h.Unpacked {
		t.Fatalf("got %+v %v", h, ok)
	}
	var zero knownImage
	if _, ok := zero.check(nil, nil); ok {
		t.Fatal("a zero knownImage knows nothing")
	}
}
