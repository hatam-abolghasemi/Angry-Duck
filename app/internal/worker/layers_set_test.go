package worker

import (
	"reflect"
	"strings"
	"testing"
)

func TestDigestSetDiffAndStrings(t *testing.T) {
	d := func(c string) string { return "sha256:" + strings.Repeat(c, 64) }
	old := newDigestSet(map[string]bool{d("1"): true, d("2"): true, d("a"): true, "sha512:x": true, "sha256:" + strings.Repeat("B", 64): true}, nil)
	cur := newDigestSet(map[string]bool{d("2"): true, d("3"): true, d("a"): true, "sha512:y": true, "sha256:" + strings.Repeat("B", 64): true, "other": false}, func(k string) bool { return k != "other" })
	add, del := diff(old, cur)
	if want := []string{d("3"), "sha512:y"}; !reflect.DeepEqual(add, want) {
		t.Fatalf("add = %v, want %v", add, want)
	}
	if want := []string{d("1"), "sha512:x"}; !reflect.DeepEqual(del, want) {
		t.Fatalf("del = %v, want %v", del, want)
	}
	want := []string{d("2"), d("3"), "sha256:" + strings.Repeat("B", 64), d("a"), "sha512:y"} // sorted as strings
	if got := cur.strings(); !reflect.DeepEqual(got, want) {
		t.Fatalf("strings = %v, want %v", got, want)
	}
	if add, del := diff(digestSet{}, cur); len(add) != 5 || len(del) != 0 {
		t.Fatalf("from nothing: add %d del %d", len(add), len(del))
	}
	again := newDigestSet(map[string]bool{d("2"): true, d("3"): true, d("a"): true, "sha512:y": true, "sha256:" + strings.Repeat("B", 64): true}, nil)
	if same := again.sameOr(cur); &same.sha[0] != &cur.sha[0] {
		t.Fatal("an unchanged scan should keep the previous set")
	}
	if same := old.sameOr(cur); &same.sha[0] != &old.sha[0] {
		t.Fatal("a changed scan should be the new set")
	}
}
