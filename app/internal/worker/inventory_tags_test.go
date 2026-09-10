package worker

import "testing"

func TestInventoryTagsMapsTagToManifestDigestNotConfigID(t *testing.T) {
	rt := newFakeRuntime()
	rt.local["reg/app:v1"] = true
	rt.digests["reg/app:v1"] = "sha256:config"       // tag-form alias -> canonical id
	rt.digests["reg/app@"+testDigest] = "sha256:config" // digest-pinned alias -> same id
	rt.digests["sha256:config"] = "sha256:config"       // bare id alias -> itself
	inv := NewInventory(rt)
	_, _, _ = inv.Get(0)

	tags := inv.Tags()
	got, ok := tags["reg/app:v1"]
	if !ok {
		t.Fatalf("Tags() = %v, missing reg/app:v1", tags)
	}
	if got != testDigest {
		t.Fatalf("Tags()[reg/app:v1] = %q, want the manifest digest %q (not the config id sha256:config)", got, testDigest)
	}
	// The bare id and the digest-pinned alias are not tags themselves and
	// must not show up as report-worthy tag entries.
	if _, ok := tags["sha256:config"]; ok {
		t.Fatal("a bare digest alias must not appear in Tags()")
	}
	if _, ok := tags["reg/app@"+testDigest]; ok {
		t.Fatal("a digest-pinned alias must not appear in Tags()")
	}
}

func TestInventoryTagsEmptyBeforeFirstListing(t *testing.T) {
	inv := NewInventory(newFakeRuntime())
	if tags := inv.Tags(); tags != nil {
		t.Fatalf("Tags() before any Get() = %v, want nil", tags)
	}
}

func TestInventoryTagsOmitsTagsWithNoKnownManifestDigest(t *testing.T) {
	rt := newFakeRuntime()
	rt.local["reg/app:v1"] = true
	// A tag alias with no corresponding "@sha256:" alias at all — the
	// runtime never reported this content's manifest digest, only its
	// (possibly synthetic) local id. Tags() must not fabricate one.
	rt.digests["reg/app:v1"] = "sha256:config-only"
	inv := NewInventory(rt)
	_, _, _ = inv.Get(0)

	tags := inv.Tags()
	if _, ok := tags["reg/app:v1"]; ok {
		t.Fatalf("Tags() = %v, expected reg/app:v1 omitted (no manifest digest known)", tags)
	}
}
