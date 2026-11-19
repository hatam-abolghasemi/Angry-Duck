//go:build integration

package worker

import (
	"archive/tar"
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/containerd/containerd/snapshots"
)

// impliedLayers is an image whose upper layers only imply the directories
// they write into, of every kind containerd's unpack resolves from the
// layers below: owned and with xattrs, missing everywhere, deleted, a
// symlink below, an opaque one, and the root directory's own entry.
func impliedLayers(salt int64) []compressedLayer {
	base := []fileEntry{
		{name: "home/", typ: tar.TypeDir, mode: 0o755},
		{name: "home/app/", typ: tar.TypeDir, mode: 0o700, uid: 1000, gid: 1000, xattrs: map[string]string{"user.owner": "app"}},
		{name: "home/app/.profile", typ: tar.TypeReg, mode: 0o600, uid: 1000, gid: 1000, body: "x"},
		{name: "srv/", typ: tar.TypeDir, mode: 0o711, uid: 33, gid: 33},
		{name: "srv/www/", typ: tar.TypeDir, mode: 0o2750, uid: 33, gid: 33},
		{name: "srv/www/old.html", typ: tar.TypeReg, mode: 0o644, uid: 33, gid: 33, body: "old"},
		{name: "lib/", typ: tar.TypeDir, mode: 0o755},
		{name: "lib/libold.so", typ: tar.TypeReg, mode: 0o755, body: "old lib"},
		{name: "gone/", typ: tar.TypeDir, mode: 0o750, uid: 7, gid: 7},
		{name: "gone/hidden", typ: tar.TypeReg, mode: 0o644, body: "hidden"},
		{name: "link", typ: tar.TypeSymlink, link: "srv"},
		{name: "tmp/", typ: tar.TypeDir, mode: 0o1777},
	}
	middle := []fileEntry{
		{name: "home/app/cache/x", typ: tar.TypeReg, mode: 0o644, uid: 1000, gid: 1000, body: "cache"},
		{name: "srv/www/index.html", typ: tar.TypeReg, mode: 0o644, uid: 33, gid: 33, body: noisyText(salt, 2<<20)},
		{name: "lib/.wh.libold.so", typ: tar.TypeReg},
		{name: "gone/.wh..wh..opq", typ: tar.TypeReg},
		{name: "gone/new", typ: tar.TypeReg, mode: 0o644, body: "new"},
		{name: "link/inside", typ: tar.TypeReg, mode: 0o644, body: "inside"},
		{name: "tmp/sock/a", typ: tar.TypeReg, mode: 0o644, body: "a"},
	}
	top := []fileEntry{
		{name: "./", typ: tar.TypeDir, mode: 0o755},
		{name: "home/app/cache/y", typ: tar.TypeReg, mode: 0o644, body: "y"},
		{name: "data/big", typ: tar.TypeReg, mode: 0o644, body: noisyText(salt+1, 8<<20)},
		{name: "srv/www/.wh.old.html", typ: tar.TypeReg},
	}
	return []compressedLayer{{entries: base}, {entries: middle}, {entries: top}}
}

// Layers arriving top first, on any containerd: each unpacks while it
// arrives, on its own when the layer below isn't there yet, and is attached
// bottom up, with exactly the files containerd's own unpack makes,
// implied directories included.
func TestContainerd_StreamUnpackEveryLayerAtOnce(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	s := streamStore(t, "whole")
	img, types := buildMixedImage(t, "angryduck.local/whole:1", impliedLayers(time.Now().UnixNano())...)
	sp, srv := streamUnpackSpread(t, s, &rateRegistry{blobs: img.blobs, rate: 4 << 20}, "whole")
	if sp.streamExtractor == nil {
		t.Fatal("stream extraction not enabled")
	}
	sp.SetUnpackSlots(4)
	streamed := unpackCount("whole", "streamed")
	for i := len(img.layers) - 1; i >= 0; i-- {
		postSpread(t, srv, "/spread/fetch", fetchOf(img, types, "wj", i))
	}
	sp.wg.Wait()
	for i, ch := range img.chains {
		info, err := s.snapshotterSvc().Stat(s.ns(ctx), ch)
		if err != nil {
			t.Fatalf("layer %d not unpacked: %v", i, err)
		}
		want := ""
		if i > 0 {
			want = img.chains[i-1]
		}
		if info.Parent != want {
			t.Fatalf("layer %d committed onto %q, want %q", i, info.Parent, want)
		}
	}
	if got := unpackCount("whole", "streamed") - streamed; got != float64(len(img.layers)) {
		t.Errorf("%v layers unpacked while they arrived, want %d", got, len(img.layers))
	}
	ref := NewContainerdStore(c, testNS(t, "wholeref"), "overlayfs")
	if err := ref.Import(ctx, bytes.NewReader(ociArchive(t, img)), "linux/amd64"); err != nil {
		t.Fatal(err)
	}
	for i, ch := range img.chains {
		want, got := tree(t, ref, ch), tree(t, s, ch)
		if want == nil {
			continue
		}
		if len(want) == 0 {
			t.Fatalf("layer %d: empty reference tree", i)
		}
		for p, w := range want {
			if g, ok := got[p]; !ok {
				t.Errorf("layer %d: missing %s", i, p)
			} else if w != g {
				t.Errorf("layer %d: %s differs:\n want %+v\n got  %+v", i, p, w, g)
			}
		}
		for p := range got {
			if _, ok := want[p]; !ok {
				t.Errorf("layer %d: unexpected %s", i, p)
			}
		}
	}
	resp := postSpread(t, srv, "/spread/finalize", spreadFinalizeOf(img, "wj"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("finalize %d", resp.StatusCode)
	}
	left := 0
	_ = s.snapshotterSvc().Walk(s.ns(ctx), func(_ context.Context, i snapshots.Info) error {
		if i.Kind != snapshots.KindCommitted {
			left++
		}
		return nil
	})
	if left != 0 {
		t.Fatalf("%d unpack snapshot(s) left behind", left)
	}
}
