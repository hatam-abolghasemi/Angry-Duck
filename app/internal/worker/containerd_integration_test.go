//go:build integration

// Integration tests against a real containerd (overlayfs + CRI). Run as
// root with a daemon listening on ANGRYDUCK_CONTAINERD:
//
//	ANGRYDUCK_CONTAINERD=/run/containerd/containerd.sock go test -tags integration ./internal/worker/
package worker

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/namespaces"
	digest "github.com/opencontainers/go-digest"
	"golang.org/x/sys/unix"

	"angryduck/internal/blobship"
	"angryduck/internal/metrics"
	"angryduck/internal/model"
	"angryduck/internal/registryauth"
)

// --- building test images ---

type fileEntry struct {
	name     string
	typ      byte
	mode     int64
	uid, gid int
	body     string
	link     string
	xattrs   map[string]string
}

type builtImage struct {
	name     string
	archive  []byte            // OCI archive for Import
	blobs    map[string][]byte // digest -> bytes, for the test registry
	manifest string
	layers   []string // compressed layer digests
	chains   []string // chainIDs
}

func layerTar(t *testing.T, es []fileEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, e := range es {
		h := &tar.Header{Name: e.name, Typeflag: e.typ, Mode: e.mode, Uid: e.uid, Gid: e.gid, Linkname: e.link,
			Size: int64(len(e.body)), ModTime: time.Unix(1700000000, 0), Format: tar.FormatPAX}
		for k, v := range e.xattrs {
			if h.PAXRecords == nil {
				h.PAXRecords = map[string]string{}
			}
			h.PAXRecords["SCHILY.xattr."+k] = v
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		io.WriteString(tw, e.body)
	}
	tw.Close()
	return buf.Bytes()
}

func sha(b []byte) string { s := sha256.Sum256(b); return "sha256:" + hex.EncodeToString(s[:]) }

func buildImage(t *testing.T, name string, layers ...[]fileEntry) builtImage {
	t.Helper()
	img := builtImage{name: name, blobs: map[string][]byte{}}
	var diffIDs []string
	type desc struct {
		MediaType   string            `json:"mediaType"`
		Digest      string            `json:"digest"`
		Size        int               `json:"size"`
		Annotations map[string]string `json:"annotations,omitempty"`
	}
	var layerDescs []desc
	chain := ""
	for _, l := range layers {
		raw := layerTar(t, l)
		var gz bytes.Buffer
		zw := gzip.NewWriter(&gz)
		zw.Write(raw)
		zw.Close()
		d := sha(gz.Bytes())
		img.blobs[d] = gz.Bytes()
		img.layers = append(img.layers, d)
		layerDescs = append(layerDescs, desc{MediaType: "application/vnd.oci.image.layer.v1.tar+gzip", Digest: d, Size: gz.Len()})
		diffID := sha(raw)
		diffIDs = append(diffIDs, diffID)
		if chain == "" {
			chain = diffID
		} else {
			chain = sha([]byte(chain + " " + diffID))
		}
		img.chains = append(img.chains, chain)
	}
	cfg, _ := json.Marshal(map[string]any{
		"architecture": "amd64", "os": "linux",
		"rootfs": map[string]any{"type": "layers", "diff_ids": diffIDs},
		"config": map[string]any{},
	})
	img.blobs[sha(cfg)] = cfg
	man, _ := json.Marshal(map[string]any{
		"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
		"config": desc{MediaType: "application/vnd.oci.image.config.v1+json", Digest: sha(cfg), Size: len(cfg)},
		"layers": layerDescs,
	})
	img.manifest = sha(man)
	img.blobs[img.manifest] = man
	index, _ := json.Marshal(map[string]any{
		"schemaVersion": 2,
		"manifests": []desc{{MediaType: "application/vnd.oci.image.manifest.v1+json", Digest: img.manifest, Size: len(man),
			Annotations: map[string]string{"io.containerd.image.name": name}}},
	})
	var arch bytes.Buffer
	tw := tar.NewWriter(&arch)
	put := func(n string, b []byte) {
		tw.WriteHeader(&tar.Header{Name: n, Mode: 0o644, Size: int64(len(b)), Typeflag: tar.TypeReg})
		tw.Write(b)
	}
	put("oci-layout", []byte(`{"imageLayoutVersion":"1.0.0"}`))
	put("index.json", index)
	digests := make([]string, 0, len(img.blobs))
	for d := range img.blobs {
		digests = append(digests, d)
	}
	sort.Strings(digests)
	for _, d := range digests {
		put("blobs/sha256/"+strings.TrimPrefix(d, "sha256:"), img.blobs[d])
	}
	tw.Close()
	img.archive = arch.Bytes()
	return img
}

// The base layer, and an app layer that exercises everything a snapshot
// has to carry: a deleted file, an opaque directory, ownership, a setuid
// bit, a symlink, a hard link, extended attributes.
var (
	baseLayer = []fileEntry{
		{name: "etc/", typ: tar.TypeDir, mode: 0o755},
		{name: "etc/os-release", typ: tar.TypeReg, mode: 0o644, body: "ID=base\n"},
		{name: "data/", typ: tar.TypeDir, mode: 0o755},
		{name: "data/a", typ: tar.TypeReg, mode: 0o644, body: "a"},
		{name: "data/b", typ: tar.TypeReg, mode: 0o644, body: "b"},
		{name: "opt/", typ: tar.TypeDir, mode: 0o755},
		{name: "opt/old/", typ: tar.TypeDir, mode: 0o755},
		{name: "opt/old/stale", typ: tar.TypeReg, mode: 0o644, body: "stale"},
		{name: "os-release", typ: tar.TypeSymlink, link: "etc/os-release"},
	}
	appLayer = []fileEntry{
		{name: "etc/", typ: tar.TypeDir, mode: 0o755},
		{name: "etc/os-release", typ: tar.TypeReg, mode: 0o644, body: "ID=app\n"},
		{name: "data/", typ: tar.TypeDir, mode: 0o755},
		{name: "data/.wh.a", typ: tar.TypeReg},
		{name: "opt/", typ: tar.TypeDir, mode: 0o755},
		{name: "opt/old/", typ: tar.TypeDir, mode: 0o755},
		{name: "opt/old/.wh..wh..opq", typ: tar.TypeReg},
		{name: "opt/old/new", typ: tar.TypeReg, mode: 0o644, body: "new"},
		{name: "home/", typ: tar.TypeDir, mode: 0o755},
		{name: "home/app/", typ: tar.TypeDir, mode: 0o750, uid: 1000, gid: 1000},
		{name: "home/app/secret", typ: tar.TypeReg, mode: 0o600, uid: 1000, gid: 1000, body: "s3cr3t",
			xattrs: map[string]string{"user.angryduck": "quack"}},
		{name: "home/app/hardlink", typ: tar.TypeLink, mode: 0o600, uid: 1000, gid: 1000, link: "home/app/secret"},
		{name: "usr/", typ: tar.TypeDir, mode: 0o755},
		{name: "usr/bin/", typ: tar.TypeDir, mode: 0o755},
		{name: "usr/bin/suid", typ: tar.TypeReg, mode: 0o4755, body: "#!/bin/true\n"},
		// cap_net_raw+p, as ping ships it.
		{name: "usr/bin/ping", typ: tar.TypeReg, mode: 0o755, body: "elf",
			xattrs: map[string]string{"security.capability": "\x01\x00\x00\x02\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"}},
	}
)

// --- harness ---

func testClient(t *testing.T) *containerd.Client {
	t.Helper()
	addr := os.Getenv("ANGRYDUCK_CONTAINERD")
	if addr == "" {
		t.Skip("ANGRYDUCK_CONTAINERD not set")
	}
	c, err := containerd.New(addr, containerd.WithTimeout(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func testNS(t *testing.T, prefix string) string {
	return fmt.Sprintf("ad-%s-%s", prefix, randomSuffix())
}

// realPeer serves one worker's rescue endpoints over a ContainerdStore.
func realPeer(t *testing.T, node string, store *ContainerdStore, override map[string]http.HandlerFunc) (*Rescue, *httptest.Server) {
	t.Helper()
	pins := NewPins(store, node, time.Hour, filepath.Join(t.TempDir(), "pins.json"))
	rs := NewRescue(store, pins, testToken, node, "linux/amd64", 2)
	mux := http.NewServeMux()
	rs.Register(mux)
	h := http.Handler(mux)
	if override != nil {
		h = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if f, ok := override[r.URL.Path]; ok {
				f(w, r)
				return
			}
			mux.ServeHTTP(w, r)
		})
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return rs, srv
}

type node struct {
	f            os.FileMode
	uid, gid     uint32
	sum, link    string
	xattrs       string
	hardlinkedTo string
}

// tree mounts a read-only view of chainID and records every file's
// metadata and content.
func tree(t *testing.T, s *ContainerdStore, chainID string) map[string]node {
	t.Helper()
	if os.Getenv("ANGRYDUCK_NO_MOUNT") != "" {
		// Running with no capabilities, to prove the worker needs none:
		// the comparison itself has to mount, so it is skipped here.
		return nil
	}
	ctx := s.ns(context.Background())
	sn := s.snapshotterSvc()
	key := "test-view-" + randomSuffix()
	mounts, err := sn.View(ctx, key, chainID)
	if err != nil {
		t.Fatalf("view %s: %v", chainID, err)
	}
	defer sn.Remove(ctx, key)
	dir := t.TempDir()
	if err := mount.All(mounts, dir); err != nil {
		t.Fatalf("mount: %v", err)
	}
	defer mount.UnmountAll(dir, 0)
	out := map[string]node{}
	inodes := map[uint64]string{}
	filepath.Walk(dir, func(p string, fi os.FileInfo, err error) error {
		if err != nil || p == dir {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		st := fi.Sys().(*syscall.Stat_t)
		n := node{f: fi.Mode(), uid: st.Uid, gid: st.Gid}
		switch {
		case fi.Mode()&os.ModeSymlink != 0:
			n.link, _ = os.Readlink(p)
		case fi.Mode().IsRegular():
			b, _ := os.ReadFile(p)
			n.sum = sha(b)
			if st.Nlink > 1 {
				if first, ok := inodes[st.Ino]; ok {
					n.hardlinkedTo = first
				} else {
					inodes[st.Ino] = rel
				}
			}
		}
		buf := make([]byte, 1024)
		if sz, err := unix.Llistxattr(p, buf); err == nil && sz > 0 {
			var xs []string
			for _, k := range strings.Split(strings.TrimRight(string(buf[:sz]), "\x00"), "\x00") {
				if !strings.HasPrefix(k, "user.") && k != "security.capability" {
					continue
				}
				v := make([]byte, 256)
				vn, _ := unix.Lgetxattr(p, k, v)
				xs = append(xs, k+"="+string(v[:max(vn, 0)]))
			}
			sort.Strings(xs)
			n.xattrs = strings.Join(xs, ",")
		}
		out[rel] = n
		return nil
	})
	return out
}

// withoutUserXattrs drops user.* xattrs: containerd's differ only carries
// security.capability (like Docker's), so a layer shipped as a snapshot
// loses any others. See docs/transfers.md.
func withoutUserXattrs(m map[string]node) map[string]node {
	if m == nil {
		return nil
	}
	out := make(map[string]node, len(m))
	for p, n := range m {
		var keep []string
		for _, x := range strings.Split(n.xattrs, ",") {
			if x != "" && !strings.HasPrefix(x, "user.") {
				keep = append(keep, x)
			}
		}
		n.xattrs = strings.Join(keep, ",")
		out[p] = n
	}
	return out
}

func sameTree(t *testing.T, want, got map[string]node) {
	t.Helper()
	if want == nil && got == nil {
		return
	}
	if !strings.Contains(want["usr/bin/ping"].xattrs, "security.capability=") {
		t.Fatalf("test image lost its file capability on import: %+v", want["usr/bin/ping"])
	}
	for p, w := range want {
		g, ok := got[p]
		if !ok {
			t.Errorf("missing %s", p)
			continue
		}
		if w != g {
			t.Errorf("%s differs:\n want %+v\n got  %+v", p, w, g)
		}
	}
	for p := range got {
		if _, ok := want[p]; !ok {
			t.Errorf("unexpected %s", p)
		}
	}
}

// setup imports the full app image on src, then drops its app layer blob
// there (as discard_unpacked_layers would), and gives dst only the base.
func setup(t *testing.T) (c *containerd.Client, src, dst *ContainerdStore, app builtImage) {
	t.Helper()
	c = testClient(t)
	ctx := context.Background()
	src = NewContainerdStore(c, testNS(t, "src"), "overlayfs")
	dst = NewContainerdStore(c, testNS(t, "dst"), "overlayfs")
	app = buildImage(t, testImage, baseLayer, appLayer)
	base := buildImage(t, "registry.example.com/team/base:1", baseLayer)
	if err := src.Import(ctx, bytes.NewReader(app.archive), "linux/amd64"); err != nil {
		t.Fatalf("import on src: %v", err)
	}
	if err := dst.Import(ctx, bytes.NewReader(base.archive), "linux/amd64"); err != nil {
		t.Fatalf("import on dst: %v", err)
	}
	if err := c.ContentStore().Delete(namespaces.WithNamespace(ctx, src.namespace), mustDigest(app.layers[1])); err != nil {
		t.Fatalf("dropping the app layer blob on src: %v", err)
	}
	src.Invalidate()
	return
}

// --- tests ---

func TestContainerd_StoreBasics(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	s := NewContainerdStore(c, testNS(t, "basic"), "overlayfs")
	img := buildImage(t, testImage, baseLayer, appLayer)
	if err := s.Import(ctx, bytes.NewReader(img.archive), "linux/amd64"); err != nil {
		t.Fatal(err)
	}
	mt, d, err := s.Resolve(ctx, testImage)
	if err != nil || d != img.manifest || mt != "application/vnd.oci.image.manifest.v1+json" {
		t.Fatalf("Resolve = %q %q %v", mt, d, err)
	}
	digests, err := s.Digests(ctx)
	if err != nil || !digests[img.layers[0]] || !digests[img.manifest] {
		t.Fatalf("Digests missing image content: %v", err)
	}
	snaps, err := s.Snapshots(ctx)
	if err != nil || !snaps[img.chains[0]] || !snaps[img.chains[1]] {
		t.Fatalf("not unpacked: %v %v", snaps, err)
	}
	b, err := s.ReadBlob(ctx, img.manifest, 1<<20)
	if err != nil || sha(b) != img.manifest {
		t.Fatalf("ReadBlob through the content API: %v", err)
	}
	var buf bytes.Buffer
	if err := s.StreamBlob(ctx, img.layers[1], int64(len(img.blobs[img.layers[1]])), &buf); err != nil || sha(buf.Bytes()) != img.layers[1] {
		t.Fatalf("StreamBlob: %v", err)
	}
	if err := s.StreamBlob(ctx, img.layers[1], 1, io.Discard); err == nil {
		t.Fatal("StreamBlob accepted a wrong size")
	}
	names, err := s.ImageNames(ctx)
	if err != nil || len(names) != 1 || names[0] != testImage {
		t.Fatalf("ImageNames = %v %v", names, err)
	}
	if err := s.DeleteImages(ctx, testImage, "never-existed:1"); err != nil {
		t.Fatalf("DeleteImages: %v", err)
	}
	if _, _, err := s.Resolve(ctx, testImage); err == nil {
		t.Fatal("image still there after DeleteImages")
	}
}

// The discard_unpacked_layers case, end to end through the real rescue
// code: the app layer exists only as a snapshot on the source, travels as
// a containerd diff, is applied by containerd on the receiver, and the
// image is then imported on top of it.
func TestContainerd_RescueShipsSnapshot(t *testing.T) {
	_, src, dst, app := setup(t)
	ctx := context.Background()
	_, srcSrv := realPeer(t, "worker14", src, nil)
	_, dstSrv := realPeer(t, "master1", dst, nil)

	code, res := order(t, dstSrv, model.RescueOrder{Image: testImage, Sources: []model.RescueSource{{NodeID: "worker14", Address: addr(srcSrv)}}}, testToken)
	if code != http.StatusOK || !res.OK || res.Snapshots != 1 {
		t.Fatalf("rescue: %d %+v", code, res)
	}
	if _, d, err := dst.Resolve(ctx, testImage); err != nil || d != app.manifest {
		t.Fatalf("image not on the receiver: %q %v", d, err)
	}
	sameTree(t, withoutUserXattrs(tree(t, src, app.chains[1])), withoutUserXattrs(tree(t, dst, app.chains[1])))

	info, err := dst.snapshotterSvc().Stat(dst.ns(ctx), app.chains[1])
	if err != nil {
		t.Fatal(err)
	}
	if _, pinned := info.Labels[gcRootLabel]; pinned {
		t.Errorf("snapshot still pinned after a successful rescue: %v", info.Labels)
	}
}

// A pre-1.8.6 source sends GNU tar of the overlay directory. This is that
// exact command, run against the source's real snapshot directory.
func TestContainerd_ReceivesFromLegacySource(t *testing.T) {
	_, src, dst, app := setup(t)
	legacy := func(w http.ResponseWriter, r *http.Request) {
		ctx := src.ns(r.Context())
		key := "angryduck-view-" + randomSuffix()
		mounts, err := src.snapshotterSvc().View(ctx, key, app.chains[1])
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		defer src.snapshotterSvc().Remove(ctx, key)
		dir := upperOrLowerTop(t, mounts)
		gz := gzip.NewWriter(w)
		cmd := exec.Command("tar", "-C", dir, "--xattrs", "--xattrs-include=*", "--numeric-owner", "-cf", "-", ".")
		var stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = gz, &stderr
		if err := cmd.Run(); err != nil {
			t.Errorf("legacy tar: %v %s", err, stderr.String())
		}
		gz.Close()
	}
	_, srcSrv := realPeer(t, "old-worker", src, map[string]http.HandlerFunc{"/snapshots/export": legacy})
	_, dstSrv := realPeer(t, "master1", dst, nil)

	code, res := order(t, dstSrv, model.RescueOrder{Image: testImage, Sources: []model.RescueSource{{NodeID: "old-worker", Address: addr(srcSrv)}}}, testToken)
	if code != http.StatusOK || !res.OK || res.Snapshots != 1 {
		t.Fatalf("rescue from a legacy source: %d %+v", code, res)
	}
	sameTree(t, tree(t, src, app.chains[1]), tree(t, dst, app.chains[1]))
}

// A pre-1.8.6 receiver asks without a format and untars into an upperdir
// itself, exactly as the old ApplySnapshot did.
func TestContainerd_ServesLegacyReceiver(t *testing.T) {
	c, src, dst, app := setup(t)
	_, srcSrv := realPeer(t, "worker14", src, nil)
	body, _ := json.Marshal(model.SnapshotExportRequest{Image: testImage, Platform: "linux/amd64", ChainID: app.chains[1]})
	req, _ := http.NewRequest(http.MethodPost, srcSrv.URL+"/snapshots/export", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get(snapshotFormatHeader) != "" {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("legacy request: %d format=%q %s", resp.StatusCode, resp.Header.Get(snapshotFormatHeader), b)
	}

	ctx := dst.ns(context.Background())
	sn := dst.snapshotterSvc()
	key := "angryduck-rescue-" + randomSuffix()
	mounts, err := sn.Prepare(ctx, key, app.chains[0])
	if err != nil {
		t.Fatal(err)
	}
	upper := ""
	for _, o := range mounts[0].Options {
		if v, ok := strings.CutPrefix(o, "upperdir="); ok {
			upper = v
		}
	}
	if upper == "" {
		t.Fatalf("no upperdir in %+v", mounts)
	}
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("tar", "-C", upper, "--xattrs", "--xattrs-include=*", "--numeric-owner", "-xpf", "-")
	cmd.Stdin = gz
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("legacy untar: %v %s", err, out)
	}
	if err := sn.Commit(ctx, app.chains[1], key); err != nil {
		t.Fatal(err)
	}
	_ = c
	sameTree(t, withoutUserXattrs(tree(t, src, app.chains[1])), withoutUserXattrs(tree(t, dst, app.chains[1])))
}

// upperOrLowerTop returns the directory holding a view's top layer.
func upperOrLowerTop(t *testing.T, ms []mount.Mount) string {
	t.Helper()
	m := ms[0]
	if m.Type == "bind" {
		return m.Source
	}
	for _, o := range m.Options {
		if v, ok := strings.CutPrefix(o, "lowerdir="); ok {
			return strings.Split(v, ":")[0]
		}
	}
	t.Fatalf("no lowerdir in %+v", m)
	return ""
}

// --- CRI ---

// testRegistry serves an image's blobs over the registry API.
func testRegistry(t *testing.T, img builtImage, repo, tag string) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, "/v2/")
		if p == "" {
			return
		}
		switch {
		case strings.HasPrefix(p, repo+"/manifests/"):
			ref := strings.TrimPrefix(p, repo+"/manifests/")
			if ref == tag {
				ref = img.manifest
			}
			b, ok := img.blobs[ref]
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
			w.Header().Set("Docker-Content-Digest", ref)
			w.Header().Set("Content-Length", fmt.Sprint(len(b)))
			if r.Method != http.MethodHead {
				w.Write(b)
			}
		case strings.HasPrefix(p, repo+"/blobs/"):
			b, ok := img.blobs[strings.TrimPrefix(p, repo+"/blobs/")]
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Length", fmt.Sprint(len(b)))
			if r.Method != http.MethodHead {
				w.Write(b)
			}
		default:
			http.NotFound(w, r)
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

func TestCRI_PullAndList(t *testing.T) {
	c := testClient(t)
	img := buildImage(t, "x", baseLayer)
	host := testRegistry(t, img, "team/cri-test", "1")
	ref := host + "/team/cri-test:1"
	rt := NewRuntime(c.Conn(), registryauth.Empty())
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := rt.PullImage(ctx, ref); err != nil {
		t.Fatalf("CRI pull: %v", err)
	}
	refs, err := rt.LocalImages()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range refs {
		if r == ref {
			found = true
		}
	}
	if !found {
		t.Fatalf("pulled image not listed: %v", refs)
	}
	if _, err := rt.ListRunningImages(); err != nil {
		t.Fatalf("ListRunningImages: %v", err)
	}
	if _, err := rt.RunningImageRepos(); err != nil {
		t.Fatalf("RunningImageRepos: %v", err)
	}
	// The pull went into the k8s.io namespace, as kubelet's would.
	k8s := NewContainerdStore(c, "k8s.io", "overlayfs")
	if _, d, err := k8s.Resolve(context.Background(), ref); err != nil || d != img.manifest {
		t.Fatalf("CRI pull not in k8s.io: %q %v", d, err)
	}
	_ = blobship.FormatOCILayer
}

func mustDigest(s string) digest.Digest { return digest.Digest(s) }

// containerd sends the hosts.toml header to the mirror on the pod IP, and
// falls back to the registry when the mirror doesn't have the content.
func TestCRI_MirrorGetsTokenFromHostsToml(t *testing.T) {
	c := testClient(t)
	certsDir := os.Getenv("ANGRYDUCK_CERTS_DIR")
	if certsDir == "" {
		t.Skip("ANGRYDUCK_CERTS_DIR (containerd's config_path) not set")
	}
	// Unique content, so nothing is already in containerd's store.
	unique := append([]fileEntry{{name: "run-id", typ: tar.TypeReg, mode: 0o644, body: randomSuffix()}}, appLayer...)
	img := buildImage(t, "x", unique)
	host := testRegistry(t, img, "team/mirror-test", "1")

	var seen []string
	mirror := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Header.Get("Authorization"))
		http.NotFound(w, r) // nothing here: containerd must fall back
	}))
	defer mirror.Close()

	h := &HostsConfig{ConfigDir: certsDir, Mirror: strings.TrimPrefix(mirror.URL, "http://"), Token: "pod-token", NodeID: "n1", Extra: []string{host}}
	h.Sync(nil, true)
	defer h.RemoveOwn()
	// The test registry is plain HTTP.
	p := filepath.Join(certsDir, host, "hosts.toml")
	b, _ := os.ReadFile(p)
	os.WriteFile(p, []byte(strings.Replace(string(b), `server = "https://`, `server = "http://`, 1)), 0o600)

	rt := NewRuntime(c.Conn(), registryauth.Empty())
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := rt.PullImage(ctx, host+"/team/mirror-test:1"); err != nil {
		t.Fatalf("pull with the mirror configured: %v", err)
	}
	if len(seen) == 0 {
		t.Fatal("containerd never asked the mirror")
	}
	for _, a := range seen {
		if a != "Bearer pod-token" {
			t.Fatalf("mirror got Authorization %q, want the hosts.toml token", a)
		}
	}
	if n := h.RemoveOwn(); n != 1 {
		t.Fatalf("RemoveOwn = %d", n)
	}
}

// The worker dials with its own options (to count calls); they must still
// connect the way containerd's defaults do.
func TestContainerd_DialOptionsConnectAndCount(t *testing.T) {
	addr := os.Getenv("ANGRYDUCK_CONTAINERD")
	if addr == "" {
		t.Skip("ANGRYDUCK_CONTAINERD not set")
	}
	c, err := containerd.New(addr, containerd.WithTimeout(5*time.Second), containerd.WithDialOpts(ContainerdDialOptions("dial-test")))
	if err != nil {
		t.Fatalf("connect with the worker's dial options: %v", err)
	}
	defer c.Close()
	if _, err := c.Version(context.Background()); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	buf := rec.Body
	if !strings.Contains(buf.String(), `node="dial-test",method="Version/Version"`) {
		t.Fatalf("call not counted:\n%s", buf.String())
	}
}
