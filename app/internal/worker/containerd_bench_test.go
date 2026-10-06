//go:build integration

package worker

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/containerd/containerd/namespaces"

	"angryduck/internal/model"
)

// Timing runs for snapshot shipping, against a real containerd. Sizes come
// from the environment so a run fits the machine:
//
//	ANGRYDUCK_CONTAINERD=/run/containerd/containerd.sock \
//	ANGRYDUCK_BENCH_BASE_MB=300 ANGRYDUCK_BENCH_APP_MB=150 \
//	go test -tags integration -run TestBench -v ./internal/worker/
//
// The base layer travels as a blob; the app layer only as a snapshot, as on
// a node with discard_unpacked_layers.

func benchMB(t *testing.T, name string, def int) int {
	t.Helper()
	if v := os.Getenv(name); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return n
	}
	return def
}

// bulkLayer is a layer of files of 100 KiB each, totalling about mb MiB.
// Contents are hex-encoded random bytes, which compress about 2:1, close
// to a typical application layer.
func bulkLayer(dir string, mb int, seed int64) []fileEntry {
	r := rand.New(rand.NewSource(seed))
	es := []fileEntry{{name: dir + "/", typ: tar.TypeDir, mode: 0o755}}
	const per = 100 << 10
	raw := make([]byte, per/2)
	for i := 0; i < mb*1024*1024/per; i++ {
		r.Read(raw)
		es = append(es, fileEntry{name: fmt.Sprintf("%s/f%05d", dir, i), typ: tar.TypeReg, mode: 0o644, body: hex.EncodeToString(raw)})
	}
	return es
}

func benchSetup(t *testing.T, baseMB, appMB int) (src, dst *ContainerdStore, app builtImage) {
	t.Helper()
	c := testClient(t)
	ctx := context.Background()
	src = NewContainerdStore(c, testNS(t, "bsrc"), "overlayfs")
	dst = NewContainerdStore(c, testNS(t, "bdst"), "overlayfs")
	for _, s := range []*ContainerdStore{src, dst} {
		s.UseBlobDir(os.Getenv("ANGRYDUCK_CONTAINERD_ROOT"), "/etc/containerd/config.toml")
	}
	base := bulkLayer("base", baseMB, 1)
	app = buildImage(t, "registry.example.com/team/bench:1", base, bulkLayer("app", appMB, 2))
	baseOnly := buildImage(t, "registry.example.com/team/bench-base:1", base)
	if err := src.Import(ctx, bytes.NewReader(app.archive), "linux/amd64"); err != nil {
		t.Fatalf("import on src: %v", err)
	}
	if err := dst.Import(ctx, bytes.NewReader(baseOnly.archive), "linux/amd64"); err != nil {
		t.Fatalf("import on dst: %v", err)
	}
	if err := c.ContentStore().Delete(namespaces.WithNamespace(ctx, src.namespace), mustDigest(app.layers[1])); err != nil {
		t.Fatalf("dropping the app layer blob on src: %v", err)
	}
	src.Invalidate()
	t.Cleanup(func() {
		cctx := context.Background()
		src.DeleteImages(cctx, app.name)
		dst.DeleteImages(cctx, app.name, baseOnly.name)
	})
	return src, dst, app
}

func TestBench_SnapshotExport(t *testing.T) {
	baseMB, appMB := benchMB(t, "ANGRYDUCK_BENCH_BASE_MB", 300), benchMB(t, "ANGRYDUCK_BENCH_APP_MB", 150)
	src, _, app := benchSetup(t, baseMB, appMB)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		start := time.Now()
		var n countingDiscard
		if err := src.ExportSnapshot(ctx, app.chains[1], app.chains[0], &n); err != nil {
			t.Fatal(err)
		}
		t.Logf("export base=%dMiB app=%dMiB: %d bytes in %s", baseMB, appMB, n, time.Since(start).Round(time.Millisecond))
	}
}

func TestBench_SnapshotRescue(t *testing.T) {
	baseMB, appMB := benchMB(t, "ANGRYDUCK_BENCH_BASE_MB", 300), benchMB(t, "ANGRYDUCK_BENCH_APP_MB", 150)
	src, dst, app := benchSetup(t, baseMB, appMB)
	_, srcSrv := realPeer(t, "worker14", src, nil)
	_, dstSrv := realPeer(t, "master1", dst, nil)
	start := time.Now()
	code, res := order(t, dstSrv, model.RescueOrder{Image: app.name, Sources: []model.RescueSource{{NodeID: "worker14", Address: addr(srcSrv)}}}, testToken)
	if code != http.StatusOK || !res.OK || res.Snapshots != 1 {
		t.Fatalf("rescue: %d %+v", code, res)
	}
	t.Logf("rescue base=%dMiB app=%dMiB as snapshot: %d bytes on the wire in %s", baseMB, appMB, res.Bytes, time.Since(start).Round(time.Millisecond))
}

type countingDiscard int64

func (c *countingDiscard) Write(p []byte) (int, error) {
	*c += countingDiscard(len(p))
	return io.Discard.Write(p)
}
