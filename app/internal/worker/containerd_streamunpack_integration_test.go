//go:build integration

package worker

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/containerd/snapshots"
	"github.com/klauspost/compress/zstd"

	"angryduck/internal/metrics"
	"angryduck/internal/model"
)

// --- images with chosen compression, served at a chosen rate ---

type compressedLayer struct {
	entries []fileEntry
	zstd    bool
}

// buildMixedImage is buildImage with a compression per layer.
func buildMixedImage(t *testing.T, name string, layers ...compressedLayer) (builtImage, []string) {
	t.Helper()
	img := builtImage{name: name, blobs: map[string][]byte{}}
	var diffIDs, types []string
	type desc struct {
		MediaType string `json:"mediaType"`
		Digest    string `json:"digest"`
		Size      int    `json:"size"`
	}
	var descs []desc
	chain := ""
	for _, l := range layers {
		raw := layerTar(t, l.entries)
		var buf bytes.Buffer
		mt := "application/vnd.oci.image.layer.v1.tar+gzip"
		if l.zstd {
			mt = "application/vnd.oci.image.layer.v1.tar+zstd"
			zw, _ := zstd.NewWriter(&buf)
			zw.Write(raw)
			zw.Close()
		} else {
			zw, _ := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
			zw.Write(raw)
			zw.Close()
		}
		d := sha(buf.Bytes())
		img.blobs[d] = buf.Bytes()
		img.layers = append(img.layers, d)
		types = append(types, mt)
		descs = append(descs, desc{MediaType: mt, Digest: d, Size: buf.Len()})
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
		"layers": descs,
	})
	img.manifest = sha(man)
	img.blobs[img.manifest] = man
	return img, types
}

func fetchOf(img builtImage, types []string, job string, i int) model.SpreadFetch {
	f := unpackFetch(img, job, i)
	f.Unpack.MediaType = types[i]
	return f
}

// rateRegistry serves blobs at rate bytes/s (0: as fast as it can), and
// can cut one blob short after cut bytes.
type rateRegistry struct {
	blobs map[string][]byte
	rate  int64
	mu    sync.Mutex
	cut   map[string]int64
}

func (r *rateRegistry) OpenBlob(ctx context.Context, _, d string) (io.ReadCloser, int64, error) {
	b, ok := r.blobs[d]
	if !ok {
		return nil, 0, errors.New("404")
	}
	r.mu.Lock()
	cut, isCut := r.cut[d]
	delete(r.cut, d)
	r.mu.Unlock()
	var src io.Reader = bytes.NewReader(b)
	if isCut {
		src = io.MultiReader(io.LimitReader(src, cut), cutReader{errors.New("connection reset")})
	}
	if r.rate > 0 {
		src = &paced{r: src, rate: r.rate, start: time.Now()}
	}
	return io.NopCloser(src), int64(len(b)), nil
}

func (r *rateRegistry) OpenManifest(ctx context.Context, repo, d string) (io.ReadCloser, int64, error) {
	return r.OpenBlob(ctx, repo, d)
}

type cutReader struct{ err error }

func (e cutReader) Read([]byte) (int, error) { return 0, e.err }

// paced reads no faster than rate bytes/s, in 256 KiB steps.
type paced struct {
	r     io.Reader
	rate  int64
	start time.Time
	n     int64
}

func (p *paced) Read(b []byte) (int, error) {
	if len(b) > 256<<10 {
		b = b[:256<<10]
	}
	n, err := p.r.Read(b)
	p.n += int64(n)
	if due := time.Duration(float64(p.n) / float64(p.rate) * float64(time.Second)); due > time.Since(p.start) {
		time.Sleep(due - time.Since(p.start))
	}
	return n, err
}

func streamUnpackSpread(t *testing.T, s *ContainerdStore, reg BlobOpener, node string) (*Spread, *httptest.Server) {
	t.Helper()
	if err := s.EnableStreamUnpack(context.Background()); err != nil {
		t.Fatalf("probe: %v", err)
	}
	sp := NewSpread(s, reg, spreadToken, node, "linux/amd64", "", 10*time.Minute)
	sp.EnableUnpack(s)
	if !sp.EnableStreamUnpack(s) {
		t.Fatal("stream unpacking refused")
	}
	mux := http.NewServeMux()
	sp.Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return sp, srv
}

func unpackCount(node, result string) float64 {
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	prefix := `angryduck_worker_spread_unpacks_total{node="` + node + `",result="` + result + `"} `
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if strings.HasPrefix(line, prefix) {
			v, _ := strconv.ParseFloat(strings.TrimPrefix(line, prefix), 64)
			return v
		}
	}
	return 0
}

// noisyText is n bytes that compress about as source code and logs do.
func noisyText(seed int64, n int) string {
	rng := rand.New(rand.NewSource(seed))
	words := []string{"quack", "duck", "layer", "blob", "snapshot", "containerd", "kubelet", "node", "image", "spread",
		"func", "return", "err", "nil", "if", "for", "range", "{", "}", "\n", "\t", "0x", "=", ":="}
	var b bytes.Buffer
	b.Grow(n)
	for b.Len() < n {
		b.WriteString(words[rng.Intn(len(words))])
		if rng.Intn(3) == 0 {
			fmt.Fprintf(&b, "%x", rng.Int63())
		}
		b.WriteByte(' ')
	}
	return b.String()[:n]
}

// --- correctness ---

// Every layer is unpacked while it arrives, with exactly the files
// containerd's own unpack makes: owners, modes, setuid, file capabilities,
// hardlinks, whiteouts, opaque directories, a directory a layer only
// implies (its owner and mode come from the layer below), gzip and zstd.
func TestContainerd_StreamUnpackMakesContainerdsFiles(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	s := streamStore(t, "sunpack")
	big := noisyText(time.Now().UnixNano(), 24<<20) // unique: blob files are shared by every namespace // big enough to stream for a while, and to drop its pages
	third := []fileEntry{
		// home/app comes from the layer below (0750, 1000:1000); cfg is new.
		{name: "home/app/cfg/settings", typ: tar.TypeReg, mode: 0o640, uid: 1000, gid: 1000, body: "debug=false\n"},
		{name: "usr/bin/.wh.suid", typ: tar.TypeReg},
		{name: "var/log/big.log", typ: tar.TypeReg, mode: 0o644, body: big},
		{name: "etc/.wh..wh..opq", typ: tar.TypeReg},
		{name: "etc/hostname", typ: tar.TypeReg, mode: 0o644, body: "duck\n"},
	}
	img, types := buildMixedImage(t, "angryduck.local/sunpack:1",
		compressedLayer{entries: baseLayer}, compressedLayer{entries: appLayer, zstd: true}, compressedLayer{entries: third})
	reg := &rateRegistry{blobs: img.blobs, rate: 16 << 20} // the big layer takes over a second
	sp, srv := streamUnpackSpread(t, s, reg, "su1")

	for i := range img.layers {
		postSpread(t, srv, "/spread/fetch", fetchOf(img, types, "j1", i))
	}
	sp.wg.Wait()
	// The small layers may land before their turn (then containerd
	// unpacks them, as before); the big one is still arriving.
	streamed, applied := unpackCount("su1", "streamed"), unpackCount("su1", "applied")
	if streamed < 1 || streamed+applied != 3 || unpackCount("su1", "stream_failed")+unpackCount("su1", "failed") != 0 {
		t.Fatalf("streamed=%v applied=%v stream_failed=%v failed=%v", streamed, applied,
			unpackCount("su1", "stream_failed"), unpackCount("su1", "failed"))
	}

	ref := NewContainerdStore(c, testNS(t, "sunpackref"), "overlayfs")
	if err := ref.Import(ctx, bytes.NewReader(ociArchive(t, img)), "linux/amd64"); err != nil {
		t.Fatal(err)
	}
	for _, ch := range img.chains[1:] {
		want, got := tree(t, ref, ch), tree(t, s, ch)
		sameTree(t, want, got)
	}
	top := tree(t, s, img.chains[2])
	if n := top["home/app"]; n.uid != 1000 || n.f.Perm() != 0o750 {
		t.Fatalf("implied directory home/app: %+v", n)
	}

	resp := postSpread(t, srv, "/spread/finalize", spreadFinalizeOf(img, "j1"))
	var res model.SpreadFinalizeResult
	_ = json.NewDecoder(resp.Body).Decode(&res)
	if resp.StatusCode != http.StatusOK || !res.OK {
		t.Fatalf("finalize: %d %+v", resp.StatusCode, res)
	}
	if _, got, err := s.Resolve(ctx, img.name); err != nil || got != img.manifest {
		t.Fatalf("resolves to %q (%v)", got, err)
	}
}

// ociArchive is the image as an OCI archive, for a reference import.
func ociArchive(t *testing.T, img builtImage) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	add := func(name string, b []byte) {
		tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(b)), Typeflag: tar.TypeReg})
		tw.Write(b)
	}
	add("oci-layout", []byte(`{"imageLayoutVersion":"1.0.0"}`))
	idx, _ := json.Marshal(map[string]any{"schemaVersion": 2, "manifests": []any{map[string]any{
		"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": img.manifest, "size": len(img.blobs[img.manifest]),
		"annotations": map[string]string{"io.containerd.image.name": img.name},
	}}})
	add("index.json", idx)
	for d, b := range img.blobs {
		add("blobs/sha256/"+d[len("sha256:"):], b)
	}
	tw.Close()
	return buf.Bytes()
}

// A layer ordered under a chainID it doesn't unpack to is never committed
// under it, neither while it arrives nor after.
func TestContainerd_StreamUnpackRefusesAWrongChainID(t *testing.T) {
	s := streamStore(t, "sunpackbad")
	img, types := buildMixedImage(t, "angryduck.local/sunpackbad:1", compressedLayer{entries: baseLayer}, compressedLayer{entries: appLayer})
	sp, srv := streamUnpackSpread(t, s, &rateRegistry{blobs: img.blobs, rate: 4 << 20}, "su2")
	o := fetchOf(img, types, "j2", 0)
	o.Unpack.ChainID = img.chains[1] // the base layer, ordered as if it were the app layer
	failedBefore := unpackCount("su2", "stream_failed")
	postSpread(t, srv, "/spread/fetch", o)
	sp.wg.Wait()
	if unpackCount("su2", "stream_failed") == failedBefore {
		t.Fatal("a wrong chainID didn't fail the unpack while arriving")
	}
	if _, err := s.snapshotterSvc().Stat(s.ns(context.Background()), img.chains[1]); err == nil {
		t.Fatal("snapshot committed under a chainID it doesn't have")
	}
}

// A transfer cut short leaves nothing committed; the layer is unpacked
// once a later transfer lands it.
func TestContainerd_StreamUnpackSurvivesACutTransfer(t *testing.T) {
	s := streamStore(t, "sunpackcut")
	img, types := buildMixedImage(t, "angryduck.local/sunpackcut:1",
		compressedLayer{entries: []fileEntry{{name: "big", typ: tar.TypeReg, mode: 0o644, body: noisyText(time.Now().UnixNano(), 8<<20)}}})
	reg := &rateRegistry{blobs: img.blobs, rate: 8 << 20, cut: map[string]int64{img.layers[0]: 3 << 20}}
	sp, srv := streamUnpackSpread(t, s, reg, "su3")
	snap := func() bool {
		_, err := s.snapshotterSvc().Stat(s.ns(context.Background()), img.chains[0])
		return err == nil
	}

	postSpread(t, srv, "/spread/fetch", fetchOf(img, types, "j3", 0))
	sp.wg.Wait()
	if snap() {
		t.Fatal("committed a layer whose transfer was cut short")
	}
	appliedBefore := unpackCount("su3", "applied")
	postSpread(t, srv, "/spread/fetch", fetchOf(img, types, "j3", 0))
	sp.wg.Wait()
	if !snap() {
		t.Fatal("not unpacked after the retry landed it")
	}
	if unpackCount("su3", "applied") == appliedBefore {
		t.Fatal("after a failed unpack while arriving, the landed layer should be unpacked through containerd")
	}
}

// --- how much faster ---

// TestContainerd_StreamUnpackBench (ANGRYDUCK_BENCH=1) spreads one image to
// a node with each unpack mode (parallel ones only where containerd
// supports them), at a few link rates, and reports how long
// the node takes from the first fetch order to a registered image, and how
// much of that comes after the last byte landed.
// parallelBenchMax: ANGRYDUCK_BENCH_PARALLEL, else as the worker's default.
func parallelBenchMax() int {
	if n, err := strconv.Atoi(os.Getenv("ANGRYDUCK_BENCH_PARALLEL")); err == nil && n > 0 {
		return n
	}
	return max(2, runtime.NumCPU()/4)
}

func TestContainerd_StreamUnpackBench(t *testing.T) {
	if os.Getenv("ANGRYDUCK_BENCH") == "" {
		t.Skip("ANGRYDUCK_BENCH not set")
	}
	c := testClient(t)
	ctx := context.Background()
	// A typical big app image: a base, dependencies, a large app layer.
	salt := time.Now().UnixNano() // blob files are shared by every namespace: fresh ones
	mk := func(seed int64, files, size int) []fileEntry {
		es := []fileEntry{}
		for i := 0; i < files; i++ {
			es = append(es, fileEntry{name: fmt.Sprintf("d%d/f%04d", seed, i), typ: tar.TypeReg, mode: 0o644, body: noisyText(salt+seed*100000+int64(i), size/files)})
		}
		return es
	}
	img, types := buildMixedImage(t, "angryduck.local/bench:1",
		compressedLayer{entries: mk(1, 2000, 80<<20)},
		compressedLayer{entries: mk(2, 4000, 300<<20)},
		compressedLayer{entries: mk(3, 600, 600<<20)})
	var compressed int64
	for _, d := range img.layers {
		compressed += int64(len(img.blobs[d]))
	}
	t.Logf("image: 3 layers, %d MiB compressed, ~980 MiB unpacked", compressed>>20)

	run := func(mode string, rate int64, round int) (total, afterLanded time.Duration) {
		s := streamStore(t, fmt.Sprintf("bench-%s-%d-%d", mode, rate>>20, round))
		reg := &rateRegistry{blobs: img.blobs, rate: rate}
		node := fmt.Sprintf("b-%s-%d-%d", mode, rate>>20, round)
		var sp *Spread
		var srv *httptest.Server
		switch mode {
		case "stream", "stream-parallel":
			sp, srv = streamUnpackSpread(t, s, reg, node)
		default:
			sp = NewSpread(s, reg, spreadToken, node, "linux/amd64", "", 10*time.Minute)
			if mode != "finalize" {
				sp.EnableUnpack(s)
			}
			mux := http.NewServeMux()
			sp.Register(mux)
			srv = httptest.NewServer(mux)
			defer srv.Close()
		}
		if strings.HasSuffix(mode, "parallel") {
			if err := s.EnableParallelUnpack(ctx); err != nil {
				t.Fatalf("%s: %v", mode, err)
			}
			sp.EnableParallelUnpack(s, parallelBenchMax())
		}
		job := "bench" + strconv.Itoa(round)
		landed := make(chan time.Time, 1)
		stop := make(chan struct{})
		go func() {
			for {
				select {
				case <-stop:
					return
				case <-time.After(5 * time.Millisecond):
				}
				all := true
				for _, d := range img.layers {
					if ok, _ := s.HasBlob(d); !ok {
						all = false
						break
					}
				}
				if all {
					landed <- time.Now()
					return
				}
			}
		}()
		start := time.Now()
		for i := range img.layers { // bottom up
			postSpread(t, srv, "/spread/fetch", fetchOf(img, types, job, i))
		}
		sp.wg.Wait()
		resp := postSpread(t, srv, "/spread/finalize", spreadFinalizeOf(img, job))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: finalize %d", mode, resp.StatusCode)
		}
		end := time.Now()
		close(stop)
		l := <-landed
		// Clean up so the next run starts cold: the image, its leases,
		// then the garbage collector takes its blobs and snapshots.
		nctx := s.ns(ctx)
		_ = s.client.ImageService().Delete(nctx, img.name)
		if ls, err := s.client.LeasesService().List(nctx); err == nil {
			for _, l := range ls {
				_ = s.client.LeasesService().Delete(nctx, l)
			}
		}
		collect(t, s.client, nctx)
		for _, d := range img.layers {
			if ok, _ := s.HasBlob(d); ok {
				t.Fatalf("%s still on disk after cleanup", d)
			}
		}
		return end.Sub(start), end.Sub(l)
	}
	modes := []string{"finalize", "arrival", "parallel", "stream", "stream-parallel"}
	if m := os.Getenv("ANGRYDUCK_BENCH_MODES"); m != "" {
		modes = strings.Split(m, ",")
	}
	if err := streamStore(t, "bench-probe").EnableParallelUnpack(ctx); err != nil {
		t.Logf("no parallel modes: %v", err)
		modes = slices.DeleteFunc(modes, func(m string) bool { return strings.HasSuffix(m, "parallel") })
	}
	rounds := 2
	if os.Getenv("ANGRYDUCK_BENCH_ONCE") != "" {
		rounds = 1
	}
	for _, rate := range []int64{100 << 20, 400 << 20} {
		for round := 0; round < rounds; round++ {
			for _, mode := range modes {
				// Drop the page cache so every run reads from disk alike.
				_ = os.WriteFile("/proc/sys/vm/drop_caches", []byte("3"), 0)
				total, after := run(mode, rate, round)
				t.Logf("link %4d MiB/s  round %d  %-15s  first order -> image registered: %6.2fs   after the last byte landed: %6.2fs",
					rate>>20, round, mode, total.Seconds(), after.Seconds())
			}
		}
	}
	_ = c
}

// TestContainerd_StreamUnpackExtractBench (ANGRYDUCK_BENCH=1): one landed
// 600 MiB layer, unpacked by containerd (as unpacking on arrival does) and
// by this worker (as stream unpacking does), gzip and zstd.
func TestContainerd_StreamUnpackExtractBench(t *testing.T) {
	if os.Getenv("ANGRYDUCK_BENCH") == "" {
		t.Skip("ANGRYDUCK_BENCH not set")
	}
	ctx := context.Background()
	salt := time.Now().UnixNano()
	var es []fileEntry
	for i := 0; i < 600; i++ {
		es = append(es, fileEntry{name: fmt.Sprintf("app/f%04d", i), typ: tar.TypeReg, mode: 0o644, body: noisyText(salt+int64(i), 1<<20)})
	}
	for _, z := range []bool{false, true} {
		img, types := buildMixedImage(t, "angryduck.local/xbench:1", compressedLayer{entries: es, zstd: z})
		d, size := img.layers[0], int64(len(img.blobs[img.layers[0]]))
		l := LayerSpec{Digest: d, Size: size, MediaType: types[0], ChainID: img.chains[0]}
		for _, who := range []string{"containerd", "worker"} {
			s := streamStore(t, "xbench")
			if err := s.EnableStreamUnpack(ctx); err != nil {
				t.Fatal(err)
			}
			if err := s.CreateLease(ctx, "x", time.Hour); err != nil {
				t.Fatal(err)
			}
			if _, err := s.WriteBlob(ctx, "x", "w-"+d, d, size, bytes.NewReader(img.blobs[d])); err != nil {
				t.Fatal(err)
			}
			_ = os.WriteFile("/proc/sys/vm/drop_caches", []byte("3"), 0)
			runtime.GC()
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			base, allocBefore := ms.HeapInuse, ms.TotalAlloc
			var peak atomic.Uint64
			stopMem := make(chan struct{})
			go func() {
				var m runtime.MemStats
				for {
					select {
					case <-stopMem:
						return
					case <-time.After(20 * time.Millisecond):
					}
					runtime.ReadMemStats(&m)
					if m.HeapInuse > peak.Load() {
						peak.Store(m.HeapInuse)
					}
				}
			}()
			start := time.Now()
			var err error
			if who == "containerd" {
				_, err = s.ApplyLayer(ctx, "x", l)
			} else {
				var rc io.ReadCloser
				rc, _, err = s.OpenContent(ctx, d)
				if err == nil {
					_, err = s.ApplyLayerStream(ctx, "x", l, rc)
					rc.Close()
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			took := time.Since(start)
			close(stopMem)
			runtime.ReadMemStats(&ms)
			comp := "gzip"
			if z {
				comp = "zstd"
			}
			t.Logf("%s, unpacked by %-10s %5.2fs  (%3.0f MiB/s of tar)  heap in use, peak above before: %4.1f MiB  allocated: %5.1f MiB",
				comp, who, took.Seconds(), 600/took.Seconds(), float64(int64(peak.Load())-int64(base))/(1<<20), float64(ms.TotalAlloc-allocBefore)/(1<<20))
			nctx := s.ns(ctx)
			_ = s.DeleteLease(nctx, "x")
			collect(t, s.client, nctx)
		}
	}
}

// The startup probe turns stream unpacking on only where the worker can
// write into containerd's snapshots, and leaves nothing behind either way.
// Run with every capability dropped (ANGRYDUCK_NO_MOUNT=1, see
// docs/development.md), it must refuse.
func TestContainerd_StreamUnpackProbe(t *testing.T) {
	ctx := context.Background()
	s := NewContainerdStore(testClient(t), testNS(t, "sprobe"), "overlayfs")
	err := s.EnableStreamUnpack(ctx)
	if os.Getenv("ANGRYDUCK_NO_MOUNT") != "" {
		if err == nil || s.StreamUnpacks() {
			t.Fatal("stream unpacking turned on without the capabilities it needs")
		}
		t.Logf("refused as it should: %v", err)
	} else if err != nil || !s.StreamUnpacks() {
		t.Fatalf("probe: %v", err)
	}
	left := 0
	_ = s.snapshotterSvc().Walk(s.ns(ctx), func(context.Context, snapshots.Info) error { left++; return nil })
	if left != 0 {
		t.Fatalf("%d snapshot(s) left behind by the probe", left)
	}
}

// Parallel unpacking turns on only where containerd commits snapshots onto
// a parent (2.2 and later), and its probe leaves nothing behind.
func TestContainerd_ParallelUnpackProbe(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	s := NewContainerdStore(c, testNS(t, "pprobe"), "overlayfs")
	v, _ := c.Version(ctx)
	err := s.EnableParallelUnpack(ctx)
	t.Logf("containerd %s: parallel=%t (%v)", v.Version, s.ParallelUnpacks(), err)
	if strings.HasPrefix(v.Version, "v1.") && (err == nil || s.ParallelUnpacks()) {
		t.Fatal("parallel unpacking on with a containerd that can't commit onto a parent")
	}
	if strings.HasPrefix(v.Version, "v2.2") && err != nil {
		t.Fatalf("containerd 2.2 commits onto a parent: %v", err)
	}
	left := 0
	_ = s.snapshotterSvc().Walk(s.ns(ctx), func(context.Context, snapshots.Info) error { left++; return nil })
	if left != 0 {
		t.Fatalf("%d snapshot(s) left behind by the probe", left)
	}
}

func parallelStore(t *testing.T, prefix string) *ContainerdStore {
	t.Helper()
	s := streamStore(t, prefix)
	if err := s.EnableParallelUnpack(context.Background()); err != nil {
		t.Skipf("this containerd can't unpack in parallel: %v", err)
	}
	return s
}

// explicitLayers is an image whose layers carry every directory they use,
// as image builders write them (see parallelunpack.go).
func explicitLayers(salt int64) []compressedLayer {
	third := []fileEntry{
		{name: "home/", typ: tar.TypeDir, mode: 0o755},
		{name: "home/app/", typ: tar.TypeDir, mode: 0o750, uid: 1000, gid: 1000},
		{name: "home/app/cfg/", typ: tar.TypeDir, mode: 0o750, uid: 1000, gid: 1000},
		{name: "home/app/cfg/settings", typ: tar.TypeReg, mode: 0o640, uid: 1000, gid: 1000, body: "debug=false\n"},
		{name: "usr/", typ: tar.TypeDir, mode: 0o755},
		{name: "usr/bin/", typ: tar.TypeDir, mode: 0o755},
		{name: "usr/bin/.wh.suid", typ: tar.TypeReg},
		{name: "var/", typ: tar.TypeDir, mode: 0o755},
		{name: "var/log/", typ: tar.TypeDir, mode: 0o755},
		{name: "var/log/big.log", typ: tar.TypeReg, mode: 0o644, body: noisyText(salt, 8<<20)},
		{name: "etc/", typ: tar.TypeDir, mode: 0o755},
		{name: "etc/.wh..wh..opq", typ: tar.TypeReg},
		{name: "etc/hostname", typ: tar.TypeReg, mode: 0o644, body: "duck\n"},
	}
	return []compressedLayer{{entries: baseLayer}, {entries: appLayer, zstd: true}, {entries: third}}
}

// Layers landing top first are unpacked side by side and committed bottom
// up onto the right parents, with the files containerd's own unpack makes.
func TestContainerd_ParallelUnpackMakesContainerdsFiles(t *testing.T) {
	for _, stream := range []bool{false, true} {
		c := testClient(t)
		ctx := context.Background()
		s := parallelStore(t, "punpack")
		img, types := buildMixedImage(t, "angryduck.local/punpack:1", explicitLayers(time.Now().UnixNano())...)
		var sp *Spread
		var srv *httptest.Server
		if stream {
			sp, srv = streamUnpackSpread(t, s, &rateRegistry{blobs: img.blobs, rate: 16 << 20}, "pu-s")
		} else {
			sp = NewSpread(s, &rateRegistry{blobs: img.blobs}, spreadToken, "pu", "linux/amd64", "", 10*time.Minute)
			sp.EnableUnpack(s)
			mux := http.NewServeMux()
			sp.Register(mux)
			srv = httptest.NewServer(mux)
			t.Cleanup(srv.Close)
		}
		if !sp.EnableParallelUnpack(s, 3) {
			t.Fatal("refused")
		}
		for i := len(img.layers) - 1; i >= 0; i-- {
			postSpread(t, srv, "/spread/fetch", fetchOf(img, types, "pj", i))
			if !stream {
				sp.wg.Wait() // each lands before the one below is even ordered
			}
		}
		sp.wg.Wait()
		for i, ch := range img.chains {
			info, err := s.snapshotterSvc().Stat(s.ns(ctx), ch)
			if err != nil {
				t.Fatalf("stream=%t: layer %d not unpacked: %v", stream, i, err)
			}
			want := ""
			if i > 0 {
				want = img.chains[i-1]
			}
			if info.Parent != want {
				t.Fatalf("stream=%t: layer %d committed onto %q, want %q", stream, i, info.Parent, want)
			}
		}
		ref := NewContainerdStore(c, testNS(t, "punpackref"), "overlayfs")
		if err := ref.Import(ctx, bytes.NewReader(ociArchive(t, img)), "linux/amd64"); err != nil {
			t.Fatal(err)
		}
		for _, ch := range img.chains[1:] {
			sameTree(t, tree(t, ref, ch), tree(t, s, ch))
		}
		resp := postSpread(t, srv, "/spread/finalize", spreadFinalizeOf(img, "pj"))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("stream=%t: finalize %d", stream, resp.StatusCode)
		}
		left := 0
		_ = s.snapshotterSvc().Walk(s.ns(ctx), func(_ context.Context, i snapshots.Info) error {
			if i.Kind != snapshots.KindCommitted {
				left++
			}
			return nil
		})
		if left != 0 {
			t.Fatalf("stream=%t: %d unpack snapshot(s) left behind", stream, left)
		}
	}
}

// A containerd that drops the parent of a commit (1.7) never keeps the
// snapshot: it is caught on the read back and removed.
func TestContainerd_ParallelCommitWithoutRebaseIsUndone(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	if v, _ := c.Version(ctx); !strings.HasPrefix(v.Version, "v1.") {
		t.Skip("for a containerd without rebase")
	}
	s := streamStore(t, "norebase")
	img, types := buildMixedImage(t, "angryduck.local/norebase:1", compressedLayer{entries: baseLayer}, compressedLayer{entries: appLayer})
	if err := s.CreateLease(ctx, "l", time.Hour); err != nil {
		t.Fatal(err)
	}
	for _, d := range img.layers {
		if _, err := s.WriteBlob(ctx, "l", "w-"+d, d, int64(len(img.blobs[d])), bytes.NewReader(img.blobs[d])); err != nil {
			t.Fatal(err)
		}
	}
	s.parallelUnpack.Store(true) // as if the probe had passed
	spec := func(i int) LayerSpec {
		l := LayerSpec{Digest: img.layers[i], Size: int64(len(img.blobs[img.layers[i]])), MediaType: types[i], ChainID: img.chains[i]}
		if i > 0 {
			l.Parent = img.chains[i-1]
		}
		return l
	}
	if _, err := s.ApplyLayer(ctx, "l", spec(0)); err != nil {
		t.Fatal(err)
	}
	x, err := s.ExtractLayer(ctx, "l", spec(1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitExtracted(ctx, "l", spec(1), x); !errors.Is(err, errRebaseIgnored) {
		t.Fatalf("err = %v, want errRebaseIgnored", err)
	}
	if _, err := s.snapshotterSvc().Stat(s.ns(ctx), img.chains[1]); err == nil {
		t.Fatal("a snapshot committed without its parent was kept")
	}
	if s.ParallelUnpacks() {
		t.Fatal("parallel unpacking still on")
	}
}
