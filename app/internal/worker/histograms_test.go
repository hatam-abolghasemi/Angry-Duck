package worker

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"testing"
	"time"

	"github.com/klauspost/compress/gzip"
)

func TestSizeClass(t *testing.T) {
	for _, c := range []struct {
		n    int64
		want string
	}{
		{0, "small"},
		{sizeSmallMax - 1, "small"},
		{sizeSmallMax, "medium"},
		{sizeMediumMax - 1, "medium"},
		{sizeMediumMax, "large"},
		{5 << 30, "large"},
	} {
		if got := sizeClass(c.n); got != c.want {
			t.Errorf("sizeClass(%d) = %q, want %q", c.n, got, c.want)
		}
	}
}

func TestCompressionOf(t *testing.T) {
	for mt, want := range map[string]string{
		"application/vnd.oci.image.layer.v1.tar+gzip":                  "gzip",
		"application/vnd.docker.image.rootfs.diff.tar.gzip":            "gzip",
		"application/vnd.oci.image.layer.nondistributable.v1.tar+gzip": "gzip",
		"application/vnd.oci.image.layer.v1.tar+zstd":                  "zstd",
		"application/vnd.oci.image.layer.v1.tar":                       "none",
		"application/vnd.docker.image.rootfs.diff.tar":                 "none",
		"application/vnd.oci.image.layer.v1.tar+lz4":                   "other",
		"": "other",
	} {
		if got := compressionOf(mt); got != want {
			t.Errorf("compressionOf(%q) = %q, want %q", mt, got, want)
		}
	}
}

// The stream unpack is told by its caller whether the blob still arrives.
func TestStreamUnpackMethodComesFromTheCaller(t *testing.T) {
	ctx := context.Background()
	if got := streamUnpackMethod(ctx); got != unpackStreamed {
		t.Errorf("default = %q, want %q", got, unpackStreamed)
	}
	// Survives the contexts derived on the way to the store.
	c, cancel := context.WithTimeout(withUnpackMethod(ctx, unpackLandedOwn), time.Minute)
	defer cancel()
	if got := streamUnpackMethod(c); got != unpackLandedOwn {
		t.Errorf("got %q, want %q", got, unpackLandedOwn)
	}
	if got := importCaller(ctx); got != "other" {
		t.Errorf("import caller default = %q", got)
	}
	if got := importCaller(withImportCaller(ctx, importByRescue)); got != importByRescue {
		t.Errorf("import caller = %q", got)
	}
}

// The helpers must not panic with the label counts the histograms were
// declared with (a mismatch would only show at scrape time otherwise).
func TestObserveHelpersMatchLabelCounts(t *testing.T) {
	SetMetricsNode("test-node")
	start := time.Now().Add(-time.Second)
	l := LayerSpec{MediaType: "application/vnd.oci.image.layer.v1.tar+gzip", Size: 100 << 20}
	observeBlobFetch("registry", 10<<20, time.Second)
	observeBlobOpen("peer", start)
	observeBlobCommit("api", 1<<20, start)
	observeUnpack(unpackStreamed, l, time.Second)
	observeUnpackStages(l, unpackStages{input: time.Second})
	observeLayerCommit(commitAttach, start)
	observeImport(withImportCaller(context.Background(), importBySpread), "publish", start)
	observeSince(spreadReadySeconds, start)
	if hnode() != "test-node" {
		t.Errorf("hnode() = %q", hnode())
	}
	if got := layerUnpackSeconds.Count("test-node", unpackStreamed, "gzip", "large"); got != 1 {
		t.Errorf("unpack count = %d, want 1", got)
	}
	if got := importSeconds.Count("test-node", importBySpread, "publish"); got != 1 {
		t.Errorf("import count = %d, want 1", got)
	}
}

// Each side of a chunk pipe adds up only the time it waited on the other.
func TestChunkPipeCountsWaits(t *testing.T) {
	p := newChunkPipe(1, 4)
	go func() {
		time.Sleep(40 * time.Millisecond) // the reader waits for this
		_, _ = p.Write([]byte("abcd"))
		_, _ = p.Write([]byte("efgh")) // waits for the reader to free the chunk
		p.closeWrite(nil)
	}()
	b := make([]byte, 2) // half the chunk: it stays held, not free
	if _, err := io.ReadFull(p, b); err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond) // the writer waits for this
	if _, err := io.ReadAll(p); err != nil {
		t.Fatal(err)
	}
	if p.readWait < 30*time.Millisecond {
		t.Errorf("readWait = %s, want about 40ms", p.readWait)
	}
	if p.writeWait < 30*time.Millisecond {
		t.Errorf("writeWait = %s, want about 40ms", p.writeWait)
	}
}

// slowStart is a blob whose bytes start arriving after a delay.
type slowStart struct {
	r     io.Reader
	delay time.Duration
	once  bool
}

func (s *slowStart) Read(b []byte) (int, error) {
	if !s.once {
		s.once = true
		time.Sleep(s.delay)
	}
	return s.r.Read(b)
}

// streamInto says where an unpack's time went: here the blob is late, so
// the time goes to input, and decompressing and writing don't count it.
func TestStreamIntoSaysWhereTheTimeWent(t *testing.T) {
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	body := bytes.Repeat([]byte("angry duck "), 4096)
	if err := tw.WriteHeader(&tar.Header{Name: "f", Mode: 0o644, Size: int64(len(body)),
		Uid: os.Getuid(), Gid: os.Getgid(), Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	_, _ = tw.Write(body)
	_ = tw.Close()
	sum := sha256.Sum256(tarBuf.Bytes())
	diffID := "sha256:" + hex.EncodeToString(sum[:])
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	_, _ = zw.Write(tarBuf.Bytes())
	_ = zw.Close()
	l := LayerSpec{Digest: "sha256:unchecked", Size: int64(gz.Len()),
		MediaType: "application/vnd.oci.image.layer.v1.tar+gzip", ChainID: chainOf("", diffID)}

	const delay = 60 * time.Millisecond
	got, st, err := streamInto(context.Background(), t.TempDir(), nil, nil, l, &slowStart{r: &gz, delay: delay})
	if err != nil {
		t.Fatal(err)
	}
	if got != diffID {
		t.Fatalf("diffID %s, want %s", got, diffID)
	}
	if st.input < delay {
		t.Errorf("input = %s, want at least %s", st.input, delay)
	}
	// Both other stages waited out the late blob without counting it.
	if st.decompress >= delay || st.decompress < 0 {
		t.Errorf("decompress = %s, want under %s", st.decompress, delay)
	}
	if st.write >= delay || st.write < 0 {
		t.Errorf("write = %s, want under %s", st.write, delay)
	}
}

// Every stage a node can go through has its series at zero from the
// start, so the first spread after a restart is seen by rate(); stages of
// features that are off get none.
func TestInitTimingsCreatesWhatTheNodeCanDo(t *testing.T) {
	SetMetricsNode("init-node")
	InitTimings(TimingSeries{Spread: true, Unpack: true, DirectWrites: true, Repair: true})
	n := "init-node"
	for _, c := range []struct {
		name string
		ok   bool
	}{
		{"ready", spreadReadySeconds.Exists(n)},
		{"hold", spreadFinalizeSeconds.Exists(n, "hold")},
		{"drain", spreadFinalizeSeconds.Exists(n, "drain")},
		{"registry fetch", blobFetchSeconds.Exists(n, "registry", "large")},
		{"peer open", blobOpenSeconds.Exists(n, "peer")},
		{"direct commit", blobCommitSeconds.Exists(n, "direct", "medium")},
		{"api commit", blobCommitSeconds.Exists(n, "api", "small")},
		{"landed zstd unpack", layerUnpackSeconds.Exists(n, unpackLanded, "zstd", "large")},
		{"in-place commit", layerCommitSeconds.Exists(n, commitInPlace)},
		{"spread import publish", importSeconds.Exists(n, importBySpread, "publish")},
		{"repair import register", importSeconds.Exists(n, importByRepair, "register")},
	} {
		if !c.ok {
			t.Errorf("%s: no series at zero", c.name)
		}
	}
	for _, c := range []struct {
		name string
		ok   bool
	}{
		{"streamed unpack (stream unpack off)", layerUnpackSeconds.Exists(n, unpackStreamed, "gzip", "small")},
		{"stages (stream unpack off)", layerUnpackStageSeconds.Exists(n, "input", "gzip")},
		{"attach (stream unpack off)", layerCommitSeconds.Exists(n, commitAttach)},
		{"onto_parent (parallel off)", layerCommitSeconds.Exists(n, commitOntoParent)},
		{"rescue import (rescue off)", importSeconds.Exists(n, importByRescue, "register")},
		{"snapshot receive (rescue off)", snapshotReceiveSeconds.Exists(n, "small")},
	} {
		if c.ok {
			t.Errorf("%s: series created for a stage this node can't go through", c.name)
		}
	}
	if got := spreadReadySeconds.Count(n); got != 0 {
		t.Errorf("ready count %d, want 0", got)
	}
}
