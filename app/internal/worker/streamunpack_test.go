package worker

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerd/containerd/mount"
	"github.com/klauspost/compress/zstd"
	digest "github.com/opencontainers/go-digest"

	"angryduck/internal/model"
)

func TestChunkPipeCarriesEveryByteInOrder(t *testing.T) {
	data := make([]byte, 3<<20+12345)
	rand.Read(data)
	p := newChunkPipe(4, 64<<10)
	go func() {
		rest := data
		for i := 1; len(rest) > 0; i++ {
			n := min(len(rest), i*7919%200000+1) // writes of every size
			if _, err := p.Write(rest[:n]); err != nil {
				t.Error(err)
				return
			}
			rest = rest[n:]
		}
		p.closeWrite(nil)
	}()
	got, err := io.ReadAll(p)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read %d bytes, err %v", len(got), err)
	}
}

func TestChunkPipeEnds(t *testing.T) {
	// The writer's error reaches the reader.
	p := newChunkPipe(2, 8)
	go func() {
		p.Write([]byte("0123456789"))
		p.closeWrite(errors.New("source gone"))
	}()
	if got, err := io.ReadAll(p); string(got) != "0123456789" || err == nil || err.Error() != "source gone" {
		t.Fatalf("got %q, %v", got, err)
	}
	// A reader that leaves stops a writer waiting for room.
	p = newChunkPipe(2, 8)
	done := make(chan error)
	go func() {
		_, err := p.Write(make([]byte, 1<<20))
		done <- err
	}()
	time.Sleep(10 * time.Millisecond)
	p.closeRead()
	select {
	case err := <-done:
		if !errors.Is(err, errChunkPipeClosed) {
			t.Fatalf("writer got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("writer still waiting after the reader left")
	}
}

func TestDecompressorReadsGzipZstdAndPlain(t *testing.T) {
	raw := bytes.Repeat([]byte("quack quack "), 50000)
	var gz, zs bytes.Buffer
	gw := gzip.NewWriter(&gz)
	gw.Write(raw)
	gw.Close()
	zw, _ := zstd.NewWriter(&zs)
	zw.Write(raw)
	zw.Close()
	for name, in := range map[string][]byte{"gzip": gz.Bytes(), "zstd": zs.Bytes(), "plain": raw} {
		r, err := decompressor(bytes.NewReader(in))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		got, err := io.ReadAll(r)
		r.Close()
		if err != nil || !bytes.Equal(got, raw) {
			t.Fatalf("%s: %d bytes, %v", name, len(got), err)
		}
	}
}

func TestWritableRootReadsTheSnapshotsMounts(t *testing.T) {
	root, parents, err := writableRoot([]mount.Mount{{Type: "bind", Source: "/s/1/fs", Options: []string{"rw", "rbind"}}})
	if err != nil || root != "/s/1/fs" || parents != nil {
		t.Fatalf("bind: %q %v %v", root, parents, err)
	}
	root, parents, err = writableRoot([]mount.Mount{{Type: "overlay", Source: "overlay",
		Options: []string{"index=off", "workdir=/s/3/work", "upperdir=/s/3/fs", "lowerdir=/s/2/fs:/s/1/fs"}}})
	if err != nil || root != "/s/3/fs" || strings.Join(parents, ",") != "/s/2/fs,/s/1/fs" {
		t.Fatalf("overlay: %q %v %v", root, parents, err)
	}
	for name, ms := range map[string][]mount.Mount{
		"userxattr":   {{Type: "overlay", Options: []string{"userxattr", "upperdir=/s/3/fs", "lowerdir=/s/2/fs"}}},
		"no upperdir": {{Type: "overlay", Options: []string{"lowerdir=/s/2/fs"}}},
		"two mounts":  {{Type: "bind", Source: "/a"}, {Type: "bind", Source: "/b"}},
		"devmapper":   {{Type: "ext4", Source: "/dev/dm-3"}},
	} {
		if _, _, err := writableRoot(ms); !errors.Is(err, errStreamUnpackUnsupported) {
			t.Errorf("%s: %v, want unsupported", name, err)
		}
	}
}

func TestStreamUnpackableLayers(t *testing.T) {
	for mt, want := range map[string]bool{
		"application/vnd.oci.image.layer.v1.tar+gzip":                  true,
		"application/vnd.docker.image.rootfs.diff.tar.gzip":            true,
		"application/vnd.oci.image.layer.v1.tar":                       true,
		"application/vnd.oci.image.layer.v1.tar+zstd":                  false, // see streamUnpackable
		"application/vnd.oci.image.layer.v1.tar+gzip+encrypted":        false,
		"application/vnd.oci.image.layer.nondistributable.v1.tar+gzip": false,
	} {
		if got := streamUnpackable(mt); got != want {
			t.Errorf("%s: %t, want %t", mt, got, want)
		}
	}
}

// fakeStreamApplier unpacks from the bytes it is handed, like
// ApplyLayerStream: the layer below first, then every byte, checked.
type fakeStreamApplier struct {
	*fakeApplier
	streamFail map[string]bool
	streamed   []string
	firstBytes chan struct{} // closed once the first bytes are read
	once       sync.Once
}

func (f *fakeStreamApplier) ApplyLayerStream(ctx context.Context, lease string, l LayerSpec, r io.Reader) (bool, error) {
	f.mu.Lock()
	if f.snaps[l.ChainID] {
		f.mu.Unlock()
		return false, nil
	}
	if l.Parent != "" && !f.snaps[l.Parent] {
		f.mu.Unlock()
		return false, errParentMissing
	}
	f.mu.Unlock()
	h := digest.Canonical.Digester()
	buf := make([]byte, 1000)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			h.Hash().Write(buf[:n])
			if f.firstBytes != nil {
				f.once.Do(func() { close(f.firstBytes) })
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return false, err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if h.Digest().String() != l.Digest {
		return false, errors.New("not the blob")
	}
	if f.streamFail[l.ChainID] {
		return false, errors.New("unpack failed")
	}
	f.snaps[l.ChainID] = true
	f.order = append(f.order, l.ChainID)
	f.streamed = append(f.streamed, l.ChainID)
	return true, nil
}

func streamUnpackTestSpread(t *testing.T, ap *fakeStreamApplier) (*Spread, string, *pacedRegistry) {
	t.Helper()
	store, reg := newIngestStore(t), newPacedRegistry()
	store.chunk = 4096
	sp, srv := newTestSpread(t, store.fakeSpreadStore, nil, newControllerSink(t), "n1")
	sp.store, sp.registry, sp.ingest = store, reg, store
	sp.EnableUnpack(ap)
	if !sp.EnableStreamUnpack(ap) {
		t.Fatal("refused")
	}
	return sp, srv.URL, reg
}

// A layer is unpacked from its blob's bytes while they still arrive.
func TestSpreadUnpacksALayerWhileItArrives(t *testing.T) {
	blob := make([]byte, 200000)
	rand.Read(blob)
	d := digestOf(blob)
	ap := &fakeStreamApplier{fakeApplier: newFakeApplier(), firstBytes: make(chan struct{})}
	sp, srv, reg := streamUnpackTestSpread(t, ap)
	reg.size[d] = int64(len(blob))
	o := model.SpreadFetch{Job: "s1", Image: "r.io/app:1", Digest: d, Size: int64(len(blob)), Path: model.SpreadPathRegistry,
		Unpack: &model.SpreadUnpack{ChainID: "sha256:" + strings.Repeat("c", 64), MediaType: "application/vnd.oci.image.layer.v1.tar+gzip"}}
	if resp := postSpreadURL(t, srv, "/spread/fetch", o); resp.StatusCode != 202 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	<-reg.ready
	reg.pipe(d).Write(blob[:100000])
	select {
	case <-ap.firstBytes:
	case <-time.After(5 * time.Second):
		t.Fatal("the unpack didn't start before the blob was complete")
	}
	reg.pipe(d).Write(blob[100000:])
	reg.pipe(d).Close()
	sp.wg.Wait()
	if len(ap.streamed) != 1 || len(ap.order) != 1 {
		t.Fatalf("streamed %v, applied %v", ap.streamed, ap.order)
	}
}

// When unpacking while it arrives fails, the layer is unpacked through
// containerd once it lands.
func TestSpreadStreamUnpackFailureFallsBackOnceLanded(t *testing.T) {
	blob := make([]byte, 50000)
	rand.Read(blob)
	d := digestOf(blob)
	chain := "sha256:" + strings.Repeat("d", 64)
	ap := &fakeStreamApplier{fakeApplier: newFakeApplier(), streamFail: map[string]bool{chain: true}}
	sp, srv, reg := streamUnpackTestSpread(t, ap)
	reg.size[d] = int64(len(blob))
	o := model.SpreadFetch{Job: "s1", Image: "r.io/app:1", Digest: d, Size: int64(len(blob)), Path: model.SpreadPathRegistry,
		Unpack: &model.SpreadUnpack{ChainID: chain, MediaType: "application/vnd.oci.image.layer.v1.tar"}}
	postSpreadURL(t, srv, "/spread/fetch", o)
	<-reg.ready
	reg.pipe(d).Write(blob)
	reg.pipe(d).Close()
	sp.wg.Wait()
	if len(ap.streamed) != 0 || len(ap.order) != 1 || ap.order[0] != chain {
		t.Fatalf("streamed %v, applied %v: want it applied once landed", ap.streamed, ap.order)
	}
}

// Layers a stream unpack doesn't take (zstd) wait for their blob.
func TestSpreadLeavesZstdLayersToUnpackOnceLanded(t *testing.T) {
	blob := make([]byte, 50000)
	rand.Read(blob)
	d := digestOf(blob)
	chain := "sha256:" + strings.Repeat("e", 64)
	ap := &fakeStreamApplier{fakeApplier: newFakeApplier()}
	sp, srv, reg := streamUnpackTestSpread(t, ap)
	reg.size[d] = int64(len(blob))
	o := model.SpreadFetch{Job: "s1", Image: "r.io/app:1", Digest: d, Size: int64(len(blob)), Path: model.SpreadPathRegistry,
		Unpack: &model.SpreadUnpack{ChainID: chain, MediaType: "application/vnd.oci.image.layer.v1.tar+zstd"}}
	postSpreadURL(t, srv, "/spread/fetch", o)
	<-reg.ready
	reg.pipe(d).Write(blob)
	reg.pipe(d).Close()
	sp.wg.Wait()
	if len(ap.streamed) != 0 || len(ap.order) != 1 {
		t.Fatalf("streamed %v, applied %v", ap.streamed, ap.order)
	}
}
