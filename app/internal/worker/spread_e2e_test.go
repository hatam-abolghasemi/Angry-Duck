package worker

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"angryduck/internal/blobship"
	"angryduck/internal/controller"
	"angryduck/internal/imageref"
	"angryduck/internal/layerindex"
	"angryduck/internal/model"
	"angryduck/internal/registryclient"
)

type oneImage struct{ res registryclient.Resolved }

func (o oneImage) Resolve(context.Context, string, string) (registryclient.Resolved, error) {
	return o.res, nil
}

// dripRegistry serves blobs slowly, counting every pull.
type dripRegistry struct {
	blobs map[string][]byte
	pulls atomic.Int32
	chunk int
	every time.Duration
}

func (r *dripRegistry) OpenBlob(ctx context.Context, _, d string) (io.ReadCloser, int64, error) {
	r.pulls.Add(1)
	b := r.blobs[d]
	pr, pw := io.Pipe()
	go func() {
		for off := 0; off < len(b); off += r.chunk {
			end := off + r.chunk
			if end > len(b) {
				end = len(b)
			}
			select {
			case <-ctx.Done():
				pw.CloseWithError(ctx.Err())
				return
			case <-time.After(r.every):
			}
			if _, err := pw.Write(b[off:end]); err != nil {
				return
			}
		}
		pw.Close()
	}()
	return pr, int64(len(b)), nil
}

// The real controller spreading over real worker handlers: a blob still
// coming from the registry is streamed on, every node ends up with the
// verified blob and the image, and the registry serves it once.
func TestSpreadEndToEndStreamsWhileTheRegistryIsStillSending(t *testing.T) {
	blob := make([]byte, 3<<20)
	if _, err := rand.Read(blob); err != nil {
		t.Fatal(err)
	}
	d := digestOf(blob)
	top := []byte(`{"top":true}`)
	topDig := digestOf(top)
	image := imageref.Normalize("registry.example.com/team/app:9.9.9")
	res := registryclient.Resolved{
		Top:      blobship.Descriptor{MediaType: blobship.MediaTypeOCIManifest, Digest: topDig, Size: int64(len(top))},
		Metadata: map[string][]byte{topDig: top},
		Layers:   []layerindex.Layer{{Digest: d, ChainID: "sha256:chain", Size: int64(len(blob))}},
	}
	reg := controller.NewRegistry(time.Hour, time.Minute)
	sp := controller.NewSpreader(reg, oneImage{res}, spreadToken, controller.SpreaderConfig{
		Interval: 200 * time.Millisecond, MaxRegistry: 1, RacePerBlob: 1, PerSource: 1, MaxUtilization: 0.9,
		RetryAfter: time.Second, BackoffMax: time.Second, TransferTimeout: time.Minute, ResolveTimeout: time.Second,
		Platform: "linux/amd64", Stream: true, StreamMinBytes: 1, StreamMaxDepth: 2,
	})
	ctlMux := http.NewServeMux()
	ctlMux.HandleFunc("/spread/done", sp.HandleDone)
	ctl := httptest.NewServer(ctlMux)
	t.Cleanup(ctl.Close)

	// About 3 s from the registry: long enough for streams to start.
	registry := &dripRegistry{blobs: map[string][]byte{d: blob}, chunk: 64 << 10, every: 60 * time.Millisecond}
	nodes := []string{"n1", "n2", "n3", "n4"}
	stores := map[string]*ingestStore{}
	spreads := map[string]*Spread{}
	for _, n := range nodes {
		st := newIngestStore(t)
		st.chunk = 32 << 10
		st.topOf = func() (string, string) { return image, topDig }
		w := NewSpread(st, registry, spreadToken, n, "linux/amd64", ctl.URL, time.Minute)
		if !w.Streams() {
			t.Fatal("worker over an ingest store must stream")
		}
		mux := http.NewServeMux()
		w.Register(mux)
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)
		stores[n], spreads[n] = st, w
		reg.Update(model.WorkerReport{NodeID: n, Address: strings.TrimPrefix(srv.URL, "http://"), Utilization: 0.2, Streams: true, Timestamp: time.Now()})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sp.Run(ctx)
	if _, err := sp.Start(ctx, image); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(20 * time.Second)
	sawStream := false
	for {
		for _, s := range sp.Status() {
			for _, n := range s.Nodes {
				for _, x := range n.Transfers {
					if x.Stream {
						sawStream = true
					}
				}
			}
		}
		all := true
		for _, n := range nodes {
			if _, top, err := stores[n].Resolve(ctx, image); err != nil || top != topDig {
				all = false
			}
		}
		if all {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not every node registered the image in time: %+v", sp.Status())
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	var wg sync.WaitGroup
	for _, w := range spreads {
		wg.Add(1)
		go func(w *Spread) { defer wg.Done(); w.wg.Wait() }(w)
	}
	wg.Wait()
	for _, n := range nodes {
		stores[n].fakeSpreadStore.mu.Lock()
		got := stores[n].blobs[d]
		stores[n].fakeSpreadStore.mu.Unlock()
		if !bytes.Equal(got, blob) {
			t.Fatalf("%s: blob differs (%d bytes)", n, len(got))
		}
	}
	if !sawStream {
		t.Fatal("no node streamed the blob while the registry was still sending it")
	}
	if p := registry.pulls.Load(); p != 1 {
		t.Fatalf("registry served the blob %d times, want once", p)
	}
}
