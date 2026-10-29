package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"angryduck/internal/model"
)

// ingestStore is fakeSpreadStore with containerd's ingest files: a write
// lands in a file as it goes, reported to the write observer, and the file
// is unlinked when the write commits or fails (a reader that opened it
// keeps reading, as with containerd's rename).
type ingestStore struct {
	*fakeSpreadStore
	dir   string
	mu    sync.Mutex
	opens int
	chunk int // write size; small by default so streams see many partial states
}

func newIngestStore(t *testing.T) *ingestStore {
	return &ingestStore{fakeSpreadStore: newFakeSpreadStore(), dir: t.TempDir()}
}

func (s *ingestStore) CanStream() bool { return true }

func (s *ingestStore) OpenIngest(ref string) (*os.File, error) {
	s.mu.Lock()
	s.opens++
	s.mu.Unlock()
	return os.Open(filepath.Join(s.dir, ref))
}

func (s *ingestStore) WriteBlob(ctx context.Context, lease, ref, d string, size int64, r io.Reader) (int64, error) {
	path := filepath.Join(s.dir, ref)
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	defer os.Remove(path)
	defer f.Close()
	obs := writeObserverFrom(ctx)
	if obs != nil {
		obs.opened()
	}
	var all bytes.Buffer
	chunk := s.chunk
	if chunk == 0 {
		chunk = 7
	}
	buf := make([]byte, chunk)
	var n int64
	for n < size {
		k, rerr := r.Read(buf)
		if k > 0 {
			if _, err := f.Write(buf[:k]); err != nil {
				return n, err
			}
			all.Write(buf[:k])
			n += int64(k)
			if obs != nil {
				obs.wrote(n)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return n, rerr
		}
	}
	if n != size || digestOf(all.Bytes()) != d {
		return n, fmt.Errorf("content mismatch: %d bytes", n)
	}
	s.fakeSpreadStore.mu.Lock()
	defer s.fakeSpreadStore.mu.Unlock()
	if s.leases[lease] == nil {
		return n, errors.New("no such lease " + lease)
	}
	s.blobs[d] = all.Bytes()
	s.leases[lease][d] = true
	return n, nil
}

// pacedRegistry hands out a blob only as fast as the test feeds it.
type pacedRegistry struct {
	mu    sync.Mutex
	pipes map[string]*io.PipeWriter
	size  map[string]int64
	ready chan string
}

func newPacedRegistry() *pacedRegistry {
	return &pacedRegistry{pipes: map[string]*io.PipeWriter{}, size: map[string]int64{}, ready: make(chan string, 8)}
}

func (p *pacedRegistry) OpenBlob(ctx context.Context, _, d string) (io.ReadCloser, int64, error) {
	pr, pw := io.Pipe()
	p.mu.Lock()
	p.pipes[d] = pw
	size := p.size[d]
	p.mu.Unlock()
	go func() {
		<-ctx.Done()
		pw.CloseWithError(ctx.Err())
	}()
	p.ready <- d
	return pr, size, nil
}

func (p *pacedRegistry) pipe(d string) *io.PipeWriter {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pipes[d]
}

func getContent(t *testing.T, base, d string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, base+"/spread/content/"+d, nil)
	req.Header.Set("Authorization", "Bearer "+spreadToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func startRegistryFetch(t *testing.T, srv string, reg *pacedRegistry, job, d string, size int64) {
	t.Helper()
	resp := postSpreadURL(t, srv, "/spread/fetch", model.SpreadFetch{Job: job, Image: "r.io/app:1", Digest: d, Size: size, Path: model.SpreadPathRegistry})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("fetch status %d", resp.StatusCode)
	}
	select {
	case <-reg.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("registry never opened")
	}
}

func postSpreadURL(t *testing.T, base, path string, v any) *http.Response {
	t.Helper()
	b, _ := jsonMarshal(v)
	req, _ := http.NewRequest(http.MethodPost, base+path, bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+spreadToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

// readN reads exactly n bytes or fails the test after a while.
func readN(t *testing.T, r io.Reader, n int) []byte {
	t.Helper()
	out := make(chan []byte, 1)
	go func() {
		b := make([]byte, n)
		k, _ := io.ReadFull(r, b)
		out <- b[:k]
	}()
	select {
	case b := <-out:
		if len(b) != n {
			t.Fatalf("read %d bytes, want %d", len(b), n)
		}
		return b
	case <-time.After(5 * time.Second):
		t.Fatalf("no %d bytes within 5s", n)
	}
	return nil
}

func TestSpreadStreamsABlobWhileItIsStillArriving(t *testing.T) {
	blob := []byte(strings.Repeat("0123456789abcdef", 4096)) // 64 KiB
	d := digestOf(blob)
	store, reg, ctl := newIngestStore(t), newPacedRegistry(), newControllerSink(t)
	reg.size[d] = int64(len(blob))
	sp, srv := newTestSpread(t, store.fakeSpreadStore, nil, ctl, "b")
	sp.store, sp.registry, sp.ingest = store, reg, store

	startRegistryFetch(t, srv.URL, reg, "s1", d, int64(len(blob)))
	half := len(blob) / 2
	if _, err := reg.pipe(d).Write(blob[:half]); err != nil {
		t.Fatal(err)
	}
	resp := getContent(t, srv.URL, d)
	if resp.StatusCode != http.StatusOK || resp.Header.Get(StreamHeader) != "1" || resp.ContentLength != int64(len(blob)) {
		t.Fatalf("status %d stream %q length %d", resp.StatusCode, resp.Header.Get(StreamHeader), resp.ContentLength)
	}
	// The first half arrives before the source has sent the rest.
	if got := readN(t, resp.Body, half); !bytes.Equal(got, blob[:half]) {
		t.Fatal("first half differs")
	}
	if _, err := reg.pipe(d).Write(blob[half:]); err != nil {
		t.Fatal(err)
	}
	reg.pipe(d).Close()
	rest, err := io.ReadAll(resp.Body)
	if err != nil || !bytes.Equal(rest, blob[half:]) {
		t.Fatalf("rest: %d bytes, err %v", len(rest), err)
	}
	sp.wg.Wait()
	if got := ctl.byPath()[model.SpreadPathRegistry]; got != model.SpreadResultOK {
		t.Fatalf("source's own transfer = %q", got)
	}
	sp.streamMu.Lock()
	left := len(sp.arriving)
	sp.streamMu.Unlock()
	if left != 0 {
		t.Fatalf("%d blobs still listed as arriving", left)
	}
}

func TestSpreadStreamIsCutWhenItsSourceFails(t *testing.T) {
	blob := []byte(strings.Repeat("x", 10000))
	d := digestOf(blob)
	store, reg, ctl := newIngestStore(t), newPacedRegistry(), newControllerSink(t)
	reg.size[d] = int64(len(blob))
	sp, srv := newTestSpread(t, store.fakeSpreadStore, nil, ctl, "b")
	sp.store, sp.registry, sp.ingest = store, reg, store

	startRegistryFetch(t, srv.URL, reg, "s1", d, int64(len(blob)))
	_, _ = reg.pipe(d).Write(blob[:3000])
	resp := getContent(t, srv.URL, d)
	readN(t, resp.Body, 3000)
	reg.pipe(d).CloseWithError(errors.New("registry went away"))
	b, err := io.ReadAll(resp.Body)
	if err == nil {
		t.Fatalf("stream ended cleanly after %d more bytes; want it cut short", len(b))
	}
	sp.wg.Wait()
}

// The blob arrives over the other path (a peer push wins the race): the
// stream fed by the losing registry pull goes on from the committed blob.
func TestSpreadStreamHandsOverToTheCommittedBlob(t *testing.T) {
	blob := []byte(strings.Repeat("handoff!", 3000))
	d := digestOf(blob)
	store, reg, ctl := newIngestStore(t), newPacedRegistry(), newControllerSink(t)
	reg.size[d] = int64(len(blob))
	sp, srv := newTestSpread(t, store.fakeSpreadStore, nil, ctl, "b")
	sp.store, sp.registry, sp.ingest = store, reg, store

	// A peer that has the whole blob, for the second path.
	peerStore := newFakeSpreadStore()
	peerStore.blobs[d] = blob
	_, peer := newTestSpread(t, peerStore, &fakeRegistry{}, nil, "a")

	startRegistryFetch(t, srv.URL, reg, "s1", d, int64(len(blob)))
	_, _ = reg.pipe(d).Write(blob[:1000])
	resp := getContent(t, srv.URL, d)
	readN(t, resp.Body, 1000)

	// The peer path commits; the registry pull is cancelled by the worker.
	r := postSpreadURL(t, srv.URL, "/spread/fetch", model.SpreadFetch{Job: "s1", Digest: d, Size: int64(len(blob)), Path: model.SpreadPathPeer,
		Peer: &model.Holder{NodeID: "a", Address: strings.TrimPrefix(peer.URL, "http://")}})
	if r.StatusCode != http.StatusAccepted {
		t.Fatalf("peer fetch status %d", r.StatusCode)
	}
	rest, err := io.ReadAll(resp.Body)
	if err != nil || !bytes.Equal(rest, blob[1000:]) {
		t.Fatalf("after the handoff: %d bytes, err %v", len(rest), err)
	}
	sp.wg.Wait()
	got := ctl.byPath()
	if got[model.SpreadPathPeer] != model.SpreadResultOK || got[model.SpreadPathRegistry] != model.SpreadResultCancelled {
		t.Fatalf("reports %v", got)
	}
}

// A receiver streaming from a peer whose own transfer fails reports it as
// the source's failure, so the controller doesn't back it off; any other
// failure is still its own.
func TestSpreadReceiverReportsAStreamCutShortAsUpstream(t *testing.T) {
	blob := []byte(strings.Repeat("u", 20000))
	d := digestOf(blob)
	srcStore, reg := newIngestStore(t), newPacedRegistry()
	reg.size[d] = int64(len(blob))
	src, srcSrv := newTestSpread(t, srcStore.fakeSpreadStore, nil, newControllerSink(t), "b")
	src.store, src.registry, src.ingest = srcStore, reg, srcStore
	startRegistryFetch(t, srcSrv.URL, reg, "s1", d, int64(len(blob)))
	_, _ = reg.pipe(d).Write(blob[:5000])

	ctl := newControllerSink(t)
	recv, recvSrv := newTestSpread(t, newFakeSpreadStore(), &fakeRegistry{}, ctl, "c")
	r := postSpreadURL(t, recvSrv.URL, "/spread/fetch", model.SpreadFetch{Job: "s1", Digest: d, Size: int64(len(blob)), Path: model.SpreadPathPeer,
		Peer: &model.Holder{NodeID: "b", Address: strings.TrimPrefix(srcSrv.URL, "http://")}, Stream: true})
	if r.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d", r.StatusCode)
	}
	time.Sleep(100 * time.Millisecond) // the receiver is reading the stream
	reg.pipe(d).CloseWithError(errors.New("registry went away"))
	recv.wg.Wait()
	src.wg.Wait()
	ctl.mu.Lock()
	reports := append([]model.SpreadDone(nil), ctl.reports...)
	ctl.mu.Unlock()
	if len(reports) != 1 || reports[0].Result != model.SpreadResultFailed || !reports[0].Upstream {
		t.Fatalf("reports %+v, want one failed with upstream", reports)
	}

	// Nothing arriving there any more and not committed: 404, still upstream.
	r = postSpreadURL(t, recvSrv.URL, "/spread/fetch", model.SpreadFetch{Job: "s1", Digest: d, Size: int64(len(blob)), Path: model.SpreadPathPeer,
		Peer: &model.Holder{NodeID: "b", Address: strings.TrimPrefix(srcSrv.URL, "http://")}, Stream: true})
	if r.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d", r.StatusCode)
	}
	recv.wg.Wait()
	ctl.mu.Lock()
	last := ctl.reports[len(ctl.reports)-1]
	ctl.mu.Unlock()
	if last.Result != model.SpreadResultFailed || !last.Upstream {
		t.Fatalf("report %+v, want failed with upstream", last)
	}

	// A plain peer fetch that fails is the receiver's own failure.
	r = postSpreadURL(t, recvSrv.URL, "/spread/fetch", model.SpreadFetch{Job: "s1", Digest: d, Size: int64(len(blob)), Path: model.SpreadPathPeer,
		Peer: &model.Holder{NodeID: "b", Address: strings.TrimPrefix(srcSrv.URL, "http://")}})
	if r.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d", r.StatusCode)
	}
	recv.wg.Wait()
	ctl.mu.Lock()
	last = ctl.reports[len(ctl.reports)-1]
	ctl.mu.Unlock()
	if last.Result != model.SpreadResultFailed || last.Upstream {
		t.Fatalf("report %+v, want failed, not upstream", last)
	}
}

// A store that can't stream serves only committed blobs, as before.
func TestSpreadWithoutIngestServesOnlyCommittedBlobs(t *testing.T) {
	blob := []byte(strings.Repeat("n", 5000))
	d := digestOf(blob)
	reg := newPacedRegistry()
	reg.size[d] = int64(len(blob))
	sp, srv := newTestSpread(t, newFakeSpreadStore(), nil, newControllerSink(t), "b")
	sp.registry = reg
	if sp.Streams() {
		t.Fatal("a store without ingest files must not stream")
	}
	startRegistryFetch(t, srv.URL, reg, "s1", d, int64(len(blob)))
	_, _ = reg.pipe(d).Write(blob[:100])
	if resp := getContent(t, srv.URL, d); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status %d, want 404 for a blob not committed", resp.StatusCode)
	}
	reg.pipe(d).CloseWithError(errors.New("done"))
	sp.wg.Wait()
}

func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }

func httptestServer(t *testing.T, h http.Handler) string {
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL
}
