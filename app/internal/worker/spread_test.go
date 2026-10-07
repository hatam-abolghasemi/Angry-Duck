package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"angryduck/internal/model"
)

const spreadToken = "0123456789abcdef0123456789abcdef"

func digestOf(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// fakeSpreadStore is a content store with leases, in memory.
type fakeSpreadStore struct {
	mu       sync.Mutex
	blobs    map[string][]byte
	leases   map[string]map[string]bool // lease -> digests
	imported []string
	images   map[string]string // name -> top digest
	topOf    func() (name, digest string)
}

func newFakeSpreadStore() *fakeSpreadStore {
	return &fakeSpreadStore{blobs: map[string][]byte{}, leases: map[string]map[string]bool{}, images: map[string]string{}}
}

func (f *fakeSpreadStore) HasBlob(d string) (bool, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.blobs[d]
	return ok, true
}
func (f *fakeSpreadStore) Digests(context.Context) (map[string]bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]bool{}
	for d := range f.blobs {
		out[d] = true
	}
	return out, nil
}
func (f *fakeSpreadStore) WriteBlob(ctx context.Context, lease, ref, d string, size int64, r io.Reader) (int64, error) {
	var buf bytes.Buffer
	n, err := io.Copy(&buf, io.LimitReader(r, size+1))
	if err != nil {
		return n, err
	}
	if n != size || digestOf(buf.Bytes()) != d {
		return n, fmt.Errorf("content mismatch: %d bytes, digest %s", n, digestOf(buf.Bytes()))
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.leases[lease] == nil {
		return n, errors.New("no such lease " + lease)
	}
	f.blobs[d] = buf.Bytes()
	f.leases[lease][d] = true
	return n, nil
}
func (f *fakeSpreadStore) CreateLease(_ context.Context, id string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.leases[id] == nil {
		f.leases[id] = map[string]bool{}
	}
	return nil
}
func (f *fakeSpreadStore) MoveLease(_ context.Context, from, to string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leases[to] = f.leases[from]
	delete(f.leases, from)
	return nil
}
func (f *fakeSpreadStore) DeleteLease(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.leases, id)
	return nil
}
func (f *fakeSpreadStore) Import(_ context.Context, r io.Reader, _ string) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.imported = append(f.imported, string(b))
	if f.topOf != nil {
		n, d := f.topOf()
		f.images[n] = d
	}
	return nil
}
func (f *fakeSpreadStore) Resolve(_ context.Context, image string) (string, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.images[image]
	if !ok {
		return "", "", errors.New("not found")
	}
	return "", d, nil
}
func (f *fakeSpreadStore) StreamContent(_ context.Context, d string, w io.Writer) error {
	f.mu.Lock()
	b, ok := f.blobs[d]
	f.mu.Unlock()
	if !ok {
		return errors.New("not found")
	}
	_, err := w.Write(b)
	return err
}
func (f *fakeSpreadStore) leaseCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.leases)
}

// fakeRegistry serves blobs; a stalled one blocks until the fetch is
// cancelled, like a registry that's very slow for this node.
type fakeRegistry struct {
	blobs   map[string][]byte
	stalled map[string]bool
}

func (r *fakeRegistry) OpenBlob(ctx context.Context, _, d string) (io.ReadCloser, int64, error) {
	b, ok := r.blobs[d]
	if !ok {
		return nil, 0, errors.New("404")
	}
	if r.stalled[d] {
		pr, pw := io.Pipe()
		go func() {
			_, _ = pw.Write(b[:1]) // some progress, then nothing
			<-ctx.Done()
			pw.CloseWithError(ctx.Err())
		}()
		return pr, int64(len(b)), nil
	}
	return io.NopCloser(bytes.NewReader(b)), int64(len(b)), nil
}

// controllerSink collects SpreadDone reports.
type controllerSink struct {
	mu      sync.Mutex
	reports []model.SpreadDone
	srv     *httptest.Server
}

func newControllerSink(t *testing.T) *controllerSink {
	c := &controllerSink{}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+spreadToken {
			t.Errorf("report without the token")
		}
		var d model.SpreadDone
		_ = json.NewDecoder(r.Body).Decode(&d)
		c.mu.Lock()
		c.reports = append(c.reports, d)
		c.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(c.srv.Close)
	return c
}

func (c *controllerSink) byPath() map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]string{}
	for _, r := range c.reports {
		out[r.Path] = r.Result
	}
	return out
}

func newTestSpread(t *testing.T, store *fakeSpreadStore, reg *fakeRegistry, ctl *controllerSink, node string) (*Spread, *httptest.Server) {
	url := ""
	if ctl != nil {
		url = ctl.srv.URL
	}
	sp := NewSpread(store, reg, spreadToken, node, "linux/amd64", url, time.Minute)
	mux := http.NewServeMux()
	sp.Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return sp, srv
}

func postSpread(t *testing.T, srv *httptest.Server, path string, v any) *http.Response {
	t.Helper()
	b, _ := json.Marshal(v)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+path, bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+spreadToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestSpreadFetchesFromRegistryUnderTheJobLeaseAndReports(t *testing.T) {
	blob := []byte(strings.Repeat("layer", 1000))
	d := digestOf(blob)
	store, ctl := newFakeSpreadStore(), newControllerSink(t)
	sp, srv := newTestSpread(t, store, &fakeRegistry{blobs: map[string][]byte{d: blob}}, ctl, "n1")
	resp := postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Image: "r.io/app:1", Digest: d, Size: int64(len(blob)), Path: model.SpreadPathRegistry})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d", resp.StatusCode)
	}
	sp.wg.Wait()
	if !store.leases["angryduck-spread-s1-"+sp.epoch+"-0"][d] {
		t.Fatalf("blob not held by the job's lease: %v", store.leases)
	}
	if got := ctl.byPath()[model.SpreadPathRegistry]; got != model.SpreadResultOK {
		t.Fatalf("report = %q", got)
	}
	// Now it's here: another order answers "present" without a slot.
	resp = postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Image: "r.io/app:1", Digest: d, Size: int64(len(blob)), Path: model.SpreadPathRegistry})
	var ack model.SpreadFetchAck
	_ = json.NewDecoder(resp.Body).Decode(&ack)
	if resp.StatusCode != http.StatusOK || !ack.Present {
		t.Fatalf("status %d ack %+v, want present", resp.StatusCode, ack)
	}
}

func TestSpreadPeerWinsTheRaceAndCancelsTheSlowRegistryPull(t *testing.T) {
	blob := []byte(strings.Repeat("big", 5000))
	d := digestOf(blob)
	ctl := newControllerSink(t)
	// The source node already holds the blob.
	srcStore := newFakeSpreadStore()
	srcStore.blobs[d] = blob
	_, srcSrv := newTestSpread(t, srcStore, &fakeRegistry{}, nil, "src")
	// The receiver's registry is stalled for this blob.
	store := newFakeSpreadStore()
	sp, srv := newTestSpread(t, store, &fakeRegistry{blobs: map[string][]byte{d: blob}, stalled: map[string]bool{d: true}}, ctl, "slow")

	reg := postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Image: "r.io/app:1", Digest: d, Size: int64(len(blob)), Path: model.SpreadPathRegistry})
	if reg.StatusCode != http.StatusAccepted {
		t.Fatalf("registry fetch: status %d", reg.StatusCode)
	}
	// One registry pull at a time: a second one is refused.
	busy := postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Image: "r.io/app:1", Digest: d, Size: int64(len(blob)), Path: model.SpreadPathRegistry})
	if busy.StatusCode != http.StatusConflict {
		t.Fatalf("second registry fetch: status %d, want 409", busy.StatusCode)
	}
	peer := postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Digest: d, Size: int64(len(blob)), Path: model.SpreadPathPeer,
		Peer: &model.Holder{NodeID: "src", Address: strings.TrimPrefix(srcSrv.URL, "http://")}})
	if peer.StatusCode != http.StatusAccepted {
		t.Fatalf("peer fetch alongside the registry pull: status %d", peer.StatusCode)
	}
	done := make(chan struct{})
	go func() { sp.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the stalled registry pull was never cancelled")
	}
	got := ctl.byPath()
	if got[model.SpreadPathPeer] != model.SpreadResultOK || got[model.SpreadPathRegistry] != model.SpreadResultCancelled {
		t.Fatalf("reports %v, want peer ok and registry cancelled", got)
	}
	if !bytes.Equal(store.blobs[d], blob) {
		t.Fatal("blob not stored")
	}
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if sp.slot[0] != nil || sp.slot[1] != nil {
		t.Fatal("slots not freed")
	}
}

func TestSpreadRefusesBadOrdersAndWrongContent(t *testing.T) {
	blob := []byte("abc")
	d := digestOf(blob)
	store, ctl := newFakeSpreadStore(), newControllerSink(t)
	// The registry returns other bytes than the digest names.
	sp, srv := newTestSpread(t, store, &fakeRegistry{blobs: map[string][]byte{d: []byte("xyz")}}, ctl, "n1")
	for _, bad := range []model.SpreadFetch{
		{Job: "../x", Image: "r.io/a:1", Digest: d, Size: 3, Path: model.SpreadPathRegistry},
		{Job: "s1", Image: "r.io/a:1", Digest: "sha256:nothex", Size: 3, Path: model.SpreadPathRegistry},
		{Job: "s1", Image: "r.io/a:1", Digest: d, Size: 0, Path: model.SpreadPathRegistry},
		{Job: "s1", Digest: d, Size: 3, Path: model.SpreadPathPeer},
		{Job: "s1", Image: "r.io/a:1", Digest: d, Size: 3, Path: "elsewhere"},
	} {
		if resp := postSpread(t, srv, "/spread/fetch", bad); resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%+v: status %d, want 400", bad, resp.StatusCode)
		}
	}
	postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Image: "r.io/a:1", Digest: d, Size: 3, Path: model.SpreadPathRegistry})
	sp.wg.Wait()
	if _, ok := store.blobs[d]; ok {
		t.Fatal("content that doesn't match its digest was stored")
	}
	if got := ctl.byPath()[model.SpreadPathRegistry]; got != model.SpreadResultFailed {
		t.Fatalf("report = %q, want failed", got)
	}
	// Without the token: nothing.
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/spread/fetch", strings.NewReader("{}"))
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated fetch: %v %v", resp, err)
	}
}

func TestSpreadFinalizeChecksMetadataRegistersAndReleases(t *testing.T) {
	blob := []byte("layer-bytes")
	d := digestOf(blob)
	store, ctl := newFakeSpreadStore(), newControllerSink(t)
	sp, srv := newTestSpread(t, store, &fakeRegistry{blobs: map[string][]byte{d: blob}}, ctl, "n1")
	postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Image: "r.io/app:1", Digest: d, Size: int64(len(blob)), Path: model.SpreadPathRegistry})
	sp.wg.Wait()
	if store.leaseCount() != 1 {
		t.Fatal("no lease while the job runs")
	}
	top := []byte(`{"schemaVersion":2}`)
	topD := digestOf(top)
	store.topOf = func() (string, string) { return "r.io/app:1", topD }
	kicked := false
	sp.OnFinalize(func() { kicked = true })

	bad := model.SpreadFinalize{Job: "s1", Image: "r.io/app:1", TopType: "application/vnd.oci.image.manifest.v1+json", TopDig: topD, TopSize: int64(len(top)),
		Metadata: map[string][]byte{topD: []byte("tampered")}}
	if resp := postSpread(t, srv, "/spread/finalize", bad); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("tampered metadata: status %d, want 400", resp.StatusCode)
	}
	good := bad
	good.Metadata = map[string][]byte{topD: top}
	resp := postSpread(t, srv, "/spread/finalize", good)
	var res model.SpreadFinalizeResult
	_ = json.NewDecoder(resp.Body).Decode(&res)
	if resp.StatusCode != http.StatusOK || !res.OK {
		t.Fatalf("finalize: status %d %+v", resp.StatusCode, res)
	}
	if len(store.imported) != 1 || !strings.Contains(store.imported[0], "r.io/app:1") {
		t.Fatalf("imported %v", store.imported)
	}
	if store.leaseCount() != 0 {
		t.Fatal("the job's lease outlived the image that references its blobs")
	}
	if !kicked {
		t.Fatal("reporter not kicked")
	}
}

func TestSpreadDropCancelsAndReleasesAndLeasesRenew(t *testing.T) {
	blob := []byte(strings.Repeat("z", 4096))
	d := digestOf(blob)
	store, ctl := newFakeSpreadStore(), newControllerSink(t)
	sp, srv := newTestSpread(t, store, &fakeRegistry{blobs: map[string][]byte{d: blob}, stalled: map[string]bool{d: true}}, ctl, "n1")
	postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Image: "r.io/app:1", Digest: d, Size: int64(len(blob)), Path: model.SpreadPathRegistry})
	// Half the lease's life gone: the next lease call moves it.
	time.Sleep(50 * time.Millisecond)
	sp.mu.Lock()
	j := sp.jobs["s1"]
	sp.mu.Unlock()
	j.mu.Lock()
	j.leasedAt = time.Now().Add(-spreadLeaseTTL)
	j.mu.Unlock()
	if _, err := sp.lease(context.Background(), "s1"); err != nil {
		t.Fatal(err)
	}
	if store.leases["angryduck-spread-s1-"+sp.epoch+"-1"] == nil || store.leases["angryduck-spread-s1-"+sp.epoch+"-0"] != nil {
		t.Fatalf("lease not renewed: %v", store.leases)
	}
	if resp := postSpread(t, srv, "/spread/drop", model.SpreadDrop{Job: "s1"}); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("drop: status %d", resp.StatusCode)
	}
	sp.wg.Wait()
	if got := ctl.byPath()[model.SpreadPathRegistry]; got != model.SpreadResultCancelled {
		t.Fatalf("report = %q, want cancelled", got)
	}
	// The transfer ended after the drop: a second drop releases what's left.
	postSpread(t, srv, "/spread/drop", model.SpreadDrop{Job: "s1"})
	if store.leaseCount() != 0 {
		t.Fatalf("leases left after drop: %v", store.leases)
	}
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if len(sp.jobs) != 0 {
		t.Fatal("job kept in memory after drop")
	}
}

func TestSpreadContentServesOnlyWhatIsHere(t *testing.T) {
	blob := []byte("served")
	d := digestOf(blob)
	store := newFakeSpreadStore()
	store.blobs[d] = blob
	_, srv := newTestSpread(t, store, &fakeRegistry{}, nil, "n1")
	get := func(dg string) *http.Response {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/spread/content/"+dg, nil)
		req.Header.Set("Authorization", "Bearer "+spreadToken)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}
	resp := get(d)
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(b, blob) {
		t.Fatalf("status %d body %q", resp.StatusCode, b)
	}
	if resp := get(digestOf([]byte("absent"))); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("absent blob: status %d", resp.StatusCode)
	}
	if resp := get("sha256:../../etc/passwd"); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad digest: status %d", resp.StatusCode)
	}
}
