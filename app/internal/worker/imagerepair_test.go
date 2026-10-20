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
	"strings"
	"sync"
	"testing"
	"time"

	"angryduck/internal/blobship"
	"angryduck/internal/model"
)

// breakImage leaves name registered but impossible to unpack, the way a
// failed import before 1.8.12 did: its top layer isn't unpacked, and that
// layer's blob is gone too.
func breakImage(store *blobship.MemStore, ti blobship.TestImage) {
	top := len(ti.Layers) - 1
	store.DeleteSnapshot(ti.Chains[top])
	store.Delete(ti.Layers[top])
}

// repairStore is a MemStore with what the repair needs beyond Store:
// leases, holds, blob writes that check digests like containerd, and
// single-blob removal.
type repairStore struct {
	*blobship.MemStore
	mu       sync.Mutex
	leases   map[string]map[string]bool
	removed  []string // DeleteContent calls
	holds    int
	writes   int
	failHold bool
}

func newRepairStore() *repairStore {
	return &repairStore{MemStore: blobship.NewMemStore(), leases: map[string]map[string]bool{}}
}

func (s *repairStore) CreateLease(_ context.Context, id string, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.leases[id] = map[string]bool{}
	return nil
}
func (s *repairStore) DeleteLease(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.leases, id)
	return nil
}
func (s *repairStore) HoldContent(ctx context.Context, lease, d string) (bool, error) {
	s.mu.Lock()
	s.holds++
	s.mu.Unlock()
	have, _ := s.Digests(ctx)
	if !have[d] {
		return false, nil
	}
	s.mu.Lock()
	s.leases[lease][d] = true
	s.mu.Unlock()
	return true, nil
}
func (s *repairStore) HoldSnapshot(_ context.Context, lease, key string) (bool, error) {
	s.mu.Lock()
	s.holds++
	s.mu.Unlock()
	if !s.HasSnapshot(key) {
		return false, nil
	}
	s.mu.Lock()
	s.leases[lease]["snap:"+key] = true
	s.mu.Unlock()
	return true, nil
}
func (s *repairStore) WriteBlob(_ context.Context, lease, _, dgst string, size int64, r io.Reader) (int64, error) {
	b, err := io.ReadAll(io.LimitReader(r, size+1))
	if err != nil {
		return 0, err
	}
	if int64(len(b)) != size {
		return 0, fmt.Errorf("got %d bytes, want %d", len(b), size)
	}
	if d := digestOf(b); d != dgst {
		return 0, fmt.Errorf("unexpected commit digest %s, expected %s", d, dgst)
	}
	s.Put(b)
	s.mu.Lock()
	s.writes++
	s.leases[lease][dgst] = true
	s.mu.Unlock()
	return int64(len(b)), nil
}
func (s *repairStore) DeleteContent(_ context.Context, d string) error {
	s.Delete(d)
	s.mu.Lock()
	s.removed = append(s.removed, d)
	s.mu.Unlock()
	return nil
}

// fakeRegistry serves blobs and manifests by digest from a store.
type fakeRepairRegistry struct {
	store     *blobship.MemStore
	down      bool
	blobs     []string
	manifests []string
}

func (f *fakeRepairRegistry) open(ctx context.Context, d string) (io.ReadCloser, int64, error) {
	if f.down || f.store == nil {
		return nil, 0, errors.New("registry unreachable")
	}
	b, err := f.store.ReadBlob(ctx, d, 1<<30)
	if err != nil {
		return nil, 0, err
	}
	return io.NopCloser(bytes.NewReader(b)), int64(len(b)), nil
}
func (f *fakeRepairRegistry) OpenBlob(ctx context.Context, _ string, d string) (io.ReadCloser, int64, error) {
	f.blobs = append(f.blobs, d)
	return f.open(ctx, d)
}
func (f *fakeRepairRegistry) OpenManifest(ctx context.Context, _ string, d string) (io.ReadCloser, int64, error) {
	f.manifests = append(f.manifests, d)
	return f.open(ctx, d)
}

// peerAndController is one server playing the controller (/layers/holders,
// naming itself as the one holder of whatever its store has) and that
// peer (/spread/content).
func peerAndController(t *testing.T, store *blobship.MemStore) *httptest.Server {
	t.Helper()
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+testToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch {
		case r.URL.Path == "/layers/holders":
			have, _ := store.Digests(r.Context())
			list := []model.Holder{}
			if have[r.URL.Query().Get("digest")] {
				list = append(list, model.Holder{NodeID: "peer1", Address: addr(srv)})
			}
			_ = json.NewEncoder(w).Encode(list)
		case strings.HasPrefix(r.URL.Path, "/spread/content/"):
			b, err := store.ReadBlob(r.Context(), strings.TrimPrefix(r.URL.Path, "/spread/content/"), 1<<30)
			if err != nil {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Length", fmt.Sprint(len(b)))
			_, _ = w.Write(b)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newTestRepair(store *repairStore, reg RepairRegistry, ctl *httptest.Server) *ImageRepair {
	r := NewImageRepair(store, reg, "linux/amd64", "n1", time.Minute, nil, nil)
	if ctl != nil {
		r.UsePeers(testToken, ctl.URL)
	}
	return r
}

func runnable(t *testing.T, s blobship.Store, image string) {
	t.Helper()
	h, err := blobship.Check(context.Background(), s, image, "linux/amd64")
	if err != nil || h.Broken() || !h.Unpacked {
		t.Fatalf("%s after the repair: %+v (%v), want unpacked", image, h, err)
	}
}

// The incident's image: top layer neither unpacked nor here. The repair
// fetches that one blob from a peer, keeps the name, removes nothing.
func TestImageRepair_FetchesTheMissingLayerFromAPeer(t *testing.T) {
	peerStore := blobship.NewMemStore()
	peerStore.AddTestImage(testImage, "base", "app")
	ctl := peerAndController(t, peerStore)

	store := newRepairStore()
	ti := store.AddTestImage(testImage, "base", "app")
	breakImage(store.MemStore, ti)
	reg := &fakeRepairRegistry{store: peerStore}
	changed := 0
	r := newTestRepair(store, reg, ctl)
	r.onChange = func() { changed++ }

	r.Pass(context.Background())
	runnable(t, store, testImage)
	if len(store.Deleted) != 0 {
		t.Fatalf("names removed: %v", store.Deleted)
	}
	if store.writes != 1 || len(reg.blobs)+len(reg.manifests) != 0 {
		t.Fatalf("%d blob write(s), registry asked for %v %v: want the one missing blob, from the peer", store.writes, reg.blobs, reg.manifests)
	}
	if _, d, _ := store.Resolve(context.Background(), testImage); d != ti.Index {
		t.Fatalf("name now points at %s, want %s", d, ti.Index)
	}
	if len(store.leases) != 0 {
		t.Fatalf("lease left behind: %v", store.leases)
	}
	if changed != 1 || len(r.Broken()) != 0 {
		t.Fatalf("onChange ran %d times, still broken: %v", changed, r.Broken())
	}
}

// No peer has it: the registry does.
func TestImageRepair_FallsBackToTheRegistry(t *testing.T) {
	origin := blobship.NewMemStore()
	origin.AddTestImage(testImage, "base", "app")
	ctl := peerAndController(t, blobship.NewMemStore()) // a peer holding nothing

	store := newRepairStore()
	ti := store.AddTestImage(testImage, "base", "app")
	breakImage(store.MemStore, ti)
	reg := &fakeRepairRegistry{store: origin}
	newTestRepair(store, reg, ctl).Pass(context.Background())
	runnable(t, store, testImage)
	if len(reg.blobs) != 1 || reg.blobs[0] != ti.Layers[1] {
		t.Fatalf("registry asked for %v, want only %s", reg.blobs, ti.Layers[1])
	}
}

// Neither peers nor the registry: the image stays as it is, reported
// broken and left out of reports, and the next check tries again.
func TestImageRepair_KeepsTheImageWhenNothingHasThePiece(t *testing.T) {
	origin := blobship.NewMemStore()
	origin.AddTestImage(testImage, "base", "app")
	store := newRepairStore()
	ti := store.AddTestImage(testImage, "base", "app")
	breakImage(store.MemStore, ti)
	reg := &fakeRepairRegistry{store: origin, down: true}
	r := newTestRepair(store, reg, nil)

	r.Pass(context.Background())
	if !store.HasImage(testImage) || len(store.Deleted) != 0 {
		t.Fatal("removed an image it couldn't repair")
	}
	if !r.Broken()[testImage] {
		t.Fatalf("Broken() = %v, want %s", r.Broken(), testImage)
	}
	reg.down = false // the registry is back
	r.Pass(context.Background())
	runnable(t, store, testImage)
	if len(r.Broken()) != 0 {
		t.Fatalf("still listed broken: %v", r.Broken())
	}
}

// The platform manifest is gone: fetched from the registry under
// /manifests/, then the image runs.
func TestImageRepair_FetchesMissingMetadata(t *testing.T) {
	origin := blobship.NewMemStore()
	origin.AddTestImage(testImage, "base", "app")
	store := newRepairStore()
	ti := store.AddTestImage(testImage, "base", "app")
	store.Delete(ti.AMD64Manifest)
	reg := &fakeRepairRegistry{store: origin}

	h, _ := blobship.Check(context.Background(), store, testImage, "linux/amd64")
	if !h.Broken() || h.Meta == nil || h.Meta.Kind != "manifest" {
		t.Fatalf("Check = %+v, want broken on the manifest", h)
	}
	newTestRepair(store, reg, nil).Pass(context.Background())
	runnable(t, store, testImage)
	if len(reg.manifests) != 1 || reg.manifests[0] != ti.AMD64Manifest || len(reg.blobs) != 0 {
		t.Fatalf("registry asked for manifests %v, blobs %v", reg.manifests, reg.blobs)
	}
}

// A config that doesn't parse is unusable: it is removed and fetched again.
func TestImageRepair_ReplacesUnusableMetadata(t *testing.T) {
	peerStore := blobship.NewMemStore()
	peerStore.AddTestImage(testImage, "base", "app")
	ctl := peerAndController(t, peerStore)
	store := newRepairStore()
	ti := store.AddTestImage(testImage, "base", "app")
	store.PutRaw(ti.Config, []byte(`{"rootfs": garbage`))

	newTestRepair(store, nil, ctl).Pass(context.Background())
	runnable(t, store, testImage)
	if len(store.removed) != 1 || store.removed[0] != ti.Config {
		t.Fatalf("removed %v, want only the config %s", store.removed, ti.Config)
	}
}

// A container create failed extracting a layer whose blob is here: that
// blob is corrupted, replaced, and the image unpacked.
func TestImageRepair_WakeReplacesALayerThatFailedToUnpack(t *testing.T) {
	peerStore := blobship.NewMemStore()
	peerStore.AddTestImage(testImage, "base", "app")
	ctl := peerAndController(t, peerStore)
	store := newRepairStore()
	ti := store.AddTestImage(testImage, "base", "app")
	store.DeleteSnapshot(ti.Chains[1]) // not unpacked; its blob is here, but bad
	r := newTestRepair(store, nil, ctl)

	r.Wake(testImage, "failed to create containerd container: error unpacking image: failed to extract layer "+ti.Layers[1]+": unexpected EOF")
	r.handleWakes(context.Background())
	runnable(t, store, testImage)
	if len(store.removed) != 1 || store.removed[0] != ti.Layers[1] || store.writes != 1 {
		t.Fatalf("removed %v, wrote %d blob(s): want the one bad layer replaced", store.removed, store.writes)
	}
}

func TestCorruptLayers(t *testing.T) {
	d := "sha256:" + strings.Repeat("ab", 32)
	for reason, want := range map[string]bool{
		"failed to extract layer " + d + ": unexpected EOF":                                                       true,
		"failed to extract layer " + d + ": gzip: invalid header":                                                 true,
		"failed to extract layer " + d + ": failed to get reader from content store: content digest x: not found": false,
		"failed to extract layer " + d + ": write /var/lib: no space left on device":                              false,
		"exec: \"/app\": no such file or directory":                                                               false,
	} {
		if got := corruptLayers(reason)[d]; got != want {
			t.Errorf("corruptLayers(%q) = %v, want %v", reason, got, want)
		}
	}
}

func TestImageRepair_LeavesABusyImage(t *testing.T) {
	store := newRepairStore()
	ti := store.AddTestImage(testImage, "base", "app")
	breakImage(store.MemStore, ti)
	reg := &fakeRepairRegistry{store: blobship.NewMemStore()}
	r := NewImageRepair(store, reg, "linux/amd64", "n1", time.Minute, func([]string) bool { return true }, nil)
	r.Pass(context.Background())
	if len(reg.blobs)+len(reg.manifests) != 0 || store.writes != 0 {
		t.Fatal("touched an image being pulled")
	}
	if !r.Broken()[testImage] {
		t.Fatal("a broken busy image must still be left out of reports")
	}
}

func TestImageRepair_IgnoresAngryDucksOwnNames(t *testing.T) {
	store := newRepairStore()
	ti := store.AddTestImage(baseImagePrefix+"x:layers-0-1", "base", "app")
	breakImage(store.MemStore, ti)
	reg := &fakeRepairRegistry{store: blobship.NewMemStore()}
	newTestRepair(store, reg, nil).Pass(context.Background())
	if len(reg.blobs)+len(reg.manifests) != 0 || len(store.Deleted) != 0 {
		t.Fatal("touched a temporary name")
	}
}

func TestRescue_ReceivesAgainAnImageThatCantBeUnpacked(t *testing.T) {
	srcStore := blobship.NewMemStore()
	srcStore.AddTestImage(testImage, "base", "app")
	_, src := peer(t, "worker14", srcStore)

	dstStore := blobship.NewMemStore()
	ti := dstStore.AddTestImage(testImage, "base", "app")
	breakImage(dstStore, ti)
	_, dstSrv := peer(t, "worker21", dstStore)

	code, res := order(t, dstSrv, model.RescueOrder{Image: testImage, Sources: []model.RescueSource{{NodeID: "worker14", Address: addr(src)}}}, testToken)
	if code != http.StatusOK || !res.OK {
		t.Fatalf("rescue: %d %+v", code, res)
	}
	if res.Source != "worker14" || len(dstStore.Imports) != 1 {
		t.Fatalf("answered %+v with %d import(s): a broken image was taken as present", res, len(dstStore.Imports))
	}
	runnable(t, dstStore, testImage)
}

// A rescue counts on a layer it listed as here, and it is gone by the time
// it is held (kubelet removed the image it came with): the rescue ships it
// instead of importing without it.
func TestRescue_ShipsALayerThatVanishedBeforeItWasHeld(t *testing.T) {
	srcStore := blobship.NewMemStore()
	ti := srcStore.AddTestImage(testImage, "base", "app")
	_, src := peer(t, "worker14", srcStore)

	dst := &vanishingStore{repairStore: newRepairStore(), victim: ti.Layers[0]}
	dst.Put([]byte("base")) // listed as here...
	rs := NewRescue(dst, nil, testToken, "worker21", "linux/amd64", 2)
	mux := http.NewServeMux()
	rs.Register(mux)
	dstSrv := httptest.NewServer(mux)
	defer dstSrv.Close()

	code, res := order(t, dstSrv, model.RescueOrder{Image: testImage, Sources: []model.RescueSource{{NodeID: "worker14", Address: addr(src)}}}, testToken)
	if code != http.StatusOK || !res.OK {
		t.Fatalf("rescue: %d %+v", code, res)
	}
	runnable(t, dst, testImage)
	if res.Blobs < 2 {
		t.Fatalf("shipped %d blob(s): the vanished base layer must be shipped too", res.Blobs)
	}
	if len(dst.leases) != 0 {
		t.Fatalf("lease left behind: %v", dst.leases)
	}
}

// vanishingStore loses victim right after the first listing that shows
// it: the garbage collector freeing it once kubelet removed the image it
// came with, between the rescue's listing and its import.
type vanishingStore struct {
	*repairStore
	victim string
	gone   bool
}

func (v *vanishingStore) Digests(ctx context.Context) (map[string]bool, error) {
	have, err := v.repairStore.Digests(ctx)
	if err == nil && have[v.victim] && !v.gone {
		v.gone = true
		v.Delete(v.victim)
	}
	return have, err
}

// A node doesn't report an image it has registered but can't run.
func TestReporter_LeavesOutBrokenImages(t *testing.T) {
	rt := newFakeRuntime()
	rt.local["r.io/app:1"] = true
	rt.local["r.io/app:2"] = true
	rp := NewReporter("n1", "x:1", "", "", time.Minute, NewInventory(rt))
	rp.SetExclude(func() map[string]bool { return map[string]bool{"r.io/app:2": true} })
	_, images, ok := rp.localInventory(0)
	if !ok || len(images) != 1 || images[0] != "r.io/app:1" {
		t.Fatalf("reported %v, want only r.io/app:1", images)
	}
}

// countingStore counts metadata reads.
type countingStore struct {
	*repairStore
	reads int
}

func (c *countingStore) ReadBlob(ctx context.Context, d string, max int64) ([]byte, error) {
	c.reads++
	return c.repairStore.ReadBlob(ctx, d, max)
}

// A pass after the first reads no metadata (immutable for a digest), and
// still finds a layer gone since.
func TestImageRepair_LaterPassesUseTheListingsOnly(t *testing.T) {
	origin := blobship.NewMemStore()
	origin.AddTestImage(testImage, "base", "app")
	store := &countingStore{repairStore: newRepairStore()}
	ti := store.AddTestImage(testImage, "base", "app")
	r := NewImageRepair(store, &fakeRepairRegistry{store: origin}, "linux/amd64", "n1", time.Minute, nil, nil)

	r.Pass(context.Background())
	first := store.reads
	r.Pass(context.Background())
	if store.reads != first {
		t.Fatalf("second pass read %d metadata blob(s), want 0", store.reads-first)
	}
	breakImage(store.MemStore, ti)
	r.Pass(context.Background())
	runnable(t, store, testImage)
}
