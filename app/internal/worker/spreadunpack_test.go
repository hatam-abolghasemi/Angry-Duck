package worker

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	digest "github.com/opencontainers/go-digest"

	"angryduck/internal/blobship"
	"angryduck/internal/model"
)

// fakeApplier unpacks layers into a set of chainIDs, the way a
// snapshotter would: a layer needs the one below it.
type fakeApplier struct {
	mu      sync.Mutex
	snaps   map[string]bool
	order   []string // chainIDs applied, in order
	leases  map[string]string
	fail    map[string]bool // chainIDs that fail to unpack
	block   chan struct{}   // when set, every apply waits on it
	started chan string     // when set, every apply announces itself
}

func newFakeApplier() *fakeApplier {
	return &fakeApplier{snaps: map[string]bool{}, leases: map[string]string{}, fail: map[string]bool{}}
}

func (f *fakeApplier) ApplyLayer(ctx context.Context, lease string, l LayerSpec) (bool, error) {
	f.mu.Lock()
	if f.snaps[l.ChainID] {
		f.mu.Unlock()
		return false, nil
	}
	if l.Parent != "" && !f.snaps[l.Parent] {
		f.mu.Unlock()
		return false, errParentMissing
	}
	block, started := f.block, f.started
	f.mu.Unlock()
	if started != nil {
		started <- l.ChainID
	}
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail[l.ChainID] {
		return false, errors.New("broken layer")
	}
	f.snaps[l.ChainID] = true
	f.order = append(f.order, l.ChainID)
	f.leases[l.ChainID] = lease
	return true, nil
}

func (f *fakeApplier) applied() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.order...)
}

// unpackTestImage is three layers with their blobs and chainIDs.
func unpackTestImage() (blobs [][]byte, chains []string, fetches []model.SpreadFetch) {
	var diffs []string
	for i := 0; i < 3; i++ {
		b := []byte(strings.Repeat(string(rune('a'+i)), 2000))
		blobs = append(blobs, b)
		diffs = append(diffs, digestOf(b))
	}
	chains = blobship.ChainIDs(diffs)
	for i, b := range blobs {
		u := &model.SpreadUnpack{ChainID: chains[i], MediaType: "application/vnd.oci.image.layer.v1.tar"}
		if i > 0 {
			u.Parent = chains[i-1]
		}
		fetches = append(fetches, model.SpreadFetch{Job: "s1", Image: "r.io/app:1", Digest: digestOf(b), Size: int64(len(b)),
			Path: model.SpreadPathRegistry, Unpack: u})
	}
	return blobs, chains, fetches
}

func TestSpreadUnpacksLayersBottomUpAsTheyArrive(t *testing.T) {
	blobs, chains, fetches := unpackTestImage()
	reg := &fakeRegistry{blobs: map[string][]byte{}}
	for _, b := range blobs {
		reg.blobs[digestOf(b)] = b
	}
	ap := newFakeApplier()
	sp, srv := newTestSpread(t, newFakeSpreadStore(), reg, newControllerSink(t), "n1")
	sp.EnableUnpack(ap)

	// The top layer first: it must wait for the two below.
	postSpread(t, srv, "/spread/fetch", fetches[2])
	sp.wg.Wait()
	if got := ap.applied(); len(got) != 0 {
		t.Fatalf("unpacked %v before the layers below arrived", got)
	}
	postSpread(t, srv, "/spread/fetch", fetches[1])
	sp.wg.Wait()
	if got := ap.applied(); len(got) != 0 {
		t.Fatalf("unpacked %v before the bottom layer arrived", got)
	}
	postSpread(t, srv, "/spread/fetch", fetches[0])
	sp.wg.Wait()
	got := ap.applied()
	if strings.Join(got, ",") != strings.Join(chains, ",") {
		t.Fatalf("unpacked %v, want bottom up %v", got, chains)
	}
	if ap.leases[chains[0]] != "angryduck-spread-s1-"+sp.epoch+"-0" {
		t.Fatalf("unpacked under lease %q, want the job's", ap.leases[chains[0]])
	}
}

func TestSpreadUnpacksABlobAlreadyHere(t *testing.T) {
	blobs, chains, fetches := unpackTestImage()
	store := newFakeSpreadStore()
	reg := &fakeRegistry{blobs: map[string][]byte{digestOf(blobs[0]): blobs[0]}}
	ap := newFakeApplier()
	sp, srv := newTestSpread(t, store, reg, newControllerSink(t), "n1")
	sp.EnableUnpack(ap)
	postSpread(t, srv, "/spread/fetch", fetches[0])
	sp.wg.Wait()
	// Another job for an image sharing that layer: "present", and unpacked.
	o := fetches[0]
	o.Job = "s2"
	resp := postSpread(t, srv, "/spread/fetch", o)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want present", resp.StatusCode)
	}
	sp.wg.Wait()
	if got := ap.applied(); len(got) != 1 || got[0] != chains[0] {
		t.Fatalf("applied %v, want just %s once", got, chains[0])
	}
}

func TestSpreadUnpackFailureLeavesTheLayerToFinalize(t *testing.T) {
	blobs, chains, fetches := unpackTestImage()
	reg := &fakeRegistry{blobs: map[string][]byte{}}
	for _, b := range blobs {
		reg.blobs[digestOf(b)] = b
	}
	ap := newFakeApplier()
	ap.fail[chains[1]] = true
	sp, srv := newTestSpread(t, newFakeSpreadStore(), reg, newControllerSink(t), "n1")
	sp.EnableUnpack(ap)
	for _, o := range fetches {
		postSpread(t, srv, "/spread/fetch", o)
		sp.wg.Wait()
	}
	if got := ap.applied(); len(got) != 1 || got[0] != chains[0] {
		t.Fatalf("applied %v, want only the bottom layer (the middle one fails, the top waits on it)", got)
	}
	// Nothing keeps running for it.
	sp.mu.Lock()
	running := sp.unpacks["s1"] != nil && sp.unpacks["s1"].running
	sp.mu.Unlock()
	if running {
		t.Fatal("unpacking still running")
	}
}

func TestSpreadFinalizeWaitsForTheLayerBeingUnpacked(t *testing.T) {
	_, chains, fetches := unpackTestImage()
	ap := newFakeApplier()
	ap.block, ap.started = make(chan struct{}), make(chan string, 4)
	sp, _ := newTestSpread(t, newFakeSpreadStore(), &fakeRegistry{}, nil, "n1")
	sp.EnableUnpack(ap)
	sp.landed("s1", fetches[0])
	if got := <-ap.started; got != chains[0] {
		t.Fatalf("started %s", got)
	}
	stopped := make(chan struct{})
	go func() {
		sp.unpackStop(context.Background(), "s1", false)
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("finalize didn't wait for the layer being unpacked")
	case <-time.After(50 * time.Millisecond):
	}
	// Arriving now: too late, finalize unpacks it.
	sp.landed("s1", fetches[1])
	close(ap.block)
	<-stopped
	sp.wg.Wait()
	if got := ap.applied(); len(got) != 1 || got[0] != chains[0] {
		t.Fatalf("applied %v, want only the layer that was running", got)
	}
}

func TestSpreadDropCancelsUnpacking(t *testing.T) {
	_, _, fetches := unpackTestImage()
	ap := newFakeApplier()
	ap.block, ap.started = make(chan struct{}), make(chan string, 4)
	sp, _ := newTestSpread(t, newFakeSpreadStore(), &fakeRegistry{}, nil, "n1")
	sp.EnableUnpack(ap)
	sp.landed("s1", fetches[0])
	<-ap.started
	sp.unpackStop(context.Background(), "s1", true)
	done := make(chan struct{})
	go func() { sp.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("unpacking didn't stop on drop")
	}
	if got := ap.applied(); len(got) != 0 {
		t.Fatalf("applied %v after the drop", got)
	}
}

func TestSpreadUnpackOffOrWithoutPlacementDoesNothing(t *testing.T) {
	_, _, fetches := unpackTestImage()
	sp, _ := newTestSpread(t, newFakeSpreadStore(), &fakeRegistry{}, nil, "n1")
	sp.landed("s1", fetches[0]) // not enabled
	ap := newFakeApplier()
	sp.EnableUnpack(ap)
	o := fetches[0]
	o.Unpack = nil
	sp.landed("s1", o) // an order from an older controller
	o.Unpack = &model.SpreadUnpack{ChainID: "not-a-digest", MediaType: "x"}
	sp.landed("s1", o)
	sp.wg.Wait()
	if got := ap.applied(); len(got) != 0 || len(sp.unpacks) != 0 {
		t.Fatalf("applied %v, jobs %d", got, len(sp.unpacks))
	}
}

func TestChainOfMatchesTheImageChainIDs(t *testing.T) {
	diffs := []string{digest.FromString("a").String(), digest.FromString("b").String(), digest.FromString("c").String()}
	want := blobship.ChainIDs(diffs)
	parent := ""
	for i, d := range diffs {
		got := chainOf(parent, d)
		if got != want[i] {
			t.Fatalf("layer %d: %s, want %s", i, got, want[i])
		}
		parent = got
	}
}
