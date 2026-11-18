package worker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeExtractor is a snapshotter that unpacks layers on their own and
// commits them onto the layer below, as containerd 2.2 does.
type fakeExtractor struct {
	mu        sync.Mutex
	snaps     map[string]bool // committed chainIDs
	order     []string        // commits, in order
	running   int             // extractions now
	peak      int             // most at once
	release   chan struct{}   // when set, extractions wait on it
	failOn    map[string]bool // chainIDs whose extraction fails
	discarded int
}

func newFakeExtractor() *fakeExtractor {
	return &fakeExtractor{snaps: map[string]bool{}, failOn: map[string]bool{}}
}

func (f *fakeExtractor) ExtractLayer(ctx context.Context, _ string, l LayerSpec) (ExtractedLayer, error) {
	f.mu.Lock()
	if f.snaps[l.ChainID] {
		f.mu.Unlock()
		return ExtractedLayer{Present: true}, nil
	}
	f.running++
	f.peak = max(f.peak, f.running)
	rel := f.release
	f.mu.Unlock()
	if rel != nil {
		select {
		case <-rel:
		case <-ctx.Done():
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.running--
	if f.failOn[l.ChainID] {
		return ExtractedLayer{}, errors.New("broken layer")
	}
	return ExtractedLayer{Key: "x-" + l.ChainID}, nil
}

func (f *fakeExtractor) CommitExtracted(_ context.Context, _ string, l LayerSpec, x ExtractedLayer) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if l.Parent != "" && !f.snaps[l.Parent] {
		return false, errParentMissing
	}
	if f.snaps[l.ChainID] {
		return false, nil
	}
	f.snaps[l.ChainID] = true
	f.order = append(f.order, l.ChainID)
	return true, nil
}

func (f *fakeExtractor) DiscardExtracted(context.Context, string, ExtractedLayer) {
	f.mu.Lock()
	f.discarded++
	f.mu.Unlock()
}

// Layers landing top first are all unpacked at once, and committed bottom
// up as soon as the layer below is.
func TestSpreadUnpacksLayersSideBySide(t *testing.T) {
	_, chains, fetches := unpackTestImage()
	fx := newFakeExtractor()
	fx.release = make(chan struct{})
	sp, _ := newTestSpread(t, newFakeSpreadStore(), &fakeRegistry{}, nil, "n1")
	sp.EnableUnpack(newFakeApplier())
	if !sp.EnableParallelUnpack(fx, 3) {
		t.Fatal("refused")
	}
	for _, i := range []int{2, 1, 0} {
		sp.landed("s1", fetches[i])
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		fx.mu.Lock()
		running := fx.running
		fx.mu.Unlock()
		if running == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d unpacking at once, want all 3", running)
		}
		time.Sleep(time.Millisecond)
	}
	close(fx.release)
	sp.wg.Wait()
	if got := strings.Join(fx.order, ","); got != strings.Join(chains, ",") {
		t.Fatalf("committed %s, want bottom up %s", got, strings.Join(chains, ","))
	}
}

// No more layers unpack at once on a node than its limit, across spreads.
func TestSpreadParallelUnpackKeepsToItsLimit(t *testing.T) {
	_, _, fetches := unpackTestImage()
	fx := newFakeExtractor()
	fx.release = make(chan struct{})
	sp, _ := newTestSpread(t, newFakeSpreadStore(), &fakeRegistry{}, nil, "n1")
	sp.EnableUnpack(newFakeApplier())
	sp.EnableParallelUnpack(fx, 2)
	for _, job := range []string{"s1", "s2"} {
		for _, o := range fetches {
			o.Job = job
			sp.landed(job, o)
		}
	}
	time.Sleep(50 * time.Millisecond)
	close(fx.release)
	sp.wg.Wait()
	if fx.peak > 2 {
		t.Fatalf("%d unpacking at once, limit 2", fx.peak)
	}
	if len(fx.order) != 3 {
		t.Fatalf("committed %v", fx.order)
	}
}

// A layer that fails is left to finalize; the ones above it wait, and are
// thrown away when the spread finalizes.
func TestSpreadParallelUnpackFailureLeavesTheRestToFinalize(t *testing.T) {
	_, chains, fetches := unpackTestImage()
	fx := newFakeExtractor()
	fx.failOn[chains[1]] = true
	sp, _ := newTestSpread(t, newFakeSpreadStore(), &fakeRegistry{}, nil, "n1")
	sp.EnableUnpack(newFakeApplier())
	sp.EnableParallelUnpack(fx, 3)
	for _, o := range fetches {
		sp.landed("s1", o)
	}
	sp.wg.Wait()
	if len(fx.order) != 1 || fx.order[0] != chains[0] {
		t.Fatalf("committed %v, want only the bottom layer", fx.order)
	}
	sp.unpackStop(context.Background(), "s1", false)
	if fx.discarded != 1 {
		t.Fatalf("%d unpacked layers thrown away, want the top one", fx.discarded)
	}
}

// A layer landing again while it waits to be committed isn't unpacked twice.
func TestSpreadParallelUnpackOncePerLayer(t *testing.T) {
	_, chains, fetches := unpackTestImage()
	fx := newFakeExtractor()
	sp, _ := newTestSpread(t, newFakeSpreadStore(), &fakeRegistry{}, nil, "n1")
	sp.EnableUnpack(newFakeApplier())
	sp.EnableParallelUnpack(fx, 3)
	sp.landed("s1", fetches[2])
	sp.wg.Wait()
	sp.landed("s1", fetches[2]) // again: waits to be committed already
	sp.wg.Wait()
	sp.mu.Lock()
	_, waiting := sp.unpacks["s1"].extracted[chains[2]]
	pending := len(sp.unpacks["s1"].pending)
	sp.mu.Unlock()
	if !waiting || pending != 0 {
		t.Fatalf("waiting=%t pending=%d", waiting, pending)
	}
	sp.landed("s1", fetches[0])
	sp.landed("s1", fetches[1])
	sp.wg.Wait()
	if strings.Join(fx.order, ",") != strings.Join(chains, ",") {
		t.Fatalf("committed %v", fx.order)
	}
}
