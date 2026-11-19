package worker

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// countingImages counts the image listings the cleanup makes.
type countingImages struct {
	*fakeImages
	lists atomic.Int32
}

func (c *countingImages) ImageTargetsLabeled(ctx context.Context, label string) (map[string]string, map[string]string, error) {
	c.lists.Add(1)
	return c.fakeImages.ImageTargetsLabeled(ctx, label)
}

func eventsFixture(t *testing.T, util *float64, running *[]string) (*ImageGC, *countingImages, *atomic.Int32) {
	f := &countingImages{fakeImages: &fakeImages{targets: map[string]string{}}}
	var inUseCalls atomic.Int32
	cfg := GCConfig{Interval: time.Hour, High: 0.70, Low: 0.60, UnusedFor: 6 * time.Hour, Batch: 2,
		ProtectSubstrings: []string{"pause"}, StatePath: filepath.Join(t.TempDir(), "gc.json")}
	g := NewImageGC(f, func() ([]string, error) { inUseCalls.Add(1); return *running, nil },
		func() (float64, error) { return *util, nil }, nil, "n1", cfg)
	return g, f, &inUseCalls
}

// While containerd's events are followed, a tick with nothing changed lists
// nothing; an event, or the safety relist, makes the next one list.
func TestGCListsOnlyAfterAChange(t *testing.T) {
	util := 0.30
	running := []string{"r.io/app:1"}
	g, f, inUse := eventsFixture(t, &util, &running)
	f.targets["r.io/app:1"] = "sha256:app"
	g.SetEventsLive(true)
	ctx := context.Background()

	g.Tick(ctx) // first: lists
	g.Tick(ctx)
	g.Tick(ctx)
	if n := f.lists.Load(); n != 1 {
		t.Fatalf("%d image listings over 3 quiet ticks, want 1", n)
	}
	if n := inUse.Load(); n != 1 {
		t.Fatalf("%d container listings over 3 quiet ticks, want 1", n)
	}
	g.Changed()
	g.Tick(ctx)
	if n := f.lists.Load(); n != 2 {
		t.Fatalf("%d image listings after an event, want 2", n)
	}
	g.mu.Lock()
	g.listedAt = g.listedAt.Add(-gcRelist)
	g.mu.Unlock()
	g.Tick(ctx)
	if n := f.lists.Load(); n != 3 {
		t.Fatalf("%d image listings after the safety relist time, want 3", n)
	}
	// Without the event stream, every tick lists.
	g.SetEventsLive(false)
	g.Tick(ctx)
	g.Tick(ctx)
	if n := f.lists.Load(); n != 5 {
		t.Fatalf("%d image listings without events, want 5", n)
	}
}

// An image in use stays in use between listings: its last use moves with
// the clock, so it never ages into removal on a kept listing.
func TestGCKeptListingKeepsInUseImagesFresh(t *testing.T) {
	util := 0.30
	running := []string{"r.io/app:1"}
	g, f, _ := eventsFixture(t, &util, &running)
	f.targets["r.io/app:1"] = "sha256:app"
	g.SetEventsLive(true)
	g.Tick(context.Background())
	g.mu.Lock()
	g.lastUsed["sha256:app"] = time.Now().Add(-7 * time.Hour)
	g.firstSeen["sha256:app"] = time.Now().Add(-7 * time.Hour)
	g.mu.Unlock()
	g.Tick(context.Background())
	if len(f.deleted) != 0 {
		t.Fatalf("removed an image in use: %v", f.deleted)
	}
	g.mu.Lock()
	age := time.Since(g.lastUsed["sha256:app"])
	g.mu.Unlock()
	if age > time.Minute {
		t.Fatalf("last use is %s old, want now", age)
	}
}

// A removal is never decided on a kept listing: an image that came due is
// checked against a fresh one first, which sees a container that started
// using it without an event reaching the cleanup.
func TestGCRelistsBeforeRemoving(t *testing.T) {
	util := 0.30
	running := []string{}
	g, f, inUse := eventsFixture(t, &util, &running)
	f.targets["r.io/old:1"] = "sha256:old"
	g.SetEventsLive(true)
	g.Tick(context.Background())
	g.mu.Lock()
	g.firstSeen["sha256:old"] = time.Now().Add(-7 * time.Hour)
	g.mu.Unlock()
	running = []string{"r.io/old:1"} // started meanwhile, no event seen
	before := inUse.Load()
	g.Tick(context.Background())
	if inUse.Load() == before {
		t.Fatal("decided on a kept listing")
	}
	if len(f.deleted) != 0 {
		t.Fatalf("removed an image a container uses: %v", f.deleted)
	}
	running = []string{}
	g.mu.Lock()
	g.lastUsed["sha256:old"] = time.Now().Add(-7 * time.Hour)
	g.mu.Unlock()
	g.Changed()
	g.Tick(context.Background())
	if len(f.deleted) == 0 {
		t.Fatal("an image unused past UnusedFor stayed")
	}
}
