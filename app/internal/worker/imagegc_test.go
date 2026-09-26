package worker

import (
	"context"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

type fakeImages struct {
	targets map[string]string
	deleted []string
}

func (f *fakeImages) ImageTargets(context.Context) (map[string]string, error) {
	out := map[string]string{}
	for k, v := range f.targets {
		out[k] = v
	}
	return out, nil
}
func (f *fakeImages) DeleteImages(_ context.Context, names ...string) error {
	f.deleted = append(f.deleted, names...)
	for _, n := range names {
		delete(f.targets, n)
	}
	return nil
}

func gcFixture(t *testing.T, util *float64, running []string) (*ImageGC, *fakeImages) {
	f := &fakeImages{targets: map[string]string{}}
	cfg := GCConfig{Interval: time.Hour, High: 0.70, Low: 0.60, UnusedFor: 6 * time.Hour, RollbackKeep: 3, Batch: 2,
		ProtectSubstrings: []string{"pause"}, StatePath: filepath.Join(t.TempDir(), "gc.json")}
	g := NewImageGC(f, func() ([]string, error) { return running, nil },
		func() (float64, error) { return *util, nil }, nil, "n1", cfg)
	return g, f
}

func addImage(g *ImageGC, f *fakeImages, repoTag, digest string, unusedFor time.Duration) {
	f.targets[repoTag] = digest
	f.targets[strings.Split(repoTag, ":")[0]+"@"+digest] = digest
	g.firstSeen[digest] = time.Now().Add(-unusedFor)
}

func removedTags(f *fakeImages) string {
	var gone []string
	for _, n := range f.deleted {
		if !strings.Contains(n, "@") {
			gone = append(gone, n)
		}
	}
	sort.Strings(gone)
	return strings.Join(gone, ",")
}

func TestGCBelowHighRemovesOnlyUnusedPastUnusedFor(t *testing.T) {
	u := 0.40
	g, f := gcFixture(t, &u, []string{"r.io/app:9"})
	g.cfg.Settle = 0
	addImage(g, f, "r.io/app:1", "sha256:a1", 30*time.Hour)  // old
	addImage(g, f, "r.io/app:2", "sha256:a2", 20*time.Hour)  // old
	addImage(g, f, "r.io/app:3", "sha256:a3", 5*time.Hour)   // recent
	addImage(g, f, "r.io/app:4", "sha256:a4", 4*time.Hour)   // rollback
	addImage(g, f, "r.io/app:5", "sha256:a5", 3*time.Hour)   // rollback
	addImage(g, f, "r.io/app:6", "sha256:a6", 2*time.Hour)   // rollback
	addImage(g, f, "r.io/app:9", "sha256:a9", 100*time.Hour) // running
	addImage(g, f, "k8s.io/pause:3.9", "sha256:p", 100*time.Hour)
	addImage(g, f, "r.io/solo:1", "sha256:s1", 100*time.Hour) // only one of its repo: rollback
	g.Tick(context.Background())
	if got := removedTags(f); got != "r.io/app:1,r.io/app:2" {
		t.Fatalf("removed %q, want only the images unused for 6h+", got)
	}
}

func TestGCAboveHighRemovesOldestUnusedEvenIfRecent(t *testing.T) {
	// The reported case: 74.8%, every image ran here within 6h. It must
	// still clean down to 60%, oldest first.
	u := 0.748
	g, f := gcFixture(t, &u, []string{"r.io/app:9"})
	g.cfg.Settle = 0
	for i := 1; i <= 7; i++ { // app:1 oldest (5h) ... app:7 newest (-1h)
		addImage(g, f, "r.io/app:"+string(rune('0'+i)), "sha256:a"+string(rune('0'+i)), time.Duration(6-i)*time.Hour-time.Minute)
	}
	addImage(g, f, "r.io/app:9", "sha256:a9", time.Minute) // running
	addImage(g, f, "k8s.io/pause:3.9", "sha256:p", 100*time.Hour)
	calls := 0
	g.util = func() (float64, error) { // each batch of 2 frees 6 points
		calls++
		return u - 0.06*float64(calls-1), nil
	}
	g.Tick(context.Background())
	// 74.8% -> app:1,app:2 -> 68.8% -> app:3,app:4 -> 62.8%. Still above
	// 60%, but only rollback images (app:5..7) are left, and those go only
	// above 70%.
	if got := removedTags(f); got != "r.io/app:1,r.io/app:2,r.io/app:3,r.io/app:4" {
		t.Fatalf("removed %q", got)
	}
	for _, n := range f.deleted {
		if strings.Contains(n, ":9") || strings.Contains(n, "pause") {
			t.Fatalf("removed %s, which must stay", n)
		}
	}
	if !g.cleaning {
		t.Fatalf("still above low, cleaning should stay on")
	}
}

func TestGCAboveHighStopsAtLow(t *testing.T) {
	u := 0.72
	g, f := gcFixture(t, &u, nil)
	g.cfg.Settle = 0
	for i := 1; i <= 8; i++ {
		addImage(g, f, "r.io/app:"+string(rune('0'+i)), "sha256:a"+string(rune('0'+i)), time.Duration(10-i)*time.Hour)
	}
	calls := 0
	g.util = func() (float64, error) { // first batch drops it to 59%
		calls++
		if calls == 1 {
			return u, nil
		}
		return 0.59, nil
	}
	g.Tick(context.Background())
	if got := removedTags(f); got != "r.io/app:1,r.io/app:2" {
		t.Fatalf("removed %q, want one batch of the oldest", got)
	}
}

func TestGCUsesRollbackOnlyAboveHighAndPersists(t *testing.T) {
	u := 0.90
	g, f := gcFixture(t, &u, nil)
	addImage(g, f, "r.io/app:1", "sha256:a1", 10*time.Hour)
	addImage(g, f, "r.io/app:2", "sha256:a2", 8*time.Hour)
	g.cfg.Settle = 0
	g.Tick(context.Background()) // disk never goes down: both rollback images go
	if len(f.deleted) != 4 {
		t.Fatalf("deleted %v", f.deleted)
	}
	// Usage times survive a restart.
	g.save(true)
	g2 := NewImageGC(f, g.running, g.util, nil, "n1", g.cfg)
	if len(g2.firstSeen) != len(g.firstSeen) {
		t.Fatalf("state not reloaded: %d vs %d", len(g2.firstSeen), len(g.firstSeen))
	}
}
