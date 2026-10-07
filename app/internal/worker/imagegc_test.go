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
	g.Tick(context.Background()) // one batch holds both rollback images
	if len(f.deleted) != 4 {
		t.Fatalf("deleted %v", f.deleted)
	}
	if g.stallUntil.IsZero() {
		t.Fatal("the disk didn't move after that batch: cleanup should stall")
	}
	// Usage times survive a restart.
	g.save(true)
	g2 := NewImageGC(f, g.inUse, g.util, nil, "n1", g.cfg)
	if len(g2.firstSeen) != len(g.firstSeen) {
		t.Fatalf("state not reloaded: %d vs %d", len(g2.firstSeen), len(g.firstSeen))
	}
}

func TestGCKeepsImageOfCrashloopingContainer(t *testing.T) {
	// The worker20 case: a crashlooping pod's container is exited most of
	// the time, so its image never showed up as running and pressure
	// removed it between restarts, forcing a re-pull every back-off.
	// ListInUseImages reports it while its pod is up; the GC must keep it.
	u := 0.90
	held := "registry.example.com/vendor-menu/dev:v5375"
	g, f := gcFixture(t, &u, []string{"sha256:held"}) // kubelet's bare image ID
	g.cfg.Settle, g.cfg.RollbackKeep = 0, 0
	addImage(g, f, held, "sha256:held", time.Hour)
	f.targets["sha256:held"] = "sha256:held"
	addImage(g, f, "registry.example.com/other/app:1", "sha256:o1", time.Hour)
	g.Tick(context.Background())
	if got := removedTags(f); strings.Contains(got, "v5375") {
		t.Fatalf("removed %q: the crashlooping pod's image went", got)
	}
	if got := removedTags(f); got != "registry.example.com/other/app:1" {
		t.Fatalf("removed %q, want the unused image", got)
	}
}

func TestGCReportsImagesThatComeBackAndForgetsThem(t *testing.T) {
	u := 0.90
	g, f := gcFixture(t, &u, nil)
	g.cfg.Settle, g.cfg.RollbackKeep = 0, 0
	addImage(g, f, "r.io/loop:1", "sha256:l1", time.Hour)
	g.Tick(context.Background()) // removed
	want := `angryduck_worker_gc_image_returns{node="n1",image="r.io/loop:1"} 1`
	if strings.Contains(scrapeMetrics(t), want) {
		t.Fatal("reported a return before the image came back")
	}
	addImage(g, f, "r.io/loop:1", "sha256:l1", 0) // kubelet pulled it again
	delete(g.firstSeen, "sha256:l1")
	g.Tick(context.Background()) // sees it back, removes it again
	if !strings.Contains(scrapeMetrics(t), want) {
		t.Fatalf("return not reported:\n%s", scrapeMetrics(t))
	}
	// An hour later both maps are empty again and the series is gone.
	g.publishChurn(time.Now().Add(2 * returnWindow))
	if len(g.removed) != 0 || len(g.returned) != 0 {
		t.Fatalf("churn state kept: removed=%d returned=%d", len(g.removed), len(g.returned))
	}
	if strings.Contains(scrapeMetrics(t), `image="r.io/loop:1"`) {
		t.Fatal("stale return still published")
	}
}

func TestDisplayName(t *testing.T) {
	for _, tc := range []struct {
		names []string
		want  string
	}{
		{[]string{"r.io/a:1", "r.io/a@sha256:x", "sha256:x"}, "r.io/a:1"},
		{[]string{"r.io/a@sha256:x", "sha256:x"}, "r.io/a@sha256:x"},
		{[]string{"sha256:x"}, "sha256:x"},
	} {
		sort.Strings(tc.names)
		if got := displayName(tc.names); got != tc.want {
			t.Errorf("displayName(%v) = %q, want %q", tc.names, got, tc.want)
		}
	}
}

func TestGCStopsWhenRemovingFreesNothingAndResumesWhenTheDiskGrows(t *testing.T) {
	// worker1's case: 73%, filled by running images, volumes and logs.
	// Removing rollback images frees nothing measurable.
	u := 0.73
	g, f := gcFixture(t, &u, nil)
	g.cfg.Settle, g.cfg.Batch = 0, 2
	for i := 1; i <= 8; i++ {
		addImage(g, f, "r.io/app"+string(rune('0'+i))+":1", "sha256:r"+string(rune('0'+i)), time.Hour) // one per repo: all rollback
	}
	addImage(g, f, "r.io/old:1", "sha256:old", 30*time.Hour)
	kicked := 0
	g.OnDone(func() { kicked++ })

	g.Tick(context.Background())
	first := len(f.deleted)
	// The old image and one more go in the first batch; then it stops.
	if first != 4 {
		t.Fatalf("deleted %v: want one batch of 2, then a stop", f.deleted)
	}
	if kicked != 1 {
		t.Fatal("pressure removals must kick the reporter")
	}
	if !strings.Contains(scrapeMetrics(t), `angryduck_worker_gc_stalled{node="n1"} 1`) {
		t.Fatal("stall not published")
	}
	g.Tick(context.Background())
	if len(f.deleted) != first {
		t.Fatalf("removed more while stalled: %v", f.deleted[first:])
	}
	// The 6h rule still applies while stalled.
	// (A repo's newest 3 are rollback images; its 4th, unused 30h, is old.)
	addImage(g, f, "r.io/older:1", "sha256:o1", 30*time.Hour)
	for i := 2; i <= 4; i++ {
		addImage(g, f, "r.io/older:"+string(rune('0'+i)), "sha256:o"+string(rune('0'+i)), time.Hour)
	}
	g.Tick(context.Background())
	if len(f.deleted) != first+2 || f.deleted[first] != "r.io/older:1" && f.deleted[first+1] != "r.io/older:1" {
		t.Fatalf("age rule skipped while stalled: %v", f.deleted[first:])
	}
	// The disk grows by 1%: removals are worth another try.
	u = 0.74
	g.Tick(context.Background())
	if len(f.deleted) <= first+2 {
		t.Fatal("cleanup didn't resume after the disk grew")
	}
}
