package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"angryduck/internal/model"
)

type fakeImages struct {
	mu      sync.Mutex
	targets map[string]string
	labels  map[string]string // name -> RetainLabel value
	deleted []string
}

func (f *fakeImages) ImageTargetsLabeled(_ context.Context, label string) (map[string]string, map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]string{}
	var labeled map[string]string
	for k, v := range f.targets {
		out[k] = v
		if l := f.labels[k]; label == RetainLabel && l != "" {
			if labeled == nil {
				labeled = map[string]string{}
			}
			labeled[v] = l
		}
	}
	return out, labeled, nil
}
func (f *fakeImages) DeleteImages(_ context.Context, names ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, names...)
	for _, n := range names {
		delete(f.targets, n)
		delete(f.labels, n)
	}
	return nil
}
func (f *fakeImages) SetImageLabel(_ context.Context, label, value string, names ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.labels == nil {
		f.labels = map[string]string{}
	}
	for _, n := range names {
		if _, ok := f.targets[n]; !ok {
			continue
		}
		if value == "" {
			delete(f.labels, n)
		} else {
			f.labels[n] = value
		}
	}
	return nil
}

func gcFixture(t *testing.T, util *float64, running []string) (*ImageGC, *fakeImages) {
	f := &fakeImages{targets: map[string]string{}}
	cfg := GCConfig{Interval: time.Hour, High: 0.70, Low: 0.60, UnusedFor: 6 * time.Hour, Batch: 2,
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
	addImage(g, f, "r.io/app:4", "sha256:a4", 4*time.Hour)   // recent
	addImage(g, f, "r.io/app:9", "sha256:a9", 100*time.Hour) // running
	addImage(g, f, "k8s.io/pause:3.9", "sha256:p", 100*time.Hour)
	addImage(g, f, "r.io/solo:1", "sha256:s1", 100*time.Hour) // old: no image is kept for its repo anymore
	g.Tick(context.Background())
	if got := removedTags(f); got != "r.io/app:1,r.io/app:2,r.io/solo:1" {
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
	// 74.8% -> app:1,app:2 -> 68.8% -> app:3,app:4 -> 62.8% -> app:5,app:6
	// -> 56.8%, below 60%: app:7 stays.
	if got := removedTags(f); got != "r.io/app:1,r.io/app:2,r.io/app:3,r.io/app:4,r.io/app:5,r.io/app:6" {
		t.Fatalf("removed %q", got)
	}
	for _, n := range f.deleted {
		if strings.Contains(n, ":9") || strings.Contains(n, "pause") {
			t.Fatalf("removed %s, which must stay", n)
		}
	}
	g.util = func() (float64, error) { return 0.568, nil }
	g.Tick(context.Background())
	if g.cleaning {
		t.Fatalf("back below low, cleaning should end")
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

func TestGCStallsWhenABatchFreesNothingAndPersists(t *testing.T) {
	u := 0.90
	g, f := gcFixture(t, &u, nil)
	addImage(g, f, "r.io/app:1", "sha256:a1", 2*time.Hour)
	addImage(g, f, "r.io/app:2", "sha256:a2", time.Hour)
	g.cfg.Settle = 0
	g.Tick(context.Background()) // one batch holds both
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
	g.cfg.Settle = 0
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
	g.cfg.Settle = 0
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
	if strings.Contains(scrapeMetrics(t), `angryduck_worker_gc_image_returns{node="n1",image="r.io/loop:1"}`) {
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
	// Removing images frees nothing measurable.
	u := 0.73
	g, f := gcFixture(t, &u, nil)
	g.cfg.Settle, g.cfg.Batch = 0, 2
	for i := 1; i <= 8; i++ {
		addImage(g, f, "r.io/app"+string(rune('0'+i))+":1", "sha256:r"+string(rune('0'+i)), time.Hour)
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
	// The age rule still applies while stalled.
	addImage(g, f, "r.io/older:1", "sha256:o1", 30*time.Hour)
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

func TestGCPublishesWhatItRemovedAndWhatComesNext(t *testing.T) {
	u := 0.40
	g, f := gcFixture(t, &u, nil)
	g.cfg.Settle = 0
	addImage(g, f, "r.io/gone:1", "sha256:g1", 7*time.Hour)
	addImage(g, f, "r.io/next:1", "sha256:n1", time.Hour)
	g.Tick(context.Background())
	out := scrapeMetrics(t)
	if !strings.Contains(out, `angryduck_worker_gc_image_removed_timestamp_seconds{node="n1",image="r.io/gone:1",tier="old"}`) {
		t.Fatalf("removal not in the history:\n%s", out)
	}
	if strings.Contains(out, `angryduck_worker_gc_image_removal_seconds{node="n1",image="r.io/gone:1"`) {
		t.Fatalf("a removed image is still listed as coming up:\n%s", out)
	}
	prefix := `angryduck_worker_gc_image_removal_seconds{node="n1",image="r.io/next:1",tier="pressure"} `
	i := strings.Index(out, prefix)
	if i < 0 {
		t.Fatalf("next removal not listed:\n%s", out)
	}
	var left float64
	fmt.Sscan(out[i+len(prefix):], &left)
	if left < 5*3600-60 || left > 5*3600 {
		t.Fatalf("removal in %.0fs, want about 5h", left)
	}
}

func retentionFixture(t *testing.T, util *float64, running []string) (*ImageGC, *fakeImages) {
	g, f := gcFixture(t, util, running)
	g.cfg.Retention, g.cfg.Settle = true, 0
	return g, f
}

func label(f *fakeImages, digest string, until time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.labels == nil {
		f.labels = map[string]string{}
	}
	for n, d := range f.targets {
		if d == digest {
			f.labels[n] = fmt.Sprint(until.Unix())
		}
	}
}

func TestGCRetentionCopySurvivesAgeAndPressureUntilItExpires(t *testing.T) {
	u := 0.95
	g, f := retentionFixture(t, &u, nil)
	addImage(g, f, "r.io/app:1", "sha256:a1", 30*time.Hour)
	addImage(g, f, "r.io/other:1", "sha256:o1", 30*time.Hour)
	label(f, "sha256:a1", time.Now().Add(time.Hour))
	g.Tick(context.Background())
	if got := removedTags(f); got != "r.io/other:1" {
		t.Fatalf("removed %q: a retention copy must outlive both the age rule and disk pressure", got)
	}
	if !strings.Contains(scrapeMetrics(t), `angryduck_worker_gc_retained_images{node="n1"} 1`) {
		t.Fatal("retention copy not counted")
	}
	// Expired: it goes, whatever the disk.
	u = 0.10
	label(f, "sha256:a1", time.Now().Add(-time.Second))
	g.Tick(context.Background())
	if got := removedTags(f); got != "r.io/app:1,r.io/other:1" {
		t.Fatalf("removed %q: an expired retention copy must go at once", got)
	}
	if !strings.Contains(scrapeMetrics(t), `angryduck_worker_gc_images_deleted_total{node="n1",tier="retention"} 1`) {
		t.Fatal("retention removal not counted under its tier")
	}
}

func TestGCRetentionCopyUsedAgainLosesItsMark(t *testing.T) {
	u := 0.10
	running := []string{}
	g, f := retentionFixture(t, &u, nil)
	g.inUse = func() ([]string, error) { return running, nil }
	addImage(g, f, "r.io/app:1", "sha256:a1", time.Minute)
	label(f, "sha256:a1", time.Now().Add(time.Hour))
	running = []string{"r.io/app:1"}
	g.Tick(context.Background())
	if len(f.labels) != 0 {
		t.Fatalf("labels left: %v", f.labels)
	}
	if len(g.retained) != 0 {
		t.Fatalf("still counted as retained: %v", g.retained)
	}
}

func TestGCRetentionOffIgnoresMarks(t *testing.T) {
	u := 0.10
	g, f := gcFixture(t, &u, nil)
	g.cfg.Settle = 0
	addImage(g, f, "r.io/app:1", "sha256:a1", 30*time.Hour)
	label(f, "sha256:a1", time.Now().Add(time.Hour))
	g.Tick(context.Background())
	if got := removedTags(f); got != "r.io/app:1" {
		t.Fatalf("removed %q: with retention off a mark means nothing", got)
	}
}

func TestGCPublishesImageStateOnlyOnChange(t *testing.T) {
	u := 0.10
	g, f := retentionFixture(t, &u, []string{"r.io/app:2"})
	var calls int
	var lastInUse []model.ImageRef
	var lastRetained map[string]int64
	g.OnState(func(inUse []model.ImageRef, retained map[string]int64) {
		calls++
		lastInUse, lastRetained = inUse, retained
	})
	addImage(g, f, "r.io/app:1", "sha256:a1", time.Minute)
	addImage(g, f, "r.io/app:2", "sha256:a2", time.Minute)
	until := time.Now().Add(time.Hour)
	label(f, "sha256:a1", until)
	g.Tick(context.Background())
	g.Tick(context.Background())
	if calls != 1 {
		t.Fatalf("published %d times, want once: nothing changed on the second tick", calls)
	}
	if len(lastInUse) != 1 || lastInUse[0].Digest != "sha256:a2" || lastInUse[0].Name != "r.io/app:2" {
		t.Fatalf("in use: %+v", lastInUse)
	}
	if lastRetained["sha256:a1"] != until.Unix() {
		t.Fatalf("retained: %v", lastRetained)
	}
}

func retainReq(t *testing.T, g *ImageGC, o model.RetainOrder) int {
	t.Helper()
	b, _ := json.Marshal(o)
	rec := httptest.NewRecorder()
	g.HandleRetain(rec, httptest.NewRequest(http.MethodPost, "/images/retain", bytes.NewReader(b)))
	return rec.Code
}

func TestHandleRetainMarksEveryNameAndUnmarks(t *testing.T) {
	u := 0.95
	g, f := retentionFixture(t, &u, nil)
	var published int
	g.OnState(func([]model.ImageRef, map[string]int64) { published++ })
	addImage(g, f, "r.io/app:1", "sha256:a1", 30*time.Hour)
	until := time.Now().Add(24 * time.Hour).Unix()
	if code := retainReq(t, g, model.RetainOrder{Digest: "sha256:a1", Until: until}); code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if f.labels["r.io/app:1"] != fmt.Sprint(until) || f.labels["r.io/app@sha256:a1"] != fmt.Sprint(until) {
		t.Fatalf("labels %v: every name must carry the mark", f.labels)
	}
	if published == 0 {
		t.Fatal("the controller must hear about it before the next tick")
	}
	g.Tick(context.Background())
	if len(f.deleted) != 0 {
		t.Fatalf("removed %v right after it was retained", f.deleted)
	}
	if code := retainReq(t, g, model.RetainOrder{Digest: "sha256:none", Until: until}); code != http.StatusNotFound {
		t.Fatalf("missing image: status %d, want 404", code)
	}
	if code := retainReq(t, g, model.RetainOrder{Digest: "sha256:a1", Until: 0}); code != http.StatusOK {
		t.Fatalf("unmark: status %d", code)
	}
	if len(f.labels) != 0 || len(g.retained) != 0 {
		t.Fatalf("still marked: %v %v", f.labels, g.retained)
	}
	g.cfg.Retention = false
	if code := retainReq(t, g, model.RetainOrder{Digest: "sha256:a1", Until: until}); code != http.StatusConflict {
		t.Fatalf("retention off: status %d, want 409", code)
	}
}

func TestGCDoesNotRemoveWhatWasRetainedAfterThePlan(t *testing.T) {
	u := 0.10
	g, f := retentionFixture(t, &u, nil)
	addImage(g, f, "r.io/app:1", "sha256:a1", 30*time.Hour)
	g.retained = map[string]time.Time{"sha256:a1": time.Now().Add(time.Hour)}
	batch := g.notRetained([]candidate{{digest: "sha256:a1"}, {digest: "sha256:b"}})
	if len(batch) != 1 || batch[0].digest != "sha256:b" {
		t.Fatalf("batch %v", batch)
	}
}

func TestUnretainClearsEveryMark(t *testing.T) {
	f := &fakeImages{targets: map[string]string{"r.io/a:1": "sha256:a", "r.io/b:1": "sha256:b"}}
	label(f, "sha256:a", time.Now().Add(time.Hour))
	Unretain(context.Background(), f, "n1")
	if len(f.labels) != 0 {
		t.Fatalf("labels left: %v", f.labels)
	}
}

// slowListing returns a listing taken before the retain order arrived.
type slowListing struct {
	*fakeImages
	during func()
}

func (s *slowListing) ImageTargetsLabeled(ctx context.Context, label string) (map[string]string, map[string]string, error) {
	t, l, err := s.fakeImages.ImageTargetsLabeled(ctx, label)
	if s.during != nil {
		fn := s.during
		s.during = nil
		fn() // the order lands after the listing was taken
	}
	return t, l, err
}

func TestGCListingOlderThanARetainOrderDoesNotUndoIt(t *testing.T) {
	u := 0.10
	g, f := retentionFixture(t, &u, nil)
	addImage(g, f, "r.io/app:1", "sha256:a1", 30*time.Hour)
	slow := &slowListing{fakeImages: f}
	g.store = slow
	slow.during = func() {
		if code := retainReq(t, g, model.RetainOrder{Digest: "sha256:a1", Until: time.Now().Add(time.Hour).Unix()}); code != http.StatusOK {
			t.Fatalf("status %d", code)
		}
	}
	g.Tick(context.Background())
	if len(f.deleted) != 0 {
		t.Fatalf("removed %v: the order came in during the listing", f.deleted)
	}
}
