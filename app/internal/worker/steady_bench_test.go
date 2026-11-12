package worker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"angryduck/internal/metrics"
	"angryduck/internal/model"
)

// Steady-state benchmarks: the loops a worker runs all the time, at the
// scale of a busy staging node (150 images, three names each; 60 pods, a
// third with a sidecar). Run with:
//
//	go test ./internal/worker -run '^$' -bench Steady -benchmem
const (
	benchImages = 150
	benchPods   = 60
)

func benchNode() (targets map[string]string, pods []PodImages) {
	targets = make(map[string]string, benchImages*3)
	for i := 0; i < benchImages; i++ {
		d := fmt.Sprintf("sha256:%064x", i)
		repo := fmt.Sprintf("registry.example.com/team/app%d/stage", i%40)
		targets[fmt.Sprintf("%s:v%d", repo, i)] = d
		targets[repo+"@"+d] = d
		targets[fmt.Sprintf("sha256:%064x", 100000+i)] = d // image ID
	}
	for p := 0; p < benchPods; p++ {
		pod := PodImages{{fmt.Sprintf("sha256:%064x", 100000+p)}}
		if p%3 == 0 {
			pod = append(pod, []string{fmt.Sprintf("sha256:%064x", 100000+benchImages-1-p%5)})
		}
		pods = append(pods, pod)
	}
	return targets, pods
}

func BenchmarkSteadyGCTick(b *testing.B) {
	targets, pods := benchNode()
	f := &fakeImages{targets: targets}
	cfg := GCConfig{Interval: time.Hour, High: 0.70, Low: 0.40, UnusedFor: time.Hour, Batch: 5,
		Retention: true, ProtectSubstrings: []string{"pause"}, StatePath: filepath.Join(b.TempDir(), "gc.json")}
	g := NewImageGC(f, nil, func() (float64, error) { return 0.5, nil }, nil, "bench", cfg)
	g.OnState(func([]model.ImageRef, map[string]int64) {})
	g.EnableCompanions(func() ([]PodImages, error) { return pods, nil },
		func(string) (int64, error) { return 1 << 20, nil }, func([]model.ImageRef, []string) {})
	repos := make([]string, 0, 40)
	for i := 0; i < 40; i++ {
		repos = append(repos, fmt.Sprintf("registry.example.com/team/app%d/stage", i))
	}
	g.SetCompanions(model.CompanionSet{Hash: "h", Ready: true, AppRepos: repos[:30]})
	now := time.Now()
	for d := range namesByTarget(targets, false) {
		g.firstSeen[d] = now // nothing old: the tick decides, removes nothing
	}
	ctx := context.Background()
	g.Tick(ctx)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.Tick(ctx)
	}
}

func BenchmarkSteadyReportInventory(b *testing.B) {
	targets, _ := benchNode()
	rt := newFakeRuntime()
	for n := range targets {
		rt.local[n] = true
	}
	rp := NewReporter("bench", "x:1", "", "", time.Minute, NewInventory(rt))
	inUse := make([]model.ImageRef, 0, 60)
	for i := 0; i < 60; i++ {
		inUse = append(inUse, model.ImageRef{Digest: fmt.Sprintf("sha256:%064x", i), Name: fmt.Sprintf("registry.example.com/team/app%d/stage:v%d", i%40, i)})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		repos, images, _ := rp.localInventory(time.Hour)
		_ = inventoryHash(repos, images, true, inUse, nil, true, inUse[:10], nil)
	}
}

func BenchmarkSteadyMetricsScrape(b *testing.B) {
	// Series a busy worker carries: per-image removal deadlines and events.
	for i := 0; i < benchImages; i++ {
		gcImageRemoval.Set(float64(i), "bench", fmt.Sprintf("registry.example.com/team/app%d/stage:v%d", i%40, i), "pressure")
	}
	h := metrics.Handler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.ServeHTTP(httptest.NewRecorder(), req)
	}
}
