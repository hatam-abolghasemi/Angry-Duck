package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"angryduck/internal/metrics"
	"angryduck/internal/model"
)

// Steady-state benchmarks: what the controller does all the time, at the
// scale of sahand-k8s-stg (21 workers reporting every 15s, 150 images each,
// 20 companion images kept). Run with:
//
//	go test ./internal/controller -run '^$' -bench Steady -benchmem
const benchNodes = 21

func benchReport(node int, full bool) model.WorkerReport {
	rep := model.WorkerReport{NodeID: fmt.Sprintf("w%d", node), Address: fmt.Sprintf("w%d:1", node), Utilization: 0.5,
		Timestamp: time.Now(), InventoryHash: "h", ImageState: true, CompanionState: true, CompanionsHash: "x"}
	if !full {
		rep.InventoryOmitted = true
		return rep
	}
	for i := 0; i < 150; i++ {
		repo := fmt.Sprintf("registry.example.com/team/app%d/stage", i%40)
		rep.Images = append(rep.Images, fmt.Sprintf("%s:v%d", repo, i), fmt.Sprintf("%s@sha256:%064x", repo, i))
		rep.Repos = append(rep.Repos, repo)
		if i < 60 {
			rep.InUse = append(rep.InUse, model.ImageRef{Digest: fmt.Sprintf("sha256:%064x", i), Name: fmt.Sprintf("%s:v%d", repo, i)})
		}
	}
	for i := 0; i < 20; i++ {
		rep.Companions = append(rep.Companions, model.ImageRef{Digest: fmt.Sprintf("sha256:%064x", 1000+i), Name: fmt.Sprintf("docker.io/library/side%d:1", i), Size: 50 << 20})
	}
	return rep
}

func benchRegistry() (*Registry, *Companions) {
	reg := NewRegistry(time.Hour, time.Minute)
	c := NewCompanions(reg, CompanionConfig{MaxBytes: 300 << 20, Warmup: 0})
	for n := 0; n < benchNodes; n++ {
		r := benchReport(n, true)
		reg.Update(r)
	}
	return reg, c
}

func BenchmarkSteadyReportOmitted(b *testing.B) {
	reg, _ := benchRegistry()
	reps := make([]model.WorkerReport, benchNodes)
	for n := range reps {
		reps[n] = benchReport(n, false)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reg.Update(reps[i%benchNodes])
	}
}

func BenchmarkSteadyCompanionsTick(b *testing.B) {
	_, c := benchRegistry()
	c.SetKeep(func(map[string]bool) {})
	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.tick(now)
	}
}

func BenchmarkSteadyPropagatorTickCompanionsDone(b *testing.B) {
	reg, _ := benchRegistry()
	p := NewPropagator(reg, rescueToken, PropagatorConfig{Interval: time.Hour, MaxConcurrent: 8, PerSource: 3, MaxUtilization: 0.7,
		RetryAfter: time.Minute, BackoffMax: time.Hour, Timeout: time.Second})
	keep := map[string]bool{}
	for i := 0; i < 20; i++ {
		img := fmt.Sprintf("registry.example.com/team/app%d/stage:v%d", i, i) // on every node already
		keep[img] = true
	}
	p.SetKeep(keep)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.tick(ctx)
	}
}

func BenchmarkSteadyMetricsScrape(b *testing.B) {
	benchRegistry()
	h := metrics.Handler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.ServeHTTP(httptest.NewRecorder(), req)
	}
}
