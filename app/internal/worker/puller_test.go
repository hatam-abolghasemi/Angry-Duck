package worker

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"angryduck/internal/model"
)

func TestPreheatedReposPrunesPastRetention(t *testing.T) {
	p := NewPuller(newFakeRuntime(), "test-node", false)
	p.mu.Lock()
	p.preheatedAt["repo/long-gone"] = time.Now().Add(-2 * time.Hour)
	p.preheatedAt["repo/recent"] = time.Now()
	p.mu.Unlock()

	got := p.PreheatedRepos(time.Hour)
	if got["repo/long-gone"] || !got["repo/recent"] || len(got) != 1 {
		t.Fatalf("got %v", got)
	}
	p.mu.Lock()
	_, stillPresent := p.preheatedAt["repo/long-gone"]
	p.mu.Unlock()
	if stillPresent {
		t.Fatal("stale entry not pruned from the map")
	}
}

func TestHandlePullRecordsPreheatedRepoAndCounts(t *testing.T) {
	rt := newFakeRuntime()
	const node = "test-node-pull"
	p := NewPuller(rt, node, false)
	orderPull(t, p, rt, "docker.io/library/nginx:1.25")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !p.PreheatedRepos(time.Hour)["docker.io/library/nginx"] {
		time.Sleep(5 * time.Millisecond)
	}
	if !p.PreheatedRepos(time.Hour)["docker.io/library/nginx"] {
		t.Fatal("repo not recorded as preheated")
	}
	for time.Now().Before(deadline) && !strings.Contains(scrapeMetrics(t), `angryduck_worker_pulls_total{node="`+node+`",result="success",registry=""} 1`) {
		time.Sleep(5 * time.Millisecond)
	}
	if !strings.Contains(scrapeMetrics(t), `angryduck_worker_pulls_total{node="`+node+`",result="success",registry=""} 1`) {
		t.Fatal("pull not counted")
	}
}

func TestCancelStopsARunningPull(t *testing.T) {
	rt := newFakeRuntime()
	const image = "registry.example.com/team/app:9"
	rt.block[image] = true
	const node = "test-node-cancel"
	p := NewPuller(rt, node, false)

	body, _ := json.Marshal(model.PullOrder{Image: image})
	rec := httptest.NewRecorder()
	p.HandlePull(rec, httptest.NewRequest(http.MethodPost, "/pull", bytes.NewReader(body)))
	if rec.Code != http.StatusAccepted || !p.Pulling(image) {
		t.Fatalf("pull not running: %d", rec.Code)
	}
	// A second order joins the running pull.
	rec = httptest.NewRecorder()
	p.HandlePull(rec, httptest.NewRequest(http.MethodPost, "/pull", bytes.NewReader(body)))
	if !strings.Contains(rec.Body.String(), "already pulling") {
		t.Fatalf("second order: %s", rec.Body.String())
	}

	rec = httptest.NewRecorder()
	p.HandleCancel(rec, httptest.NewRequest(http.MethodPost, "/pull/cancel", bytes.NewReader(body)))
	var ack model.PullAck
	_ = json.NewDecoder(rec.Body).Decode(&ack)
	if !ack.Accepted {
		t.Fatal("cancel should report a running pull")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && p.Pulling(image) {
		time.Sleep(5 * time.Millisecond)
	}
	if p.Pulling(image) {
		t.Fatal("pull still running after cancel")
	}
	want := `angryduck_worker_pulls_total{node="` + node + `",result="cancelled",registry=""} 1`
	for time.Now().Before(deadline) && !strings.Contains(scrapeMetrics(t), want) {
		time.Sleep(5 * time.Millisecond)
	}
	if !strings.Contains(scrapeMetrics(t), want) {
		t.Fatal("cancelled pull not counted")
	}
}
