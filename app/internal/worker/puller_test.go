package worker

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestPreheatedReposPrunesPastRetention confirms PreheatedRepos returns
// only repos preheated within the given retention window, and prunes
// anything older while it's at it.
func TestPreheatedReposPrunesPastRetention(t *testing.T) {
	rt := newFakeRuntime()
	p := NewPuller(rt, "test-node", false, "", 0)

	p.mu.Lock()
	p.preheatedAt["repo/long-gone"] = time.Now().Add(-2 * time.Hour)
	p.preheatedAt["repo/recent"] = time.Now()
	p.mu.Unlock()

	got := p.PreheatedRepos(time.Hour)
	if got["repo/long-gone"] {
		t.Errorf("expected repo/long-gone to be pruned (past retention), got present")
	}
	if !got["repo/recent"] {
		t.Errorf("expected repo/recent to be present (within retention)")
	}
	if len(got) != 1 {
		t.Errorf("got %d repos, want 1", len(got))
	}

	// The stale entry should actually be gone from the map now, not just
	// excluded from this one result.
	p.mu.Lock()
	_, stillPresent := p.preheatedAt["repo/long-gone"]
	p.mu.Unlock()
	if stillPresent {
		t.Errorf("expected PreheatedRepos to prune the stale entry, but it's still in the map")
	}
}

// TestHandlePullRecordsPreheatedRepoOnSuccess confirms a successful pull
// through the normal HandlePull path (not a direct map write, like the
// test above) ends up recorded in preheatedAt under the image's bare
// repo, which is what PreheatMonitor actually depends on in production.
func TestHandlePullRecordsPreheatedRepoOnSuccess(t *testing.T) {
	rt := newFakeRuntime()
	p := NewPuller(rt, "test-node", false, "", 0)

	orderPull(t, p, rt, "docker.io/library/nginx:1.25")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.PreheatedRepos(time.Hour)["docker.io/library/nginx"] {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected repo docker.io/library/nginx to be recorded as preheated after a successful pull")
}

// TestHandlePullSetsDurationGauge confirms a successful pull sets
// pullDurationSeconds under the same node/result/registry labels
// pullsTotal uses, plus the bare-repo image label — the two metrics time
// and count the same event, from the same call site.
func TestHandlePullSetsDurationGauge(t *testing.T) {
	rt := newFakeRuntime()
	const nodeID = "test-node-duration"
	p := NewPuller(rt, nodeID, false, "", 0)

	orderPull(t, p, rt, "docker.io/library/nginx:1.25")

	wantPrefix := `angryduck_worker_pull_duration_seconds{node="` + nodeID + `",result="success",registry="",image="docker.io/library/nginx",spegel=""} `
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(scrapeMetrics(t), wantPrefix) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected a line starting with %q in metrics output after a successful pull", wantPrefix)
}

// TestSpegelPresenceDisabledByDefault confirms an empty
// spegelImageSubstring (the default) reports "" — detection off, no
// runtime call made at all.
func TestSpegelPresenceDisabledByDefault(t *testing.T) {
	rt := newFakeRuntime()
	p := NewPuller(rt, "test-node", false, "", 0)
	if got := p.spegelPresence(); got != "" {
		t.Errorf("spegelPresence() = %q, want empty string when disabled", got)
	}
}

// TestSpegelPresenceDetectsRunningContainer confirms enabling detection
// with a substring correctly reports "true" when a matching container is
// running, and "false" when none is.
func TestSpegelPresenceDetectsRunningContainer(t *testing.T) {
	rt := newFakeRuntime()
	rt.running["ghcr.io/spegel-org/spegel:v0.7.4"] = true

	p := NewPuller(rt, "test-node", false, "spegel", 0)
	if got := p.spegelPresence(); got != "true" {
		t.Errorf("spegelPresence() = %q, want \"true\"", got)
	}

	rtNoSpegel := newFakeRuntime()
	rtNoSpegel.running["docker.io/library/nginx:1.25"] = true
	pNoSpegel := NewPuller(rtNoSpegel, "test-node", false, "spegel", 0)
	if got := pNoSpegel.spegelPresence(); got != "false" {
		t.Errorf("spegelPresence() = %q, want \"false\"", got)
	}
}

// TestHandlePullLabelsSpegelPresence confirms a real pull through
// HandlePull actually threads the Spegel-presence label into the
// duration gauge, not just that the standalone method works.
func TestHandlePullLabelsSpegelPresence(t *testing.T) {
	rt := newFakeRuntime()
	rt.running["ghcr.io/spegel-org/spegel:v0.7.4"] = true

	const nodeID = "test-node-spegel-label"
	p := NewPuller(rt, nodeID, false, "spegel", 0)

	orderPull(t, p, rt, "docker.io/library/nginx:1.25")

	wantPrefix := `angryduck_worker_pull_duration_seconds{node="` + nodeID + `",result="success",registry="",image="docker.io/library/nginx",spegel="true"} `
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(scrapeMetrics(t), wantPrefix) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected a line starting with %q in metrics output", wantPrefix)
}

// TestResetPullDurationClearsGauge confirms the extracted reset action
// itself works — Run's ticker loop just calls this on a timer, so this is
// the part actually worth a direct test (see PreheatMonitor.tick for the
// same split-for-testability pattern).
func TestResetPullDurationClearsGauge(t *testing.T) {
	rt := newFakeRuntime()
	const nodeID = "test-node-reset"
	p := NewPuller(rt, nodeID, false, "", 0)

	orderPull(t, p, rt, "docker.io/library/nginx:1.25")

	wantPrefix := `angryduck_worker_pull_duration_seconds{node="` + nodeID + `",result="success",registry="",image="docker.io/library/nginx",spegel=""} `
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !strings.Contains(scrapeMetrics(t), wantPrefix) {
		time.Sleep(5 * time.Millisecond)
	}
	if !strings.Contains(scrapeMetrics(t), wantPrefix) {
		t.Fatalf("expected %q to be set before reset", wantPrefix)
	}

	p.ResetPullDuration()

	if strings.Contains(scrapeMetrics(t), wantPrefix) {
		t.Fatalf("expected ResetPullDuration to clear the value, but it's still present")
	}
}

// TestPullerRunDisabledWithZeroInterval confirms Run returns immediately
// (no ticker started, no panic from time.NewTicker(0)) when
// pullDurationResetInterval is 0.
func TestPullerRunDisabledWithZeroInterval(t *testing.T) {
	rt := newFakeRuntime()
	p := NewPuller(rt, "test-node", false, "", 0)

	done := make(chan struct{})
	go func() {
		p.Run(context.Background()) // must return on its own; a real ticker would block forever
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("expected Run to return immediately when pullDurationResetInterval is 0")
	}
}
