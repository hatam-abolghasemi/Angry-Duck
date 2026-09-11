package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeLister is a scriptable podLister: each call to listStuckImages pops
// the next queued response, so a test can simulate the same image
// staying stuck across several ticks.
type fakeLister struct {
	mu        sync.Mutex
	calls     int
	responses [][]podStuckImage
	err       error
}

func (f *fakeLister) listStuckImages(ctx context.Context, nodeID string) ([]podStuckImage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := f.calls
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	if len(f.responses) == 0 {
		return nil, nil
	}
	if i >= len(f.responses) {
		i = len(f.responses) - 1
	}
	return f.responses[i], nil
}

func newTestRescueWatch(client podLister, cooldown time.Duration, rescue func(context.Context, string) error) *RescueWatch {
	return &RescueWatch{
		nodeID:      "test-node",
		client:      client,
		interval:    time.Second, // irrelevant to tick() calls made directly
		cooldown:    cooldown,
		rescue:      rescue,
		attempted:   make(map[string]time.Time),
		lastWarnLog: make(map[string]time.Time),
	}
}

func TestRescueWatchTick_CallsRescueOncePerDistinctImage(t *testing.T) {
	lister := &fakeLister{responses: [][]podStuckImage{{
		{Namespace: "ns", Pod: "p1", Container: "c", Image: "img-a", Reason: "ImagePullBackOff"},
		{Namespace: "ns", Pod: "p2", Container: "c", Image: "img-a", Reason: "ImagePullBackOff"},
		{Namespace: "ns", Pod: "p3", Container: "c", Image: "img-b", Reason: "ErrImagePull"},
	}}}
	var attempted []string
	var mu sync.Mutex
	rw := newTestRescueWatch(lister, time.Minute, func(_ context.Context, image string) error {
		mu.Lock()
		attempted = append(attempted, image)
		mu.Unlock()
		return nil
	})
	rw.tick(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if len(attempted) != 2 {
		t.Fatalf("rescue attempted %d times for 3 stuck containers across 2 distinct images, want 2: %v", len(attempted), attempted)
	}
}

func TestRescueWatchTick_NoStuckPodsDoesNothing(t *testing.T) {
	lister := &fakeLister{}
	called := false
	rw := newTestRescueWatch(lister, time.Minute, func(_ context.Context, _ string) error {
		called = true
		return nil
	})
	rw.tick(context.Background())
	if called {
		t.Fatal("rescue must not be attempted when nothing is stuck")
	}
}

func TestRescueWatchTick_NilRescueStillTracksWithoutPanicking(t *testing.T) {
	lister := &fakeLister{responses: [][]podStuckImage{{
		{Namespace: "ns", Pod: "p1", Container: "c", Image: "img-a", Reason: "ImagePullBackOff"},
	}}}
	rw := newTestRescueWatch(lister, time.Minute, nil)
	rw.tick(context.Background())
}

func TestRescueWatchTick_ListErrorIsNonFatal(t *testing.T) {
	lister := &fakeLister{err: errors.New("api server unreachable")}
	called := false
	rw := newTestRescueWatch(lister, time.Minute, func(_ context.Context, _ string) error {
		called = true
		return nil
	})
	rw.tick(context.Background())
	if called {
		t.Fatal("rescue must not be attempted when the list itself failed")
	}
}

// This is the core "no infinite loop" behavior: exactly one attempt per
// cooldown window, regardless of how many ticks see the same stuck image.
func TestRescueWatchTick_CooldownPreventsRetryWithinWindow(t *testing.T) {
	lister := &fakeLister{responses: [][]podStuckImage{
		{{Namespace: "ns", Pod: "p1", Container: "c", Image: "img-a", Reason: "ImagePullBackOff"}},
		{{Namespace: "ns", Pod: "p1", Container: "c", Image: "img-a", Reason: "ImagePullBackOff"}},
		{{Namespace: "ns", Pod: "p1", Container: "c", Image: "img-a", Reason: "ImagePullBackOff"}},
	}}
	calls := 0
	rw := newTestRescueWatch(lister, time.Hour, func(_ context.Context, _ string) error {
		calls++
		return nil
	})
	rw.tick(context.Background())
	rw.tick(context.Background())
	rw.tick(context.Background())
	if calls != 1 {
		t.Fatalf("rescue attempted %d times across three ticks within the cooldown window, want 1", calls)
	}
}

func TestRescueWatchTick_RetriesAfterCooldownExpires(t *testing.T) {
	lister := &fakeLister{responses: [][]podStuckImage{
		{{Namespace: "ns", Pod: "p1", Container: "c", Image: "img-a", Reason: "ImagePullBackOff"}},
		{{Namespace: "ns", Pod: "p1", Container: "c", Image: "img-a", Reason: "ImagePullBackOff"}},
	}}
	calls := 0
	rw := newTestRescueWatch(lister, 5*time.Millisecond, func(_ context.Context, _ string) error {
		calls++
		return nil
	})
	rw.tick(context.Background())
	time.Sleep(10 * time.Millisecond)
	rw.tick(context.Background())
	if calls != 2 {
		t.Fatalf("rescue attempted %d times after the cooldown expired, want 2", calls)
	}
}

func TestRescueWatchTick_FailedAttemptStillRespectsCooldown(t *testing.T) {
	// A FAILED attempt must still start the cooldown -- otherwise a
	// perpetually-unfixable image (no source anywhere) gets hammered
	// every single poll tick forever, exactly the infinite loop this
	// design exists to avoid.
	lister := &fakeLister{responses: [][]podStuckImage{
		{{Namespace: "ns", Pod: "p1", Container: "c", Image: "img-a", Reason: "ImagePullBackOff"}},
		{{Namespace: "ns", Pod: "p1", Container: "c", Image: "img-a", Reason: "ImagePullBackOff"}},
	}}
	calls := 0
	rw := newTestRescueWatch(lister, time.Hour, func(_ context.Context, _ string) error {
		calls++
		return errors.New("no source")
	})
	rw.tick(context.Background())
	rw.tick(context.Background())
	if calls != 1 {
		t.Fatalf("rescue attempted %d times, want exactly 1 even though it fails every time", calls)
	}
}

func TestRescueWatchTick_ExcludedNamespaceIsFullyIgnored(t *testing.T) {
	lister := &fakeLister{responses: [][]podStuckImage{{
		{Namespace: "kyverno-test", Pod: "nginx", Container: "nginx", Image: "myjob:12", Reason: "ImagePullBackOff"},
	}}}
	called := false
	rw := newTestRescueWatch(lister, time.Minute, func(_ context.Context, _ string) error {
		called = true
		return nil
	})
	rw.excludeNamespaces = []string{"kyverno-test", "k8sgpt-demo"}
	rw.tick(context.Background())
	if called {
		t.Fatal("rescue must not be attempted for an excluded namespace")
	}
}

func TestRescueWatchTick_ExcludedImageIsFullyIgnored(t *testing.T) {
	lister := &fakeLister{responses: [][]podStuckImage{{
		{Namespace: "some-ns", Pod: "p", Container: "c", Image: "repo-afra.internal-dev.example.com/nginx:this-tag-does-not-exist-9999", Reason: "ImagePullBackOff"},
	}}}
	called := false
	rw := newTestRescueWatch(lister, time.Minute, func(_ context.Context, _ string) error {
		called = true
		return nil
	})
	rw.excludeImages = []string{"this-tag-does-not-exist"}
	rw.tick(context.Background())
	if called {
		t.Fatal("rescue must not be attempted for an excluded image substring")
	}
}

func TestRescueWatchTick_RepeatedSightingLogsWarnOnceThenDebug(t *testing.T) {
	lister := &fakeLister{responses: [][]podStuckImage{
		{{Namespace: "ns", Pod: "p1", Container: "c", Image: "img-a", Reason: "ImagePullBackOff"}},
		{{Namespace: "ns", Pod: "p1", Container: "c", Image: "img-a", Reason: "ImagePullBackOff"}},
	}}
	rw := newTestRescueWatch(lister, time.Hour, nil)
	key := "ns/p1/c"
	if rw.recentlyWarned(key) {
		t.Fatal("must not be considered warned before any tick")
	}
	rw.tick(context.Background())
	if !rw.recentlyWarned(key) {
		t.Fatal("first sighting should mark this stuck container as warned")
	}
	before := rw.lastWarnLog[key]
	rw.tick(context.Background())
	if !rw.lastWarnLog[key].Equal(before) {
		t.Fatal("a repeat sighting within the cooldown window must not refresh the warn timestamp")
	}
}

// --- podList/containerStatus JSON decoding ----------------------------------

func TestPodListDecoding_OnlyStuckReasonsSurface(t *testing.T) {
	raw := `{
		"items": [
			{
				"metadata": {"name": "config-guard-agent-btcls", "namespace": "config-guard"},
				"status": {
					"initContainerStatuses": [
						{"name": "script-setup", "image": "repo-afra.internal-dev.example.com/rich-ubuntu:22.04",
						 "state": {"waiting": {"reason": "ImagePullBackOff"}}}
					],
					"containerStatuses": [
						{"name": "agent", "image": "repo-afra.internal-dev.example.com/rich-ubuntu:22.04",
						 "state": {"waiting": {"reason": "PodInitializing"}}}
					]
				}
			},
			{
				"metadata": {"name": "healthy-pod", "namespace": "default"},
				"status": {
					"containerStatuses": [
						{"name": "app", "image": "nginx:1.27",
						 "state": {"running": {"startedAt": "2026-01-01T00:00:00Z"}}}
					]
				}
			}
		]
	}`
	var parsed podList
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatalf("decode: %v", err)
	}
	var out []podStuckImage
	for _, item := range parsed.Items {
		for _, cs := range item.Status.InitContainerStatuses {
			if cs.State.Waiting != nil && stuckReasons[cs.State.Waiting.Reason] {
				out = append(out, podStuckImage{Namespace: item.Metadata.Namespace, Pod: item.Metadata.Name, Container: cs.Name, Image: cs.Image, Reason: cs.State.Waiting.Reason})
			}
		}
		for _, cs := range item.Status.ContainerStatuses {
			if cs.State.Waiting != nil && stuckReasons[cs.State.Waiting.Reason] {
				out = append(out, podStuckImage{Namespace: item.Metadata.Namespace, Pod: item.Metadata.Name, Container: cs.Name, Image: cs.Image, Reason: cs.State.Waiting.Reason})
			}
		}
	}
	if len(out) != 1 {
		t.Fatalf("got %d stuck entries, want exactly 1 (PodInitializing and Running must not count): %+v", len(out), out)
	}
	if out[0].Image != "repo-afra.internal-dev.example.com/rich-ubuntu:22.04" || out[0].Reason != "ImagePullBackOff" {
		t.Fatalf("unexpected stuck entry: %+v", out[0])
	}
}

func TestStuckReasons(t *testing.T) {
	if !stuckReasons["ImagePullBackOff"] || !stuckReasons["ErrImagePull"] {
		t.Fatal("both known backoff reasons must be recognized")
	}
	if stuckReasons["PodInitializing"] || stuckReasons["ContainerCreating"] || stuckReasons[""] {
		t.Fatal("transient/non-stuck reasons must not be treated as stuck")
	}
}
