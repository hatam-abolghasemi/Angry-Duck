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
	mu    sync.Mutex
	calls int
	// responses[i] is returned on the (i+1)th call; the last entry
	// repeats for any call beyond len(responses).
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

func newTestPodWatch(client podLister, backoff time.Duration, fix func(context.Context, string) error) *PodWatch {
	return &PodWatch{
		nodeID:    "test-node",
		client:    client,
		interval:  time.Second, // irrelevant to tick() calls made directly
		backoff:   backoff,
		fix:       fix,
		attempted: make(map[string]time.Time),
	}
}

func TestPodWatchTick_CallsFixOncePerDistinctImage(t *testing.T) {
	lister := &fakeLister{responses: [][]podStuckImage{{
		{Namespace: "ns", Pod: "p1", Container: "c", Image: "img-a", Reason: "ImagePullBackOff"},
		{Namespace: "ns", Pod: "p2", Container: "c", Image: "img-a", Reason: "ImagePullBackOff"}, // same image, different pod
		{Namespace: "ns", Pod: "p3", Container: "c", Image: "img-b", Reason: "ErrImagePull"},
	}}}
	var fixed []string
	var mu sync.Mutex
	pw := newTestPodWatch(lister, time.Minute, func(_ context.Context, image string) error {
		mu.Lock()
		fixed = append(fixed, image)
		mu.Unlock()
		return nil
	})
	pw.tick(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if len(fixed) != 2 {
		t.Fatalf("fix called %d times for 3 stuck containers across 2 distinct images, want 2: %v", len(fixed), fixed)
	}
}

func TestPodWatchTick_NoStuckPodsDoesNothing(t *testing.T) {
	lister := &fakeLister{}
	called := false
	pw := newTestPodWatch(lister, time.Minute, func(_ context.Context, _ string) error {
		called = true
		return nil
	})
	pw.tick(context.Background())
	if called {
		t.Fatal("fix must not be called when nothing is stuck")
	}
}

func TestPodWatchTick_NilFixStillTracksWithoutPanicking(t *testing.T) {
	lister := &fakeLister{responses: [][]podStuckImage{{
		{Namespace: "ns", Pod: "p1", Container: "c", Image: "img-a", Reason: "ImagePullBackOff"},
	}}}
	pw := newTestPodWatch(lister, time.Minute, nil)
	pw.tick(context.Background()) // must not panic with fix == nil
}

func TestPodWatchTick_ListErrorIsNonFatal(t *testing.T) {
	lister := &fakeLister{err: errors.New("api server unreachable")}
	called := false
	pw := newTestPodWatch(lister, time.Minute, func(_ context.Context, _ string) error {
		called = true
		return nil
	})
	pw.tick(context.Background()) // must not panic
	if called {
		t.Fatal("fix must not be called when the list itself failed")
	}
}

func TestPodWatchTick_BackoffPreventsRetryWithinWindow(t *testing.T) {
	lister := &fakeLister{responses: [][]podStuckImage{
		{{Namespace: "ns", Pod: "p1", Container: "c", Image: "img-a", Reason: "ImagePullBackOff"}},
		{{Namespace: "ns", Pod: "p1", Container: "c", Image: "img-a", Reason: "ImagePullBackOff"}}, // still stuck next tick
	}}
	calls := 0
	pw := newTestPodWatch(lister, time.Hour, func(_ context.Context, _ string) error {
		calls++
		return nil
	})
	pw.tick(context.Background())
	pw.tick(context.Background())
	if calls != 1 {
		t.Fatalf("fix called %d times across two ticks within the backoff window, want 1", calls)
	}
}

func TestPodWatchTick_RetriesAfterBackoffExpires(t *testing.T) {
	lister := &fakeLister{responses: [][]podStuckImage{
		{{Namespace: "ns", Pod: "p1", Container: "c", Image: "img-a", Reason: "ImagePullBackOff"}},
		{{Namespace: "ns", Pod: "p1", Container: "c", Image: "img-a", Reason: "ImagePullBackOff"}},
	}}
	calls := 0
	pw := newTestPodWatch(lister, 5*time.Millisecond, func(_ context.Context, _ string) error {
		calls++
		return nil
	})
	pw.tick(context.Background())
	time.Sleep(10 * time.Millisecond)
	pw.tick(context.Background())
	if calls != 2 {
		t.Fatalf("fix called %d times after the backoff window expired, want 2", calls)
	}
}

func TestPodWatchTick_FixErrorDoesNotPanicOrBlockOthers(t *testing.T) {
	lister := &fakeLister{responses: [][]podStuckImage{{
		{Namespace: "ns", Pod: "p1", Container: "c", Image: "img-a", Reason: "ImagePullBackOff"},
		{Namespace: "ns", Pod: "p2", Container: "c", Image: "img-b", Reason: "ImagePullBackOff"},
	}}}
	var fixed []string
	pw := newTestPodWatch(lister, time.Minute, func(_ context.Context, image string) error {
		fixed = append(fixed, image)
		if image == "img-a" {
			return errors.New("boom")
		}
		return nil
	})
	pw.tick(context.Background())
	if len(fixed) != 2 {
		t.Fatalf("expected both images attempted despite the first failing, got %v", fixed)
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
