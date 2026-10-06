package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"angryduck/internal/imageref"
	"angryduck/internal/metrics"
	"angryduck/internal/model"
)

// fakeRuntime is an in-memory Runtime double for testing the puller and
// preheat-attribution monitor without shelling out to a real container
// runtime. Shared across this package's test files.
type fakeRuntime struct {
	mu       sync.Mutex
	local    map[string]bool
	running  map[string]bool
	pullErrs map[string]error
	block    map[string]bool // PullImage waits for ctx to end
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{
		local:    make(map[string]bool),
		running:  make(map[string]bool),
		pullErrs: make(map[string]error),
		block:    make(map[string]bool),
	}
}

func (f *fakeRuntime) PullImage(ctx context.Context, image string) error {
	f.mu.Lock()
	blocked := f.block[image]
	f.mu.Unlock()
	if blocked {
		<-ctx.Done()
		return ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.pullErrs[image]; ok {
		return err
	}
	f.local[image] = true
	return nil
}

func (f *fakeRuntime) LocalImages() ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var refs []string
	for img := range f.local {
		refs = append(refs, img)
	}
	return refs, nil
}

func (f *fakeRuntime) ListRunningImages() ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for img := range f.running {
		out = append(out, img)
	}
	return out, nil
}

// RunningImageRepos counts each entry in f.running once — tests populate
// that set one entry per fake "container", so no alias-deduplication
// concern applies here the way it does for the real CRI runtime.
func (f *fakeRuntime) RunningImageRepos() (map[string]int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	counts := make(map[string]int)
	for img := range f.running {
		if repo := imageref.Repo(img); repo != "" {
			counts[repo]++
		}
	}
	return counts, nil
}

// orderPull drives Puller.HandlePull synchronously enough to record the
// order (the actual pull happens in a goroutine, so we poll briefly for it
// to land in the fake runtime).
func orderPull(t *testing.T, p *Puller, rt *fakeRuntime, image string) {
	t.Helper()
	body, _ := json.Marshal(model.PullOrder{Image: image, OrderedAt: time.Now()})
	req := httptest.NewRequest(http.MethodPost, "/pull", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	p.HandlePull(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("HandlePull(%s) = %d, want %d", image, rec.Code, http.StatusAccepted)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		local, _ := rt.LocalImages()
		for _, img := range local {
			if img == image {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("image %s never appeared locally after pull order", image)
}

// scrapeMetrics returns the full Prometheus exposition text from the
// package's shared metrics registry, so a test can check for a specific
// label combination without a getter into the unexported counter maps.
func scrapeMetrics(t *testing.T) string {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metrics.Handler().ServeHTTP(rec, req)
	return rec.Body.String()
}
