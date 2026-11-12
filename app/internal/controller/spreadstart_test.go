package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"angryduck/internal/model"
	"angryduck/internal/registryclient"
)

// flakyResolver fails the first fails reads of each image with err, then
// answers.
type flakyResolver struct {
	mu    sync.Mutex
	fails int
	err   error
	asked map[string]int
	block chan struct{} // when set, reads wait on it (or ctx)
}

func (f *flakyResolver) Resolve(ctx context.Context, image, _ string) (registryclient.Resolved, error) {
	f.mu.Lock()
	f.asked[image]++
	n := f.asked[image]
	block := f.block
	f.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return registryclient.Resolved{}, ctx.Err()
		}
	}
	if n <= f.fails {
		return registryclient.Resolved{}, f.err
	}
	return testResolved(), nil
}

func (f *flakyResolver) count(image string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.asked[image]
}

func beginSetup(t *testing.T, r *flakyResolver, deadline time.Duration) *Spreader {
	s, _, _ := spreadSetup(t, 1)
	s.resolver = r
	s.cfg.StartDeadline = deadline
	return s
}

type fallbackRec struct {
	mu     sync.Mutex
	images []string
}

func (f *fallbackRec) fn(image string, _ error) {
	f.mu.Lock()
	f.images = append(f.images, image)
	f.mu.Unlock()
}

func (f *fallbackRec) got() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.images...)
}

// A slow registry is retried: the spread starts once the manifest reads,
// and the image is never seeded whole.
func TestBeginRetriesUntilTheManifestReads(t *testing.T) {
	r := &flakyResolver{fails: 2, err: context.DeadlineExceeded, asked: map[string]int{}}
	s := beginSetup(t, r, time.Minute)
	var fb fallbackRec
	s.Begin(img, fb.fn)
	s.Begin(img, fb.fn) // already starting: ignored
	s.wg.Wait()
	if s.jobFor(img) == nil {
		t.Fatal("no spread after the manifest read on the third attempt")
	}
	if n := r.count(img); n != 3 {
		t.Fatalf("read %d times, want 3 (one start, not two)", n)
	}
	if got := fb.got(); len(got) != 0 {
		t.Fatalf("fell back for %v", got)
	}
}

// A registry refusing Angry Duck's credentials won't change its mind:
// fall back at once.
func TestBeginFallsBackAtOnceWhenRefused(t *testing.T) {
	r := &flakyResolver{fails: 100, err: &registryclient.StatusError{Method: "GET", Code: 403}, asked: map[string]int{}}
	s := beginSetup(t, r, time.Minute)
	var fb fallbackRec
	s.Begin(img, fb.fn)
	s.wg.Wait()
	if got := fb.got(); len(got) != 1 || got[0] != img || r.count(img) != 1 {
		t.Fatalf("fallback %v after %d reads, want one fallback after one read", got, r.count(img))
	}
}

// Past the deadline, the image is seeded whole, once.
func TestBeginFallsBackAfterTheDeadline(t *testing.T) {
	r := &flakyResolver{fails: 100, err: errors.New("unreachable"), asked: map[string]int{}}
	s := beginSetup(t, r, 1500*time.Millisecond)
	var fb fallbackRec
	start := time.Now()
	s.Begin(img, fb.fn)
	s.wg.Wait()
	if got := fb.got(); len(got) != 1 {
		t.Fatalf("fallback %v, want exactly one", got)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("fell back after %s, want about the 1.5s deadline", took)
	}
	if s.jobFor(img) != nil {
		t.Fatal("no spread job may exist for an image that fell back")
	}
}

// A newer tag of the repo replaces a start still reading: no fallback for
// the old one, and the new one spreads.
func TestBeginSupersededByANewerTag(t *testing.T) {
	r := &flakyResolver{fails: 0, asked: map[string]int{}, block: make(chan struct{})}
	s := beginSetup(t, r, time.Minute)
	var fb fallbackRec
	s.Begin(img, fb.fn)
	newer := strings.Replace(img, "1.5.0", "1.6.0", 1)
	for r.count(img) == 0 {
		time.Sleep(time.Millisecond)
	}
	s.Begin(newer, fb.fn)
	r.mu.Lock()
	close(r.block)
	r.block = nil
	r.mu.Unlock()
	s.wg.Wait()
	if got := fb.got(); len(got) != 0 {
		t.Fatalf("fell back for %v", got)
	}
	if s.jobFor(newer) == nil {
		t.Fatal("the newer tag must spread")
	}
}

// The webhook answers at once, without seeds, while the manifest is read
// in the background.
func TestPreheatAnswersWithoutWaitingOnTheRegistry(t *testing.T) {
	r := &flakyResolver{fails: 0, asked: map[string]int{}, block: make(chan struct{})}
	s, _, reg := spreadSetup(t, 1)
	s.resolver = r
	s.cfg.StartDeadline = time.Minute
	srv := NewServer(reg, NewRanker(reg, 1, time.Hour, nil, false, false))
	srv.SetSpreader(s)
	done := make(chan *httptest.ResponseRecorder)
	go func() {
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/webhook/preheat", strings.NewReader(`{"image":"`+img+`"}`)))
		done <- rec
	}()
	select {
	case rec := <-done:
		var resp model.PreheatResponse
		_ = json.NewDecoder(rec.Body).Decode(&resp)
		if rec.Code != http.StatusAccepted || !resp.Accepted || resp.Status != "spreading" || len(resp.OrderedNodes) != 0 {
			t.Fatalf("%d %+v", rec.Code, resp)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the webhook waited on the registry")
	}
	close(r.block)
	s.wg.Wait()
	if s.jobFor(img) == nil {
		t.Fatal("the spread must start once the manifest reads")
	}
}
