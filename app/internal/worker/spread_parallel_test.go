package worker

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"angryduck/internal/model"
)

// Different blobs come in side by side over the same path; only the same
// blob over the same path is refused while it runs.
func TestSpreadFetchesDifferentBlobsAtOnce(t *testing.T) {
	a, b := []byte(strings.Repeat("a", 4096)), []byte(strings.Repeat("b", 4096))
	da, db := digestOf(a), digestOf(b)
	store, ctl := newFakeSpreadStore(), newControllerSink(t)
	reg := &fakeRegistry{blobs: map[string][]byte{da: a, db: b}, stalled: map[string]bool{da: true, db: true}}
	sp, srv := newTestSpread(t, store, reg, ctl, "n1")
	for _, d := range []string{da, db} {
		resp := postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Image: "r.io/app:1", Digest: d, Size: 4096, Path: model.SpreadPathRegistry})
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("registry fetch of %s: status %d, want 202 while another blob comes in", d[:15], resp.StatusCode)
		}
	}
	if resp := postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Image: "r.io/app:1", Digest: da, Size: 4096, Path: model.SpreadPathRegistry}); resp.StatusCode != http.StatusConflict {
		t.Fatalf("same blob, same path again: status %d, want 409", resp.StatusCode)
	}
	if out := scrapeMetrics(t); !strings.Contains(out, `angryduck_worker_spread_active{node="n1",path="registry"} 2`) {
		t.Fatalf("two registry transfers not shown running:\n%s", out)
	}
	postSpread(t, srv, "/spread/drop", model.SpreadDrop{Job: "s1"})
	done := make(chan struct{})
	go func() { sp.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("drop didn't cancel both transfers")
	}
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if len(sp.xfers) != 0 {
		t.Fatalf("transfers left: %v", sp.xfers)
	}
}

// Two images register at the same time; a second finalize of the same
// image waits for the first.
func TestSpreadFinalizesDifferentImagesSideBySide(t *testing.T) {
	store := newFakeSpreadStore()
	sp, srv := newTestSpread(t, store, &fakeRegistry{}, nil, "n1")
	top := []byte(`{"schemaVersion":2}`)
	topD := digestOf(top)
	store.images["r.io/app:1"], store.images["r.io/web:1"] = topD, topD

	release := make(chan struct{})
	started := make(chan struct{}, 4)
	store.onImport = func() {
		started <- struct{}{}
		<-release
	}
	finalize := func(image string, wg *sync.WaitGroup) {
		defer wg.Done()
		resp := postSpread(t, srv, "/spread/finalize", model.SpreadFinalize{Job: "s1", Image: image,
			TopType: "application/vnd.oci.image.manifest.v1+json", TopDig: topD, TopSize: int64(len(top)), Metadata: map[string][]byte{topD: top}})
		if resp.StatusCode != http.StatusOK {
			t.Errorf("finalize %s: status %d", image, resp.StatusCode)
		}
	}
	waitStarted := func(what string) {
		t.Helper()
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s never started importing", what)
		}
	}

	var wg sync.WaitGroup
	wg.Add(3)
	go finalize("r.io/app:1", &wg)
	waitStarted("app")
	go finalize("r.io/web:1", &wg)
	waitStarted("web, while app is still importing,")
	if out := scrapeMetrics(t); !strings.Contains(out, `angryduck_worker_spread_finalizing{node="n1"} 2`) {
		t.Fatalf("two finalizes not shown running:\n%s", out)
	}
	go finalize("r.io/app:1", &wg) // a retry of app while app still runs
	select {
	case <-started:
		t.Fatal("a second finalize of the same image started while the first ran")
	case <-time.After(300 * time.Millisecond):
	}
	close(release) // app and web finish; the waiting app retry goes next
	waitStarted("the app retry")
	wg.Wait()
	if n := sp.finalizing.len(); n != 0 {
		t.Fatalf("%d lock entries left after every finalize ended", n)
	}
	if out := scrapeMetrics(t); !strings.Contains(out, `angryduck_worker_spread_finalizing{node="n1"} 0`) {
		t.Fatalf("finalizing gauge not back to 0:\n%s", out)
	}
}

func TestKeyedLockGivesUpWhenTheCallerDoes(t *testing.T) {
	k := newKeyedLock()
	unlock, err := k.lock(context.Background(), "x")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := k.lock(ctx, "x"); err == nil {
		t.Fatal("got a held key")
	}
	other, err := k.lock(context.Background(), "y")
	if err != nil {
		t.Fatal("another key waited on x")
	}
	other()
	unlock()
	if k.len() != 0 {
		t.Fatalf("%d entries left", k.len())
	}
}
