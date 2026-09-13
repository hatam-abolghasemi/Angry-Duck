package controller

import (
	"io"
	"net/http/httptest"
	"strings"
	"time"

	"testing"

	"angryduck/internal/model"
)

const rescueImg = "registry.internal-registry.example.com/team/app:v1"

func reportWithImages(nodeID, addr string, images []string) model.WorkerReport {
	return model.WorkerReport{
		NodeID: nodeID, Address: addr, Images: images, Timestamp: time.Now(),
	}
}

func TestRescueSourceFor_NoWorkerHasIt(t *testing.T) {
	r := NewRegistry(30*time.Second, time.Minute)
	r.Update(reportWithImages("a", "a:1", []string{"other:latest"}))
	if _, ok := r.RescueSourceFor(rescueImg, ""); ok {
		t.Fatal("expected ok=false when no worker reports the image")
	}
}

func TestRescueSourceFor_FindsAFreshHolder(t *testing.T) {
	r := NewRegistry(30*time.Second, time.Minute)
	r.Update(reportWithImages("a", "10.0.0.1:18081", []string{rescueImg}))
	addr, ok := r.RescueSourceFor(rescueImg, "")
	if !ok || addr != "10.0.0.1:18081" {
		t.Fatalf("RescueSourceFor = (%q, %v), want (10.0.0.1:18081, true)", addr, ok)
	}
}

func TestRescueSourceFor_ExcludesTheAskingNode(t *testing.T) {
	r := NewRegistry(30*time.Second, time.Minute)
	r.Update(reportWithImages("a", "10.0.0.1:18081", []string{rescueImg}))
	if _, ok := r.RescueSourceFor(rescueImg, "a"); ok {
		t.Fatal("expected ok=false when the only holder is the asking node itself")
	}
}

func TestRescueSourceFor_IgnoresStaleWorkers(t *testing.T) {
	r := NewRegistry(1*time.Millisecond, time.Minute)
	r.Update(reportWithImages("a", "10.0.0.1:18081", []string{rescueImg}))
	time.Sleep(5 * time.Millisecond)
	if _, ok := r.RescueSourceFor(rescueImg, ""); ok {
		t.Fatal("expected ok=false for a worker that hasn't reported recently")
	}
}

func TestRescueSourceFor_ExactMatchOnly(t *testing.T) {
	r := NewRegistry(30*time.Second, time.Minute)
	r.Update(reportWithImages("a", "10.0.0.1:18081", []string{"registry.internal-registry.example.com/team/app:v2"}))
	// Same repo, different tag: must NOT match. Rescue is exact-reference
	// only, unlike preheat's repo-locality heuristic.
	if _, ok := r.RescueSourceFor(rescueImg, ""); ok {
		t.Fatal("expected ok=false for a different tag of the same repo")
	}
}

func TestRescueSourceFor_UpdateReplacesOldImageSet(t *testing.T) {
	r := NewRegistry(30*time.Second, time.Minute)
	r.Update(reportWithImages("a", "10.0.0.1:18081", []string{rescueImg}))
	// Node no longer has it (GC'd it) — a fresh report must stop offering it.
	r.Update(reportWithImages("a", "10.0.0.1:18081", []string{"unrelated:latest"}))
	if _, ok := r.RescueSourceFor(rescueImg, ""); ok {
		t.Fatal("expected ok=false after the node's report stopped listing the image")
	}
}

func TestHandleRescueSource_Found(t *testing.T) {
	reg := NewRegistry(30*time.Second, time.Minute)
	reg.Update(reportWithImages("a", "10.0.0.1:18081", []string{rescueImg}))
	srv := newTestControllerServer(t, reg)

	body := getJSON(t, srv, "/rescue-source?image="+rescueImg)
	if !containsAll(body, `"address":"10.0.0.1:18081"`) {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandleRescueSource_NotFoundReturnsEmptyAddressNot404(t *testing.T) {
	reg := NewRegistry(30*time.Second, time.Minute)
	srv := newTestControllerServer(t, reg)

	status, body := getJSONWithStatus(t, srv, "/rescue-source?image=nobody-has-this:latest")
	if status != 200 {
		t.Fatalf("status = %d, want 200 (empty address, not an error)", status)
	}
	if containsAll(body, `"address"`) {
		t.Fatalf("expected no address field when omitempty and empty, got: %s", body)
	}
}

func TestHandleRescueSource_RequiresImage(t *testing.T) {
	reg := NewRegistry(30*time.Second, time.Minute)
	srv := newTestControllerServer(t, reg)
	status, _ := getJSONWithStatus(t, srv, "/rescue-source")
	if status != 400 {
		t.Fatalf("status = %d, want 400", status)
	}
}

// --- test helpers ------------------------------------------------------

func newTestControllerServer(t *testing.T, reg *Registry) *httptest.Server {
	t.Helper()
	ranker := NewRanker(reg, 1, time.Hour, nil, true, false)
	srv := httptest.NewServer(NewServer(reg, ranker).Handler())
	t.Cleanup(srv.Close)
	return srv
}

func getJSON(t *testing.T, srv *httptest.Server, path string) string {
	t.Helper()
	_, body := getJSONWithStatus(t, srv, path)
	return body
}

func getJSONWithStatus(t *testing.T, srv *httptest.Server, path string) (int, string) {
	t.Helper()
	resp, err := srv.Client().Get(srv.URL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	return resp.StatusCode, string(b)
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}
