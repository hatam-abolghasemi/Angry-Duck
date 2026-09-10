package controller

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"angryduck/internal/model"
)

const tagRef = "repo-afra.internal-dev.example.com/rich-ubuntu:22.04"

func TestResolveTagUnknownTag(t *testing.T) {
	r := NewRegistry(30*time.Second, time.Minute)
	if _, _, ok := r.ResolveTag(tagRef); ok {
		t.Fatal("expected ok=false for a tag nobody has ever reported")
	}
}

func TestUpdateTagsThenResolve(t *testing.T) {
	r := NewRegistry(30*time.Second, time.Minute)
	r.UpdateTags(map[string]string{tagRef: dg})
	digest, observedAt, ok := r.ResolveTag(tagRef)
	if !ok {
		t.Fatal("expected the tag to resolve after UpdateTags")
	}
	if digest != dg {
		t.Fatalf("digest = %q, want %q", digest, dg)
	}
	if observedAt.IsZero() || time.Since(observedAt) > time.Second {
		t.Fatalf("observedAt = %v, want ~now", observedAt)
	}
}

// This is the behavior the whole fallback-safety story depends on: a
// digest that keeps getting re-reported unchanged must not look freshly
// confirmed just because a heartbeat happened. Only an actual digest
// CHANGE may move observedAt forward.
func TestUpdateTagsDoesNotRefreshObservedAtOnUnchangedDigest(t *testing.T) {
	r := NewRegistry(30*time.Second, time.Minute)
	r.UpdateTags(map[string]string{tagRef: dg})
	_, first, _ := r.ResolveTag(tagRef)

	time.Sleep(5 * time.Millisecond)
	r.UpdateTags(map[string]string{tagRef: dg}) // same digest, re-reported
	_, second, _ := r.ResolveTag(tagRef)

	if !first.Equal(second) {
		t.Fatalf("observedAt moved (%v -> %v) on an unchanged re-report; a stale mapping would look artificially fresh", first, second)
	}
}

func TestUpdateTagsBumpsObservedAtOnDigestChange(t *testing.T) {
	r := NewRegistry(30*time.Second, time.Minute)
	r.UpdateTags(map[string]string{tagRef: dg})
	_, first, _ := r.ResolveTag(tagRef)

	time.Sleep(5 * time.Millisecond)
	const dg2 = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	r.UpdateTags(map[string]string{tagRef: dg2})
	digest, second, _ := r.ResolveTag(tagRef)

	if digest != dg2 {
		t.Fatalf("digest = %q, want the new one %q", digest, dg2)
	}
	if !second.After(first) {
		t.Fatalf("observedAt did not advance on a genuine digest change: %v -> %v", first, second)
	}
}

func TestUpdateTagsIgnoresEmptyEntries(t *testing.T) {
	r := NewRegistry(30*time.Second, time.Minute)
	r.UpdateTags(map[string]string{"": dg, tagRef: ""})
	if _, _, ok := r.ResolveTag(tagRef); ok {
		t.Fatal("an empty digest must not create a resolvable entry")
	}
	if _, _, ok := r.ResolveTag(""); ok {
		t.Fatal("an empty tag must not create a resolvable entry")
	}
}

func TestUpdateTagsHandlesNilAndEmptyMapWithoutPanicking(t *testing.T) {
	r := NewRegistry(30*time.Second, time.Minute)
	r.UpdateTags(nil)
	r.UpdateTags(map[string]string{})
}

func TestHandleResolve(t *testing.T) {
	reg := NewRegistry(30*time.Second, time.Minute)
	reg.UpdateTags(map[string]string{tagRef: dg})
	srv := httptest.NewServer(NewServer(reg, NewRanker(reg, 1, time.Hour, nil, true), 3).Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/resolve?tag=" + tagRef + "&node=asker")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var rr model.ResolveResponse
	if err := json.NewDecoder(resp.Body).Decode(&rr); err != nil {
		t.Fatal(err)
	}
	if rr.Digest != dg {
		t.Fatalf("digest = %q, want %q", rr.Digest, dg)
	}
	if rr.ObservedAt.IsZero() {
		t.Fatal("expected a non-zero ObservedAt")
	}
}

func TestHandleResolveUnknownTagIs404(t *testing.T) {
	reg := NewRegistry(30*time.Second, time.Minute)
	srv := httptest.NewServer(NewServer(reg, NewRanker(reg, 1, time.Hour, nil, true), 3).Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/resolve?tag=nobody-has-this:latest")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestHandleResolveRequiresTag(t *testing.T) {
	reg := NewRegistry(30*time.Second, time.Minute)
	srv := httptest.NewServer(NewServer(reg, NewRanker(reg, 1, time.Hour, nil, true), 3).Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/resolve")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleResolveRejectsNonGet(t *testing.T) {
	reg := NewRegistry(30*time.Second, time.Minute)
	srv := httptest.NewServer(NewServer(reg, NewRanker(reg, 1, time.Hour, nil, true), 3).Handler())
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/resolve?tag=x", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", resp.StatusCode)
	}
}

// handleReport must feed UpdateTags too, not just Update — this checks
// the end-to-end wire-up, not just the registry method in isolation.
func TestHandleReportFeedsTagsThroughToResolve(t *testing.T) {
	reg := NewRegistry(30*time.Second, time.Minute)
	srv := httptest.NewServer(NewServer(reg, NewRanker(reg, 1, time.Hour, nil, true), 3).Handler())
	defer srv.Close()

	rep := model.WorkerReport{
		NodeID: "a", Address: "a:1", Timestamp: time.Now(),
		Tags: map[string]string{tagRef: dg},
	}
	body, _ := json.Marshal(rep)
	resp, err := http.Post(srv.URL+"/report", "application/json", bytes.NewReader(body))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("report: %v %v", err, resp)
	}

	digest, _, ok := reg.ResolveTag(tagRef)
	if !ok || digest != dg {
		t.Fatalf("ResolveTag after /report = (%q, %v), want (%q, true)", digest, ok, dg)
	}
}
