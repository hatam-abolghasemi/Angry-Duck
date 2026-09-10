package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"angryduck/internal/model"
)

// fakeCtrOnPath drops a no-op "ctr" script on PATH so tagLocally's `ctr
// images tag` call succeeds without a real containerd — this codebase's
// own tests never exercise a real ctr subprocess for the same reason
// (see the empty-peers trick in TestMirrorAnswers404AtOnceWhenNoPeerHasIt),
// so this stays within that existing boundary: it validates the Go-level
// control flow around the command, not containerd itself.
func fakeCtrOnPath(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "ctr"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func TestResolveTagFound(t *testing.T) {
	m, _ := newTestMirror(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/resolve" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if r.URL.Query().Get("tag") != "reg/app:v1" {
			t.Errorf("tag = %q", r.URL.Query().Get("tag"))
		}
		_ = json.NewEncoder(w).Encode(model.ResolveResponse{Digest: testDigest, ObservedAt: time.Now()})
	}))
	digest, observedAt, ok, err := m.resolveTag(context.Background(), "reg/app:v1")
	if err != nil {
		t.Fatalf("resolveTag error: %v", err)
	}
	if !ok || digest != testDigest || observedAt.IsZero() {
		t.Fatalf("resolveTag = digest=%q observedAt=%v ok=%v", digest, observedAt, ok)
	}
}

func TestResolveTagNotFoundIsNotAnError(t *testing.T) {
	m, _ := newTestMirror(t, http.NotFoundHandler())
	_, _, ok, err := m.resolveTag(context.Background(), "reg/app:v1")
	if err != nil {
		t.Fatalf("expected err=nil for an unknown tag (404), got %v", err)
	}
	if ok {
		t.Fatal("expected ok=false for an unknown tag")
	}
}

func TestResolveTagServerErrorIsAnError(t *testing.T) {
	m, _ := newTestMirror(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	_, _, ok, err := m.resolveTag(context.Background(), "reg/app:v1")
	if err == nil || ok {
		t.Fatalf("expected an error for a 500, got ok=%v err=%v", ok, err)
	}
}

func TestFallbackImportNoDigestKnownAnywhere(t *testing.T) {
	m, _ := newTestMirror(t, http.NotFoundHandler()) // /resolve -> 404
	err := m.FallbackImport(context.Background(), "reg/app:v1")
	if err == nil || !strings.Contains(err.Error(), "no node in the fleet") {
		t.Fatalf("err = %v, want a 'no node in the fleet' error", err)
	}
}

func TestFallbackImportAlreadyLocalSkipsPeerLookupAndTags(t *testing.T) {
	fakeCtrOnPath(t)
	m, inv := newTestMirror(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/peers" {
			t.Error("must not look up peers when the digest is already local")
		}
		_ = json.NewEncoder(w).Encode(model.ResolveResponse{Digest: testDigest, ObservedAt: time.Now()})
	}))
	inv.Add(testDigest, "reg/app@"+testDigest)

	if err := m.FallbackImport(context.Background(), "reg/app:v1"); err != nil {
		t.Fatalf("FallbackImport: %v", err)
	}
	if !inv.Has(testDigest) {
		t.Fatal("digest should still be present in inventory")
	}
}

func TestFallbackImportNoPeerAvailable(t *testing.T) {
	m, _ := newTestMirror(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/resolve":
			_ = json.NewEncoder(w).Encode(model.ResolveResponse{Digest: testDigest, ObservedAt: time.Now()})
		case "/peers":
			_ = json.NewEncoder(w).Encode(model.PeersResponse{})
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	err := m.FallbackImport(context.Background(), "reg/app:v1")
	if err == nil || !strings.Contains(err.Error(), "no fresh peer") {
		t.Fatalf("err = %v, want a 'no fresh peer' error", err)
	}
}

func TestFallbackImportTagFailureWhenCtrUnresolvable(t *testing.T) {
	t.Setenv("PATH", t.TempDir()) // deliberately no ctr anywhere on PATH
	m, inv := newTestMirror(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(model.ResolveResponse{Digest: testDigest, ObservedAt: time.Now()})
	}))
	inv.Add(testDigest, "reg/app@"+testDigest)

	err := m.FallbackImport(context.Background(), "reg/app:v1")
	if err == nil {
		t.Fatal("expected an error when ctr cannot be resolved on PATH")
	}
}

func TestFallbackImportUnknownDigestFailsTagLocally(t *testing.T) {
	// Resolves to a digest that isn't in local inventory AND has no fresh
	// peer — a slightly different shape than "no peer" alone, exercising
	// tagLocally's own guard (Lookup fails) if it were ever reached with
	// nothing imported. Here it should stop at the peer-lookup stage.
	fakeCtrOnPath(t)
	m, _ := newTestMirror(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/resolve":
			_ = json.NewEncoder(w).Encode(model.ResolveResponse{Digest: testDigest, ObservedAt: time.Now()})
		case "/peers":
			_ = json.NewEncoder(w).Encode(model.PeersResponse{})
		}
	}))
	err := m.FallbackImport(context.Background(), "reg/app:v1")
	if err == nil {
		t.Fatal("expected an error")
	}
}
