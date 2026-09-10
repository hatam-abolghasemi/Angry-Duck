package worker

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func fakeCtr(t *testing.T, images map[string]string, content map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	imageFile := filepath.Join(dir, "images")
	contentFile := filepath.Join(dir, "content")
	for ref, digest := range images {
		f, _ := os.OpenFile(imageFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		_, _ = f.WriteString(ref + " application/vnd.oci.image.manifest.v1+json " + digest + " 1 B linux/amd64 -\n")
		_ = f.Close()
	}
	for digest, body := range content {
		f, _ := os.OpenFile(contentFile+"."+digest[7:], os.O_CREATE|os.O_WRONLY, 0o600)
		_, _ = f.WriteString(body)
		_ = f.Close()
	}

	script := filepath.Join(dir, "ctr")
	body := "#!/bin/sh\n"
	body += "if [ \"$4\" = \"list\" ]; then cat '" + imageFile + "'; exit 0; fi\n"
	body += "if [ \"$4\" = \"get\" ]; then cat \"" + contentFile + ".${5#sha256:}\"; exit 0; fi\n"
	body += "exit 1\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	return script
}

func TestRegistryMirrorServesLocalManifest(t *testing.T) {
	const (
		image  = "registry.example.com/app:v1"
		digest = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
		body   = `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json"}`
	)
	ctr := fakeCtr(t, map[string]string{image: digest}, map[string]string{digest: body})
	m := NewRegistryMirror(ctr, "", "node-a", true, 3, 2, time.Minute, time.Second)
	req := httptest.NewRequest(http.MethodGet, "/v2/app/manifests/v1?ns=registry.example.com", nil)
	rr := httptest.NewRecorder()
	m.Handle(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("Docker-Content-Digest") != digest {
		t.Fatalf("digest=%q", rr.Header().Get("Docker-Content-Digest"))
	}
	if rr.Body.String() != body {
		t.Fatalf("body=%q", rr.Body.String())
	}
}

func TestRegistryMirrorFallsThroughToPeer(t *testing.T) {
	const (
		image  = "registry.example.com/app:v1"
		digest = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
		body   = `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json"}`
	)
	ctr := fakeCtr(t, nil, nil)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-AngryDuck-Registry") != "registry.example.com" {
			t.Fatalf("peer registry header=%q", r.Header.Get("X-AngryDuck-Registry"))
		}
		w.Header().Set("Docker-Content-Digest", digest)
		w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	defer peer.Close()

	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"candidates":[{"node_id":"peer","address":"` + peer.Listener.Addr().String() + `"}]}`))
	}))
	defer controller.Close()

	m := NewRegistryMirror(ctr, controller.URL, "node-a", true, 3, 2, time.Minute, time.Second)
	req := httptest.NewRequest(http.MethodGet, "/v2/app/manifests/v1?ns=registry.example.com", nil)
	rr := httptest.NewRecorder()
	m.Handle(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if rr.Body.String() != body {
		t.Fatalf("body=%q", rr.Body.String())
	}
}

func TestParseDistributionRequest(t *testing.T) {
	cases := []struct {
		path, kind, repo, ref, digest string
	}{
		{path: "/v2/library/app/manifests/latest", kind: "manifest", repo: "library/app", ref: "latest"},
		{path: "/v2/library/app/manifests/sha256:abc", kind: "manifest", repo: "library/app", ref: "sha256:abc", digest: "sha256:abc"},
		{path: "/v2/library/app/blobs/sha256:abc", kind: "blob", repo: "library/app", digest: "sha256:abc"},
	}
	for _, tc := range cases {
		r := httptest.NewRequest(http.MethodGet, tc.path, nil)
		repo, kind, ref, digest, ok := parseDistributionRequest(r)
		if !ok || repo != tc.repo || kind != tc.kind || ref != tc.ref || digest != tc.digest {
			t.Fatalf("%s -> ok=%v repo=%q kind=%q ref=%q digest=%q", tc.path, ok, repo, kind, ref, digest)
		}
	}
}
