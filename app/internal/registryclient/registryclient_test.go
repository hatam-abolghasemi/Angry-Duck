package registryclient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"angryduck/internal/blobship"
	"angryduck/internal/registryauth"
)

func dig(b []byte) string {
	s := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(s[:])
}

type fakeRegistry struct {
	srv         *httptest.Server
	tokenHits   int32
	blobs       map[string][]byte
	contentType map[string]string
	tags        map[string]string
	scheme      string // "bearer" or "basic"
	wantUser    string
	layers      []string
	diffIDs     []string
}

func newFake(t *testing.T, scheme string) *fakeRegistry {
	f := &fakeRegistry{blobs: map[string][]byte{}, contentType: map[string]string{}, tags: map[string]string{}, scheme: scheme, wantUser: "deploy:secret"}
	f.srv = httptest.NewTLSServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)

	put := func(b []byte, ct string) string {
		d := dig(b)
		f.blobs[d], f.contentType[d] = b, ct
		return d
	}
	f.diffIDs = []string{dig([]byte("base-tar")), dig([]byte("app-tar"))}
	f.layers = []string{put([]byte("base-gz"), "application/octet-stream"), put([]byte("app-gz"), "application/octet-stream")}
	cfg, _ := json.Marshal(map[string]any{"os": "linux", "architecture": "amd64", "rootfs": map[string]any{"type": "layers", "diff_ids": f.diffIDs}})
	cfgD := put(cfg, "application/octet-stream")
	man, _ := json.Marshal(map[string]any{"schemaVersion": 2, "mediaType": blobship.MediaTypeOCIManifest,
		"config": map[string]any{"digest": cfgD, "size": len(cfg)},
		"layers": []map[string]any{{"digest": f.layers[0], "size": 7}, {"digest": f.layers[1], "size": 6}}})
	manD := put(man, blobship.MediaTypeOCIManifest)
	idx, _ := json.Marshal(map[string]any{"schemaVersion": 2, "mediaType": blobship.MediaTypeOCIIndex, "manifests": []map[string]any{
		{"digest": dig([]byte("att")), "size": 3, "platform": map[string]string{"os": "unknown", "architecture": "unknown"}, "annotations": map[string]string{"vnd.docker.reference.type": "attestation-manifest"}},
		{"digest": dig([]byte("arm")), "size": 3, "platform": map[string]string{"os": "linux", "architecture": "arm64"}},
		{"digest": manD, "size": len(man), "platform": map[string]string{"os": "linux", "architecture": "amd64"}},
	}})
	f.tags["v1"] = put(idx, blobship.MediaTypeOCIIndex)
	return f
}

func (f *fakeRegistry) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/token" {
		atomic.AddInt32(&f.tokenHits, 1)
		u, p, _ := r.BasicAuth()
		if u+":"+p != f.wantUser || r.URL.Query().Get("scope") != "repository:team/app:pull" || r.URL.Query().Get("service") != "https://reg/v2/token" {
			http.Error(w, "bad token request", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"token": "T1", "expires_in": 300})
		return
	}
	authed := false
	switch f.scheme {
	case "bearer":
		authed = r.Header.Get("Authorization") == "Bearer T1"
	case "basic":
		u, p, _ := r.BasicAuth()
		authed = u+":"+p == f.wantUser
	}
	if !authed {
		if f.scheme == "bearer" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+f.srv.URL+`/token",service="https://reg/v2/token"`)
		} else {
			w.Header().Set("WWW-Authenticate", `Basic realm="nexus"`)
		}
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/v2/team/app/")
	kind, ref, _ := strings.Cut(rest, "/")
	if kind == "manifests" {
		if d, ok := f.tags[ref]; ok {
			ref = d
		}
	}
	b, ok := f.blobs[ref]
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", f.contentType[ref])
	_, _ = w.Write(b)
}

func client(t *testing.T, f *fakeRegistry) (*Client, string) {
	host := strings.TrimPrefix(f.srv.URL, "https://")
	dir := t.TempDir()
	cfg := `{"auths":{"` + host + `":{"username":"deploy","password":"secret"}}}`
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	creds, err := registryauth.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	return New(creds, 5*time.Second, time.Minute).WithHTTPClient(f.srv.Client()), host + "/team/app:v1"
}

func TestLayersThroughIndexWithBearerChallenge(t *testing.T) {
	f := newFake(t, "bearer")
	c, image := client(t, f)
	layers, err := c.Layers(context.Background(), image, "linux/amd64")
	if err != nil {
		t.Fatal(err)
	}
	chains := blobship.ChainIDs(f.diffIDs)
	if len(layers) != 2 || layers[0].Digest != f.layers[0] || layers[1].ChainID != chains[1] || layers[0].Size != 7 {
		t.Fatalf("layers = %+v", layers)
	}
	// A second image of the same repo reuses the token; a repeat of the
	// same image is served from cache.
	c.cache = map[string]cached{}
	if _, err := c.Layers(context.Background(), image, "linux/amd64"); err != nil {
		t.Fatal(err)
	}
	if hits := atomic.LoadInt32(&f.tokenHits); hits != 1 {
		t.Fatalf("token fetched %d times, want 1", hits)
	}
}

func TestLayersWithBasicChallenge(t *testing.T) {
	f := newFake(t, "basic")
	c, image := client(t, f)
	if _, err := c.Layers(context.Background(), image, "linux/amd64"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Layers(context.Background(), image, "linux/s390x"); err == nil {
		t.Fatal("expected an error for a platform the index lacks")
	}
}

func TestParseChallenge(t *testing.T) {
	cases := map[string][3]string{
		`Bearer realm="https://registry.example.com/v2/token",service="https://registry.example.com/v2/token"`: {"Bearer", "https://registry.example.com/v2/token", "https://registry.example.com/v2/token"},
		`Bearer realm="https://git.example.com/jwt/auth",service="container_registry"`:                         {"Bearer", "https://git.example.com/jwt/auth", "container_registry"},
		`Basic realm="Sonatype Nexus Repository Manager"`:                                                      {"Basic", "Sonatype Nexus Repository Manager", ""},
	}
	for h, want := range cases {
		s, p := ParseChallenge(h)
		if s != want[0] || p["realm"] != want[1] || p["service"] != want[2] {
			t.Fatalf("%s -> %s %v", h, s, p)
		}
	}
}

func TestParseRef(t *testing.T) {
	r, err := ParseRef("nginx:1.27")
	if err != nil || r.APIHost != "registry-1.docker.io" || r.Repo != "library/nginx" || r.Reference != "1.27" {
		t.Fatalf("%+v %v", r, err)
	}
	r, err = ParseRef("registry.example.com:8443/team/app@sha256:abc")
	if err != nil || r.Host != "registry.example.com:8443" || r.Repo != "team/app" || r.Reference != "sha256:abc" {
		t.Fatalf("%+v %v", r, err)
	}
}

func TestResolveKeepsVerifiedMetadataForRegistering(t *testing.T) {
	f := newFake(t, "bearer")
	c, image := client(t, f)
	res, err := c.Resolve(context.Background(), image, "linux/amd64")
	if err != nil {
		t.Fatal(err)
	}
	if res.Top.Digest != f.tags["v1"] || res.Top.MediaType != blobship.MediaTypeOCIIndex || res.Top.Size != int64(len(f.blobs[f.tags["v1"]])) {
		t.Fatalf("top = %+v", res.Top)
	}
	// Index, platform manifest, config: all a node needs besides layers.
	if len(res.Metadata) != 3 {
		t.Fatalf("%d metadata blobs, want 3", len(res.Metadata))
	}
	for d, b := range res.Metadata {
		if dig(b) != d {
			t.Fatalf("metadata %s doesn't match its digest", d)
		}
	}
}

func TestResolveRejectsContentThatDoesntMatchItsDigest(t *testing.T) {
	f := newFake(t, "bearer")
	c, image := client(t, f)
	// Corrupt the config the manifest points at.
	for d, b := range f.blobs {
		if strings.Contains(string(b), "diff_ids") {
			f.blobs[d] = []byte(strings.Replace(string(b), "amd64", "arm64", 1))
		}
	}
	if _, err := c.Resolve(context.Background(), image, "linux/amd64"); err == nil || !strings.Contains(err.Error(), "config has digest") {
		t.Fatalf("err = %v, want a config digest mismatch", err)
	}
}

func TestOpenBlobStreamsOneLayerWithAuth(t *testing.T) {
	f := newFake(t, "bearer")
	c, image := client(t, f)
	body, size, err := c.OpenBlob(context.Background(), image, f.layers[1])
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	b := make([]byte, 64)
	n, _ := body.Read(b)
	if string(b[:n]) != "app-gz" || (size != -1 && size != 6) {
		t.Fatalf("got %q size %d", b[:n], size)
	}
	if _, _, err := c.OpenBlob(context.Background(), image, dig([]byte("absent"))); err == nil {
		t.Fatal("expected an error for a missing blob")
	}
}
