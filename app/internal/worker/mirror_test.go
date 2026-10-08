package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"angryduck/internal/blobship"
	"angryduck/internal/model"
)

func TestMirrorServesLocalThenPeerThenMisses(t *testing.T) {
	local := blobship.NewMemStore()
	ti := local.AddTestImage(testImage, "base", "app")
	peerStore := blobship.NewMemStore()
	onlyPeer := peerStore.Put([]byte("peer-only layer"))

	peerMirror := NewMirror(peerStore, "w2", testToken, "")
	peerMux := http.NewServeMux()
	peerMirror.RegisterPeer(peerMux)
	peerSrv := httptest.NewServer(peerMux)
	defer peerSrv.Close()

	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+testToken || r.URL.Query().Get("exclude") != "w1" {
			http.Error(w, "no", http.StatusUnauthorized)
			return
		}
		var out []model.Holder
		if r.URL.Query().Get("digest") == onlyPeer {
			out = append(out, model.Holder{NodeID: "w2", Address: strings.TrimPrefix(peerSrv.URL, "http://")})
		}
		_ = json.NewEncoder(w).Encode(out)
	}))
	defer controller.Close()

	m := NewMirror(local, "w1", testToken, controller.URL)
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	get := func(path string) (int, string, string) {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b), resp.Header.Get("Content-Type")
	}
	if code, _, _ := get("/v2/"); code != 200 {
		t.Fatalf("/v2/ = %d", code)
	}
	if code, body, _ := get("/v2/team/app/blobs/" + ti.Layers[1]); code != 200 || body != "app" {
		t.Fatalf("local blob: %d %q", code, body)
	}
	if code, _, ct := get("/v2/team/app/manifests/" + ti.Index); code != 200 || ct != blobship.MediaTypeOCIIndex {
		t.Fatalf("local manifest: %d %s", code, ct)
	}
	if code, body, _ := get("/v2/team/app/blobs/" + onlyPeer); code != 200 || body != "peer-only layer" {
		t.Fatalf("peer blob: %d %q", code, body)
	}
	if code, _, _ := get("/v2/team/app/blobs/sha256:" + strings.Repeat("0", 64)); code != 404 {
		t.Fatalf("unknown blob should miss, got %d", code)
	}
	if code, _, _ := get("/v2/team/app/manifests/latest"); code != 404 {
		t.Fatalf("tags must go to the registry, got %d", code)
	}
}

func TestHostsConfigOnlyTouchesItsOwnFiles(t *testing.T) {
	root := t.TempDir()
	dir := "/etc/containerd/certs.d"
	foreign := filepath.Join(root, dir, "other.example.com", "hosts.toml")
	_ = os.MkdirAll(filepath.Dir(foreign), 0o755)
	_ = os.WriteFile(foreign, []byte("server = \"http://insecure\"\n"), 0o644)

	h := &HostsConfig{HostRoot: root, ConfigDir: dir, Mirror: "127.0.0.1:18082", NodeID: "w1", Extra: []string{"docker.io"}}
	h.Sync([]string{"registry.example.com/devops/x:1", "registry.example.com/y:2", "other.example.com/z:3", "sha256:abc"}, true)
	b, err := os.ReadFile(filepath.Join(root, dir, "registry.example.com", "hosts.toml"))
	if err != nil || !strings.Contains(string(b), `server = "https://registry.example.com"`) || !strings.Contains(string(b), `capabilities = ["pull"]`) {
		t.Fatalf("hosts.toml = %q %v", b, err)
	}
	if b, _ := os.ReadFile(filepath.Join(root, dir, "docker.io", "hosts.toml")); !strings.Contains(string(b), "registry-1.docker.io") {
		t.Fatalf("docker.io hosts.toml = %q", b)
	}
	if b, _ := os.ReadFile(foreign); string(b) != "server = \"http://insecure\"\n" {
		t.Fatal("a hosts.toml Angry Duck doesn't own was changed")
	}
	h.Sync(nil, false)
	if _, err := os.Stat(filepath.Join(root, dir, "registry.example.com", "hosts.toml")); !os.IsNotExist(err) {
		t.Fatal("own hosts.toml not removed when the mirror is off")
	}
	if _, err := os.Stat(foreign); err != nil {
		t.Fatal("foreign hosts.toml removed")
	}
}

// brokenLocal claims every blob it holds but can't open any of them.
type brokenLocal struct{ *blobship.MemStore }

func (brokenLocal) OpenContent(context.Context, string) (io.ReadCloser, int64, error) {
	return nil, 0, errors.New("disk said no")
}

// A local blob that can't be read is a miss, not an error page or an empty
// 200: containerd only moves on to the next host (the registry) on a 404.
func TestMirrorTreatsAnUnreadableLocalBlobAsAMiss(t *testing.T) {
	local := blobship.NewMemStore()
	d := local.Put([]byte("layer"))
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]model.Holder{})
	}))
	defer controller.Close()
	srv := httptest.NewServer(NewMirror(brokenLocal{local}, "w1", testToken, controller.URL).Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v2/team/app/blobs/" + d)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status %d, want 404 so containerd falls back to the registry", resp.StatusCode)
	}
}

// A peer that holds a blob but can't read it answers 404, so the asking
// mirror moves on to its next holder instead of forwarding an empty 200.
func TestMirrorSkipsAPeerThatCantReadItsBlob(t *testing.T) {
	broken := blobship.NewMemStore()
	d := broken.Put([]byte("layer"))
	healthy := blobship.NewMemStore()
	healthy.Put([]byte("layer"))
	peer := func(store MirrorStore, name string) string {
		mux := http.NewServeMux()
		NewMirror(store, name, testToken, "").RegisterPeer(mux)
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)
		return strings.TrimPrefix(srv.URL, "http://")
	}
	holders := []model.Holder{{NodeID: "b", Address: peer(brokenLocal{broken}, "b")}, {NodeID: "c", Address: peer(healthy, "c")}}
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(holders)
	}))
	defer controller.Close()
	srv := httptest.NewServer(NewMirror(blobship.NewMemStore(), "a", testToken, controller.URL).Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v2/team/app/blobs/" + d)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "layer" {
		t.Fatalf("status %d body %q, want the blob from the second peer", resp.StatusCode, body)
	}
}

// An empty holder list isn't cached: a blob that lands on a peer a moment
// later is found on the next request. A list whose holders all failed is
// dropped too.
func TestMirrorAsksAgainAfterAMiss(t *testing.T) {
	peerStore := blobship.NewMemStore()
	d := peerStore.Put([]byte("layer"))
	mux := http.NewServeMux()
	NewMirror(peerStore, "b", testToken, "").RegisterPeer(mux)
	peerSrv := httptest.NewServer(mux)
	defer peerSrv.Close()
	var asks int
	var holders []model.Holder
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asks++
		_ = json.NewEncoder(w).Encode(holders)
	}))
	defer controller.Close()
	srv := httptest.NewServer(NewMirror(blobship.NewMemStore(), "a", testToken, controller.URL).Handler())
	defer srv.Close()
	status := func() int {
		resp, err := http.Get(srv.URL + "/v2/team/app/blobs/" + d)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if c := status(); c != 404 {
		t.Fatalf("nobody holds it yet: %d", c)
	}
	holders = []model.Holder{{NodeID: "b", Address: strings.TrimPrefix(peerSrv.URL, "http://")}}
	if c := status(); c != 200 || asks != 2 {
		t.Fatalf("status %d after %d asks: the empty answer must not have been cached", c, asks)
	}
	if c := status(); c != 200 || asks != 2 {
		t.Fatalf("status %d after %d asks: a useful answer is reused", c, asks)
	}
}
