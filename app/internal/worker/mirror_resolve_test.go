package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"angryduck/internal/blobship"
	"angryduck/internal/model"
	"angryduck/internal/registryclient"
)

// fakeOrigin answers ManifestDigest with digest or err, counting calls.
type fakeOrigin struct {
	digest string
	err    error
	calls  atomic.Int32
}

func (o *fakeOrigin) ManifestDigest(context.Context, string) (string, error) {
	o.calls.Add(1)
	return o.digest, o.err
}

// resolveRig is a mirror on node "a" whose only peer, "b", holds
// testImage; the controller says so.
type resolveRig struct {
	t       *testing.T
	srv     *httptest.Server
	origin  *fakeOrigin
	image   blobship.TestImage
	holders int
}

func newResolveRig(t *testing.T, node string, downFor time.Duration) *resolveRig {
	rig := &resolveRig{t: t, origin: &fakeOrigin{}, holders: 1}
	peerStore := blobship.NewMemStore()
	rig.image = peerStore.AddTestImage(testImage, "base", "app")
	peerMux := http.NewServeMux()
	NewMirror(peerStore, "b", testToken, "").RegisterPeer(peerMux)
	peer := httptest.NewServer(peerMux)
	t.Cleanup(peer.Close)
	holder := model.Holder{NodeID: "b", Address: strings.TrimPrefix(peer.URL, "http://")}

	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+testToken {
			http.Error(w, "no", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/images/tag":
			if r.URL.Query().Get("image") != testImage {
				t.Errorf("asked about %q", r.URL.Query().Get("image"))
			}
			ans := model.TagHolders{Holders: rig.holders}
			if r.URL.Query().Get("newest") == "1" {
				ans.Digest = rig.image.Index
			}
			_ = json.NewEncoder(w).Encode(ans)
		case "/layers/holders":
			_ = json.NewEncoder(w).Encode([]model.Holder{holder})
		}
	}))
	t.Cleanup(controller.Close)

	m := NewMirror(blobship.NewMemStore(), node, testToken, controller.URL)
	m.EnableResolve(rig.origin, time.Second, downFor, time.Second)
	rig.srv = httptest.NewServer(m.Handler())
	t.Cleanup(rig.srv.Close)
	return rig
}

// resolve sends containerd's tag request and returns the status and the
// digest the mirror answered with.
func (rig *resolveRig) resolve(method, query string) (int, string) {
	req, _ := http.NewRequest(method, rig.srv.URL+"/v2/team/app/manifests/1.5.0"+query, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		rig.t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode, resp.Header.Get("Docker-Content-Digest")
}

func TestMirrorResolvesTagsWithTheRegistryFirst(t *testing.T) {
	const node = "resolve-fresh"
	rig := newResolveRig(t, node, time.Minute)

	rig.origin.digest = rig.image.Index
	if code, d := rig.resolve(http.MethodHead, "?ns=registry.example.com"); code != 200 || d != rig.image.Index {
		t.Fatalf("registry's digest held by a peer: %d %s", code, d)
	}
	rig.origin.digest = "sha256:" + strings.Repeat("1", 64)
	if code, _ := rig.resolve(http.MethodHead, "?ns=registry.example.com"); code != 404 {
		t.Fatalf("a tag the registry moved to a digest no node holds must go to the registry, got %d", code)
	}
	rig.holders = 0
	if code, _ := rig.resolve(http.MethodHead, "?ns=registry.example.com"); code != 404 || rig.origin.calls.Load() != 2 {
		t.Fatalf("a tag no node holds: %d after %d registry calls, want 404 without asking", code, rig.origin.calls.Load())
	}
	if code, _ := rig.resolve(http.MethodGet, ""); code != 404 {
		t.Fatalf("without ns the registry is unknown: %d", code)
	}
	for _, want := range []string{
		`angryduck_worker_mirror_resolves_total{node="` + node + `",origin="ok",result="peer"} 1`,
		`angryduck_worker_mirror_resolves_total{node="` + node + `",origin="ok",result="miss"} 1`,
		`angryduck_worker_mirror_resolves_total{node="` + node + `",origin="unchecked",result="miss"} 1`,
	} {
		waitFor(t, want, func() bool { return strings.Contains(scrapeMetrics(t), want) })
	}
}

// Whatever the registry says short of a digest, peers still share.
func TestMirrorServesTheNewestNodeCopyWhenTheRegistryCantAnswer(t *testing.T) {
	const node = "resolve-fallback"
	for _, c := range []struct {
		err    error
		origin string
	}{
		{&registryclient.StatusError{Code: http.StatusNotFound}, "missing"},
		{&registryclient.StatusError{Code: http.StatusUnauthorized}, "denied"},
		{errors.New("dial tcp: i/o timeout"), "unreachable"},
	} {
		rig := newResolveRig(t, node, 0)
		rig.origin.err = c.err
		if code, d := rig.resolve(http.MethodGet, "?ns=registry.example.com"); code != 200 || d != rig.image.Index {
			t.Fatalf("%s: %d %s, want the peer's copy", c.origin, code, d)
		}
		want := `angryduck_worker_mirror_resolves_total{node="` + node + `",origin="` + c.origin + `",result="peer"} 1`
		waitFor(t, want, func() bool { return strings.Contains(scrapeMetrics(t), want) })
	}
}

func TestMirrorLeavesAnUnreachableRegistryAloneForAWhile(t *testing.T) {
	rig := newResolveRig(t, "resolve-down", time.Minute)
	rig.origin.err = errors.New("connection refused")
	for range 3 {
		if code, _ := rig.resolve(http.MethodHead, "?ns=registry.example.com"); code != 200 {
			t.Fatalf("status %d, want the peer's copy", code)
		}
	}
	if n := rig.origin.calls.Load(); n != 1 {
		t.Fatalf("registry asked %d times, want once until it may have recovered", n)
	}
}

func TestHostsConfigResolveCapability(t *testing.T) {
	if s := hostsToml("registry.example.com", "10.0.0.1:18082", "", true); !strings.Contains(s, `capabilities = ["pull", "resolve"]`) {
		t.Fatalf("resolve on:\n%s", s)
	}
	if s := hostsToml("registry.example.com", "10.0.0.1:18082", "", false); !strings.Contains(s, `capabilities = ["pull"]`) {
		t.Fatalf("resolve off:\n%s", s)
	}
}
