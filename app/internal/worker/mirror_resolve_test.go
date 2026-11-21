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
	digest   string
	err      error
	calls    atomic.Int32
	servErr  error // what Servable answers
	servings atomic.Int32
}

func (o *fakeOrigin) Servable(context.Context, string, string) error {
	o.servings.Add(1)
	return o.servErr
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
	// noInventory: the controller's layer inventory places nothing (not
	// reported yet, or it just restarted); only the tag's holders are known.
	noInventory bool
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
				ans.Nodes = []model.Holder{holder}
			}
			_ = json.NewEncoder(w).Encode(ans)
		case "/layers/holders":
			if rig.noInventory {
				_ = json.NewEncoder(w).Encode([]model.Holder{})
				return
			}
			_ = json.NewEncoder(w).Encode([]model.Holder{holder})
		}
	}))
	t.Cleanup(controller.Close)

	m := NewMirror(blobship.NewMemStore(), node, testToken, controller.URL)
	m.EnableResolve(rig.origin, time.Second, downFor, time.Second)
	m.EnableVerify("linux/amd64", time.Second)
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

// During an outage, a tag served from a node's copy brings the rest of the
// image (platform manifest, config, layers) from the nodes holding that
// tag, even when the layer inventory doesn't place that content yet.
func TestMirrorFetchesATagsContentFromItsHoldersWithoutInventory(t *testing.T) {
	rig := newResolveRig(t, "resolve-noinv", 0)
	rig.noInventory = true
	rig.origin.err = errors.New("dial tcp: i/o timeout")
	if code, d := rig.resolve(http.MethodGet, "?ns=registry.example.com"); code != 200 || d != rig.image.Index {
		t.Fatalf("tag: %d %s, want the holder's index", code, d)
	}
	for _, p := range []string{
		"/v2/team/app/manifests/" + rig.image.AMD64Manifest,
		"/v2/team/app/blobs/" + rig.image.Config,
		"/v2/team/app/blobs/" + rig.image.Layers[0],
	} {
		resp, err := http.Get(rig.srv.URL + p + "?ns=registry.example.com")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("%s: %d, want it from the tag's holder", p, resp.StatusCode)
		}
	}
	// Another repo's content isn't sent to this tag's holders.
	resp, err := http.Get(rig.srv.URL + "/v2/team/other/blobs/" + rig.image.Config + "?ns=registry.example.com")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("other repo: %d, want 404", resp.StatusCode)
	}
}

// A registry that names a digest no node holds, while nodes hold the tag
// at another, is trusted only if it can serve that image; a proxy whose
// upstream is down answers tags but fails on the rest.
func TestMirrorServesTheNodeCopyWhenTheRegistryCantServeItsAnswer(t *testing.T) {
	const node = "resolve-unservable"
	rig := newResolveRig(t, node, time.Minute)
	rig.noInventory = true // the rig's inventory would place any digest on the peer
	rig.origin.digest = "sha256:" + strings.Repeat("2", 64)
	rig.origin.servErr = &registryclient.StatusError{Code: http.StatusInternalServerError}
	for range 2 {
		if code, d := rig.resolve(http.MethodGet, "?ns=registry.example.com"); code != 200 || d != rig.image.Index {
			t.Fatalf("%d %s, want the node copy", code, d)
		}
	}
	if n := rig.origin.servings.Load(); n != 1 {
		t.Fatalf("checked %d times, want once per digest per TTL", n)
	}
	want := `angryduck_worker_mirror_resolves_total{node="` + node + `",origin="unservable",result="peer"} 2`
	waitFor(t, want, func() bool { return strings.Contains(scrapeMetrics(t), want) })

	// The registry naming the digest the nodes hold is never checked.
	rig.origin.digest = rig.image.Index
	before := rig.origin.servings.Load()
	rig.resolve(http.MethodHead, "?ns=registry.example.com")
	if n := rig.origin.servings.Load(); n != before {
		t.Fatalf("the nodes' own digest was checked: %d -> %d", before, n)
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
