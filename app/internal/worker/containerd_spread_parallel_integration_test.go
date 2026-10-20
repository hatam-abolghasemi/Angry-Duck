//go:build integration

package worker

import (
	"archive/tar"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"angryduck/internal/model"
)

// Two images sharing a base layer arrive blob by blob, all blobs at once,
// and register side by side on a real containerd: both resolve and both
// are unpacked.
func TestContainerd_SpreadTwoImagesAtOnce(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	s := NewContainerdStore(c, testNS(t, "spread2"), "overlayfs")
	webLayer := []fileEntry{
		{name: "srv/", typ: tar.TypeDir, mode: 0o755},
		{name: "srv/index.html", typ: tar.TypeReg, mode: 0o644, body: "<h1>quack</h1>\n"},
	}
	app := buildImage(t, testImage, baseLayer, appLayer)
	web := buildImage(t, "registry.example.com/team/web:1", baseLayer, webLayer)
	blobs := map[string][]byte{}
	for _, img := range []builtImage{app, web} {
		for d, b := range img.blobs {
			blobs[d] = b
		}
	}
	sp := NewSpread(s, builtRegistry{blobs: blobs}, spreadToken, "n1", "linux/amd64", "", time.Minute)
	mux := http.NewServeMux()
	sp.Register(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Every distinct layer at once, over the registry path.
	jobs := map[string]string{app.layers[0]: "s1", app.layers[1]: "s1", web.layers[1]: "s2"}
	for d, job := range jobs {
		resp := postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: job, Image: testImage, Digest: d, Size: int64(len(blobs[d])), Path: model.SpreadPathRegistry})
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("fetch %s: status %d, want every blob accepted at once", d, resp.StatusCode)
		}
	}
	sp.wg.Wait()

	finalize := func(img builtImage, job string) model.SpreadFinalize {
		f := model.SpreadFinalize{Job: job, Image: img.name, TopType: "application/vnd.oci.image.manifest.v1+json",
			TopDig: img.manifest, TopSize: int64(len(img.blobs[img.manifest])), Metadata: map[string][]byte{}}
		isLayer := map[string]bool{}
		for _, l := range img.layers {
			isLayer[l] = true
		}
		for d, b := range img.blobs {
			if !isLayer[d] {
				f.Metadata[d] = b
			}
		}
		return f
	}
	var wg sync.WaitGroup
	for img, job := range map[*builtImage]string{&app: "s1", &web: "s2"} {
		wg.Add(1)
		go func(img *builtImage, job string) {
			defer wg.Done()
			resp := postSpread(t, srv, "/spread/finalize", finalize(*img, job))
			var res model.SpreadFinalizeResult
			_ = json.NewDecoder(resp.Body).Decode(&res)
			if resp.StatusCode != http.StatusOK || !res.OK {
				t.Errorf("finalize %s: %d %+v", img.name, resp.StatusCode, res)
			}
		}(img, job)
	}
	wg.Wait()
	for _, img := range []builtImage{app, web} {
		if _, got, err := s.Resolve(ctx, img.name); err != nil || got != img.manifest {
			t.Fatalf("%s resolves to %q (%v), want %s", img.name, got, err, img.manifest)
		}
		if _, err := s.snapshotterSvc().Stat(s.ns(ctx), img.chains[len(img.chains)-1]); err != nil {
			t.Fatalf("%s not unpacked: %v", img.name, err)
		}
	}
}
