package worker

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"angryduck/internal/imageref"
	"angryduck/internal/metrics"
	"angryduck/internal/model"
)

// fakeRuntime is an in-memory Runtime double for testing GC and Puller
// without shelling out to a real container runtime.
type fakeRuntime struct {
	mu       sync.Mutex
	local    map[string]bool
	running  map[string]bool
	digests  map[string]string // ref (any alias) -> digest
	sizes    map[string]int64  // ref (any alias) -> byte size, for freed-bytes metric tests
	removed  []string
	pullErrs map[string]error
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{
		local:    make(map[string]bool),
		running:  make(map[string]bool),
		digests:  make(map[string]string),
		sizes:    make(map[string]int64),
		pullErrs: make(map[string]error),
	}
}

func (f *fakeRuntime) PullImage(image string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.pullErrs[image]; ok {
		return err
	}
	f.local[image] = true
	return nil
}

func (f *fakeRuntime) LocalImages() ([]string, map[string]string, map[string]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var refs []string
	for img := range f.local {
		refs = append(refs, img)
	}
	digests := make(map[string]string, len(f.digests))
	for k, v := range f.digests {
		digests[k] = v
	}
	sizes := make(map[string]int64, len(f.sizes))
	for k, v := range f.sizes {
		sizes[k] = v
	}
	return refs, digests, sizes, nil
}

func (f *fakeRuntime) ListRunningImages() ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for img := range f.running {
		out = append(out, img)
	}
	return out, nil
}

// RunningImageRepos counts each entry in f.running once — tests populate
// that set one entry per fake "container", so no alias-deduplication
// concern applies here the way it does for the real crictl backend.
func (f *fakeRuntime) RunningImageRepos() (map[string]int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	counts := make(map[string]int)
	for img := range f.running {
		if repo := imageref.Repo(img); repo != "" {
			counts[repo]++
		}
	}
	return counts, nil
}

func (f *fakeRuntime) RemoveImage(image string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.local, image)
	f.removed = append(f.removed, image)
	return nil
}

func (f *fakeRuntime) wasRemoved(image string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, img := range f.removed {
		if img == image {
			return true
		}
	}
	return false
}

// orderPull drives Puller.HandlePull synchronously enough to record the
// order (the actual pull happens in a goroutine, so we poll briefly for it
// to land in the fake runtime).
func orderPull(t *testing.T, p *Puller, rt *fakeRuntime, image string) {
	t.Helper()
	body, _ := json.Marshal(model.PullOrder{Image: image, OrderedAt: time.Now()})
	req := httptest.NewRequest(http.MethodPost, "/pull", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	p.HandlePull(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("HandlePull(%s) = %d, want %d", image, rec.Code, http.StatusAccepted)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		local, _, _, _ := rt.LocalImages()
		for _, img := range local {
			if img == image {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("image %s never appeared locally after pull order", image)
}

// TestGCRemovesAnyUnusedImageAfterThreshold proves the restored node-wide
// scan: an image Angry Duck never pulled itself (simulating a system image
// like calico/kube-proxy that's genuinely unused) still gets removed after
// enough consecutive misses. This is the operator's explicit choice — Angry
// Duck manages disk space for the whole node, not just its own preheating
// overhead — and is safe specifically because ListRunningImages() here
// returns correctly-parsed values, not the empty-string result the old
// substring-marker bug produced.
func TestGCRemovesAnyUnusedImageAfterThreshold(t *testing.T) {
	rt := newFakeRuntime()
	rt.local["some-system-image:v1"] = true // never ordered via Angry Duck's puller

	puller := NewPuller(rt, 0, "test-node", false, "", 0) // zero grace period
	gc := NewGC(rt, NewInventory(rt), 0, puller, time.Millisecond, 3, false, nil, "test-node", false)

	for i := 0; i < 2; i++ {
		gc.tick()
	}
	if rt.wasRemoved("some-system-image:v1") {
		t.Fatalf("image removed before reaching miss threshold")
	}

	gc.tick() // third consecutive miss
	if !rt.wasRemoved("some-system-image:v1") {
		t.Fatalf("expected unused image to be removed after threshold misses, regardless of who pulled it")
	}
}

// TestGCNeverRemovesImageThatIsActuallyRunning is the regression test for
// the real production bug: it exercises the exact ctrContainerInfo JSON
// path (not raw map membership) to prove a running image survives, even
// though the pretty-printed JSON `ctr containers info` actually emits has a
// space after each colon — the format that broke the old substring-marker
// extraction.
func TestGCNeverRemovesImageThatIsActuallyRunning(t *testing.T) {
	// Simulates exactly what `ctr -n k8s.io containers info <id>` prints:
	// pretty-printed JSON with a space after the colon.
	const prettyPrintedInfo = `{
    "ID": "abc123",
    "Image": "registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.0.2",
    "Runtime": {}
}`
	var parsed ctrContainerInfo
	if err := json.Unmarshal([]byte(prettyPrintedInfo), &parsed); err != nil {
		t.Fatalf("unexpected JSON parse error: %v", err)
	}
	if parsed.Image != "registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.0.2" {
		t.Fatalf("failed to extract Image field from pretty-printed JSON, got %q", parsed.Image)
	}

	// Now prove GC actually spares it end-to-end using that extracted value.
	rt := newFakeRuntime()
	image := "registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.0.2"
	rt.local[image] = true
	rt.running[image] = true // what ListRunningImages() would now correctly report

	puller := NewPuller(rt, 0, "test-node", false, "", 0)
	gc := NewGC(rt, NewInventory(rt), 0, puller, time.Millisecond, 1, false, nil, "test-node", false)

	for i := 0; i < 10; i++ {
		gc.tick()
	}

	if rt.wasRemoved(image) {
		t.Fatalf("GC removed its own actively-running image — this is the exact bug that hit production")
	}
}

// TestGCSparesEveryAliasOfARunningImage is the regression test for the
// second confirmed production bug: containerd reports one running
// container's image via only ONE of that image's several valid aliases (a
// tag, a digest-pinned ref, or a bare digest "image ID"), while
// LocalImages() enumerates ALL of them as separate entries. Before
// digest-based matching, GC correctly spared whichever single alias
// happened to match the running container's reported string, but treated
// the image's OTHER aliases as separate, unused images — including on the
// worker's own running image and the node's `pause` image. This uses the
// exact digests and reference shapes captured from that real incident.
func TestGCSparesEveryAliasOfARunningImage(t *testing.T) {
	const workerDigest = "sha256:a46eb1fcc56fbd68951fbb5c5318534abec151ffac85de937ce7fcef51757efd"
	const pauseDigest = "sha256:278fb9dbcca9518083ad1e11276933a2e96f23de604a3a08cc3c80002767d24c"

	workerTag := "registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.0.5"
	workerDigestRef := "registry.internal-registry.example.com/devops/generic/angry-duck-worker@" + workerDigest
	workerBareDigest := workerDigest // containerd also lists the bare digest as its own ref row

	pauseTag := "repo-sahand.internal-dev.example.com/pause:3.10.1"
	pauseDigestRef := "repo-sahand.internal-dev.example.com/pause@" + pauseDigest

	rt := newFakeRuntime()
	// All the aliases containerd would list for these two pieces of content.
	for _, ref := range []string{workerTag, workerDigestRef, workerBareDigest} {
		rt.local[ref] = true
		rt.digests[ref] = workerDigest
	}
	for _, ref := range []string{pauseTag, pauseDigestRef} {
		rt.local[ref] = true
		rt.digests[ref] = pauseDigest
	}
	// A running container only ever reports ONE alias — the tag, in this
	// case, matching what was actually observed in production logs.
	rt.running[workerTag] = true
	rt.running[pauseTag] = true

	puller := NewPuller(rt, 0, "test-node", false, "", 0)
	gc := NewGC(rt, NewInventory(rt), 0, puller, time.Millisecond, 1, false, nil, "test-node", false)

	for i := 0; i < 5; i++ {
		gc.tick()
	}

	for _, ref := range []string{workerTag, workerDigestRef, workerBareDigest, pauseTag, pauseDigestRef} {
		if rt.wasRemoved(ref) {
			t.Fatalf("GC removed alias=%q of a running image — this is the exact production bug (6 local images, 12 running, only 2 spared)", ref)
		}
	}
}

// TestGCRemovesTrulyUnusedImageEvenWithDigestMatchingEnabled proves digest
// matching doesn't just spare everything indiscriminately — an image whose
// digest genuinely doesn't match anything running should still be removed.
func TestGCRemovesTrulyUnusedImageEvenWithDigestMatchingEnabled(t *testing.T) {
	rt := newFakeRuntime()
	rt.local["stale-image:v1"] = true
	rt.digests["stale-image:v1"] = "sha256:deadbeef"
	rt.running["something-else:v1"] = true
	rt.digests["something-else:v1"] = "sha256:cafef00d"

	puller := NewPuller(rt, 0, "test-node", false, "", 0)
	gc := NewGC(rt, NewInventory(rt), 0, puller, time.Millisecond, 1, false, nil, "test-node", false)

	for i := 0; i < 3; i++ {
		gc.tick()
	}

	if !rt.wasRemoved("stale-image:v1") {
		t.Fatalf("expected genuinely unused image to still be removed with digest matching enabled")
	}
}

func TestGCSparesRunningImage(t *testing.T) {
	rt := newFakeRuntime()
	puller := NewPuller(rt, 0, "test-node", false, "", 0)
	gc := NewGC(rt, NewInventory(rt), 0, puller, time.Millisecond, 1, false, nil, "test-node", false)

	orderPull(t, puller, rt, "registry.example.com/myapp:1.0.0")
	rt.mu.Lock()
	rt.running["registry.example.com/myapp:1.0.0"] = true
	rt.mu.Unlock()

	for i := 0; i < 5; i++ {
		gc.tick()
	}

	if rt.wasRemoved("registry.example.com/myapp:1.0.0") {
		t.Fatalf("GC removed an image that is actively running")
	}
}

func TestGCSparesImageInGracePeriod(t *testing.T) {
	rt := newFakeRuntime()
	puller := NewPuller(rt, time.Hour, "test-node", false, "", 0) // long grace period
	gc := NewGC(rt, NewInventory(rt), 0, puller, time.Millisecond, 1, false, nil, "test-node", false)

	orderPull(t, puller, rt, "registry.example.com/myapp:1.0.0")

	for i := 0; i < 5; i++ {
		gc.tick()
	}

	if rt.wasRemoved("registry.example.com/myapp:1.0.0") {
		t.Fatalf("GC removed an image still within its grace period")
	}
}

// TestGCNeverRemovesExcludedImageEvenWhenNeverObservedRunning is the
// regression test for the pause-image gap: pause backs every pod sandbox
// on the node, but containerd/CRI tracks sandboxes separately from regular
// containers, so ListRunningImages() (backed by `crictl ps`) can never see
// it as running — no digest-matching fix can change that, since the image
// genuinely never appears in the running set. excludeSubstrings must spare
// it unconditionally, without ever incrementing its miss counter.
func TestGCNeverRemovesExcludedImageEvenWhenNeverObservedRunning(t *testing.T) {
	rt := newFakeRuntime()
	rt.local["repo-sahand.internal-dev.example.com/pause:3.10"] = true
	// Deliberately never added to rt.running — simulates crictl ps never
	// reporting the sandbox container, exactly as in production.

	puller := NewPuller(rt, 0, "test-node", false, "", 0)
	gc := NewGC(rt, NewInventory(rt), 0, puller, time.Millisecond, 1, false, []string{"pause"}, "test-node", false)

	for i := 0; i < 10; i++ {
		gc.tick()
	}

	if rt.wasRemoved("repo-sahand.internal-dev.example.com/pause:3.10") {
		t.Fatalf("GC removed an image matching an excluded substring — exclusion must be unconditional")
	}
}

// TestGCExcludeSubstringsOnlyMatchesConfiguredPatterns proves the exclusion
// list is scoped to what's configured — an unrelated genuinely-unused image
// still gets removed on schedule even when some exclusion list is active.
func TestGCExcludeSubstringsOnlyMatchesConfiguredPatterns(t *testing.T) {
	rt := newFakeRuntime()
	rt.local["repo-sahand.internal-dev.example.com/pause:3.10"] = true
	rt.local["registry.example.com/some-stale-app:v1"] = true

	puller := NewPuller(rt, 0, "test-node", false, "", 0)
	gc := NewGC(rt, NewInventory(rt), 0, puller, time.Millisecond, 2, false, []string{"pause", "node-exporter"}, "test-node", false)

	for i := 0; i < 3; i++ {
		gc.tick()
	}

	if rt.wasRemoved("repo-sahand.internal-dev.example.com/pause:3.10") {
		t.Fatalf("excluded image was removed")
	}
	if !rt.wasRemoved("registry.example.com/some-stale-app:v1") {
		t.Fatalf("expected the genuinely unused, non-excluded image to still be removed")
	}
}

// TestGCExcludedImageResetsMissCountIfPreviouslyTracked covers the edge
// case of a config change mid-flight: an image already partway toward
// removal that newly matches an exclusion must have its miss counter
// cleared, not just frozen at its current value.
func TestGCExcludedImageResetsMissCountIfPreviouslyTracked(t *testing.T) {
	rt := newFakeRuntime()
	rt.local["repo-sahand.internal-dev.example.com/pause:3.10"] = true

	puller := NewPuller(rt, 0, "test-node", false, "", 0)
	gc := NewGC(rt, NewInventory(rt), 0, puller, time.Millisecond, 5, false, nil, "test-node", false) // no exclusions yet

	gc.tick()
	gc.tick()
	if gc.missCounts["repo-sahand.internal-dev.example.com/pause:3.10"] != 2 {
		t.Fatalf("expected miss count 2 before exclusion is configured, got %d", gc.missCounts["repo-sahand.internal-dev.example.com/pause:3.10"])
	}

	gc.excludeSubstrings = []string{"pause"} // simulate the config now excluding it
	gc.tick()

	if _, tracked := gc.missCounts["repo-sahand.internal-dev.example.com/pause:3.10"]; tracked {
		t.Fatalf("expected miss count to be cleared once the image became excluded")
	}
}

// TestGCDryRunNeverActuallyRemoves proves dry-run mode reaches the same
// removal decision (past miss threshold, not running, not in grace period)
// but never calls RemoveImage — the safety valve for validating GC's
// decisions against real production data before trusting it to delete.
func TestGCDryRunNeverActuallyRemoves(t *testing.T) {
	rt := newFakeRuntime()
	rt.local["some-image:v1"] = true

	puller := NewPuller(rt, 0, "test-node", false, "", 0)
	gc := NewGC(rt, NewInventory(rt), 0, puller, time.Millisecond, 1, true, nil, "test-node", false) // dryRun=true

	for i := 0; i < 10; i++ {
		gc.tick()
	}

	if rt.wasRemoved("some-image:v1") {
		t.Fatalf("dry-run GC actually removed an image — dry-run must never call RemoveImage")
	}
	// The image should still be reported as present, since nothing removed it.
	local, _, _, _ := rt.LocalImages()
	found := false
	for _, img := range local {
		if img == "some-image:v1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected image to remain present under dry-run")
	}
}

// scrapeMetrics returns the full Prometheus exposition text from the
// package's shared metrics registry, so a test can check for a specific
// label combination without a getter into the unexported counter maps.
func scrapeMetrics(t *testing.T) string {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metrics.Handler().ServeHTTP(rec, req)
	return rec.Body.String()
}

// TestGCLabelsRegistryWhenEnabled proves METRICS_LABEL_REGISTRY=true makes
// GC's removal metrics carry the removed image's actual registry host.
// Uses a node ID unique to this test so the assertion can't pass on a
// stale series left behind by another test sharing the same package-level
// counters.
func TestGCLabelsRegistryWhenEnabled(t *testing.T) {
	rt := newFakeRuntime()
	const image = "registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.0.2"
	rt.local[image] = true

	const nodeID = "test-node-registry-on"
	puller := NewPuller(rt, 0, nodeID, false, "", 0)
	gc := NewGC(rt, NewInventory(rt), 0, puller, time.Millisecond, 1, false, nil, nodeID, true) // labelRegistry=true

	gc.tick()
	if !rt.wasRemoved(image) {
		t.Fatalf("expected image to be removed after one miss (threshold=1)")
	}

	out := scrapeMetrics(t)
	want := `angryduck_worker_images_deleted_total{node="` + nodeID + `",registry="registry.internal-registry.example.com"} 1`
	if !strings.Contains(out, want) {
		t.Errorf("expected metrics output to contain %q, got:\n%s", want, out)
	}
}

// TestGCOmitsRegistryLabelWhenDisabled proves the default
// (METRICS_LABEL_REGISTRY=false) collapses the registry label to a single
// constant empty value regardless of the removed image's actual registry
// — the cardinality kill switch this flag exists for.
func TestGCOmitsRegistryLabelWhenDisabled(t *testing.T) {
	rt := newFakeRuntime()
	const image = "registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.0.2"
	rt.local[image] = true

	const nodeID = "test-node-registry-off"
	puller := NewPuller(rt, 0, nodeID, false, "", 0)
	gc := NewGC(rt, NewInventory(rt), 0, puller, time.Millisecond, 1, false, nil, nodeID, false) // labelRegistry=false (default)

	gc.tick()
	if !rt.wasRemoved(image) {
		t.Fatalf("expected image to be removed after one miss (threshold=1)")
	}

	out := scrapeMetrics(t)
	if strings.Contains(out, `registry="registry.internal-registry.example.com"`) {
		t.Errorf("expected no registry-host label when disabled, got:\n%s", out)
	}
	want := `angryduck_worker_images_deleted_total{node="` + nodeID + `",registry=""} 1`
	if !strings.Contains(out, want) {
		t.Errorf("expected metrics output to contain %q, got:\n%s", want, out)
	}
}
