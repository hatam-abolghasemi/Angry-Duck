package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"

	"angryduck/internal/model"
)

// In a pod running an app image, every other image is a companion; pods
// without an app image contribute nothing, and an app image is never its
// own companion.
func TestCompanionsOfTakesTheSidecarsOfAppPods(t *testing.T) {
	targets := map[string]string{
		"registry.example.com/team/app:1":          "sha256:app",
		"docker.io/library/nginx:1.27":             "sha256:nginx",
		"quay.io/prometheus/php-fpm-exporter:2":    "sha256:exp",
		"docker.io/library/redis:7":                "sha256:redis",
		"registry.example.com/team/sidecar:3":      "sha256:own",
		"docker.io/library/busybox@sha256:busybox": "sha256:bb",
	}
	pods := []PodImages{
		{{"registry.example.com/team/app:1"}, {"nginx:1.27", "sha256:nginx"}, {"quay.io/prometheus/php-fpm-exporter:2"}},
		{{"docker.io/library/redis:7"}}, // no app image: nothing
		{{"registry.example.com/team/app:1"}, {"registry.example.com/team/sidecar:3"}}, // both app images
		{{"registry.example.com/team/app:1"}, {"docker.io/library/busybox@sha256:busybox"}},
		{{"registry.example.com/team/app:1"}, {"unknown.io/not-here:1"}}, // not an image here: skipped
	}
	got := companionsOf(pods, map[string]bool{"registry.example.com": true}, targets)
	want := []string{"sha256:bb", "sha256:exp", "sha256:nginx"}
	if len(got) != len(want) {
		t.Fatalf("companions %v, want %v", got, want)
	}
	for _, d := range want {
		if !got[d] {
			t.Fatalf("companions %v, want %v", got, want)
		}
	}
	if c := companionsOf(pods, nil, targets); len(c) != 0 {
		t.Fatalf("no app registries known: %v, want none", c)
	}
}

// inUsePods groups the containers the cleanup keeps by pod sandbox: a
// running container, or any container of a ready sandbox.
func TestInUsePodsGroupsBySandbox(t *testing.T) {
	c := func(id, sandbox, image string, state runtimeapi.ContainerState) *runtimeapi.Container {
		return &runtimeapi.Container{Id: id, PodSandboxId: sandbox, State: state,
			Image: &runtimeapi.ImageSpec{Image: image}, ImageRef: "sha256:" + image}
	}
	cs := []*runtimeapi.Container{
		c("1", "p1", "app", runtimeapi.ContainerState_CONTAINER_RUNNING),
		c("2", "p1", "nginx", runtimeapi.ContainerState_CONTAINER_EXITED), // ready sandbox: counts
		c("3", "p2", "job", runtimeapi.ContainerState_CONTAINER_EXITED),   // sandbox gone: doesn't
		c("4", "p3", "exp", runtimeapi.ContainerState_CONTAINER_RUNNING),
	}
	pods := inUsePods(cs, map[string]bool{"p1": true})
	if len(pods) != 2 || len(pods[0]) != 2 || len(pods[1]) != 1 {
		t.Fatalf("pods %v, want p1 with two containers and p3 with one", pods)
	}
	if pods[0][1][0] != "nginx" || pods[0][1][1] != "sha256:nginx" {
		t.Fatalf("container refs %v", pods[0][1])
	}
}

func companionFixture(t *testing.T, util *float64, pods []PodImages) (*ImageGC, *fakeImages, *[][]model.ImageRef) {
	g, f := gcFixture(t, util, nil)
	g.cfg.Settle = 0
	var published [][]model.ImageRef
	sizeCalls := map[string]int{}
	g.EnableCompanions(func() ([]PodImages, error) { return pods, nil },
		func(name string) (int64, error) {
			sizeCalls[name]++
			if sizeCalls[name] > 1 {
				t.Errorf("size of %s asked %d times, want once", name, sizeCalls[name])
			}
			if strings.Contains(name, "nosize") {
				return 0, errors.New("no status")
			}
			return 42 << 20, nil
		},
		func(refs []model.ImageRef) { published = append(published, refs) })
	return g, f, &published
}

// Companions running here are reported once with their size; the age rule
// never removes a companion the controller keeps, and disk pressure takes
// companions only after every other unused image.
func TestGCKeepsCompanionsFromAgeAndRemovesThemLastUnderPressure(t *testing.T) {
	u := 0.40
	pods := []PodImages{{{"registry.example.com/team/app:1"}, {"docker.io/library/nginx:1.27"}}}
	g, f, published := companionFixture(t, &u, pods)
	addImage(g, f, "registry.example.com/team/app:1", "sha256:app", time.Minute) // running
	addImage(g, f, "docker.io/library/nginx:1.27", "sha256:nginx", time.Minute)  // running beside it
	addImage(g, f, "docker.io/library/exporter:1", "sha256:exp", 30*time.Hour)   // a companion elsewhere, unused here
	addImage(g, f, "r.io/old:1", "sha256:old", 30*time.Hour)
	addImage(g, f, "r.io/recent:1", "sha256:recent", time.Hour)

	// No set from the controller yet: the age rule waits.
	g.Tick(context.Background())
	if len(f.deleted) != 0 {
		t.Fatalf("removed %v before the companion set arrived", f.deleted)
	}
	if len(*published) != 1 || len((*published)[0]) != 0 {
		t.Fatalf("published %v, want one empty list: companions on, none known without app registries", *published)
	}

	// App registries first (not ready): companions are found and reported.
	g.SetCompanions(model.CompanionSet{Hash: "h0", AppRegistries: []string{"registry.example.com"}})
	g.Tick(context.Background())
	if len(*published) != 2 || len((*published)[1]) != 1 || (*published)[1][0].Digest != "sha256:nginx" || (*published)[1][0].Size != 42<<20 {
		t.Fatalf("published %+v, want nginx with its size", *published)
	}
	if len(f.deleted) != 0 {
		t.Fatalf("removed %v: the set isn't ready, the age rule still waits", f.deleted)
	}

	// The cluster keeps nginx and exp: only r.io/old goes for age.
	g.SetCompanions(model.CompanionSet{Hash: "h1", Ready: true, Digests: []string{"sha256:nginx", "sha256:exp"}, AppRegistries: []string{"registry.example.com"}})
	g.Tick(context.Background())
	if got := removedTags(f); got != "r.io/old:1" {
		t.Fatalf("removed %q, want only r.io/old:1: exp is a companion", got)
	}
	if len(*published) != 2 {
		t.Fatalf("published again without a change: %+v", *published)
	}

	// Disk pressure: recent goes before the companion.
	f.deleted = nil
	u = 0.75
	calls := 0
	g.util = func() (float64, error) {
		calls++
		if calls <= 1 {
			return 0.75, nil
		}
		return 0.65, nil // still above low after the first batch
	}
	g.cfg.Batch = 1
	g.Tick(context.Background())
	if len(f.deleted) == 0 || !strings.Contains(f.deleted[0], "recent") {
		t.Fatalf("deleted %v, want r.io/recent first", f.deleted)
	}
	if got := removedTags(f); got != "docker.io/library/exporter:1,r.io/recent:1" {
		t.Fatalf("removed %q, want recent then the companion", got)
	}
	for _, n := range f.deleted {
		if strings.Contains(n, "nginx") || strings.Contains(n, "team/app") {
			t.Fatalf("removed %s, which is in use", n)
		}
	}
}

// A worker that never hears from a controller with companions stops
// waiting after companionWarmup.
func TestGCAgeRuleWaitsAtMostTheWarmup(t *testing.T) {
	u := 0.40
	g, f, _ := companionFixture(t, &u, nil)
	addImage(g, f, "r.io/old:1", "sha256:old", 30*time.Hour)
	g.comp.started = time.Now().Add(-companionWarmup - time.Second)
	g.Tick(context.Background())
	if got := removedTags(f); got != "r.io/old:1" {
		t.Fatalf("removed %q after the warmup, want r.io/old:1", got)
	}
}
