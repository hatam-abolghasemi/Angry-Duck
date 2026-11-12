package worker

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"

	"angryduck/internal/model"
)

// In a pod running an app image (a repo CI preheats), every other image is
// a companion, whatever registry it comes from; pods without an app image
// contribute nothing, and an app image is never its own companion.
// Containers are often known only by image ID: their image's names decide.
func TestCompanionsOfTakesTheSidecarsOfAppPods(t *testing.T) {
	targets := map[string]string{
		"registry.example.com/team/app:1":          "sha256:app",
		"sha256:idapp":                             "sha256:app",
		"registry.example.com/team/app:2":          "sha256:app2",
		"registry.example.com/devops/openresty:1":  "sha256:resty",
		"sha256:idresty":                           "sha256:resty",
		"docker.io/library/nginx:1.27":             "sha256:nginx",
		"quay.io/prometheus/php-fpm-exporter:2":    "sha256:exp",
		"docker.io/library/redis:7":                "sha256:redis",
		"docker.io/library/busybox@sha256:busybox": "sha256:bb",
	}
	groups := namesByTarget(targets, false)
	pods := []PodImages{
		{{"sha256:idapp"}, {"sha256:idresty"}}, // by image ID only
		{{"registry.example.com/team/app:1"}, {"nginx:1.27", "sha256:nginx"}, {"quay.io/prometheus/php-fpm-exporter:2"}},
		{{"docker.io/library/redis:7"}},                                            // no app image: nothing
		{{"registry.example.com/team/app:1"}, {"registry.example.com/team/app:2"}}, // both of the app repo
		{{"registry.example.com/team/app:1"}, {"docker.io/library/busybox@sha256:busybox"}},
		{{"registry.example.com/team/app:1"}, {"unknown.io/not-here:1"}}, // not an image here: skipped
	}
	got := companionsOf(pods, map[string]bool{"registry.example.com/team/app": true}, targets, groups)
	want := []string{"sha256:bb", "sha256:exp", "sha256:nginx", "sha256:resty"}
	if len(got) != len(want) {
		t.Fatalf("companions %v, want %v", got, want)
	}
	for _, d := range want {
		if !got[d] {
			t.Fatalf("companions %v, want %v", got, want)
		}
	}
	if c := companionsOf(pods, nil, targets, groups); len(c) != 0 {
		t.Fatalf("no app repos known: %v, want none", c)
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

type published struct {
	refs  []model.ImageRef
	repos []string
}

func companionFixture(t *testing.T, util *float64, pods []PodImages) (*ImageGC, *fakeImages, *[]published) {
	g, f := gcFixture(t, util, nil)
	g.cfg.Settle = 0
	var pub []published
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
		func(refs []model.ImageRef, repos []string) { pub = append(pub, published{refs, repos}) })
	return g, f, &pub
}

const appRepo = "registry.example.com/team/app"

// Companions running here are reported once with their size, with the app
// repos held; the age rule never removes a companion the controller keeps,
// and disk pressure takes companions only after every other unused image.
func TestGCKeepsCompanionsFromAgeAndRemovesThemLastUnderPressure(t *testing.T) {
	u := 0.40
	pods := []PodImages{{{"sha256:idapp"}, {"registry.example.com/devops/openresty:1"}}}
	g, f, pub := companionFixture(t, &u, pods)
	addImage(g, f, "registry.example.com/team/app:1", "sha256:app", time.Minute) // running
	f.targets["sha256:idapp"] = "sha256:app"
	addImage(g, f, "registry.example.com/devops/openresty:1", "sha256:resty", time.Minute) // beside it
	addImage(g, f, "docker.io/library/exporter:1", "sha256:exp", 30*time.Hour)             // a companion elsewhere, unused here
	addImage(g, f, "r.io/old:1", "sha256:old", 30*time.Hour)
	addImage(g, f, "r.io/recent:1", "sha256:recent", time.Hour)

	// No set from the controller yet: the age rule waits.
	g.Tick(context.Background())
	if len(f.deleted) != 0 {
		t.Fatalf("removed %v before the companion set arrived", f.deleted)
	}
	if len(*pub) != 1 || len((*pub)[0].refs) != 0 {
		t.Fatalf("published %v, want one empty list: no app repos yet", *pub)
	}

	// A controller that just started: app repos only, taken by a worker
	// holding none. openresty comes from the app's registry but not from a
	// preheated repo: a companion.
	g.SetCompanions(model.CompanionSet{Hash: "h0", AppRepos: []string{appRepo}})
	g.Tick(context.Background())
	if len(*pub) != 2 || len((*pub)[1].refs) != 1 || (*pub)[1].refs[0].Digest != "sha256:resty" || (*pub)[1].refs[0].Size != 42<<20 || len((*pub)[1].repos) != 1 {
		t.Fatalf("published %+v, want openresty with its size and the app repo", *pub)
	}
	if len(f.deleted) != 0 {
		t.Fatalf("removed %v: the set isn't ready, the age rule still waits", f.deleted)
	}

	// The cluster keeps openresty and exp: only r.io/old goes for age.
	g.SetCompanions(model.CompanionSet{Hash: "h1", Ready: true, Digests: []string{"sha256:resty", "sha256:exp"}, AppRepos: []string{appRepo}})
	g.Tick(context.Background())
	if got := removedTags(f); got != "r.io/old:1" {
		t.Fatalf("removed %q, want only r.io/old:1: exp is a companion", got)
	}
	if len(*pub) != 2 {
		t.Fatalf("published again without a change: %+v", *pub)
	}

	// Disk pressure: recent goes before the companion.
	f.deleted = nil
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
		if strings.Contains(n, "openresty") || strings.Contains(n, "team/app") {
			t.Fatalf("removed %s, which is in use", n)
		}
	}
}

// The set is saved on the node: a restarted worker keeps protecting it at
// once, and a set that isn't ready doesn't replace it.
func TestCompanionSetSurvivesAWorkerRestart(t *testing.T) {
	u := 0.40
	g, _, _ := companionFixture(t, &u, nil)
	g.SetCompanions(model.CompanionSet{Hash: "h1", Ready: true, Digests: []string{"sha256:resty"}, AppRepos: []string{appRepo}})

	g2 := NewImageGC(&fakeImages{targets: map[string]string{}}, func() ([]string, error) { return nil, nil },
		func() (float64, error) { return u, nil }, nil, "n1", g.cfg)
	g2.EnableCompanions(func() ([]PodImages, error) { return nil, nil }, nil, nil)
	if !g2.comp.ready || !g2.comp.keep["sha256:resty"] || !g2.comp.repos[appRepo] {
		t.Fatalf("after restart: ready=%v keep=%v repos=%v, want the saved set", g2.comp.ready, g2.comp.keep, g2.comp.repos)
	}
	if g2.holdAgeRule(time.Now()) {
		t.Fatal("a saved set needs no wait")
	}
	g2.SetCompanions(model.CompanionSet{Hash: "h2", AppRepos: []string{"other/repo"}})
	if !g2.comp.keep["sha256:resty"] || !g2.comp.repos[appRepo] {
		t.Fatal("a set that isn't ready must not replace the held one")
	}
	if filepath.Base(g2.comp.path) != "companions.json" {
		t.Fatalf("saved at %s", g2.comp.path)
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
