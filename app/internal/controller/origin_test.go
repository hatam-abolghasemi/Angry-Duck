package controller

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"angryduck/internal/model"
	"angryduck/internal/registryclient"
)

type fakeOrigin struct {
	mu      sync.Mutex
	answers map[string]error
	asked   []string
}

func (f *fakeOrigin) CheckManifest(_ context.Context, image string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.asked = append(f.asked, image)
	return f.answers[image]
}

func TestOriginCheckFlagsImagesTheRegistryNoLongerServes(t *testing.T) {
	reg := NewRegistry(time.Minute, time.Minute)
	ok := "registry.example.com/team/app:v1"
	gone := "registry.example.com/team/app:v0"
	locked := "private.example.com/team/db:v3"
	other := "docker.io/library/nginx:1.27"
	reg.Update(model.WorkerReport{NodeID: "n1", Address: "n1:1", Timestamp: time.Now(), Images: []string{ok, gone, locked, other}})
	reg.Update(model.WorkerReport{NodeID: "n2", Address: "n2:1", Timestamp: time.Now(), Images: []string{gone}})

	f := &fakeOrigin{answers: map[string]error{
		gone:   &registryclient.StatusError{Method: "HEAD", Code: 404},
		locked: &registryclient.StatusError{Method: "HEAD", Code: 403},
	}}
	o := NewOriginChecker(reg, f, OriginConfig{Interval: time.Hour, Tick: 30 * time.Second, Registries: []string{"example.com"}})
	now := time.Now()
	o.tick(context.Background(), now)

	if len(f.asked) != 3 {
		t.Fatalf("asked %v, want the three example.com images", f.asked)
	}
	out := scrape(t)
	for _, want := range []string{
		`angryduck_controller_origin_unavailable{image="registry.example.com/team/app:v0",status="missing"} 2`,
		`angryduck_controller_origin_unavailable{image="private.example.com/team/db:v3",status="denied"} 1`,
		`angryduck_controller_origin_images{registry="registry.example.com",status="ok"} 1`,
		`angryduck_controller_origin_images{registry="registry.example.com",status="missing"} 1`,
		`angryduck_controller_origin_images{registry="private.example.com",status="denied"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s", want)
		}
	}
	if strings.Contains(out, `image="registry.example.com/team/app:v1"`) || strings.Contains(out, "docker.io") {
		t.Error("only unserved images of the selected registries are listed")
	}

	f.asked = nil
	o.tick(context.Background(), now.Add(time.Minute))
	if len(f.asked) != 0 {
		t.Fatalf("rechecked too soon: %v", f.asked)
	}
	delete(f.answers, gone)
	o.tick(context.Background(), now.Add(16*time.Minute))
	if len(f.asked) != 2 {
		t.Fatalf("failing images are rechecked after a quarter interval, asked %v", f.asked)
	}
	if strings.Contains(scrape(t), `image="registry.example.com/team/app:v0"`) {
		t.Error("an image served again is no longer listed")
	}
}

func TestOriginCheckSpreadsChecksOverTheInterval(t *testing.T) {
	reg := NewRegistry(time.Minute, time.Minute)
	var images []string
	for i := 0; i < 600; i++ {
		images = append(images, "registry.example.com/app:"+strings.Repeat("x", i%7)+string(rune('a'+i%26))+time.Duration(i).String())
	}
	reg.Update(model.WorkerReport{NodeID: "n1", Address: "n1:1", Timestamp: time.Now(), Images: images})
	f := &fakeOrigin{answers: map[string]error{}}
	o := NewOriginChecker(reg, f, OriginConfig{Interval: time.Hour, Tick: 30 * time.Second})
	o.tick(context.Background(), time.Now())
	if len(f.asked) != 10 {
		t.Fatalf("one tick sent %d checks, want 10 (600 images, twice an hour, every 30s)", len(f.asked))
	}
	if !strings.Contains(scrape(t), `angryduck_controller_origin_images{registry="registry.example.com",status="unchecked"} 590`) {
		t.Error("images not checked yet are counted as unchecked")
	}
}
