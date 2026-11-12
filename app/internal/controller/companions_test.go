package controller

import (
	"context"
	"testing"
	"time"

	"angryduck/internal/model"
)

const (
	nginx = "mirror.example.org/library/nginx:1.27"
	exp   = "quay.io/prometheus/php-fpm-exporter:2"
	huge  = "mirror.example.org/library/huge:1"
)

func reportCompanions(reg *Registry, node, addr string, refs ...model.ImageRef) model.ReportAck {
	return reg.Update(model.WorkerReport{NodeID: node, Address: addr, Utilization: 0.2, Timestamp: time.Now(),
		CompanionState: true, Companions: refs, CompanionsHash: "old"})
}

// The set goes out only once the warmup is over; before that, acks carry
// the app registries (credentials minus public registries, plus what CI
// preheated from) with Ready unset, so workers keep the digests they hold.
// Images above the cap aren't kept, and an unchanged set isn't re-sent.
func TestCompanionsSetAfterWarmupWithCap(t *testing.T) {
	reg := NewRegistry(time.Hour, time.Minute)
	c := NewCompanions(reg, CompanionConfig{
		CredRegistries: []string{"docker.io", "registry.example.com"},
		MaxBytes:       100 << 20,
		Warmup:         time.Hour,
	})
	c.NoteRegistry("other.example.com/team/web:3")

	ack := reportCompanions(reg, "w1", "w1:1",
		model.ImageRef{Digest: "sha256:n", Name: nginx, Size: 10 << 20},
		model.ImageRef{Digest: "sha256:h", Name: huge, Size: 500 << 20})
	reportCompanions(reg, "w2", "w2:1",
		model.ImageRef{Digest: "sha256:n", Name: nginx, Size: 10 << 20},
		model.ImageRef{Digest: "sha256:e", Name: exp}) // no size yet
	if ack.Companions == nil || ack.Companions.Ready || len(ack.Companions.Digests) != 0 {
		t.Fatalf("before the warmup: %+v, want app registries only", ack.Companions)
	}
	if got := ack.Companions.AppRegistries; len(got) != 2 || got[0] != "other.example.com" || got[1] != "registry.example.com" {
		t.Fatalf("app registries %v, want the webhook's and the credentials' minus docker.io", got)
	}

	c.tick(time.Now()) // still warming up
	if a := reportCompanions(reg, "w1", "w1:1", model.ImageRef{Digest: "sha256:n", Name: nginx, Size: 10 << 20}, model.ImageRef{Digest: "sha256:h", Name: huge, Size: 500 << 20}); a.Companions == nil || a.Companions.Ready {
		t.Fatalf("still warming up: %+v", a.Companions)
	}

	var kept map[string]bool
	c.SetKeep(func(m map[string]bool) { kept = m })
	c.tick(time.Now().Add(2 * time.Hour))
	set := c.Ack("")
	if set == nil || !set.Ready || len(set.Digests) != 1 || set.Digests[0] != "sha256:n" {
		t.Fatalf("set %+v, want only nginx: huge is too large, exp has no size yet", set)
	}
	if len(kept) != 1 || !kept[nginx] {
		t.Fatalf("spread %v, want nginx", kept)
	}
	if c.Ack(set.Hash) != nil {
		t.Fatal("a worker holding the set must not get it again")
	}
	st := c.Status()
	reasons := map[string]string{}
	for _, s := range st {
		reasons[s.Image] = s.Reason
	}
	if reasons[huge] != "too_large" || reasons[exp] != "size_unknown" || reasons[nginx] != "" {
		t.Fatalf("status %+v", st)
	}

	// The exporter's size arrives: kept too, and the hash changes.
	reportCompanions(reg, "w2", "w2:1",
		model.ImageRef{Digest: "sha256:n", Name: nginx, Size: 10 << 20},
		model.ImageRef{Digest: "sha256:e", Name: exp, Size: 20 << 20})
	c.tick(time.Now().Add(2 * time.Hour))
	if next := c.Ack(set.Hash); next == nil || len(next.Digests) != 2 {
		t.Fatalf("after the exporter's size: %+v, want nginx and the exporter", next)
	}
	if len(kept) != 2 || !kept[exp] {
		t.Fatalf("spread %v, want both", kept)
	}
}

// COMPANION_APP_REGISTRIES replaces detection entirely.
func TestCompanionsOverrideReplacesDetection(t *testing.T) {
	reg := NewRegistry(time.Hour, time.Minute)
	c := NewCompanions(reg, CompanionConfig{AppRegistries: []string{"git.example.com"}, CredRegistries: []string{"registry.example.com"}})
	c.NoteRegistry("other.example.com/a:1")
	set := c.Ack("")
	if len(set.AppRegistries) != 1 || set.AppRegistries[0] != "git.example.com" {
		t.Fatalf("app registries %v, want only the override", set.AppRegistries)
	}
}

// Companion jobs: never superseded by a push of the same repo, never
// superseding one, nothing new while a push spreads, and dropped when no
// longer companions. A push of a companion image stays a push until done.
func TestPropagatorKeepJobs(t *testing.T) {
	f := newFleet()
	reg := NewRegistry(time.Hour, time.Minute)
	report(reg, "holder", f.worker(t, "holder"), 0.1, nginx)
	report(reg, "w1", f.worker(t, "w1"), 0.2)
	p := testPropagator(reg)
	busy := true
	p.SetBusy(func() bool { return busy })

	p.SetKeep(map[string]bool{nginx: true})
	p.Start("mirror.example.org/library/nginx:1.28") // same repo: the companion stays
	if j := p.jobs[nginx]; j == nil || !j.keep || j.push {
		t.Fatalf("companion job %+v", j)
	}
	p.tick(context.Background())
	p.wg.Wait()
	f.mu.Lock()
	if len(f.orders["w1"]) != 0 {
		t.Fatalf("orders %v while a push spreads, want none", f.orders)
	}
	f.mu.Unlock()

	busy = false
	p.tick(context.Background())
	p.wg.Wait()
	f.mu.Lock()
	if len(f.orders["w1"]) != 1 || f.orders["w1"][0] != "holder" {
		t.Fatalf("orders %v, want w1 from holder", f.orders)
	}
	f.mu.Unlock()
	report(reg, "w1", reg.workers["w1"].Address, 0.2, nginx)
	p.tick(context.Background())
	if p.jobs[nginx] == nil {
		t.Fatal("a companion job never ends on its own")
	}

	p.SetKeep(map[string]bool{})
	if p.jobs[nginx] != nil {
		t.Fatal("no longer a companion: its job goes")
	}
	if p.jobs["mirror.example.org/library/nginx:1.28"] == nil {
		t.Fatal("the push must stay")
	}

	// A push that is also a companion stays when it stops being one.
	p.SetKeep(map[string]bool{"mirror.example.org/library/nginx:1.28": true})
	p.SetKeep(map[string]bool{})
	if j := p.jobs["mirror.example.org/library/nginx:1.28"]; j == nil || j.keep || !j.push {
		t.Fatalf("push job %+v, want a push again", j)
	}
}
