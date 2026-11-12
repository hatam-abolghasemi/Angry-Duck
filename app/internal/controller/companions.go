package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"sync"
	"time"

	"angryduck/internal/imageref"
	"angryduck/internal/logging"
	"angryduck/internal/metrics"
	"angryduck/internal/model"
)

var (
	companionImages = metrics.NewGaugeVec(
		"angryduck_controller_companion_images",
		"Companion images (run beside an app image in the same pod), by state: kept (spread to and kept on every node), too_large (above COMPANION_MAX_MB), size_unknown (no node reported its size yet).",
		"state",
	)
	companionBytes = metrics.NewGaugeVec(
		"angryduck_controller_companion_bytes",
		"Bytes of the kept companion images together: what they take on a node that holds them all.",
	)
	companionNodes = metrics.NewGaugeVec(
		"angryduck_controller_companion_nodes",
		"Nodes per kept companion image, by state: have, missing (eligible, not yet there), skipped (excluded or too full).",
		"image", "state",
	)
)

// publicRegistries are never app registries by way of the credentials
// file: credentials for them are usually there to lift pull rate limits,
// not because the cluster's own images live there.
var publicRegistries = map[string]bool{
	"docker.io": true, "index.docker.io": true, "registry-1.docker.io": true,
	"ghcr.io": true, "quay.io": true, "gcr.io": true, "registry.k8s.io": true,
	"k8s.gcr.io": true, "mcr.microsoft.com": true, "public.ecr.aws": true,
}

// CompanionConfig holds the companion tracker's tunables.
type CompanionConfig struct {
	// AppRegistries, when set (COMPANION_APP_REGISTRIES), are the app
	// registries, and nothing is detected.
	AppRegistries []string
	// CredRegistries are the registries in the credentials file; the
	// public ones are dropped.
	CredRegistries []string
	MaxBytes       int64         // COMPANION_MAX_MB in bytes; 0: no limit
	Interval       time.Duration // re-evaluated at least this often
	// Warmup is how long after start the set is held back, so that every
	// live worker has reported what it runs (WORKER_STALE_AFTER_S).
	Warmup         time.Duration
	MaxUtilization float64  // as propagation: nodes fuller than this are skipped
	Exclude        []string // as propagation: nodes never sent companions
}

// Companions tracks the cluster's companion images: images a pod runs
// beside an app image, an image from one of the app registries (sidecars:
// exporters, proxies, agents). Their pods land on new nodes all the time,
// and each time the sidecar's image is pulled again unless the node still
// has it. So every node keeps them: they are spread to every eligible
// node, and image cleanup there never removes them for age, only under
// disk pressure, after every other unused image.
//
// The app registries are COMPANION_APP_REGISTRIES when set; otherwise the
// registries CI preheats from (the webhook) and those in the credentials
// file, which is how they are known again right after a restart. Workers
// get them in their report acks, tell companions apart themselves from
// what their containers run, and report each with its size. Images above
// COMPANION_MAX_MB aren't kept.
//
// After a restart the set isn't sent until every live worker reported
// (Warmup): workers keep the set they hold until then, so a restart never
// lifts protection for a moment.
type Companions struct {
	registry *Registry
	cfg      CompanionConfig
	keep     func(images map[string]bool) // the propagator's SetKeep; nil: not spread
	wakeup   waker

	mu      sync.Mutex
	started time.Time
	webhook map[string]bool // registries preheats came from
	ready   bool
	set     model.CompanionSet
	status  []model.CompanionStatus
}

// NewCompanions builds the tracker and hooks it into the registry's report
// acks and image-state changes.
func NewCompanions(registry *Registry, cfg CompanionConfig) *Companions {
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	c := &Companions{registry: registry, cfg: cfg, wakeup: newWaker(), started: time.Now(), webhook: map[string]bool{}}
	for _, s := range []string{"kept", "too_large", "size_unknown"} {
		companionImages.Set(0, s)
	}
	companionBytes.Set(0)
	c.set = c.buildSetLocked(nil)
	registry.OnImageState(c.Wake)
	registry.SetCompanionAck(c.Ack)
	return c
}

// SetKeep makes the tracker spread kept companions with fn (the
// propagator's SetKeep).
func (c *Companions) SetKeep(fn func(images map[string]bool)) { c.keep = fn }

// Wake asks for a re-evaluation now. It never blocks.
func (c *Companions) Wake() { c.wakeup.wake() }

// NoteRegistry records that CI preheated an image of ref's registry.
func (c *Companions) NoteRegistry(ref string) {
	host := imageref.RegistryHost(imageref.Normalize(ref))
	if host == "" || len(c.cfg.AppRegistries) > 0 {
		return
	}
	c.mu.Lock()
	added := !c.webhook[host]
	c.webhook[host] = true
	if added {
		c.set = c.buildSetLocked(c.set.Digests) // acks carry it from now on
	}
	c.mu.Unlock()
	if added {
		logging.Infof("angryduck-controller: companions: %s is an app registry (CI preheats from it)", host)
		c.Wake()
	}
}

// Ack is the registry's report-ack hook: the set to send a worker holding
// hash, or nil when it already holds it.
func (c *Companions) Ack(held string) *model.CompanionSet {
	c.mu.Lock()
	defer c.mu.Unlock()
	if held == c.set.Hash {
		return nil
	}
	set := c.set // Digests and AppRegistries are replaced, never modified
	return &set
}

// Run re-evaluates on every wake-up (a report changed what a node runs)
// and every Interval, until ctx is done.
func (c *Companions) Run(ctx context.Context) {
	logging.Infof("angryduck-controller: companions started: app_registries=%v (from credentials: %v) max_mb=%d warmup=%s",
		c.cfg.AppRegistries, c.credRegistries(), c.cfg.MaxBytes>>20, c.cfg.Warmup)
	t := time.NewTicker(c.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-c.wakeup:
			if !c.wakeup.settle(ctx) {
				return
			}
		}
		c.tick(time.Now())
	}
}

func (c *Companions) credRegistries() []string {
	var out []string
	for _, h := range c.cfg.CredRegistries {
		if !publicRegistries[h] {
			out = append(out, h)
		}
	}
	return out
}

// appRegistriesLocked is the app registries, sorted.
func (c *Companions) appRegistriesLocked() []string {
	if len(c.cfg.AppRegistries) > 0 {
		out := append([]string(nil), c.cfg.AppRegistries...)
		sort.Strings(out)
		return out
	}
	seen := map[string]bool{}
	for _, h := range c.credRegistries() {
		seen[h] = true
	}
	for h := range c.webhook {
		seen[h] = true
	}
	out := make([]string, 0, len(seen))
	for h := range seen {
		out = append(out, h)
	}
	sort.Strings(out)
	return out
}

// buildSetLocked is the set to send for digests (nil: not ready, the
// workers keep theirs).
func (c *Companions) buildSetLocked(digests []string) model.CompanionSet {
	set := model.CompanionSet{AppRegistries: c.appRegistriesLocked(), Digests: digests, Ready: c.ready}
	h := sha256.New()
	for _, r := range set.AppRegistries {
		h.Write([]byte(r))
		h.Write([]byte{0})
	}
	h.Write([]byte{1})
	if set.Ready {
		h.Write([]byte{2})
		for _, d := range digests {
			h.Write([]byte(d))
			h.Write([]byte{0})
		}
	}
	set.Hash = hex.EncodeToString(h.Sum(nil)[:12])
	return set
}

// companion is one image as reported, across nodes.
type companion struct {
	name    string
	size    int64
	running []string
}

func (c *Companions) tick(now time.Time) {
	fresh := c.registry.FreshWorkers()
	all := map[string]*companion{}
	reporting := 0
	for _, w := range fresh {
		if !w.CompanionState {
			continue
		}
		reporting++
		for _, ref := range w.Companions {
			e := all[ref.Digest]
			if e == nil {
				e = &companion{name: ref.Name}
				all[ref.Digest] = e
			} else if ref.Name < e.name {
				e.name = ref.Name // any node's name works; the smallest is stable
			}
			if ref.Size > e.size {
				e.size = ref.Size
			}
			e.running = append(e.running, w.NodeID)
		}
	}

	digests := make([]string, 0, len(all))
	for d := range all {
		digests = append(digests, d)
	}
	sort.Strings(digests)
	var kept []string
	keep := map[string]bool{}
	var keptBytes int64
	counts := map[string]int{"kept": 0, "too_large": 0, "size_unknown": 0}
	status := make([]model.CompanionStatus, 0, len(digests))
	for _, d := range digests {
		e := all[d]
		sort.Strings(e.running)
		st := model.CompanionStatus{Image: e.name, Digest: d, Size: e.size, Running: e.running}
		switch {
		case c.cfg.MaxBytes > 0 && e.size == 0:
			st.Reason = "size_unknown"
		case c.cfg.MaxBytes > 0 && e.size > c.cfg.MaxBytes:
			st.Reason = "too_large"
		default:
			st.Kept = true
			kept = append(kept, d)
			keep[e.name] = true
			keptBytes += e.size
		}
		if st.Kept {
			counts["kept"]++
		} else {
			counts[st.Reason]++
		}
		status = append(status, st)
	}
	for s, n := range counts {
		companionImages.Set(float64(n), s)
	}
	companionBytes.Set(float64(keptBytes))
	c.nodeGauges(fresh, status)

	c.mu.Lock()
	wasReady := c.ready
	if !c.ready && now.Sub(c.started) >= c.cfg.Warmup {
		c.ready = true
	}
	prev := c.set.Hash
	c.set = c.buildSetLocked(kept)
	changed := c.set.Hash != prev
	c.status = status
	ready := c.ready
	c.mu.Unlock()

	if ready && !wasReady {
		logging.Infof("angryduck-controller: companions: %d worker(s) reporting, %d companion image(s) kept on every node (%d bytes), %d not kept",
			reporting, len(kept), keptBytes, len(digests)-len(kept))
	} else if changed && ready {
		logging.Infof("angryduck-controller: companions: now %d kept (%d bytes), %d not kept", len(kept), keptBytes, len(digests)-len(kept))
	}
	if ready && c.keep != nil {
		c.keep(keep)
	}
}

// nodeGauges sets have/missing/skipped per kept image.
func (c *Companions) nodeGauges(fresh []*workerEntry, status []model.CompanionStatus) {
	companionNodes.Reset()
	for _, st := range status {
		if !st.Kept {
			continue
		}
		var have, missing, skipped int
		for _, w := range fresh {
			switch {
			case excludedNode(w.NodeID, c.cfg.Exclude):
				skipped++
			case w.HasImage(st.Image) || w.HasImage(imageref.Repo(st.Image)+"@"+st.Digest):
				have++
			case c.cfg.MaxUtilization > 0 && w.Utilization > c.cfg.MaxUtilization:
				skipped++
			default:
				missing++
			}
		}
		companionNodes.Set(float64(have), st.Image, "have")
		companionNodes.Set(float64(missing), st.Image, "missing")
		companionNodes.Set(float64(skipped), st.Image, "skipped")
	}
}

func excludedNode(node string, subs []string) bool {
	for _, s := range subs {
		if s != "" && strings.Contains(node, s) {
			return true
		}
	}
	return false
}

// Status returns every companion image, for /status.
func (c *Companions) Status() []model.CompanionStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.status
}
