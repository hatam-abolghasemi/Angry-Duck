package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"angryduck/internal/imageref"
	"angryduck/internal/layerindex"
	"angryduck/internal/logging"
	"angryduck/internal/metrics"
	"angryduck/internal/model"
	"angryduck/internal/sharedtoken"
)

var (
	propagationTransfersTotal = metrics.NewCounterVec(
		"angryduck_controller_propagation_transfers_total",
		"Peer-to-peer transfers ordered while spreading a pushed image, by receiving node and result.",
		"node", "result",
	)
	propagationNodes = metrics.NewGaugeVec(
		"angryduck_controller_propagation_nodes",
		"Nodes per image being propagated, by state: have, missing, in_flight, seeding, skipped.",
		"image", "state",
	)
	propagationsTotal = metrics.NewCounterVec(
		"angryduck_controller_propagations_total",
		"Finished propagations: complete (every eligible node has the image), superseded (a newer tag of the same repo was pushed) or expired (PROPAGATE_WINDOW_S ran out).",
		"result",
	)
	seedTimeoutsTotal = metrics.NewCounterVec(
		"angryduck_controller_seed_timeouts_total",
		"Seeds whose registry pull took too long: the pull was cancelled and the node got the image from a peer instead.",
		"node",
	)
)

// PropagatorConfig holds the propagator's tunables.
type PropagatorConfig struct {
	Interval time.Duration // how often active propagations are advanced
	// Window bounds how long a push keeps spreading. 0 (the default) means
	// until every eligible node has it, or a newer tag of the same repo
	// replaces it.
	Window                time.Duration
	MaxConcurrent         int           // transfers in flight across the cluster
	PerSource             int           // transfers one node serves at once (1: one node fully, then the next)
	MaxUtilization        float64       // skip nodes whose disk is fuller than this (0-1)
	ExcludeNodeSubstrings []string      // skip nodes whose name contains any of these
	RetryAfter            time.Duration // wait after a failed transfer; doubles per failure
	BackoffMax            time.Duration
	Timeout               time.Duration // max wait for one transfer
	// A seed still pulling from the registry is left alone for
	// max(SeedTimeoutMin, SeedTimeoutFactor x the first seed's pull time),
	// or SeedTimeoutMax while no seed has finished. Then its pull is
	// cancelled and a peer ships it the image.
	SeedTimeoutMin    time.Duration
	SeedTimeoutMax    time.Duration
	SeedTimeoutFactor float64
	Platform          string // for layer lookups ("linux/amd64")
}

// Propagator spreads a freshly pushed image to every eligible node after
// the seeds have pulled it from the registry, so only the seeds ever touch
// the registry.
//
// Every tick it pairs nodes that lack the image with nodes that have it
// and orders each receiver's worker to fetch it from peers (the same
// /rescue path as a stuck-pod rescue: blobs first, snapshots only as a
// last resort). Each node that receives the image becomes a source for the
// next ones. With PerSource=1 a source sends to one node at a time, which
// finishes it completely before starting the next, so the first nodes are
// ready as early as possible and the number of holders doubles every
// round: 5 seeds, then 10, 20, 40.
//
// Receivers are served in this order: nodes where a pod is already waiting
// for the image, then nodes that lack the fewest bytes of it (they finish
// soonest and become sources soonest).
//
// Seeds are not receivers while their own registry pull runs; a seed that
// takes too long has its pull cancelled first, so two writers never fill
// one content store. Nodes that are excluded or above MaxUtilization are
// skipped until their disk has room again (the image cleanup makes room),
// so propagation never pushes a node into kubelet's disk-pressure GC.
type Propagator struct {
	registry *Registry
	token    string
	cfg      PropagatorConfig
	client   *http.Client
	sem      chan struct{}

	// Optional inputs; nil means "not known".
	seeds    func(image string) map[string]time.Time // seed -> ordered at
	waiting  func() map[string]map[string]bool       // image -> nodes with a pod waiting for it
	resolver LayerResolver

	mu        sync.Mutex
	jobs      map[string]*propagation
	perSource map[string]int // node -> transfers it is serving now
	wg        sync.WaitGroup
}

type propagation struct {
	image         string
	started       time.Time
	firstSeedDone time.Time
	firstSeedTook time.Duration
	inFlight      map[string]bool
	failures      map[string]int
	next          map[string]time.Time
	done          map[string]bool // received it through us (reports may lag)
	cancelled     map[string]bool // seeds whose pull we cancelled
}

// NewPropagator builds a propagator.
func NewPropagator(registry *Registry, token string, cfg PropagatorConfig) *Propagator {
	if cfg.MaxConcurrent < 1 {
		cfg.MaxConcurrent = 1
	}
	if cfg.PerSource < 1 {
		cfg.PerSource = 1
	}
	if cfg.BackoffMax < cfg.RetryAfter {
		cfg.BackoffMax = cfg.RetryAfter
	}
	if cfg.SeedTimeoutFactor <= 0 {
		cfg.SeedTimeoutFactor = 3
	}
	if cfg.SeedTimeoutMax < cfg.SeedTimeoutMin {
		cfg.SeedTimeoutMax = cfg.SeedTimeoutMin
	}
	if cfg.Platform == "" {
		cfg.Platform = "linux/amd64"
	}
	return &Propagator{
		registry:  registry,
		token:     token,
		cfg:       cfg,
		client:    &http.Client{Timeout: cfg.Timeout},
		sem:       make(chan struct{}, cfg.MaxConcurrent),
		jobs:      make(map[string]*propagation),
		perSource: make(map[string]int),
	}
}

// SetSeeds tells the propagator which nodes are seeding an image.
func (p *Propagator) SetSeeds(f func(image string) map[string]time.Time) { p.seeds = f }

// SetWaiting tells the propagator where pods wait for images.
func (p *Propagator) SetWaiting(f func() map[string]map[string]bool) { p.waiting = f }

// SetLayerResolver lets the propagator order receivers by missing bytes.
func (p *Propagator) SetLayerResolver(r LayerResolver) { p.resolver = r }

// Start begins propagating image. A job for another tag of the same repo
// is superseded: spreading a version that is about to be replaced
// everywhere would only fill disks.
func (p *Propagator) Start(image string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if job, ok := p.jobs[image]; ok {
		job.started = time.Now()
		return
	}
	repo := imageref.Repo(image)
	for other := range p.jobs {
		if other != image && repo != "" && imageref.Repo(other) == repo {
			logging.Infof("angryduck-controller: propagator: image=%s superseded by image=%s", other, image)
			propagationsTotal.Inc("superseded")
			delete(p.jobs, other)
		}
	}
	p.jobs[image] = &propagation{
		image: image, started: time.Now(),
		inFlight: map[string]bool{}, failures: map[string]int{},
		next: map[string]time.Time{}, done: map[string]bool{}, cancelled: map[string]bool{},
	}
	if p.cfg.Window > 0 {
		logging.Infof("angryduck-controller: propagator: will spread image=%s to every eligible node for %s", image, p.cfg.Window)
	} else {
		logging.Infof("angryduck-controller: propagator: will spread image=%s to every eligible node", image)
	}
}

// Images returns the images being propagated (the image cleanup never
// removes them).
func (p *Propagator) Images() map[string]bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]bool, len(p.jobs))
	for img := range p.jobs {
		out[img] = true
	}
	return out
}

// Run blocks, ticking until ctx is done.
func (p *Propagator) Run(ctx context.Context) {
	t := time.NewTicker(p.cfg.Interval)
	defer t.Stop()
	logging.Infof("angryduck-controller: propagator started: interval=%s window=%s max_concurrent=%d per_source=%d max_utilization=%.2f seed_timeout=[%s..%s, x%.1f] exclude=%v",
		p.cfg.Interval, p.cfg.Window, p.cfg.MaxConcurrent, p.cfg.PerSource, p.cfg.MaxUtilization, p.cfg.SeedTimeoutMin, p.cfg.SeedTimeoutMax, p.cfg.SeedTimeoutFactor, p.cfg.ExcludeNodeSubstrings)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.tick(ctx)
		}
	}
}

// view sorts the fresh workers into what they are for one image.
type propView struct {
	holders  []*workerEntry // have it, least-loaded first
	missing  []*workerEntry // eligible receivers, in serving order
	have     []string       // eligible nodes that have it
	seeding  []string       // seeds still pulling from the registry
	skipped  []string
	inFlight []string
	failing  []string
}

func (p *Propagator) viewLocked(job *propagation, fresh []*workerEntry, seeds map[string]time.Time, now time.Time) propView {
	var v propView
	for _, w := range fresh {
		has := w.HasImage(job.image) || job.done[w.NodeID]
		if has {
			v.holders = append(v.holders, w)
		}
		orderedAt, isSeed := seeds[w.NodeID]
		switch {
		case p.excluded(w.NodeID):
			v.skipped = append(v.skipped, w.NodeID)
		case has:
			v.have = append(v.have, w.NodeID)
			if isSeed && job.firstSeedDone.IsZero() {
				job.firstSeedDone, job.firstSeedTook = now, now.Sub(orderedAt)
			}
		case job.inFlight[w.NodeID]:
			v.inFlight = append(v.inFlight, w.NodeID)
		case isSeed && !job.cancelled[w.NodeID] && now.Before(orderedAt.Add(p.seedTimeout(job))):
			v.seeding = append(v.seeding, w.NodeID)
		case w.Utilization > p.cfg.MaxUtilization:
			v.skipped = append(v.skipped, w.NodeID)
		default:
			v.missing = append(v.missing, w)
			if job.failures[w.NodeID] > 0 {
				v.failing = append(v.failing, w.NodeID)
			}
		}
	}
	// Spread the load: sources serving the fewest transfers first.
	sort.SliceStable(v.holders, func(i, j int) bool {
		return p.perSource[v.holders[i].NodeID] < p.perSource[v.holders[j].NodeID]
	})
	return v
}

// seedTimeout is how long a seed's registry pull is left alone.
func (p *Propagator) seedTimeout(job *propagation) time.Duration {
	if job.firstSeedDone.IsZero() {
		return p.cfg.SeedTimeoutMax
	}
	d := time.Duration(float64(job.firstSeedTook) * p.cfg.SeedTimeoutFactor)
	if d < p.cfg.SeedTimeoutMin {
		d = p.cfg.SeedTimeoutMin
	}
	if d > p.cfg.SeedTimeoutMax {
		d = p.cfg.SeedTimeoutMax
	}
	return d
}

func (p *Propagator) excluded(node string) bool {
	for _, sub := range p.cfg.ExcludeNodeSubstrings {
		if sub != "" && strings.Contains(node, sub) {
			return true
		}
	}
	return false
}

// order sorts receivers: a waiting pod first, then fewest missing bytes,
// then (stable) least-full.
func (p *Propagator) order(image string, missing []*workerEntry, waiting map[string]bool, layers []layerindex.Layer) {
	var miss map[string]int64
	if len(layers) > 0 {
		miss = make(map[string]int64, len(missing))
		for _, w := range missing {
			if m, ok := p.registry.Layers().Missing(w.NodeID, layers); ok {
				miss[w.NodeID] = m
			} else {
				miss[w.NodeID] = 1 << 62 // unknown inventory: after the known ones
			}
		}
	}
	sort.SliceStable(missing, func(i, j int) bool {
		a, b := missing[i].NodeID, missing[j].NodeID
		if waiting[a] != waiting[b] {
			return waiting[a]
		}
		return miss[a] < miss[b]
	})
}

func (p *Propagator) tick(ctx context.Context) {
	fresh := p.registry.FreshWorkers() // least-full first
	now := time.Now()
	var waiting map[string]map[string]bool
	if p.waiting != nil {
		waiting = p.waiting()
	}

	p.mu.Lock()
	images := make([]string, 0, len(p.jobs))
	for img := range p.jobs {
		images = append(images, img)
	}
	p.mu.Unlock()
	sort.Strings(images)

	// Layer lookups are cached by the resolver, but can still go to the
	// registry: do them without holding the lock.
	layers := make(map[string][]layerindex.Layer, len(images))
	if p.resolver != nil {
		for _, img := range images {
			rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			if ls, err := p.resolver.Layers(rctx, img, p.cfg.Platform); err == nil {
				layers[img] = ls
			}
			cancel()
		}
	}
	seeds := make(map[string]map[string]time.Time, len(images))
	if p.seeds != nil {
		for _, img := range images {
			seeds[img] = p.seeds(img)
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	propagationNodes.Reset()
	for _, img := range images {
		job, ok := p.jobs[img]
		if !ok {
			continue // superseded meanwhile
		}
		v := p.viewLocked(job, fresh, seeds[img], now)
		inFlight := len(job.inFlight)

		if len(v.missing) == 0 && inFlight == 0 && len(v.seeding) == 0 && len(v.have) > 0 && len(v.skipped) == len(v.skippedExcluded(p)) {
			logging.Infof("angryduck-controller: propagator: image=%s is on every eligible node (%d), done in %s (excluded: %v)",
				img, len(v.have), now.Sub(job.started).Round(time.Second), v.skipped)
			propagationsTotal.Inc("complete")
			delete(p.jobs, img)
			continue
		}
		expired := p.cfg.Window > 0 && now.Sub(job.started) > p.cfg.Window
		if expired && inFlight == 0 {
			logging.Warnf("angryduck-controller: propagator: window for image=%s ran out with %d node(s) still missing it: %v",
				img, len(v.missing), nodeIDs(v.missing))
			propagationsTotal.Inc("expired")
			delete(p.jobs, img)
			continue
		}
		propagationNodes.Set(float64(len(v.have)), img, "have")
		propagationNodes.Set(float64(len(v.missing)), img, "missing")
		propagationNodes.Set(float64(inFlight), img, "in_flight")
		propagationNodes.Set(float64(len(v.seeding)), img, "seeding")
		propagationNodes.Set(float64(len(v.skipped)), img, "skipped")
		if expired || len(v.holders) == 0 {
			continue // winding down, or the seeds haven't reported the image yet
		}
		p.order(img, v.missing, waiting[img], layers[img])

		for _, target := range v.missing {
			if now.Before(job.next[target.NodeID]) {
				continue
			}
			sources := p.pickSourcesLocked(v.holders, target.NodeID, layers[img])
			if len(sources) == 0 {
				break // every holder is busy serving; more will free up next tick
			}
			select {
			case p.sem <- struct{}{}:
			default:
				return // cluster-wide transfer limit reached
			}
			_, isSeed := seeds[img][target.NodeID]
			job.inFlight[target.NodeID] = true
			p.perSource[sources[0].NodeID]++
			p.wg.Add(1)
			go p.send(ctx, job, target, sources, isSeed && !job.cancelled[target.NodeID])
			if isSeed {
				job.cancelled[target.NodeID] = true
			}
		}
	}
}

// skippedExcluded is the part of skipped that is excluded by name (never
// coming back), as opposed to too full (may come back after cleanup).
func (v propView) skippedExcluded(p *Propagator) []string {
	var out []string
	for _, n := range v.skipped {
		if p.excluded(n) {
			out = append(out, n)
		}
	}
	return out
}

// pickSourcesLocked returns a primary source with spare capacity, plus up
// to two more holders the receiver can take missing blobs from (or fall
// back to). Holders with the image's blobs come first: blobs are
// digest-verified on arrival, snapshot copies are not.
func (p *Propagator) pickSourcesLocked(holders []*workerEntry, target string, layers []layerindex.Layer) []model.RescueSource {
	cands := make([]*workerEntry, 0, len(holders))
	for _, h := range holders {
		if h.NodeID != target {
			cands = append(cands, h)
		}
	}
	if len(layers) > 0 {
		blob := make(map[string]int64, len(cands))
		for _, h := range cands {
			blob[h.NodeID] = p.registry.Layers().BlobBytes(h.NodeID, layers)
		}
		sort.SliceStable(cands, func(i, j int) bool { return blob[cands[i].NodeID] > blob[cands[j].NodeID] })
	}
	var out []model.RescueSource
	for _, h := range cands {
		if len(out) == 0 && p.perSource[h.NodeID] >= p.cfg.PerSource {
			continue
		}
		out = append(out, model.RescueSource{NodeID: h.NodeID, Address: h.Address})
		if len(out) == 3 {
			break
		}
	}
	return out
}

func (p *Propagator) send(ctx context.Context, job *propagation, target *workerEntry, sources []model.RescueSource, cancelSeed bool) {
	defer p.wg.Done()
	defer func() { <-p.sem }()

	if cancelSeed {
		logging.Warnf("angryduck-controller: propagator: seed node=%s still hasn't pulled image=%s after %s; cancelling its registry pull and shipping it from peers",
			target.NodeID, job.image, p.seedTimeout(job).Round(time.Second))
		seedTimeoutsTotal.Inc(target.NodeID)
		cancelPull(ctx, target.Address, job.image, p.token)
	}

	start := time.Now()
	res, err := sendRescueOrder(ctx, p.client, p.token, target.Address, model.RescueOrder{Image: job.image, Sources: sources, Reason: "propagate"})

	p.mu.Lock()
	delete(job.inFlight, target.NodeID)
	p.perSource[sources[0].NodeID]--
	if err != nil {
		job.failures[target.NodeID]++
		wait := p.cfg.RetryAfter
		for i := 1; i < job.failures[target.NodeID] && wait < p.cfg.BackoffMax; i++ {
			wait *= 2
		}
		if wait > p.cfg.BackoffMax {
			wait = p.cfg.BackoffMax
		}
		job.next[target.NodeID] = time.Now().Add(wait)
		p.mu.Unlock()
		logging.Warnf("angryduck-controller: propagator: node=%s failed to get image=%s from %s (next try in %s): %v",
			target.NodeID, job.image, sources[0].NodeID, wait, err)
		propagationTransfersTotal.Inc(target.NodeID, "failure")
		return
	}
	job.done[target.NodeID] = true
	delete(job.failures, target.NodeID)
	p.mu.Unlock()
	logging.Infof("angryduck-controller: propagator: node=%s got image=%s from %s (%d blob(s), %d snapshot(s), %d bytes, %s)",
		target.NodeID, job.image, orDash(res.Source), res.Blobs, res.Snapshots, res.Bytes, time.Since(start).Round(time.Millisecond))
	propagationTransfersTotal.Inc(target.NodeID, "success")
}

// cancelPull asks a worker to stop pulling image from the registry.
// Best-effort: a worker that already finished answers false, which is fine.
func cancelPull(ctx context.Context, addr, image, token string) {
	body, _ := json.Marshal(model.PullOrder{Image: image})
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/pull/cancel", bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	sharedtoken.SetIfAny(req, token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logging.Warnf("angryduck-controller: cancelling pull of image=%s on %s: %v", image, addr, err)
		return
	}
	resp.Body.Close()
}

// Status returns every active propagation, for /status.
func (p *Propagator) Status() []model.PropagationStatus {
	fresh := p.registry.FreshWorkers()
	p.mu.Lock()
	images := make([]string, 0, len(p.jobs))
	for img := range p.jobs {
		images = append(images, img)
	}
	p.mu.Unlock()
	seeds := map[string]map[string]time.Time{}
	if p.seeds != nil {
		for _, img := range images {
			seeds[img] = p.seeds(img)
		}
	}
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []model.PropagationStatus
	for _, job := range p.jobs {
		v := p.viewLocked(job, fresh, seeds[job.image], now)
		st := model.PropagationStatus{
			Image: job.image, StartedAt: job.started,
			Have: sorted(v.have), Missing: sorted(nodeIDs(v.missing)), InFlight: sorted(v.inFlight),
			Seeding: sorted(v.seeding), Skipped: sorted(v.skipped), Failing: sorted(v.failing),
		}
		if p.cfg.Window > 0 {
			st.EndsAt = job.started.Add(p.cfg.Window)
		}
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Image < out[j].Image })
	return out
}

func nodeIDs(ws []*workerEntry) []string {
	out := make([]string, len(ws))
	for i, w := range ws {
		out[i] = w.NodeID
	}
	return out
}

func sorted(s []string) []string {
	sort.Strings(s)
	return s
}
