package controller

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"angryduck/internal/logging"
	"angryduck/internal/metrics"
	"angryduck/internal/model"
)

var (
	propagationTransfersTotal = metrics.NewCounterVec(
		"angryduck_controller_propagation_transfers_total",
		"Peer-to-peer transfers ordered while spreading a pushed image, by receiving node and result.",
		"node", "result",
	)
	propagationNodes = metrics.NewGaugeVec(
		"angryduck_controller_propagation_nodes",
		"Nodes per image being propagated, by state: have, missing, in_flight, skipped.",
		"image", "state",
	)
	propagationsTotal = metrics.NewCounterVec(
		"angryduck_controller_propagations_total",
		"Finished propagations: \"complete\" (every eligible node has the image) or \"expired\" (window ran out first).",
		"result",
	)
)

// PropagatorConfig holds the propagator's tunables.
type PropagatorConfig struct {
	Interval              time.Duration // how often active propagations are advanced
	Window                time.Duration // how long after a push to keep spreading it
	MaxConcurrent         int           // transfers in flight across the cluster
	PerSource             int           // transfers one node serves at once
	MaxUtilization        float64       // skip nodes whose disk is fuller than this (0-1)
	ExcludeNodeSubstrings []string      // skip nodes whose name contains any of these
	RetryAfter            time.Duration // wait after a failed transfer; doubles per failure
	BackoffMax            time.Duration
	Timeout               time.Duration // max wait for one transfer
}

// Propagator spreads a freshly pushed image to every eligible node after
// the preheat seeds have pulled it from origin, so only the seeds ever
// touch the registry.
//
// Every tick it pairs nodes that lack the image with nodes that have it
// and orders each receiver's worker to fetch it from its peer (the same
// /rescue path as a stuck-pod rescue, so blobs or snapshots both work).
// Each node that receives the image becomes a source for the next ones,
// so it spreads like a tree: 2 seeds, then 6 nodes, then 18. A node is
// ordered at most once at a time, a source serves at most PerSource
// transfers at once, and nodes that are excluded or above MaxUtilization
// are left out, so propagation never pushes a node into kubelet's
// disk-pressure image GC.
//
// Images propagated this way are not used by any pod yet, so kubelet's
// time-based image GC (imageMaximumGCAge) removes them from nodes that
// never end up running them.
type Propagator struct {
	registry *Registry
	token    string
	cfg      PropagatorConfig
	client   *http.Client
	sem      chan struct{}

	mu        sync.Mutex
	jobs      map[string]*propagation
	perSource map[string]int // node -> transfers it is serving now
	wg        sync.WaitGroup
}

type propagation struct {
	image    string
	started  time.Time
	inFlight map[string]bool
	failures map[string]int
	next     map[string]time.Time
	done     map[string]bool // received it through us (reports may lag)
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

// Start begins (or restarts the window of) propagating image.
func (p *Propagator) Start(image string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if job, ok := p.jobs[image]; ok {
		job.started = time.Now()
		return
	}
	p.jobs[image] = &propagation{
		image: image, started: time.Now(),
		inFlight: map[string]bool{}, failures: map[string]int{},
		next: map[string]time.Time{}, done: map[string]bool{},
	}
	logging.Infof("angryduck-controller: propagator: will spread image=%s to every eligible node for %s", image, p.cfg.Window)
}

// Run blocks, ticking until ctx is done.
func (p *Propagator) Run(ctx context.Context) {
	t := time.NewTicker(p.cfg.Interval)
	defer t.Stop()
	logging.Infof("angryduck-controller: propagator started: interval=%s window=%s max_concurrent=%d per_source=%d max_utilization=%.2f exclude=%v",
		p.cfg.Interval, p.cfg.Window, p.cfg.MaxConcurrent, p.cfg.PerSource, p.cfg.MaxUtilization, p.cfg.ExcludeNodeSubstrings)
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
	missing  []*workerEntry // eligible, don't have it, least-full first
	have     []string       // eligible nodes that have it
	skipped  []string
	inFlight []string
	failing  []string
}

func (p *Propagator) viewLocked(job *propagation, fresh []*workerEntry) propView {
	var v propView
	for _, w := range fresh {
		has := w.HasImage(job.image) || job.done[w.NodeID]
		if has {
			v.holders = append(v.holders, w)
		}
		switch {
		case p.excluded(w.NodeID):
			v.skipped = append(v.skipped, w.NodeID)
		case has:
			v.have = append(v.have, w.NodeID)
		case job.inFlight[w.NodeID]:
			v.inFlight = append(v.inFlight, w.NodeID)
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

func (p *Propagator) excluded(node string) bool {
	for _, sub := range p.cfg.ExcludeNodeSubstrings {
		if sub != "" && strings.Contains(node, sub) {
			return true
		}
	}
	return false
}

func (p *Propagator) tick(ctx context.Context) {
	fresh := p.registry.FreshWorkers() // least-full first
	now := time.Now()

	p.mu.Lock()
	defer p.mu.Unlock()
	propagationNodes.Reset()

	images := make([]string, 0, len(p.jobs))
	for img := range p.jobs {
		images = append(images, img)
	}
	sort.Strings(images) // older pushes don't starve newer ones by map order luck

	for _, img := range images {
		job := p.jobs[img]
		v := p.viewLocked(job, fresh)
		inFlight := len(job.inFlight)

		if len(v.missing) == 0 && inFlight == 0 && len(v.have) > 0 {
			logging.Infof("angryduck-controller: propagator: image=%s is on every eligible node (%d), done in %s (skipped: %v)",
				img, len(v.have), now.Sub(job.started).Round(time.Second), v.skipped)
			propagationsTotal.Inc("complete")
			delete(p.jobs, img)
			continue
		}
		if now.Sub(job.started) > p.cfg.Window && inFlight == 0 {
			logging.Warnf("angryduck-controller: propagator: window for image=%s ran out with %d node(s) still missing it: %v",
				img, len(v.missing), nodeIDs(v.missing))
			propagationsTotal.Inc("expired")
			delete(p.jobs, img)
			continue
		}
		propagationNodes.Set(float64(len(v.have)), img, "have")
		propagationNodes.Set(float64(len(v.missing)), img, "missing")
		propagationNodes.Set(float64(inFlight), img, "in_flight")
		propagationNodes.Set(float64(len(v.skipped)), img, "skipped")
		if now.Sub(job.started) > p.cfg.Window || len(v.holders) == 0 {
			continue // winding down, or the seeds haven't reported the image yet
		}

		for _, target := range v.missing {
			if now.Before(job.next[target.NodeID]) {
				continue
			}
			sources := p.pickSourcesLocked(v.holders, target.NodeID)
			if len(sources) == 0 {
				break // every holder is busy serving; more will free up next tick
			}
			select {
			case p.sem <- struct{}{}:
			default:
				return // cluster-wide transfer limit reached
			}
			job.inFlight[target.NodeID] = true
			p.perSource[sources[0].NodeID]++
			p.wg.Add(1)
			go p.send(ctx, job, target, sources)
		}
	}
}

// pickSourcesLocked returns a primary source with spare capacity, plus up
// to two more holders as fallbacks should the primary fail mid-transfer.
func (p *Propagator) pickSourcesLocked(holders []*workerEntry, target string) []model.RescueSource {
	var out []model.RescueSource
	for _, h := range holders {
		if h.NodeID == target {
			continue
		}
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

func (p *Propagator) send(ctx context.Context, job *propagation, target *workerEntry, sources []model.RescueSource) {
	defer p.wg.Done()
	defer func() { <-p.sem }()

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

// Status returns every active propagation, for /status.
func (p *Propagator) Status() []model.PropagationStatus {
	fresh := p.registry.FreshWorkers()
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []model.PropagationStatus
	for _, job := range p.jobs {
		v := p.viewLocked(job, fresh)
		out = append(out, model.PropagationStatus{
			Image: job.image, StartedAt: job.started, EndsAt: job.started.Add(p.cfg.Window),
			Have: sorted(v.have), Missing: sorted(nodeIDs(v.missing)), InFlight: sorted(v.inFlight),
			Skipped: sorted(v.skipped), Failing: sorted(v.failing),
		})
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
