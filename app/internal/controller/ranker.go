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
)

// pullOrdersTotal counts /pull requests the controller has sent to
// workers, split by node and whether the worker accepted it. This is the
// controller-side half of the pull story; angryduck_worker_pulls_total
// (see worker/puller.go) is the worker-side half.
//
// registry is only populated when METRICS_LABEL_REGISTRY is enabled —
// registry host is an unbounded-ish label (unlike node), so it's opt-in.
// seedRankingsTotal counts how seed candidates were ranked: "layers"
// (bytes already on each node, from the layer inventory and the image's
// manifest), "repo" (fallback: nodes with any tag of the repo first) or
// "utilization" (fallback when locality is off).
var seedRankingsTotal = metrics.NewCounterVec(
	"angryduck_controller_seed_rankings_total",
	"Seed selections, by what ranked the candidates: layers (bytes each node already holds), repo (fallback) or utilization (fallback).",
	"basis",
)

// seedMissingBytes is, per seed ordered for the latest image, how many
// bytes of the image it lacked: what its registry pull downloads.
var seedMissingBytes = metrics.NewGaugeVec(
	"angryduck_controller_seed_missing_bytes",
	"Bytes of the latest seeded image each seed node lacked when ordered (its registry download). Only set when layer ranking was used.",
	"node",
)

// LayerResolver returns an image's layers from its registry.
type LayerResolver interface {
	Layers(ctx context.Context, image, platform string) ([]layerindex.Layer, error)
}

var pullOrdersTotal = metrics.NewCounterVec(
	"angryduck_controller_pull_orders_total",
	"Total pull orders sent to workers, by node and result. registry is only populated when METRICS_LABEL_REGISTRY=true.",
	"node", "result", "registry",
)

// Ranker periodically re-ranks fresh workers by utilization and orders the
// least-utilized top-N to pull the current target image. It also exposes
// OrderNow for the webhook handler to trigger an immediate order without
// waiting for the next tick.
//
// A node is only ever ordered once per target image: orderedNodes tracks
// which nodes have already been sent a /pull for orderedForImage, and
// resets the moment the target image changes. Without this, every tick
// within TARGET_TTL_S re-ranks and re-fires at the same lowest-utilization
// nodes (utilization barely moves between 10s ticks), so the same image
// was being ordered to the same handful of nodes a dozen times over one
// preheat window.
//
// And at most topN nodes are ever ordered per target image. Before this,
// the dedup above had a side effect: each tick picked topN *new* nodes, so
// over TARGET_TTL_S one push preheated the entire fleet from origin (seen
// on stg: 18 of 18 workers ordered within 30s). Now preheat seeds topN
// nodes and stops; every other node gets the image when a pod actually
// needs it, from a seed, through its mirror. Ticks within the TTL only
// replace seeds whose order failed. The ranker itself never blocks waiting on a pull to
// finish either way — sendPullOrder fires in its own goroutine and only
// confirms the worker *accepted* the order (HTTP 2xx), never that the pull
// itself completed.
type Ranker struct {
	registry            *Registry
	topN                int
	interval            time.Duration
	excludeSubstrings   []string
	preferImageLocality bool
	httpClient          *http.Client

	mu              sync.Mutex
	orderedForImage string
	orderedNodes    map[string]bool
	// slotsUsed counts orders for orderedForImage that were sent and not
	// rejected. Preheat seeds exactly topN nodes per image; every other
	// node gets the image on demand through its mirror. A failed order
	// frees its slot (the node stays in orderedNodes, so it isn't picked
	// again) and the next tick fills it with the next-best node.
	slotsUsed int
	// labelRegistryHost controls whether pullOrdersTotal is labeled by
	// registry host (METRICS_LABEL_REGISTRY) — named to avoid confusion
	// with the registry *Registry field above, which is the worker
	// registry, not a container registry.
	labelRegistryHost bool

	resolver       LayerResolver // nil: no layer ranking
	platform       string
	resolveTimeout time.Duration
	// seedsAt remembers when each seed was ordered, per image, so the
	// propagator can leave seeds alone while their registry pull runs.
	seedsAt map[string]map[string]time.Time
}

// SetLayerResolver turns on layer ranking: seeds are the nodes that
// already hold the most bytes of the image. Resolving the image's layers
// is bounded by timeout; past it, or on any error, ranking falls back to
// repo locality.
func (rk *Ranker) SetLayerResolver(r LayerResolver, platform string, timeout time.Duration) {
	rk.resolver, rk.platform, rk.resolveTimeout = r, platform, timeout
}

// Seeds returns when each seed of image was ordered.
func (rk *Ranker) Seeds(image string) map[string]time.Time {
	rk.mu.Lock()
	defer rk.mu.Unlock()
	out := make(map[string]time.Time, len(rk.seedsAt[image]))
	for n, t := range rk.seedsAt[image] {
		out[n] = t
	}
	return out
}

// NewRanker builds a ranker bound to the given registry. excludeSubstrings
// is a list of case-sensitive substrings (e.g. "master", "control-plane");
// any worker whose NodeID contains one is still tracked and still reports
// in as normal — it's simply never selected as a preheat target. This
// exists because master/control-plane nodes still need Angry Duck's
// worker running on them for its disk-utilization/locality reporting, but
// pass nil here (or an empty slice) to disable the behavior entirely.
//
// Matching is done here, at selection time, rather than by keeping masters
// out of the registry or off the DaemonSet: the constraint is "never pick
// them as a target," not "never run there."
//
// preferImageLocality controls whether candidates that already have the
// target image's repo present locally (any tag) are ranked ahead of ones
// that don't, before either group is sorted by utilization — see
// rankByLocality. Pass false (RANK_PREFER_IMAGE_LOCALITY=false) to fall
// back to the old utilization-only ordering.
//
// labelRegistryHost controls whether pullOrdersTotal is labeled by
// container-registry host (METRICS_LABEL_REGISTRY) — off by default
// since registry host is an unbounded-ish label, unlike node.
func NewRanker(registry *Registry, topN int, interval time.Duration, excludeSubstrings []string, preferImageLocality bool, labelRegistryHost bool) *Ranker {
	return &Ranker{
		registry:            registry,
		topN:                topN,
		interval:            interval,
		excludeSubstrings:   excludeSubstrings,
		preferImageLocality: preferImageLocality,
		labelRegistryHost:   labelRegistryHost,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// Run blocks, re-ranking and re-ordering on every tick until ctx is done.
func (rk *Ranker) Run(ctx context.Context) {
	ticker := time.NewTicker(rk.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rk.tick()
		}
	}
}

func (rk *Ranker) tick() {
	image, _, active := rk.registry.CurrentTarget()
	if !active {
		return
	}
	rk.orderLowestN(image)
}

// OrderNow is called synchronously by the webhook handler so the very first
// pull order goes out immediately, rather than waiting up to `interval`
// seconds for the next ranker tick.
func (rk *Ranker) OrderNow(image string) []string {
	return rk.orderLowestN(image)
}

// orderLowestN ranks fresh, eligible workers, picks the lowest topN by
// utilization among those not already ordered for this image, and fires
// off pull orders to each concurrently. Returns the node IDs newly
// ordered (for logging / API responses) — nodes skipped because they were
// already ordered for the current target are not included.
func (rk *Ranker) orderLowestN(image string) []string {
	fresh := rk.registry.FreshWorkers()
	if len(fresh) == 0 {
		logging.Warnf("angryduck-controller: cannot order preheat for image=%s: zero fresh workers", image)
		return nil
	}

	eligible := rk.excludeMatching(fresh)
	if len(eligible) == 0 {
		logging.Warnf("angryduck-controller: cannot order preheat for image=%s: %d fresh worker(s) reported, but all matched an excluded-node substring (%v)",
			image, len(fresh), rk.excludeSubstrings)
		return nil
	}

	candidates, remaining := rk.notYetOrdered(image, eligible)
	if remaining <= 0 {
		logging.Debugf("angryduck-controller: image=%s already has its %d seed(s), nothing new to do right now", image, rk.topN)
		return nil
	}
	if len(candidates) == 0 {
		logging.Debugf("angryduck-controller: all %d eligible worker(s) already ordered for image=%s, nothing new to do right now", len(eligible), image)
		return nil
	}

	ranked, missing := rk.rank(image, candidates)

	n := remaining
	if n > len(ranked) {
		n = len(ranked)
	}
	chosen := ranked[:n]

	logging.Debugf("angryduck-controller: ranked %d fresh worker(s) (%d eligible after exclusions, %d not yet ordered), choosing top %d (image locality, then utilization) for image=%s",
		len(fresh), len(eligible), len(candidates), n, image)
	for _, w := range eligible {
		logging.Debugf("angryduck-controller: candidate node=%s utilization=%.1f%%", w.NodeID, w.Utilization*100)
	}

	rk.mu.Lock()
	if rk.seedsAt == nil {
		rk.seedsAt = map[string]map[string]time.Time{}
	}
	for img, seeds := range rk.seedsAt {
		stale := true
		for _, t := range seeds {
			if time.Since(t) < 6*time.Hour {
				stale = false
			}
		}
		if stale {
			delete(rk.seedsAt, img)
		}
	}
	if rk.seedsAt[image] == nil {
		rk.seedsAt[image] = map[string]time.Time{}
	}
	ordered := make([]string, 0, len(chosen))
	for _, w := range chosen {
		ordered = append(ordered, w.NodeID)
		rk.seedsAt[image][w.NodeID] = time.Now()
		if m, ok := missing[w.NodeID]; ok {
			seedMissingBytes.Set(float64(m), w.NodeID)
		}
		rk.orderedNodes[w.NodeID] = true
		rk.slotsUsed++
		go rk.sendPullOrder(w.NodeID, w.Address, image)
	}
	rk.mu.Unlock()
	return ordered
}

// notYetOrdered resets the ordered-nodes set the moment image differs from
// the last image this ranker ordered for (a new push landed), then returns
// the subset of eligible workers not yet recorded as ordered — preserving
// eligible's existing ascending-utilization order — and how many seed
// slots are still open for this image.
func (rk *Ranker) notYetOrdered(image string, eligible []*workerEntry) ([]*workerEntry, int) {
	rk.mu.Lock()
	defer rk.mu.Unlock()

	if image != rk.orderedForImage {
		rk.orderedForImage = image
		rk.orderedNodes = make(map[string]bool)
		rk.slotsUsed = 0
	}
	remaining := rk.topN - rk.slotsUsed

	candidates := make([]*workerEntry, 0, len(eligible))
	for _, w := range eligible {
		if !rk.orderedNodes[w.NodeID] {
			candidates = append(candidates, w)
		}
	}
	return candidates, remaining
}

// releaseSlot frees a seed slot after a failed order, if the order was for
// the image still being seeded.
func (rk *Ranker) releaseSlot(image string) {
	rk.mu.Lock()
	defer rk.mu.Unlock()
	if image == rk.orderedForImage && rk.slotsUsed > 0 {
		rk.slotsUsed--
	}
}

// rank orders candidates for seeding. With layer ranking on and the
// image's layers resolvable, nodes that lack the fewest bytes come first
// (utilization breaks ties, as candidates arrive sorted by it); nodes that
// never sent a layer inventory follow, in repo-locality order. Otherwise
// it is repo locality alone. missing holds each scored node's missing
// bytes.
func (rk *Ranker) rank(image string, candidates []*workerEntry) (ranked []*workerEntry, missing map[string]int64) {
	if rk.resolver != nil {
		ctx, cancel := context.WithTimeout(context.Background(), rk.resolveTimeout)
		layers, err := rk.resolver.Layers(ctx, image, rk.platform)
		cancel()
		if err != nil {
			logging.Warnf("angryduck-controller: layer ranking unavailable for image=%s, falling back to repo locality: %v", image, err)
		} else {
			missing = make(map[string]int64, len(candidates))
			var known, unknown []*workerEntry
			for _, w := range candidates {
				m, ok := rk.registry.Layers().Missing(w.NodeID, layers)
				if !ok {
					unknown = append(unknown, w)
					continue
				}
				missing[w.NodeID] = m
				known = append(known, w)
			}
			if len(known) > 0 {
				sort.SliceStable(known, func(i, j int) bool { return missing[known[i].NodeID] < missing[known[j].NodeID] })
				var total int64
				for _, l := range layers {
					total += l.Size
				}
				for _, w := range known {
					logging.Debugf("angryduck-controller: seed candidate node=%s lacks %d of %d bytes of image=%s (utilization %.1f%%)",
						w.NodeID, missing[w.NodeID], total, image, w.Utilization*100)
				}
				seedRankingsTotal.Inc("layers")
				return append(known, rk.rankByLocality(image, unknown)...), missing
			}
		}
	}
	if rk.preferImageLocality {
		seedRankingsTotal.Inc("repo")
	} else {
		seedRankingsTotal.Inc("utilization")
	}
	return rk.rankByLocality(image, candidates), nil
}

// rankByLocality reorders candidates — already sorted ascending by
// utilization via FreshWorkers() — so that nodes which already have the
// target image's repo present locally (any tag) come first, ahead of
// nodes that don't, regardless of raw utilization. Pulling a new tag onto
// a node that already has an older tag of the same repo is typically a
// small delta (usually just the top application layer changed), so it
// beats even a much emptier node paying for a fully cold pull. Ordering
// within each of the two groups is preserved from the input, so the
// existing ascending-utilization tiebreak still applies inside each
// group.
//
// This is a repo-level heuristic, not a real layer-diff calculation:
// checking actual shared-layer overlap would mean a registry manifest
// call per candidate, and the entire point of preheat is to beat ArgoCD's
// sync loop, so a network round trip per decision isn't worth it. "Same
// repo, any tag" is assumed close enough in practice.
func (rk *Ranker) rankByLocality(image string, candidates []*workerEntry) []*workerEntry {
	if !rk.preferImageLocality {
		return candidates
	}
	repo := imageref.Repo(image)
	if repo == "" {
		return candidates
	}

	withRepo := make([]*workerEntry, 0, len(candidates))
	without := make([]*workerEntry, 0, len(candidates))
	for _, w := range candidates {
		if w.HasRepo(repo) {
			withRepo = append(withRepo, w)
		} else {
			without = append(without, w)
		}
	}
	if len(withRepo) == 0 {
		return without
	}
	logging.Debugf("angryduck-controller: %d of %d candidate(s) already have repo=%s locally (any tag), prioritizing them for image=%s",
		len(withRepo), len(candidates), repo, image)
	return append(withRepo, without...)
}

// excludeMatching drops any worker whose NodeID contains one of
// rk.excludeSubstrings, preserving the input's utilization ordering.
// Excluded workers are still fresh and still reporting — they're just
// never handed a pull order, since master/control-plane nodes will never
// actually have a real pod scheduled onto them to benefit from the
// pre-pull.
func (rk *Ranker) excludeMatching(workers []*workerEntry) []*workerEntry {
	if len(rk.excludeSubstrings) == 0 {
		return workers
	}
	kept := make([]*workerEntry, 0, len(workers))
	for _, w := range workers {
		excluded := false
		for _, sub := range rk.excludeSubstrings {
			if sub != "" && strings.Contains(w.NodeID, sub) {
				excluded = true
				break
			}
		}
		if excluded {
			logging.Debugf("angryduck-controller: excluding node=%s from preheat selection (matches excluded-node substring)", w.NodeID)
			continue
		}
		kept = append(kept, w)
	}
	return kept
}

func (rk *Ranker) sendPullOrder(nodeID, addr, image string) {
	registryLabel := imageref.RegistryLabel(image, rk.labelRegistryHost)
	order := model.PullOrder{Image: image, OrderedAt: time.Now()}
	body, err := json.Marshal(order)
	if err != nil {
		logging.Errorf("angryduck-controller: failed to marshal pull order for node=%s: %v", nodeID, err)
		return
	}
	url := "http://" + addr + "/pull"
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		logging.Errorf("angryduck-controller: failed to build pull request for node=%s: %v", nodeID, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := rk.httpClient.Do(req)
	if err != nil {
		logging.Warnf("angryduck-controller: pull order to node=%s addr=%s failed: %v", nodeID, addr, err)
		pullOrdersTotal.Inc(nodeID, "failure", registryLabel)
		rk.releaseSlot(image)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		logging.Warnf("angryduck-controller: pull order to node=%s addr=%s rejected: status=%d", nodeID, addr, resp.StatusCode)
		pullOrdersTotal.Inc(nodeID, "failure", registryLabel)
		rk.releaseSlot(image)
		return
	}
	logging.Infof("angryduck-controller: ordered node=%s to pull image=%s", nodeID, image)
	pullOrdersTotal.Inc(nodeID, "success", registryLabel)
}
