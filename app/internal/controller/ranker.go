package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"angryduck/internal/logging"
	"angryduck/internal/metrics"
	"angryduck/internal/model"
)

// pullOrdersTotal counts /pull requests the controller has sent to
// workers, split by node and whether the worker accepted it. This is the
// controller-side half of the pull story; angryduck_worker_pulls_total
// (see worker/puller.go) is the worker-side half.
var pullOrdersTotal = metrics.NewCounterVec(
	"angryduck_controller_pull_orders_total",
	"Total pull orders sent to workers, by node and result.",
	"node", "result",
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
// preheat window. The ranker itself never blocks waiting on a pull to
// finish either way — sendPullOrder fires in its own goroutine and only
// confirms the worker *accepted* the order (HTTP 2xx), never that the pull
// itself completed.
type Ranker struct {
	registry          *Registry
	topN              int
	interval          time.Duration
	excludeSubstrings []string
	httpClient        *http.Client

	mu              sync.Mutex
	orderedForImage string
	orderedNodes    map[string]bool
}

// NewRanker builds a ranker bound to the given registry. excludeSubstrings
// is a list of case-sensitive substrings (e.g. "master", "control-plane");
// any worker whose NodeID contains one is still tracked and still runs its
// own local GC loop as normal — it's simply never selected as a preheat
// target. This exists because master/control-plane nodes still need
// Angry Duck's worker running on them for disk GC, but pass nil here (or
// an empty slice) to disable the behavior entirely.
//
// Matching is done here, at selection time, rather than by keeping masters
// out of the registry or off the DaemonSet: excluding them from scheduling
// entirely would mean nobody GCs their disk, which is a real regression —
// the constraint is "never pick them as a target," not "never run there."
func NewRanker(registry *Registry, topN int, interval time.Duration, excludeSubstrings []string) *Ranker {
	return &Ranker{
		registry:          registry,
		topN:              topN,
		interval:          interval,
		excludeSubstrings: excludeSubstrings,
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

	candidates := rk.notYetOrdered(image, eligible)
	if len(candidates) == 0 {
		logging.Debugf("angryduck-controller: all %d eligible worker(s) already ordered for image=%s, nothing new this tick", len(eligible), image)
		return nil
	}

	n := rk.topN
	if n > len(candidates) {
		n = len(candidates)
	}
	chosen := candidates[:n]

	logging.Debugf("angryduck-controller: ranked %d fresh worker(s) (%d eligible after exclusions, %d not yet ordered), choosing lowest %d by utilization for image=%s",
		len(fresh), len(eligible), len(candidates), n, image)
	for _, w := range eligible {
		logging.Debugf("angryduck-controller: candidate node=%s utilization=%.1f%%", w.NodeID, w.Utilization*100)
	}

	rk.mu.Lock()
	ordered := make([]string, 0, len(chosen))
	for _, w := range chosen {
		ordered = append(ordered, w.NodeID)
		rk.orderedNodes[w.NodeID] = true
		go rk.sendPullOrder(w.NodeID, w.Address, image)
	}
	rk.mu.Unlock()
	return ordered
}

// notYetOrdered resets the ordered-nodes set the moment image differs from
// the last image this ranker ordered for (a new push landed), then returns
// the subset of eligible workers not yet recorded as ordered — preserving
// eligible's existing ascending-utilization order.
func (rk *Ranker) notYetOrdered(image string, eligible []*workerEntry) []*workerEntry {
	rk.mu.Lock()
	defer rk.mu.Unlock()

	if image != rk.orderedForImage {
		rk.orderedForImage = image
		rk.orderedNodes = make(map[string]bool)
	}

	candidates := make([]*workerEntry, 0, len(eligible))
	for _, w := range eligible {
		if !rk.orderedNodes[w.NodeID] {
			candidates = append(candidates, w)
		}
	}
	return candidates
}

// excludeMatching drops any worker whose NodeID contains one of
// rk.excludeSubstrings, preserving the input's utilization ordering.
// Excluded workers are still fresh, still reporting, and still running
// their own local GC — they're just never handed a pull order, since
// master/control-plane nodes will never actually have a real pod
// scheduled onto them to benefit from the pre-pull.
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
		pullOrdersTotal.Inc(nodeID, "failure")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		logging.Warnf("angryduck-controller: pull order to node=%s addr=%s rejected: status=%d", nodeID, addr, resp.StatusCode)
		pullOrdersTotal.Inc(nodeID, "failure")
		return
	}
	logging.Infof("angryduck-controller: ordered node=%s to pull image=%s", nodeID, image)
	pullOrdersTotal.Inc(nodeID, "success")
}
