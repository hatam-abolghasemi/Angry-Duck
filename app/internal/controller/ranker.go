package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"angryduck/internal/logging"
	"angryduck/internal/model"
)

// Ranker periodically re-ranks fresh workers by utilization and orders the
// least-utilized top-N to pull the current target image. It also exposes
// OrderNow for the webhook handler to trigger an immediate order without
// waiting for the next tick.
type Ranker struct {
	registry          *Registry
	topN              int
	interval          time.Duration
	excludeSubstrings []string
	httpClient        *http.Client
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
// utilization, and fires off pull orders to each concurrently. Returns the
// node IDs ordered (for logging / API responses).
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

	n := rk.topN
	if n > len(eligible) {
		n = len(eligible)
	}
	chosen := eligible[:n]

	logging.Debugf("angryduck-controller: ranked %d fresh worker(s) (%d eligible after exclusions), choosing lowest %d by utilization for image=%s",
		len(fresh), len(eligible), n, image)
	for _, w := range eligible {
		logging.Debugf("angryduck-controller: candidate node=%s utilization=%.1f%%", w.NodeID, w.Utilization*100)
	}

	ordered := make([]string, 0, len(chosen))
	for _, w := range chosen {
		ordered = append(ordered, w.NodeID)
		go rk.sendPullOrder(w.NodeID, w.Address, image)
	}
	return ordered
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
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		logging.Warnf("angryduck-controller: pull order to node=%s addr=%s rejected: status=%d", nodeID, addr, resp.StatusCode)
		return
	}
	logging.Infof("angryduck-controller: ordered node=%s to pull image=%s", nodeID, image)
}
