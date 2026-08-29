package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"angryduck/internal/model"
)

// Ranker periodically re-ranks fresh workers by utilization and orders the
// least-utilized top-N to pull the current target image. It also exposes
// OrderNow for the webhook handler to trigger an immediate order without
// waiting for the next tick.
type Ranker struct {
	registry   *Registry
	topN       int
	interval   time.Duration
	httpClient *http.Client
}

// NewRanker builds a ranker bound to the given registry.
func NewRanker(registry *Registry, topN int, interval time.Duration) *Ranker {
	return &Ranker{
		registry: registry,
		topN:     topN,
		interval: interval,
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

// orderLowestN ranks fresh workers, picks the lowest topN by utilization,
// and fires off pull orders to each concurrently. Returns the node IDs
// ordered (for logging / API responses).
func (rk *Ranker) orderLowestN(image string) []string {
	fresh := rk.registry.FreshWorkers()
	if len(fresh) == 0 {
		return nil
	}
	n := rk.topN
	if n > len(fresh) {
		n = len(fresh)
	}
	chosen := fresh[:n]

	ordered := make([]string, 0, len(chosen))
	for _, w := range chosen {
		ordered = append(ordered, w.NodeID)
		go rk.sendPullOrder(w.NodeID, w.Address, image)
	}
	return ordered
}

func (rk *Ranker) sendPullOrder(nodeID, addr, image string) {
	order := model.PullOrder{Image: image, OrderedAt: time.Now()}
	body, err := json.Marshal(order)
	if err != nil {
		log.Printf("angryduck-controller: failed to marshal pull order for %s: %v", nodeID, err)
		return
	}
	url := "http://" + addr + "/pull"
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		log.Printf("angryduck-controller: failed to build pull request for %s: %v", nodeID, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := rk.httpClient.Do(req)
	if err != nil {
		log.Printf("angryduck-controller: pull order to node=%s addr=%s failed: %v", nodeID, addr, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		log.Printf("angryduck-controller: pull order to node=%s addr=%s rejected: status=%d", nodeID, addr, resp.StatusCode)
		return
	}
	log.Printf("angryduck-controller: ordered node=%s to pull image=%s", nodeID, image)
}
