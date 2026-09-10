package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"angryduck/internal/imageref"
	"angryduck/internal/logging"
	"angryduck/internal/model"
)

// Reporter periodically scrapes node-exporter and pushes disk utilization,
// plus this node's local image inventory, to the controller.
type Reporter struct {
	nodeID        string
	selfAddress   string
	metricsURL    string
	controllerURL string
	interval      time.Duration
	inventory     *Inventory
	httpClient    *http.Client
	kick          chan struct{}
}

// NewReporter builds a reporter. rt is used to list local images each
// report tick so the controller can rank preheat targets by image
// locality (see internal/controller/ranker.go's rankByLocality) —
// pulling a new tag onto a node that already has an older tag of the same
// repo is typically far cheaper than a cold pull.
//
// The listing goes through the shared Inventory, which also feeds GC and
// the mirror's digest index, so one `crictl images -o json` per interval
// serves all three.
func NewReporter(nodeID, selfAddress, metricsURL, controllerURL string, interval time.Duration, inv *Inventory) *Reporter {
	return &Reporter{
		nodeID:        nodeID,
		selfAddress:   selfAddress,
		metricsURL:    metricsURL,
		controllerURL: controllerURL,
		interval:      interval,
		inventory:     inv,
		httpClient:    &http.Client{Timeout: 5 * time.Second},
		kick:          make(chan struct{}, 1),
	}
}

// Kick asks for one report right now instead of at the next tick. Used
// after a preheat pull succeeds, so the controller can start pointing
// peers at this node immediately rather than up to REPORT_INTERVAL_S
// later — the rollout is racing the preheat, and those seconds are the
// window where requesters would otherwise find no peer and go to origin.
// Non-blocking; multiple kicks before the loop wakes collapse into one.
func (rp *Reporter) Kick() {
	select {
	case rp.kick <- struct{}{}:
	default:
	}
}

// Run blocks, reporting on every tick until ctx is done.
func (rp *Reporter) Run(ctx context.Context) {
	ticker := time.NewTicker(rp.interval)
	defer ticker.Stop()
	logging.Infof("angryduck-worker[%s]: reporter started: interval=%s metrics_url=%s controller_url=%s",
		rp.nodeID, rp.interval, rp.metricsURL, rp.controllerURL)
	// Send one report immediately so the controller doesn't wait a full
	// interval to learn this worker exists.
	rp.reportOnce(0)
	for {
		select {
		case <-ctx.Done():
			logging.Infof("angryduck-worker[%s]: reporter stopping", rp.nodeID)
			return
		case <-ticker.C:
			rp.reportOnce(rp.interval / 2)
		case <-rp.kick:
			rp.reportOnce(0) // a pull just landed; the listing must include it
		}
	}
}

// reportOnce sends one report. listMaxAge lets a tick reuse a listing GC
// took moments ago instead of running the same command again.
func (rp *Reporter) reportOnce(listMaxAge time.Duration) {
	result, err := FetchRootUtilization(rp.metricsURL, 5*time.Second)
	if err != nil {
		logging.Warnf("angryduck-worker[%s]: failed to read node-exporter metrics from %s: %v", rp.nodeID, rp.metricsURL, err)
		return
	}

	// Log the actual math, not just the final ratio — the raw byte figures
	// make it obvious at a glance whether a suspicious utilization number
	// comes from real disk pressure or from something like node-exporter
	// reporting a filesystem that's much smaller than expected.
	logging.Debugf("angryduck-worker[%s]: computed root fs utilization: used=%.0f bytes, free=%.0f bytes, size=%.0f bytes, utilization=%.4f (%.1f%%)",
		rp.nodeID, result.UsedBytes, result.FreeBytes, result.SizeBytes, result.Utilization, result.Utilization*100)

	repos := rp.localRepos(listMaxAge)

	report := model.WorkerReport{
		NodeID:      rp.nodeID,
		Address:     rp.selfAddress,
		Utilization: result.Utilization,
		Repos:       repos,
		Digests:     rp.inventory.Digests(),
		Timestamp:   time.Now(),
	}
	body, err := json.Marshal(report)
	if err != nil {
		logging.Errorf("angryduck-worker[%s]: failed to marshal report: %v", rp.nodeID, err)
		return
	}

	url := rp.controllerURL + "/report"
	resp, err := rp.httpClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		logging.Warnf("angryduck-worker[%s]: failed to push report to %s: %v", rp.nodeID, url, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		logging.Warnf("angryduck-worker[%s]: controller rejected report: status=%d", rp.nodeID, resp.StatusCode)
		return
	}
	logging.Debugf("angryduck-worker[%s]: report accepted by controller: utilization=%.1f%%, repos=%d", rp.nodeID, result.Utilization*100, len(repos))
}

// localRepos asks the runtime for every local image reference and reduces
// it to the deduplicated set of bare repo identities (imageref.Repo),
// stripping tag/digest so the controller only needs to know "does this
// node have SOME build of this repo", not which exact tag.
//
// The listing comes from the shared Inventory: GC and the mirror's digest
// index use the same one, so this is the only regular image listing the
// worker performs.
func (rp *Reporter) localRepos(listMaxAge time.Duration) []string {
	refs, _, err := rp.inventory.Get(listMaxAge)
	if err != nil {
		logging.Warnf("angryduck-worker[%s]: failed to list local images for repo report: %v", rp.nodeID, err)
		return nil
	}

	seen := make(map[string]struct{}, len(refs))
	repos := make([]string, 0, len(refs))
	for _, ref := range refs {
		repo := imageref.Repo(ref)
		if repo == "" {
			continue // bare digest alias, no repo identity to report
		}
		if _, ok := seen[repo]; ok {
			continue
		}
		seen[repo] = struct{}{}
		repos = append(repos, repo)
	}
	return repos
}
