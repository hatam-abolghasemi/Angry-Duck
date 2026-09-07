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
	runtime       Runtime
	httpClient    *http.Client
}

// NewReporter builds a reporter. rt is used to list local images each
// report tick so the controller can rank preheat targets by image
// locality (see internal/controller/ranker.go's rankByLocality) —
// pulling a new tag onto a node that already has an older tag of the same
// repo is typically far cheaper than a cold pull.
func NewReporter(nodeID, selfAddress, metricsURL, controllerURL string, interval time.Duration, rt Runtime) *Reporter {
	return &Reporter{
		nodeID:        nodeID,
		selfAddress:   selfAddress,
		metricsURL:    metricsURL,
		controllerURL: controllerURL,
		interval:      interval,
		runtime:       rt,
		httpClient:    &http.Client{Timeout: 5 * time.Second},
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
	rp.reportOnce()
	for {
		select {
		case <-ctx.Done():
			logging.Infof("angryduck-worker[%s]: reporter stopping", rp.nodeID)
			return
		case <-ticker.C:
			rp.reportOnce()
		}
	}
}

func (rp *Reporter) reportOnce() {
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

	repos := rp.localRepos()

	report := model.WorkerReport{
		NodeID:      rp.nodeID,
		Address:     rp.selfAddress,
		Utilization: result.Utilization,
		Repos:       repos,
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
// This is a second, independent call to Runtime.LocalImages() — GC (see
// gc.go) already calls it once per its own tick for digest matching, on a
// different schedule (GC_CHECK_INTERVAL_S, default 60s, vs
// REPORT_INTERVAL_S, default 15s). They aren't merged into one shared
// cache: LocalImages() on every backend is a single, cheap subprocess call
// (`crictl images -o json`, `ctr images list`, or `docker images`), not
// the kind of N+1-per-container cost ListRunningImages() had on the
// containerd backend, so paying for it twice on two independent tickers
// is simpler than coordinating a shared cache across two goroutines for a
// negligible amount of CPU.
func (rp *Reporter) localRepos() []string {
	refs, _, err := rp.runtime.LocalImages()
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
