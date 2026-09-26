package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
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
// The listing goes through the shared Inventory, so a report right after
// a recent listing reuses it instead of running the command again.
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
// after a preheat pull succeeds, so the controller's repo-locality view of
// this node (used to prefer it for a later preheat of the same repo) is
// fresh immediately rather than up to REPORT_INTERVAL_S stale.
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

// reportOnce sends one report. listMaxAge lets a tick reuse a listing
// taken moments ago instead of running the same command again.
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

	repos, images := rp.localInventory(listMaxAge)

	report := model.WorkerReport{
		NodeID:      rp.nodeID,
		Address:     rp.selfAddress,
		Utilization: result.Utilization,
		Repos:       repos,
		Images:      images,
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

// localInventory asks the runtime for every local image reference once,
// and returns the deduplicated set of bare repo identities (for preheat
// locality ranking) plus every full reference except bare image IDs (for
// finding rescue sources). Bounded by how many images actually sit
// on this node — tens, not thousands — so this doesn't grow without
// bound over the node's lifetime.
//
// The listing comes from the shared Inventory — the only regular image
// listing the worker performs.
func (rp *Reporter) localInventory(listMaxAge time.Duration) (repos, images []string) {
	refs, err := rp.inventory.Get(listMaxAge)
	if err != nil {
		logging.Warnf("angryduck-worker[%s]: failed to list local images for report: %v", rp.nodeID, err)
		return nil, nil
	}

	seenRepo := make(map[string]struct{}, len(refs))
	seenImage := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		if ref == "" || strings.HasPrefix(ref, "sha256:") {
			continue // bare image ID: no name at all
		}
		if _, ok := seenImage[ref]; !ok {
			seenImage[ref] = struct{}{}
			images = append(images, ref)
		}
		if strings.Contains(ref, "@sha256:") {
			continue // digest-pinned alias: kept in images above, but not a tag for repo locality
		}

		repo := imageref.Repo(ref)
		if repo == "" {
			continue
		}
		if _, ok := seenRepo[repo]; ok {
			continue
		}
		seenRepo[repo] = struct{}{}
		repos = append(repos, repo)
	}
	return repos, images
}
