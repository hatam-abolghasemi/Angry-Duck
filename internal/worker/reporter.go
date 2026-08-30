package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"angryduck/internal/logging"
	"angryduck/internal/model"
)

// Reporter periodically scrapes node-exporter and pushes disk utilization
// to the controller.
type Reporter struct {
	nodeID        string
	selfAddress   string
	metricsURL    string
	controllerURL string
	interval      time.Duration
	httpClient    *http.Client
}

// NewReporter builds a reporter.
func NewReporter(nodeID, selfAddress, metricsURL, controllerURL string, interval time.Duration) *Reporter {
	return &Reporter{
		nodeID:        nodeID,
		selfAddress:   selfAddress,
		metricsURL:    metricsURL,
		controllerURL: controllerURL,
		interval:      interval,
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

	report := model.WorkerReport{
		NodeID:      rp.nodeID,
		Address:     rp.selfAddress,
		Utilization: result.Utilization,
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
	logging.Debugf("angryduck-worker[%s]: report accepted by controller: utilization=%.1f%%", rp.nodeID, result.Utilization*100)
}
