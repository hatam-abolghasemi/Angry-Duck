package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

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
	// Send one report immediately so the controller doesn't wait a full
	// interval to learn this worker exists.
	rp.reportOnce()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rp.reportOnce()
		}
	}
}

func (rp *Reporter) reportOnce() {
	util, err := FetchRootUtilization(rp.metricsURL, 5*time.Second)
	if err != nil {
		log.Printf("angryduck-worker[%s]: failed to read node-exporter metrics: %v", rp.nodeID, err)
		return
	}

	report := model.WorkerReport{
		NodeID:      rp.nodeID,
		Address:     rp.selfAddress,
		Utilization: util,
		Timestamp:   time.Now(),
	}
	body, err := json.Marshal(report)
	if err != nil {
		log.Printf("angryduck-worker[%s]: failed to marshal report: %v", rp.nodeID, err)
		return
	}

	url := rp.controllerURL + "/report"
	resp, err := rp.httpClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("angryduck-worker[%s]: failed to push report to %s: %v", rp.nodeID, url, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		log.Printf("angryduck-worker[%s]: controller rejected report: status=%d", rp.nodeID, resp.StatusCode)
	}
}
