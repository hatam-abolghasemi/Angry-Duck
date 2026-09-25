package worker

import (
	"context"
	"time"

	"angryduck/internal/logging"
	"angryduck/internal/metrics"
)

// preheatedContainersRunning is a point-in-time count, sampled once per
// PreheatMonitor tick, of how many currently-running containers on this
// node belong to a repo Angry Duck itself preheated onto this node within
// PREHEAT_ATTRIBUTION_RETENTION_S. It's the closest thing to "how many
// pods are benefiting from our preheat" Angry Duck can answer without
// watching every pod's scheduling and pull events in real time (nothing
// here does that today).
//
// Labeled by node and repo ONLY — deliberately not by pod name or full
// image reference. Repo already bounds this to roughly "how many distinct
// applications does this cluster run," which is the same order of
// magnitude the registry-host label accepted elsewhere; pod name or a
// tag-specific image reference would not be bounded the same way, and
// would turn this into a live pod inventory rather than a coarse sample.
//
// Only repos actually preheated here get a series — a repo nobody
// preheated is not an interesting "false" data point for this metric the
// way it might be for a general inventory, so it's simply absent rather
// than reported as preheated="false" (an idea considered and dropped:
// splitting on a preheated/not-preheated bool would double every repo's
// series for no benefit here).
var preheatedContainersRunning = metrics.NewGaugeVec(
	"angryduck_worker_preheated_containers_running",
	"Currently-running containers on this node whose image repo was preheated here within the retention window.",
	"node", "repo",
)

// PreheatMonitor periodically samples which repos Angry Duck has
// preheated onto this node are still backing running containers. It
// deliberately runs on its own, coarser interval than the reporter
// (PREHEAT_ATTRIBUTION_INTERVAL_S, default 300s): this is a periodic
// sample for a dashboard question ("is preheat pulling its weight"), not
// an event a rollout needs to react to immediately.
type PreheatMonitor struct {
	runtime   Runtime
	puller    *Puller
	interval  time.Duration
	retention time.Duration
	nodeID    string
}

// NewPreheatMonitor builds a PreheatMonitor. retention is how long after
// a successful preheat pull its repo still counts as "preheated" for this
// sample — see Puller.PreheatedRepos.
func NewPreheatMonitor(runtime Runtime, puller *Puller, interval, retention time.Duration, nodeID string) *PreheatMonitor {
	return &PreheatMonitor{
		runtime:   runtime,
		puller:    puller,
		interval:  interval,
		retention: retention,
		nodeID:    nodeID,
	}
}

// Run blocks, sampling on every tick until ctx is done.
func (m *PreheatMonitor) Run(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	logging.Infof("angryduck-worker-preheat-monitor: started — sampling every %s, counting repos preheated here within the last %s", m.interval, m.retention)
	for {
		select {
		case <-ctx.Done():
			logging.Infof("angryduck-worker-preheat-monitor: stopped")
			return
		case <-ticker.C:
			m.tick()
		}
	}
}

func (m *PreheatMonitor) tick() {
	preheated := m.puller.PreheatedRepos(m.retention)
	if len(preheated) == 0 {
		// Nothing preheated here recently (a quiet node, or one that's
		// never been a preheat seed) — clear any stale series from an
		// earlier, now-expired preheat rather than reporting nothing this
		// tick and leaving old values sitting at their last count.
		preheatedContainersRunning.Reset()
		return
	}

	running, err := m.runtime.RunningImageRepos()
	if err != nil {
		logging.Errorf("angryduck-worker-preheat-monitor: could not list running containers, skipping this sample: %v", err)
		return
	}

	preheatedContainersRunning.Reset()
	var totalMatched, totalRepos int64
	for repo, count := range running {
		if !preheated[repo] {
			continue
		}
		preheatedContainersRunning.Set(float64(count), m.nodeID, repo)
		totalMatched += int64(count)
		totalRepos++
	}
	logging.Debugf("angryduck-worker-preheat-monitor: sampled %d running container(s) across %d preheated repo(s) (of %d repos preheated here within the retention window)",
		totalMatched, totalRepos, len(preheated))
}
