package worker

import (
	"context"
	"time"

	"angryduck/internal/logging"
)

// GC periodically compares every image present on the node against
// currently running containers, and removes any image that has gone unused
// for missThreshold consecutive checks — unless still within its
// post-pull-order grace period.
//
// This scans ALL local images, not just ones Angry Duck itself ordered
// pulled — a deliberate operator choice to have Angry Duck manage disk
// space for the whole node, not just its own preheating overhead. This is
// safe only because ListLocalImages()/ListRunningImages() now parse real
// JSON output (see runtime.go) instead of the substring-marker extraction
// bug that previously made every image on the node look permanently
// unused. Do not revert that extraction to substring scraping — this GC
// loop has no independent safety net against a broken "is this running?"
// check, since it deliberately scans every image, including ones critical
// to cluster operation (calico, kube-proxy, coredns, etc.).
//
// Every decision GC makes is logged, at a level matching its consequence:
//   - DEBUG: routine, expected-to-be-boring per-image facts (this image is
//     running, this image is in its grace period) — high volume, off by
//     default.
//   - INFO: normal progress worth seeing without asking for DEBUG (miss
//     count increments, tick summaries).
//   - WARN: an actual or would-be removal — a decision with real
//     consequences, whether or not dry-run is on.
//   - ERROR: something GC could not even evaluate (a runtime command
//     failed).
type GC struct {
	runtime       Runtime
	puller        *Puller
	interval      time.Duration
	missThreshold int
	missCounts    map[string]int
	dryRun        bool
}

// NewGC builds a GC loop. When dryRun is true, GC logs exactly what it would
// remove and why, but never actually calls RemoveImage — use this to watch
// the matching logic decide against real production data before trusting it
// to delete anything, especially right after a matching-logic change.
func NewGC(runtime Runtime, puller *Puller, interval time.Duration, missThreshold int, dryRun bool) *GC {
	return &GC{
		runtime:       runtime,
		puller:        puller,
		interval:      interval,
		missThreshold: missThreshold,
		missCounts:    make(map[string]int),
		dryRun:        dryRun,
	}
}

// Run blocks, running a GC pass on every tick until ctx is done.
func (g *GC) Run(ctx context.Context) {
	ticker := time.NewTicker(g.interval)
	defer ticker.Stop()
	logging.Infof("angryduck-worker-gc: loop started: check_interval=%s miss_threshold=%d dry_run=%v", g.interval, g.missThreshold, g.dryRun)
	for {
		select {
		case <-ctx.Done():
			logging.Infof("angryduck-worker-gc: loop stopping")
			return
		case <-ticker.C:
			g.tick()
		}
	}
}

func (g *GC) tick() {
	tickStart := time.Now()

	local, err := g.runtime.ListLocalImages()
	if err != nil {
		logging.Errorf("angryduck-worker-gc: failed to list local images: %v", err)
		return
	}
	running, err := g.runtime.ListRunningImages()
	if err != nil {
		logging.Errorf("angryduck-worker-gc: failed to list running images: %v", err)
		return
	}
	runningSet := make(map[string]bool, len(running))
	for _, img := range running {
		if img != "" {
			runningSet[img] = true
		}
	}

	logging.Infof("angryduck-worker-gc: tick start: %d local image(s), %d running image(s), %d already tracked as unused",
		len(local), len(runningSet), len(g.missCounts))

	seen := make(map[string]bool, len(local))
	removedCount, sparedRunning, sparedGrace, trackedCount := 0, 0, 0, 0
	for _, img := range local {
		if img == "" {
			continue
		}
		seen[img] = true

		if runningSet[img] {
			if g.missCounts[img] > 0 {
				logging.Infof("angryduck-worker-gc: image=%s now in use again, resetting miss count from %d to 0", img, g.missCounts[img])
			} else {
				logging.Debugf("angryduck-worker-gc: image=%s is running, no action", img)
			}
			delete(g.missCounts, img)
			sparedRunning++
			continue
		}

		if g.puller.InGracePeriod(img) {
			logging.Debugf("angryduck-worker-gc: image=%s not running but still within its post-pull grace period, skipping this check", img)
			sparedGrace++
			continue
		}

		g.missCounts[img]++
		miss := g.missCounts[img]

		if miss >= g.missThreshold {
			if g.dryRun {
				logging.Warnf("angryduck-worker-gc: [DRY RUN] would remove image=%s: unused for %d/%d consecutive checks (would NOT actually remove — GC_DRY_RUN=true)",
					img, miss, g.missThreshold)
			} else {
				logging.Warnf("angryduck-worker-gc: removing image=%s: unused for %d/%d consecutive checks", img, miss, g.missThreshold)
				if err := g.runtime.RemoveImage(img); err != nil {
					logging.Errorf("angryduck-worker-gc: failed to remove image=%s: %v", img, err)
					continue
				}
				logging.Infof("angryduck-worker-gc: image=%s removed successfully", img)
			}
			delete(g.missCounts, img)
			removedCount++
		} else {
			logging.Infof("angryduck-worker-gc: image=%s unused this check: miss count %d/%d (removal happens at %d)",
				img, miss, g.missThreshold, g.missThreshold)
			trackedCount++
		}
	}

	// Drop counters for images that no longer exist locally at all.
	for img := range g.missCounts {
		if !seen[img] {
			logging.Debugf("angryduck-worker-gc: image=%s no longer present locally, dropping its miss counter", img)
			delete(g.missCounts, img)
		}
	}

	logging.Infof("angryduck-worker-gc: tick done in %s: %d spared (running), %d spared (grace period), %d tracked below threshold, %d removed",
		time.Since(tickStart).Round(time.Millisecond), sparedRunning, sparedGrace, trackedCount, removedCount)
}
