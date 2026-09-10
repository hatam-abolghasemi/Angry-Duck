package worker

import (
	"context"
	"strings"
	"time"

	"angryduck/internal/logging"
	"angryduck/internal/metrics"
)

// imagesDeletedTotal counts images GC has actually removed on this worker.
// Dry-run "would remove" decisions are deliberately not counted here —
// this metric answers "how much did GC actually delete," and a dry-run
// pass never touches disk.
var imagesDeletedTotal = metrics.NewCounterVec(
	"angryduck_worker_images_deleted_total",
	"Total images actually removed by GC on this worker (excludes dry-run).",
	"node",
)

// GC periodically compares every image present on the node against
// currently running containers, and removes any image that has gone unused
// for missThreshold consecutive checks — unless still within its
// post-pull-order grace period.
//
// This scans ALL local images, not just ones Angry Duck itself ordered
// pulled — a deliberate operator choice to have Angry Duck manage disk
// space for the whole node, not just its own preheating overhead. This is
// safe only because the "is this running?" check compares images by their
// canonical content digest (via runtime.LocalImages()), not by raw
// reference string. Two confirmed production bugs motivated this:
//
//  1. ListRunningImages() originally used a substring marker that never
//     matched ctr's pretty-printed JSON at all, so every image on the node
//     looked permanently unused regardless of reality. Fixed by parsing
//     real JSON.
//  2. Even with correct JSON parsing, comparing by raw string still
//     misjudged images as unused: containerd gives one piece of content
//     multiple valid aliases (a tag, a digest-pinned ref, and a bare
//     digest "image ID"), and a running container's reported Image field
//     is only ever one of those aliases. GC spared the alias that
//     happened to match and scheduled the image's OTHER aliases for
//     removal — even though they were the exact same content backing that
//     same running container. Confirmed in production: 6 local images, 12
//     running images, only 2 spared. Fixed by resolving every reference to
//     its digest before comparing, since the digest is identical across
//     all aliases of one piece of content.
//
// Do not revert to raw-string comparison. This GC loop has no independent
// safety net against a broken "is this running?" check, since it
// deliberately scans every image, including ones critical to cluster
// operation (calico, kube-proxy, coredns, the pause image every pod
// sandbox depends on, etc.).
//
// One image in particular can NEVER be proven "running" by digest matching
// no matter how correct that matching is: the pause image. containerd/CRI
// tracks the container backing a pod sandbox as a PodSandbox, not as a
// regular Container — so `crictl ps` (which lists Containers) will never
// report it, and ListRunningImages() will never see it, even though every
// running pod on the node depends on it. excludeSubstrings exists for
// exactly this case: an explicit "never eligible for removal regardless of
// observed running-state" allowlist, matched by substring against the
// image reference, for images GC should never even ask "is this running?"
// about — pause and node-exporter being the two seen in practice.
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
	runtime           Runtime
	inventory         *Inventory
	listMaxAge        time.Duration
	puller            *Puller
	interval          time.Duration
	missThreshold     int
	missCounts        map[string]int
	dryRun            bool
	excludeSubstrings []string
	nodeID            string
}

// NewGC builds a GC loop. When dryRun is true, GC logs exactly what it would
// remove and why, but never actually calls RemoveImage — use this to watch
// the matching logic decide against real production data before trusting it
// to delete anything, especially right after a matching-logic change.
// excludeSubstrings is a list of case-sensitive substrings matched against
// each local image reference; any match is spared unconditionally, without
// ever being asked whether it's running — see the pause-image note above
// for why that distinction matters. Pass nil to exclude nothing.
//
// inventory supplies the local image listing; a listing no older than
// listMaxAge (the report interval, in practice) is reused instead of
// running the runtime's image listing again — see Inventory.
func NewGC(runtime Runtime, inventory *Inventory, listMaxAge time.Duration, puller *Puller, interval time.Duration, missThreshold int, dryRun bool, excludeSubstrings []string, nodeID string) *GC {
	return &GC{
		runtime:           runtime,
		inventory:         inventory,
		listMaxAge:        listMaxAge,
		puller:            puller,
		interval:          interval,
		missThreshold:     missThreshold,
		missCounts:        make(map[string]int),
		dryRun:            dryRun,
		excludeSubstrings: excludeSubstrings,
		nodeID:            nodeID,
	}
}

// isExcluded reports whether image matches one of the configured
// always-spare substrings.
func (g *GC) isExcluded(image string) bool {
	for _, sub := range g.excludeSubstrings {
		if sub != "" && strings.Contains(image, sub) {
			return true
		}
	}
	return false
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

	if pruned := g.puller.PruneExpired(); pruned > 0 {
		logging.Debugf("angryduck-worker-gc: pruned %d expired pull-order record(s) from the puller's grace-period tracker", pruned)
	}

	// One listing for both the local ref list and the digest map, and
	// usually not even a fresh one: the reporter refreshes the shared
	// inventory every REPORT_INTERVAL_S, so GC reuses that listing rather
	// than running `crictl images -o json` a second time for the same
	// answer. Only if the reporter hasn't listed recently (node-exporter
	// down, say) does GC list for itself.
	local, digests, err := g.inventory.Get(g.listMaxAge)
	if err != nil {
		logging.Errorf("angryduck-worker-gc: failed to list local images: %v", err)
		return
	}
	if digests == nil {
		// Not fatal: fall back to raw-string matching only. Log loudly
		// since this silently re-exposes the alias-mismatch bug — better
		// to know GC is running degraded than to wonder why.
		logging.Errorf("angryduck-worker-gc: runtime returned no digest map, falling back to raw reference matching only (this re-exposes the alias-mismatch bug for this tick)")
		digests = map[string]string{}
	}
	running, err := g.runtime.ListRunningImages()
	if err != nil {
		logging.Errorf("angryduck-worker-gc: failed to list running images: %v", err)
		return
	}

	runningSet := make(map[string]bool, len(running))
	runningDigests := make(map[string]bool, len(running))
	for _, img := range running {
		if img == "" {
			continue
		}
		runningSet[img] = true
		if d, ok := digests[img]; ok {
			runningDigests[d] = true
		}
	}

	logging.Infof("angryduck-worker-gc: tick start: %d local image(s), %d running image(s), %d already tracked as unused",
		len(local), len(runningSet), len(g.missCounts))

	seen := make(map[string]bool, len(local))
	removedCount, sparedRunning, sparedGrace, sparedExcluded, trackedCount := 0, 0, 0, 0, 0
	for _, img := range local {
		if img == "" {
			continue
		}
		seen[img] = true

		if g.isExcluded(img) {
			if g.missCounts[img] > 0 {
				logging.Infof("angryduck-worker-gc: image=%s now matches an excluded image pattern, resetting miss count from %d to 0", img, g.missCounts[img])
			} else {
				logging.Debugf("angryduck-worker-gc: image=%s matches an excluded image pattern, never eligible for removal regardless of running-state", img)
			}
			delete(g.missCounts, img)
			sparedExcluded++
			continue
		}

		inUse := runningSet[img]
		matchedVia := "direct reference match"
		if !inUse {
			if d, ok := digests[img]; ok && runningDigests[d] {
				inUse = true
				matchedVia = "digest match (different alias of a running image)"
			}
		}

		if inUse {
			if g.missCounts[img] > 0 {
				logging.Infof("angryduck-worker-gc: image=%s now in use again (%s), resetting miss count from %d to 0", img, matchedVia, g.missCounts[img])
			} else {
				logging.Debugf("angryduck-worker-gc: image=%s is running (%s), no action", img, matchedVia)
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
				imagesDeletedTotal.Inc(g.nodeID)
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

	logging.Infof("angryduck-worker-gc: tick done in %s: %d spared (running), %d spared (grace period), %d spared (excluded by config), %d tracked below threshold, %d removed",
		time.Since(tickStart).Round(time.Millisecond), sparedRunning, sparedGrace, sparedExcluded, trackedCount, removedCount)
}
