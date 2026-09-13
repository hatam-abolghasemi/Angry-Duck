package worker

import (
	"context"
	"fmt"
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

// imagesDeletedBytesTotal tracks how much disk space GC has actually
// freed on this worker. crictl reports each image's size exactly, since
// it's already part of the same JSON LocalImages() parses; ctr and docker
// only expose a human-rounded string in their bulk listing ("14.7 MiB",
// "5.58MB"), so on those two backends this is a best-effort
// reconstruction of that rounded value, not the original exact size — see
// parseApproxBytes. When the runtime couldn't report a size at all for an
// image, the deletion still counts against imagesDeletedTotal but adds
// nothing here — an undercount is preferable to a fabricated number.
var imagesDeletedBytesTotal = metrics.NewCounterVec(
	"angryduck_worker_images_deleted_byte_size_total",
	"Total bytes freed by images actually removed by GC on this worker (excludes dry-run; exact on crictl, approximate on ctr/docker).",
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
	logging.Infof("angryduck-worker-gc: cleanup started — checking every %s, and removing an image once it has been unused for %d check(s) in a row (dry_run=%v: when true, only logs what it would remove, without deleting anything)", g.interval, g.missThreshold, g.dryRun)
	for {
		select {
		case <-ctx.Done():
			logging.Infof("angryduck-worker-gc: cleanup stopped")
			return
		case <-ticker.C:
			g.tick()
		}
	}
}

func (g *GC) tick() {
	tickStart := time.Now()

	if pruned := g.puller.PruneExpired(); pruned > 0 {
		logging.Debugf("angryduck-worker-gc: %d image(s) aged out of their post-pull protection window", pruned)
	}

	// One listing for both the local ref list and the digest map, and
	// usually not even a fresh one: the reporter refreshes the shared
	// inventory every REPORT_INTERVAL_S, so GC reuses that listing rather
	// than running `crictl images -o json` a second time for the same
	// answer. Only if the reporter hasn't listed recently (node-exporter
	// down, say) does GC list for itself.
	local, digests, sizes, err := g.inventory.Get(g.listMaxAge)
	if err != nil {
		logging.Errorf("angryduck-worker-gc: could not get the list of images on this node, skipping this check: %v", err)
		return
	}
	if digests == nil {
		// Not fatal: fall back to raw-string matching only. Log loudly
		// since this silently re-exposes the alias-mismatch bug — better
		// to know GC is running degraded than to wonder why.
		logging.Errorf("angryduck-worker-gc: could not match images by their actual content this time, falling back to a less reliable name-based check — an image could be wrongly treated as unused if its running container refers to it by a different name")
		digests = map[string]string{}
	}
	running, err := g.runtime.ListRunningImages()
	if err != nil {
		logging.Errorf("angryduck-worker-gc: could not get the list of currently running containers, skipping this check: %v", err)
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

	logging.Debugf("angryduck-worker-gc: checking this node's images — %d present, %d in use by running containers, %d already flagged as unused from an earlier check",
		len(local), len(runningSet), len(g.missCounts))

	seen := make(map[string]bool, len(local))
	removedCount, sparedRunning, sparedGrace, sparedExcluded, trackedCount, newlyTracked := 0, 0, 0, 0, 0, 0
	for _, img := range local {
		if img == "" {
			continue
		}
		seen[img] = true

		if g.isExcluded(img) {
			if g.missCounts[img] > 0 {
				logging.Infof("angryduck-worker-gc: image=%s is on the never-remove list, no longer counting it as unused (had been unused for %d check(s) in a row)", img, g.missCounts[img])
			} else {
				logging.Debugf("angryduck-worker-gc: image=%s is on the never-remove list, skipping", img)
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
				logging.Infof("angryduck-worker-gc: image=%s is being used again, no longer counts as unused (had been unused for %d check(s) in a row; matched via %s)", img, g.missCounts[img], matchedVia)
			} else {
				logging.Debugf("angryduck-worker-gc: image=%s is currently in use (%s), nothing to do", img, matchedVia)
			}
			delete(g.missCounts, img)
			sparedRunning++
			continue
		}

		if g.puller.InGracePeriod(img) {
			logging.Debugf("angryduck-worker-gc: image=%s isn't running, but it was pulled very recently — giving it more time before treating it as unused", img)
			sparedGrace++
			continue
		}

		g.missCounts[img]++
		miss := g.missCounts[img]

		if miss >= g.missThreshold {
			freed, knownSize := sizes[img]
			if g.dryRun {
				if knownSize {
					logging.Warnf("angryduck-worker-gc: [DRY RUN] image=%s has been unused for %d check(s) in a row and would be removed now, freeing an estimated %d bytes — nothing was actually deleted, because dry_run is enabled",
						img, miss, freed)
				} else {
					logging.Warnf("angryduck-worker-gc: [DRY RUN] image=%s has been unused for %d check(s) in a row and would be removed now — nothing was actually deleted, because dry_run is enabled",
						img, miss)
				}
			} else {
				logging.Warnf("angryduck-worker-gc: image=%s has been unused for %d check(s) in a row, removing it now", img, miss)
				if err := g.runtime.RemoveImage(img); err != nil {
					logging.Errorf("angryduck-worker-gc: tried to remove image=%s but it failed: %v", img, err)
					continue
				}
				if knownSize {
					logging.Infof("angryduck-worker-gc: image=%s removed successfully, freed %d bytes", img, freed)
					imagesDeletedBytesTotal.Add(freed, g.nodeID)
				} else {
					logging.Infof("angryduck-worker-gc: image=%s removed successfully (size unknown)", img)
				}
				imagesDeletedTotal.Inc(g.nodeID)
			}
			delete(g.missCounts, img)
			removedCount++
		} else {
			// Only the FIRST tick an image becomes unused is new
			// information worth INFO — every subsequent tick just
			// repeats the same fact with the count one higher. Left at
			// INFO, an image that churns every few minutes (a periodic
			// job's image, say) reprints an identical-shaped line on
			// every node, every tick, for the entire threshold window —
			// exactly the noise a live troubleshooting session doesn't
			// want to scroll through. DEBUG still has the full count-up
			// for whenever that detail actually matters.
			if miss == 1 {
				logging.Infof("angryduck-worker-gc: image=%s is now unused — it will be removed if that continues for %d check(s) in a row", img, g.missThreshold)
				newlyTracked++
			} else {
				logging.Debugf("angryduck-worker-gc: image=%s hasn't been used for %d check(s) in a row — it will be removed once that reaches %d",
					img, miss, g.missThreshold)
			}
			trackedCount++
		}
	}

	// Drop counters for images that no longer exist locally at all.
	for img := range g.missCounts {
		if !seen[img] {
			logging.Debugf("angryduck-worker-gc: image=%s is gone from this node, no longer tracking it", img)
			delete(g.missCounts, img)
		}
	}

	// Same reasoning as the per-image line above: a tick where nothing
	// changed (nothing new flagged, nothing removed) is exactly what a
	// healthy, quiet node looks like almost all the time, and repeating
	// that fact at INFO every interval, on every node, adds up to a wall
	// of text with no decisions in it. Only log at INFO when this tick
	// actually did something; otherwise the identical line is still
	// available at DEBUG.
	summary := fmt.Sprintf("finished checking images in %s — %d in use, %d recently pulled (protected for now), %d on the never-remove list, %d unused but not yet due for removal, %d removed",
		time.Since(tickStart).Round(time.Millisecond), sparedRunning, sparedGrace, sparedExcluded, trackedCount, removedCount)
	if removedCount > 0 || newlyTracked > 0 {
		logging.Infof("angryduck-worker-gc: %s", summary)
	} else {
		logging.Debugf("angryduck-worker-gc: %s", summary)
	}
}
