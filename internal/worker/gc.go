package worker

import (
	"context"
	"log"
	"time"
)

// GC periodically compares locally-present images against currently
// running containers, and removes any image that has gone unused for
// missThreshold consecutive checks — unless that image is still within its
// post-pull-order grace period.
type GC struct {
	runtime       Runtime
	puller        *Puller
	interval      time.Duration
	missThreshold int
	missCounts    map[string]int
}

// NewGC builds a GC loop.
func NewGC(runtime Runtime, puller *Puller, interval time.Duration, missThreshold int) *GC {
	return &GC{
		runtime:       runtime,
		puller:        puller,
		interval:      interval,
		missThreshold: missThreshold,
		missCounts:    make(map[string]int),
	}
}

// Run blocks, running a GC pass on every tick until ctx is done.
func (g *GC) Run(ctx context.Context) {
	ticker := time.NewTicker(g.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.tick()
		}
	}
}

func (g *GC) tick() {
	local, err := g.runtime.ListLocalImages()
	if err != nil {
		log.Printf("angryduck-worker-gc: failed to list local images: %v", err)
		return
	}
	running, err := g.runtime.ListRunningImages()
	if err != nil {
		log.Printf("angryduck-worker-gc: failed to list running images: %v", err)
		return
	}
	runningSet := make(map[string]bool, len(running))
	for _, img := range running {
		runningSet[img] = true
	}

	seen := make(map[string]bool, len(local))
	for _, img := range local {
		seen[img] = true

		if runningSet[img] {
			// In use: reset its miss counter.
			delete(g.missCounts, img)
			continue
		}

		if g.puller.InGracePeriod(img) {
			// Ordered recently, hasn't been picked up by a scheduled pod
			// yet. Don't let it accumulate misses while protected.
			continue
		}

		g.missCounts[img]++
		if g.missCounts[img] >= g.missThreshold {
			log.Printf("angryduck-worker-gc: removing unused image=%s (unused for %d consecutive checks)", img, g.missCounts[img])
			if err := g.runtime.RemoveImage(img); err != nil {
				log.Printf("angryduck-worker-gc: failed to remove image=%s: %v", img, err)
				continue
			}
			delete(g.missCounts, img)
		}
	}

	// Drop counters for images that no longer exist locally at all.
	for img := range g.missCounts {
		if !seen[img] {
			delete(g.missCounts, img)
		}
	}
}
