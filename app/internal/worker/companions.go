package worker

import (
	"sort"
	"time"

	"angryduck/internal/imageref"
	"angryduck/internal/logging"
	"angryduck/internal/model"
)

// Companion images: images a pod runs beside an app image (one from an
// app registry), such as exporters, proxies and agents. The controller
// spreads them to every node; here, image cleanup keeps them: never
// removed for age, and under disk pressure only after every other unused
// image. See controller.Companions.
//
// The cleanup's own listing already says which containers share a pod, so
// telling companions apart costs no extra call: each sample groups the
// in-use containers by pod and, in every pod running an app image, takes
// the other images. Each new one's size is asked of the runtime once.

// companionWarmup is how long a freshly started worker holds back the age
// rule while it waits for the controller's companion set.
const companionWarmup = 5 * time.Minute

// gcCompanions is the cleanup's companion state. Fields set by the
// controller (app, keep, ready) and those of the last sample (local) are
// under ImageGC.mu; sizes and pub belong to the Tick goroutine.
type gcCompanions struct {
	pods    func() ([]PodImages, error)
	sizeOf  func(image string) (int64, error)
	onState func([]model.ImageRef)
	started time.Time

	app       map[string]bool // app registry hosts
	keep      map[string]bool // target digests kept on every node
	ready     bool            // the controller's set arrived
	waitNoted bool

	local map[string]bool  // target digests running here beside an app image
	sizes map[string]int64 // target digest -> size, once known
	pub   map[string]int64 // what onState last got: digest -> size
}

// EnableCompanions makes the cleanup list in-use containers by pod with
// pods (replacing inUse), report the companion images running here with
// their sizes (sizeOf) to onState, and keep the cluster's companions (see
// SetCompanions). Call before Run.
func (g *ImageGC) EnableCompanions(pods func() ([]PodImages, error), sizeOf func(string) (int64, error), onState func([]model.ImageRef)) {
	g.comp = &gcCompanions{pods: pods, sizeOf: sizeOf, onState: onState, started: time.Now(),
		sizes: map[string]int64{}}
	imagesDeletedTotal.Add(0, g.nodeID, "companion")
	gcCandidates.Set(0, g.nodeID, "companion")
}

// SetCompanions takes the controller's companion set (from a report ack).
// A set that isn't ready only updates the app registries: the digests
// held stay until the controller has heard from every node.
func (g *ImageGC) SetCompanions(set model.CompanionSet) {
	if g.comp == nil {
		return
	}
	app := make(map[string]bool, len(set.AppRegistries))
	for _, h := range set.AppRegistries {
		app[h] = true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.comp.app = app
	if set.Ready {
		keep := make(map[string]bool, len(set.Digests))
		for _, d := range set.Digests {
			keep[d] = true
		}
		if !g.comp.ready {
			logging.Infof("angryduck-worker[%s]: image cleanup: keeping %d companion image(s) of the cluster; app registries %v", g.nodeID, len(keep), set.AppRegistries)
		}
		g.comp.keep, g.comp.ready = keep, true
	}
}

// listInUse is the in-use listing: by pod when companions are on.
func (g *ImageGC) listInUse() (used []string, pods []PodImages, err error) {
	if g.comp == nil {
		used, err = g.inUse()
		return used, nil, err
	}
	pods, err = g.comp.pods()
	if err != nil {
		return nil, nil, err
	}
	for _, pod := range pods {
		for _, refs := range pod {
			used = append(used, refs...)
		}
	}
	return used, pods, nil
}

// companionsOf returns the target digests of the images that run, in any
// pod, beside a container of an app image, and aren't app images
// themselves.
func companionsOf(pods []PodImages, app map[string]bool, targets map[string]string) map[string]bool {
	if len(app) == 0 {
		return nil
	}
	isApp := func(refs []string) bool {
		for _, r := range refs {
			if h := imageref.RegistryHost(imageref.Normalize(r)); h != "" && app[h] {
				return true
			}
		}
		return false
	}
	target := func(refs []string) string {
		for _, r := range refs {
			if d, ok := targets[r]; ok {
				return d
			}
			if d, ok := targets[imageref.Normalize(r)]; ok {
				return d
			}
		}
		return ""
	}
	out := map[string]bool{}
	for _, pod := range pods {
		hasApp := false
		for _, refs := range pod {
			if isApp(refs) {
				hasApp = true
				break
			}
		}
		if !hasApp {
			continue
		}
		for _, refs := range pod {
			if isApp(refs) {
				continue
			}
			if d := target(refs); d != "" {
				out[d] = true
			}
		}
	}
	return out
}

// noteCompanions records the companions of the last sample. Caller holds
// g.mu.
func (g *ImageGC) noteCompanionsLocked(pods []PodImages, targets map[string]string) {
	if g.comp == nil {
		return
	}
	g.comp.local = companionsOf(pods, g.comp.app, targets)
}

// isCompanion reports whether d is kept as a companion. Caller holds g.mu.
func (g *ImageGC) isCompanionLocked(d string) bool {
	return g.comp != nil && g.comp.keep[d]
}

// holdAgeRule reports whether the age rule waits for the controller's
// companion set (a fresh worker), logging it once.
func (g *ImageGC) holdAgeRule(now time.Time) bool {
	if g.comp == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.comp.ready || now.Sub(g.comp.started) >= companionWarmup {
		return false
	}
	if !g.comp.waitNoted {
		g.comp.waitNoted = true
		logging.Infof("angryduck-worker[%s]: image cleanup: waiting for the controller's companion images before removing images for age (at most %s)", g.nodeID, companionWarmup)
	}
	return true
}

// publishCompanions hands onState the companions running here, with their
// sizes, when they changed. Sizes are asked of the runtime once per image.
func (g *ImageGC) publishCompanions(groups map[string][]string) {
	if g.comp == nil || g.comp.onState == nil {
		return
	}
	g.mu.Lock()
	local := g.comp.local
	g.mu.Unlock()
	c := g.comp
	for d := range c.sizes {
		if _, ok := groups[d]; !ok {
			delete(c.sizes, d) // gone from the node
		}
	}
	refs := make([]model.ImageRef, 0, len(local))
	same := len(local) == len(c.pub)
	for d := range local {
		name := displayName(groups[d])
		size, ok := c.sizes[d]
		if !ok && c.sizeOf != nil {
			if s, err := c.sizeOf(name); err == nil && s > 0 {
				size, c.sizes[d] = s, s
			} else if err != nil {
				logging.Debugf("angryduck-worker[%s]: image cleanup: size of companion %s: %v", g.nodeID, name, err)
			}
		}
		if p, ok := c.pub[d]; !ok || p != size {
			same = false
		}
		refs = append(refs, model.ImageRef{Digest: d, Name: name, Size: size})
	}
	if same && c.pub != nil {
		return
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].Digest < refs[j].Digest })
	pub := make(map[string]int64, len(refs))
	for _, r := range refs {
		pub[r.Digest] = r.Size
	}
	c.pub = pub
	c.onState(refs)
}
