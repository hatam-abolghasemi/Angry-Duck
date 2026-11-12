package worker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"

	"angryduck/internal/imageref"
	"angryduck/internal/logging"
	"angryduck/internal/model"
)

// Companion images: images a pod runs beside an app image (one of a repo
// CI preheats), such as exporters, proxies and agents, whatever registry
// they come from. The controller spreads them to every node; here, image
// cleanup keeps them: never removed for age, and under disk pressure only
// after every other unused image. See controller.Companions.
//
// The cleanup's own listing already says which containers share a pod, so
// telling companions apart costs no extra call: each sample groups the
// in-use containers by pod and, in every pod running an app image, takes
// the other images. A container is matched by every name its image has
// here, since kubelet often creates containers by image ID alone. Each new
// companion's size is asked of the runtime once. The set the controller
// sent is saved next to the cleanup's state, so a restarted worker keeps
// protecting it, and reported back, so a restarted controller learns its
// app repos again.

// companionWarmup is how long a freshly started worker holds back the age
// rule while it waits for the controller's companion set.
const companionWarmup = 5 * time.Minute

// gcCompanions is the cleanup's companion state. Fields set by the
// controller (app, keep, ready) and those of the last sample (local) are
// under ImageGC.mu; sizes and pub belong to the Tick goroutine.
type gcCompanions struct {
	pods    func() ([]PodImages, error)
	sizeOf  func(image string) (int64, error)
	onState func([]model.ImageRef, []string)
	started time.Time

	repos     map[string]bool // app repos
	keep      map[string]bool // target digests kept on every node
	ready     bool            // a ready set arrived, or was saved here
	waitNoted bool
	path      string   // where the set is saved ("" = memory only)
	held      []string // app repos, sorted: reported back

	local map[string]bool  // target digests running here beside an app image
	sizes map[string]int64 // target digest -> size, once known
	pub   map[string]int64 // what onState last got: digest -> size
	pubR  []string         // the app repos onState last got
}

// savedCompanions is the set as saved on the node.
type savedCompanions struct {
	Hash     string   `json:"hash"`
	Digests  []string `json:"digests"`
	AppRepos []string `json:"app_repos"`
}

// EnableCompanions makes the cleanup list in-use containers by pod with
// pods (replacing inUse), report the companion images running here with
// their sizes (sizeOf) to onState, and keep the cluster's companions (see
// SetCompanions). Call before Run.
func (g *ImageGC) EnableCompanions(pods func() ([]PodImages, error), sizeOf func(string) (int64, error), onState func([]model.ImageRef, []string)) {
	g.comp = &gcCompanions{pods: pods, sizeOf: sizeOf, onState: onState, started: time.Now(),
		sizes: map[string]int64{}}
	if g.cfg.StatePath != "" {
		g.comp.path = filepath.Join(filepath.Dir(g.cfg.StatePath), "companions.json")
		g.loadCompanions()
	}
	imagesDeletedTotal.Add(0, g.nodeID, "companion")
	gcCandidates.Set(0, g.nodeID, "companion")
}

// loadCompanions takes the set saved on the node, if any.
func (g *ImageGC) loadCompanions() {
	b, err := os.ReadFile(g.comp.path)
	if err != nil {
		return
	}
	var st savedCompanions
	if json.Unmarshal(b, &st) != nil {
		return
	}
	g.applyCompanionsLocked(st.Digests, st.AppRepos, true)
	logging.Infof("angryduck-worker[%s]: image cleanup: loaded %d companion image(s) and %d app repo(s) from %s", g.nodeID, len(st.Digests), len(st.AppRepos), g.comp.path)
}

// applyCompanionsLocked replaces the set. Caller holds g.mu (or is the
// only one touching g.comp yet).
func (g *ImageGC) applyCompanionsLocked(digests, repos []string, ready bool) {
	keep := make(map[string]bool, len(digests))
	for _, d := range digests {
		keep[d] = true
	}
	rs := make(map[string]bool, len(repos))
	held := make([]string, 0, len(repos))
	for _, r := range repos {
		if !rs[r] {
			rs[r] = true
			held = append(held, r)
		}
	}
	sort.Strings(held)
	g.comp.repos, g.comp.held = rs, held
	if ready {
		g.comp.keep, g.comp.ready = keep, true
	}
}

// SetCompanions takes the controller's companion set (from a report ack)
// and saves it on the node. A set that isn't ready yet (a controller that
// just started) is taken only by a worker holding none, for its app repos:
// one holding a set keeps it until the controller has heard from every node.
func (g *ImageGC) SetCompanions(set model.CompanionSet) {
	if g.comp == nil {
		return
	}
	g.mu.Lock()
	switch {
	case set.Ready:
		if !g.comp.ready {
			logging.Infof("angryduck-worker[%s]: image cleanup: keeping %d companion image(s) of the cluster; %d app repo(s)", g.nodeID, len(set.Digests), len(set.AppRepos))
		}
		g.applyCompanionsLocked(set.Digests, set.AppRepos, true)
	case len(g.comp.held) == 0:
		g.applyCompanionsLocked(nil, set.AppRepos, false)
		g.mu.Unlock()
		return
	default:
		g.mu.Unlock()
		return
	}
	path := g.comp.path
	g.mu.Unlock()
	if path == "" {
		return
	}
	b, _ := json.Marshal(savedCompanions{Hash: set.Hash, Digests: set.Digests, AppRepos: set.AppRepos})
	tmp := path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err == nil {
		if err := os.WriteFile(tmp, b, 0o644); err == nil {
			_ = os.Rename(tmp, path)
		}
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
// themselves. A container's image is found by its references (often a
// bare image ID) in targets, and judged by every name it has (groups).
func companionsOf(pods []PodImages, repos map[string]bool, targets map[string]string, groups map[string][]string) map[string]bool {
	if len(repos) == 0 {
		return nil
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
	isApp := func(refs []string, d string) bool {
		names := refs
		if d != "" {
			names = append(append([]string(nil), refs...), groups[d]...)
		}
		for _, n := range names {
			if repo := imageref.Repo(imageref.Normalize(n)); repo != "" && repos[repo] {
				return true
			}
		}
		return false
	}
	out := map[string]bool{}
	ds := make([]string, 0, 4)
	apps := make([]bool, 0, 4)
	for _, pod := range pods {
		ds, apps = ds[:0], apps[:0]
		hasApp := false
		for _, refs := range pod {
			d := target(refs)
			app := isApp(refs, d)
			ds, apps = append(ds, d), append(apps, app)
			hasApp = hasApp || app
		}
		if !hasApp {
			continue
		}
		for i, d := range ds {
			if !apps[i] && d != "" {
				out[d] = true
			}
		}
	}
	return out
}

// noteCompanions records the companions of the last sample. Caller holds
// g.mu.
func (g *ImageGC) noteCompanionsLocked(pods []PodImages, targets map[string]string, groups map[string][]string) {
	if g.comp == nil {
		return
	}
	g.comp.local = companionsOf(pods, g.comp.repos, targets, groups)
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
	local, held := g.comp.local, g.comp.held
	g.mu.Unlock()
	c := g.comp
	for d := range c.sizes {
		if _, ok := groups[d]; !ok {
			delete(c.sizes, d) // gone from the node
		}
	}
	refs := make([]model.ImageRef, 0, len(local))
	same := len(local) == len(c.pub) && equalStringSlices(held, c.pubR)
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
	c.pub, c.pubR = pub, held
	c.onState(refs, held)
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
