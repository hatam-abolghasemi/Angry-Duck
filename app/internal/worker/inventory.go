package worker

import (
	"strings"
	"sync"
	"time"
)

// Inventory is the one place this worker's view of local images lives.
//
// Before it existed the reporter (every 15s) and GC (every 60s) each ran
// the runtime's full image listing on their own timers — the same
// subprocess and the same JSON parse, twice, for the same answer. Now
// whichever of them runs first refreshes the listing and the other reuses
// it if it's young enough. The mirror never lists at all: it reads the
// digest index built from whatever listing already happened.
//
// Reusing a listing that is up to one report interval old is safe for GC:
// an image pulled after the listing simply isn't evaluated until the next
// one (it can't be wrongly removed if it isn't seen), and an image removed
// after it only produces a harmless "not found" from RemoveImage.
type Inventory struct {
	runtime Runtime

	fetchMu sync.Mutex // serializes refreshes so two callers never list at once

	mu       sync.RWMutex
	refs     []string
	digests  map[string]string // any alias -> runtime's canonical id (GC input)
	byDigest map[string]string // manifest digest -> exportable "repo@digest" ref
	added    map[string]time.Time
	bad      map[string]struct{} // manifest digests whose export failed
	listedAt time.Time
	haveData bool
}

// NewInventory builds an empty inventory backed by rt.
func NewInventory(rt Runtime) *Inventory {
	return &Inventory{
		runtime:  rt,
		byDigest: make(map[string]string),
		added:    make(map[string]time.Time),
		bad:      make(map[string]struct{}),
	}
}

// Get returns the local image listing, reusing the cached one if it is no
// older than maxAge, otherwise refreshing it with one runtime call.
func (inv *Inventory) Get(maxAge time.Duration) ([]string, map[string]string, error) {
	if refs, digests, ok := inv.cached(maxAge); ok {
		return refs, digests, nil
	}
	inv.fetchMu.Lock()
	defer inv.fetchMu.Unlock()
	// Another caller may have refreshed while we waited for fetchMu.
	if refs, digests, ok := inv.cached(maxAge); ok {
		return refs, digests, nil
	}
	started := time.Now()
	refs, digests, err := inv.runtime.LocalImages()
	if err != nil {
		return nil, nil, err
	}
	inv.replace(refs, digests, started)
	return refs, digests, nil
}

func (inv *Inventory) cached(maxAge time.Duration) ([]string, map[string]string, bool) {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	if !inv.haveData || time.Since(inv.listedAt) > maxAge {
		return nil, nil, false
	}
	return inv.refs, inv.digests, true
}

// replace installs a fresh listing and rebuilds the manifest-digest index.
// Every runtime backend reports digest-pinned aliases ("repo@sha256:D")
// as keys of its digest map — crictl via repoDigests, ctr as REF rows —
// and D there is the manifest (or index) digest, which is exactly what
// containerd asks a mirror for. Deriving the index from those keys keeps
// it backend-agnostic and costs no extra call.
//
// Entries Add()ed after the listing started survive: an import that
// finished while the listing subprocess was running must not be dropped
// by a listing that couldn't have seen it.
func (inv *Inventory) replace(refs []string, digests map[string]string, started time.Time) {
	byDigest := make(map[string]string, len(digests)/2)
	for key := range digests {
		if at := strings.Index(key, "@sha256:"); at > 0 {
			byDigest[key[at+1:]] = key
		}
	}
	inv.mu.Lock()
	defer inv.mu.Unlock()
	for d, t := range inv.added {
		if t.After(started) {
			if _, ok := byDigest[d]; !ok {
				byDigest[d] = inv.byDigest[d]
			}
		} else {
			delete(inv.added, d)
		}
	}
	for d := range inv.bad {
		if _, ok := byDigest[d]; !ok {
			delete(inv.bad, d) // image gone; a re-pull gets a clean slate
		}
	}
	inv.refs, inv.digests, inv.byDigest = refs, digests, byDigest
	inv.listedAt, inv.haveData = started, true
}

// Add records an image that just landed locally (a peer import) without
// waiting for the next listing.
func (inv *Inventory) Add(digest, ref string) {
	inv.mu.Lock()
	inv.byDigest[digest] = ref
	inv.added[digest] = time.Now()
	delete(inv.bad, digest)
	inv.mu.Unlock()
}

// Lookup returns the local ref to export for a manifest digest.
func (inv *Inventory) Lookup(digest string) (string, bool) {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	if _, bad := inv.bad[digest]; bad {
		return "", false
	}
	ref, ok := inv.byDigest[digest]
	return ref, ok
}

// Has reports whether a manifest digest is present locally.
func (inv *Inventory) Has(digest string) bool {
	_, ok := inv.Lookup(digest)
	return ok
}

// MarkUnexportable stops this node advertising a digest whose export just
// failed (typically content pruned by discard_unpacked_layers before that
// setting was fixed). It is cleared when the image leaves the node.
func (inv *Inventory) MarkUnexportable(digest string) {
	inv.mu.Lock()
	inv.bad[digest] = struct{}{}
	inv.mu.Unlock()
}

// Tags returns every local tag-form reference mapped to its manifest
// digest, derived from the same cached listing GC and the mirror already
// use — no extra runtime call. This is what the report sends so the
// controller can answer /resolve (see internal/controller's
// Registry.ResolveTag) for a pod stuck in ImagePullBackOff on some other
// node, when origin itself can't resolve the tag.
//
// The join is: find every digest-pinned alias ("repo@sha256:D") to build
// canonical-id -> manifest-digest, then look up each tag-form alias's
// canonical id in that map. digests' values are the runtime's own
// canonical id (crictl's image "id", or ctr's DIGEST column) — NOT
// itself the manifest digest crictl's id can be a config digest rather
// than a manifest digest — so this must go through the digest-pinned
// alias the same way byDigest does in replace(), rather than trusting
// the raw value directly.
func (inv *Inventory) Tags() map[string]string {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	if !inv.haveData || len(inv.digests) == 0 {
		return nil
	}
	idToManifest := make(map[string]string, len(inv.digests))
	for alias, id := range inv.digests {
		if at := strings.Index(alias, "@sha256:"); at > 0 {
			idToManifest[id] = alias[at+1:]
		}
	}
	tags := make(map[string]string)
	for alias, id := range inv.digests {
		if strings.HasPrefix(alias, "sha256:") || strings.Contains(alias, "@sha256:") {
			continue // bare digest or digest-pinned alias, not a tag
		}
		if d, ok := idToManifest[id]; ok {
			tags[alias] = d
		}
	}
	return tags
}

// Digests returns every advertisable manifest digest, for the report.
func (inv *Inventory) Digests() []string {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	out := make([]string, 0, len(inv.byDigest))
	for d := range inv.byDigest {
		if _, bad := inv.bad[d]; !bad {
			out = append(out, d)
		}
	}
	return out
}
