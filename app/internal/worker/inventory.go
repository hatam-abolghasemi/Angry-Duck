package worker

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// CachedRuntime shares one short-lived local-image inventory between the
// reporter, GC, and P2P source checks. The underlying runtime command is
// expensive relative to a map lookup, and all three consumers otherwise tend
// to ask the exact same question during a busy rollout.
type CachedRuntime struct {
	Runtime
	maxAge time.Duration

	mu        sync.Mutex
	refreshed time.Time
	refs      []string
	digests   map[string]string
}

func NewCachedRuntime(runtime Runtime, maxAge time.Duration) Runtime {
	if runtime == nil || maxAge <= 0 {
		return runtime
	}
	return &CachedRuntime{Runtime: runtime, maxAge: maxAge}
}

func (r *CachedRuntime) LocalImages() ([]string, map[string]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.refreshed.IsZero() && time.Since(r.refreshed) < r.maxAge {
		// Cached snapshots are immutable: refresh replaces the slice/map as a
		// whole rather than mutating either in place, so readers can use them
		// without allocating a defensive copy.
		return r.refs, r.digests, nil
	}

	refs, digests, err := r.Runtime.LocalImages()
	if err != nil {
		return nil, nil, err
	}
	r.refs = refs
	r.digests = digests
	r.refreshed = time.Now()
	return r.refs, r.digests, nil
}

func (r *CachedRuntime) invalidate() {
	r.mu.Lock()
	r.refreshed = time.Time{}
	r.refs = nil
	r.digests = nil
	r.mu.Unlock()
}

func (r *CachedRuntime) PullImage(image string) error {
	err := r.Runtime.PullImage(image)
	if err == nil {
		r.invalidate()
	}
	return err
}

func (r *CachedRuntime) RemoveImage(image string) error {
	err := r.Runtime.RemoveImage(image)
	if err == nil {
		r.invalidate()
	}
	return err
}

func (r *CachedRuntime) ExportImage(dst io.Writer, image string) error {
	if rt, ok := r.Runtime.(interface{ ExportImage(io.Writer, string) error }); ok {
		return rt.ExportImage(dst, image)
	}
	return fmt.Errorf("runtime does not support image export")
}

func (r *CachedRuntime) ImportImage(src io.Reader) error {
	if rt, ok := r.Runtime.(interface{ ImportImage(io.Reader) error }); ok {
		err := rt.ImportImage(src)
		if err == nil {
			r.invalidate()
		}
		return err
	}
	return fmt.Errorf("runtime does not support image import")
}
