package kube

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"angryduck/internal/logging"
)

// watchTimeout is how long one watch request may run; the API server ends
// it then, and the watch resumes from the last version seen.
const watchTimeout = 5 * time.Minute

// PodWatch keeps the pods matching a field selector, following a watch
// instead of listing them on a timer, and calls changed after every change.
// It lists once, then watches from that list's version; the watch resumes
// where it stopped, and a full list happens again only when the API server
// no longer has that version (410), after an error, or every resync as a
// safety net against a missed event.
//
// When the service account may list but not watch pods (an older RBAC
// rule), it lists every fallback instead, as before.
type PodWatch struct {
	c        *Client
	stream   *http.Client
	selector string
	resync   time.Duration
	fallback time.Duration
	changed  func()

	mu     sync.Mutex
	pods   map[string]Pod
	synced bool
}

// NewPodWatch builds a watch of pods matching fieldSelector.
func NewPodWatch(c *Client, fieldSelector string, resync, fallback time.Duration, changed func()) *PodWatch {
	// A watch is one long response: no client-wide timeout, the request's
	// context bounds it instead.
	stream := &http.Client{Transport: c.http.Transport}
	return &PodWatch{c: c, stream: stream, selector: fieldSelector, resync: resync, fallback: fallback, changed: changed, pods: map[string]Pod{}}
}

// ListPods returns the watched pods. The selector is fixed at NewPodWatch;
// asking for another one is an error, as is asking before the first list.
func (w *PodWatch) ListPods(_ context.Context, fieldSelector string) ([]Pod, error) {
	if fieldSelector != w.selector {
		return nil, fmt.Errorf("pod watch follows %q, not %q", w.selector, fieldSelector)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.synced {
		return nil, errors.New("pod watch hasn't listed pods yet")
	}
	out := make([]Pod, 0, len(w.pods))
	for _, p := range w.pods {
		out = append(out, p)
	}
	return out, nil
}

func podKey(p *Pod) string { return p.Metadata.Namespace + "/" + p.Metadata.Name }

// Run follows the pods until ctx is done.
func (w *PodWatch) Run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		rv, err := w.list(ctx)
		if err != nil {
			logging.Warnf("angryduck-controller: listing pods (%s) failed, retrying in %s: %v", w.selector, backoff, err)
			if !sleep(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, time.Minute)
			continue
		}
		backoff = time.Second
		deadline := time.Now().Add(w.resync)
		for ctx.Err() == nil && time.Now().Before(deadline) {
			next, err := w.watch(ctx, rv, time.Until(deadline))
			if err == nil {
				rv = next // ended on its timeout: resume where it stopped
				continue
			}
			var se *StatusError
			if errors.As(err, &se) && se.Code == http.StatusForbidden {
				logging.Warnf("angryduck-controller: not allowed to watch pods, listing them every %s instead (add \"watch\" to the controller's ClusterRole for pods): %v", w.fallback, err)
				w.poll(ctx)
				return
			}
			if !errors.Is(err, errGone) {
				logging.Warnf("angryduck-controller: pod watch (%s) broke, listing again: %v", w.selector, err)
				if !sleep(ctx, time.Second) {
					return
				}
			}
			break // relist
		}
	}
}

// list replaces the cache with a fresh list and returns its version.
func (w *PodWatch) list(ctx context.Context) (string, error) {
	lctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	pods, rv, err := w.c.listPods(lctx, w.selector)
	if err != nil {
		return "", err
	}
	m := make(map[string]Pod, len(pods))
	for i := range pods {
		m[podKey(&pods[i])] = pods[i]
	}
	w.mu.Lock()
	w.pods, w.synced = m, true
	w.mu.Unlock()
	w.changed()
	return rv, nil
}

// poll is the fallback without watch permission.
func (w *PodWatch) poll(ctx context.Context) {
	for sleep(ctx, w.fallback) {
		if _, err := w.list(ctx); err != nil {
			logging.Warnf("angryduck-controller: listing pods (%s) failed: %v", w.selector, err)
		}
	}
}

var errGone = errors.New("resource version too old")

type watchEvent struct {
	Type   string          `json:"type"`
	Object json.RawMessage `json:"object"`
}

// watch follows one watch request from rv for up to max and returns the
// last version seen. A nil error means the server ended it on its timeout.
func (w *PodWatch) watch(ctx context.Context, rv string, max time.Duration) (string, error) {
	secs := int(min(max, watchTimeout) / time.Second)
	if secs < 1 {
		return rv, nil
	}
	q := url.Values{}
	q.Set("watch", "1")
	q.Set("fieldSelector", w.selector)
	q.Set("resourceVersion", rv)
	q.Set("allowWatchBookmarks", "true")
	q.Set("timeoutSeconds", fmt.Sprint(secs))
	// The server ends it after secs; the margin only catches a dead stream.
	wctx, cancel := context.WithTimeout(ctx, time.Duration(secs)*time.Second+time.Minute)
	defer cancel()
	resp, err := w.c.open(wctx, w.stream, "/api/v1/pods?"+q.Encode())
	if err != nil {
		return rv, err
	}
	defer resp.Body.Close()
	dec := json.NewDecoder(resp.Body)
	for {
		var ev watchEvent
		if err := dec.Decode(&ev); err != nil {
			if ctx.Err() == nil && wctx.Err() == nil && errors.Is(err, io.EOF) {
				return rv, nil
			}
			return rv, err
		}
		if ev.Type == "ERROR" {
			var st struct {
				Code int `json:"code"`
			}
			_ = json.Unmarshal(ev.Object, &st)
			if st.Code == http.StatusGone {
				return rv, errGone
			}
			return rv, fmt.Errorf("watch error %s", ev.Object)
		}
		var p Pod
		if err := json.Unmarshal(ev.Object, &p); err != nil {
			return rv, err
		}
		if p.Metadata.ResourceVersion != "" {
			rv = p.Metadata.ResourceVersion
		}
		w.mu.Lock()
		switch ev.Type {
		case "ADDED", "MODIFIED":
			w.pods[podKey(&p)] = p
		case "DELETED": // gone, or no longer matching the selector
			delete(w.pods, podKey(&p))
		default: // BOOKMARK: only the version moved
			w.mu.Unlock()
			continue
		}
		w.mu.Unlock()
		w.changed()
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
