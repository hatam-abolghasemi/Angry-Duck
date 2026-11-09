package controller

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"angryduck/internal/imageref"
	"angryduck/internal/model"
	"angryduck/internal/sharedtoken"
)

// tagLookup finds which digest the newest copy of a tag on the nodes
// points at, for mirrors whose registry couldn't say (down, refusing the
// credentials, or no longer knowing the tag). It asks the holders
// themselves, once per tag for everyone asking within ttl, so a
// rollout during a registry outage costs each holder one small request
// per tag, not one per pulling node.
type tagLookup struct {
	token   string
	ttl     time.Duration // how long a tag's answer is reused
	timeout time.Duration // how long the holders get to answer
	client  *http.Client

	mu    sync.Mutex
	cache map[string]*tagEntry
}

type tagEntry struct {
	digest string
	at     time.Time // zero while the holders are being asked
	done   chan struct{}
}

func newTagLookup(token string, ttl, timeout time.Duration) *tagLookup {
	return &tagLookup{
		token:   token,
		ttl:     ttl,
		timeout: timeout,
		// No keep-alive: the holders are asked only while a registry
		// can't answer, and idle connections to every node would stay.
		client: &http.Client{Transport: &http.Transport{
			Proxy:             nil,
			DialContext:       (&net.Dialer{Timeout: 3 * time.Second}).DialContext,
			DisableKeepAlives: true,
		}},
		cache: map[string]*tagEntry{},
	}
}

// newest returns the digest image points at on whichever of holders
// changed it last, or "" when none answered.
func (t *tagLookup) newest(ctx context.Context, image string, holders []model.Holder) string {
	t.mu.Lock()
	e, ok := t.cache[image]
	if !ok || (!e.at.IsZero() && time.Since(e.at) >= t.ttl) {
		for k, old := range t.cache {
			if !old.at.IsZero() && time.Since(old.at) >= t.ttl {
				delete(t.cache, k)
			}
		}
		e = &tagEntry{done: make(chan struct{})}
		t.cache[image] = e
		t.mu.Unlock()
		digest := t.ask(image, holders)
		t.mu.Lock()
		e.digest, e.at = digest, time.Now()
		close(e.done)
	}
	t.mu.Unlock()
	select {
	case <-e.done:
		return e.digest
	case <-ctx.Done():
		return ""
	}
}

// ask asks every holder at once.
func (t *tagLookup) ask(image string, holders []model.Holder) string {
	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()
	answers := make(chan model.TagTarget, len(holders))
	for _, h := range holders {
		go func() {
			var ans model.TagTarget
			defer func() { answers <- ans }()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+h.Address+"/mirror/tag?image="+url.QueryEscape(image), nil)
			if err != nil {
				return
			}
			sharedtoken.Set(req, t.token)
			resp, err := t.client.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				_ = json.NewDecoder(io.LimitReader(resp.Body, 4<<10)).Decode(&ans)
			}
		}()
	}
	var best model.TagTarget
	for range holders {
		if a := <-answers; a.Digest != "" && a.Updated.After(best.Updated) {
			best = a
		}
	}
	return best.Digest
}

// EnableTagLookup mounts /images/tag behind the shared token (call after
// SetToken). The newest digest found for a tag is reused for ttl; holders
// get timeout to answer, which should be shorter than the mirrors' wait
// for the controller.
func (s *Server) EnableTagLookup(ttl, timeout time.Duration) {
	s.tags = newTagLookup(s.auth.Token, ttl, timeout)
	s.mux.HandleFunc("/images/tag", sharedtoken.Require(s.auth.Token, s.handleTag))
}

// handleTag answers a mirror resolving a tag: how many fresh nodes hold
// it and, with newest=1, the digest of the newest copy.
func (s *Server) handleTag(w http.ResponseWriter, r *http.Request) {
	image := imageref.Normalize(r.URL.Query().Get("image"))
	if image == "" {
		http.Error(w, "image required", http.StatusBadRequest)
		return
	}
	holders := s.registry.ImageHolderList(image)
	ans := model.TagHolders{Holders: len(holders)}
	if len(holders) > 0 && r.URL.Query().Get("newest") == "1" {
		ans.Digest = s.tags.newest(r.Context(), image, holders)
	}
	writeJSON(w, http.StatusOK, ans)
}
