package controller

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"angryduck/internal/model"
)

// tagWorker serves /mirror/tag like a worker whose tag points at digest
// since updated, counting requests.
func tagWorker(t *testing.T, digest string, updated time.Time, hits *atomic.Int32) string {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get("Authorization") != "Bearer "+rescueToken || r.URL.Path != "/mirror/tag" || r.URL.Query().Get("image") != img {
			http.Error(w, "no", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(model.TagTarget{Digest: digest, Updated: updated})
	}))
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

// /images/tag counts the fresh nodes holding a tag and, when asked, finds
// the digest the newest copy points at, asking each holder once for all
// the mirrors asking at the same time.
func TestImagesTagFindsTheNewestCopyOnce(t *testing.T) {
	reg := NewRegistry(time.Minute, time.Minute)
	srv := NewServer(reg, NewRanker(reg, 1, time.Hour, nil, false, false))
	srv.SetToken(rescueToken)
	srv.EnableTagLookup(30*time.Second, 2*time.Second)
	var hits atomic.Int32
	now := time.Now()
	old, fresh := "sha256:"+strings.Repeat("a", 64), "sha256:"+strings.Repeat("b", 64)
	for n, addr := range map[string]string{
		"w0": tagWorker(t, old, now.Add(-time.Hour), &hits),
		"w1": tagWorker(t, fresh, now, &hits),
		"w2": "127.0.0.1:1", // holds it, doesn't answer
	} {
		rep := model.WorkerReport{NodeID: n, Address: addr, Images: []string{img}, Timestamp: now}
		body, _ := json.Marshal(rep)
		req := httptest.NewRequest(http.MethodPost, "/report", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+rescueToken)
		srv.Handler().ServeHTTP(httptest.NewRecorder(), req)
	}
	ask := func(query string) model.TagHolders {
		req := httptest.NewRequest(http.MethodGet, "/images/tag?"+query, nil)
		req.Header.Set("Authorization", "Bearer "+rescueToken)
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		var ans model.TagHolders
		_ = json.NewDecoder(rec.Body).Decode(&ans)
		return ans
	}

	if ans := ask("image=" + img); ans.Holders != 3 || ans.Digest != "" || hits.Load() != 0 {
		t.Fatalf("count only: %+v after %d worker requests", ans, hits.Load())
	}
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			if ans := ask("image=" + img + "&newest=1"); ans.Digest != fresh {
				t.Errorf("newest = %+v, want %s", ans, fresh)
			}
		})
	}
	wg.Wait()
	if n := hits.Load(); n != 2 {
		t.Fatalf("%d worker requests for 10 mirrors asking at once, want one per answering holder", n)
	}
	if ans := ask("image=registry.example.com/team/other:1&newest=1"); ans.Holders != 0 || ans.Digest != "" {
		t.Fatalf("a tag nobody holds: %+v", ans)
	}
}
