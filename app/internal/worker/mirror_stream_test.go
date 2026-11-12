package worker

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"angryduck/internal/blobship"
	"angryduck/internal/metrics"
	"angryduck/internal/model"
)

// A holder still receiving the blob (Arriving) is asked at /spread/content,
// which streams the bytes as they land, and the request counts as a stream.
// Holders come first; manifests never go to an arriving node.
func TestMirrorStreamsFromANodeStillReceiving(t *testing.T) {
	const layer = "still arriving"
	d := blobship.NewMemStore().Put([]byte(layer))
	var asked []string
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked = append(asked, r.URL.Path)
		if r.Header.Get("Authorization") != "Bearer "+testToken {
			http.Error(w, "no token", http.StatusUnauthorized)
			return
		}
		if r.URL.Path != "/spread/content/"+d {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Length", "14")
		w.Header().Set(StreamHeader, "1")
		_, _ = io.WriteString(w, layer)
	}))
	defer peer.Close()
	addr := strings.TrimPrefix(peer.URL, "http://")
	holders := []model.Holder{{NodeID: "b", Address: addr, Arriving: true}}
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(holders)
	}))
	defer controller.Close()
	srv := httptest.NewServer(NewMirror(blobship.NewMemStore(), "stream-a", testToken, controller.URL).Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v2/team/app/blobs/" + d)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != layer {
		t.Fatalf("status %d body %q, want the blob streamed from the arriving node", resp.StatusCode, body)
	}
	if len(asked) != 1 || asked[0] != "/spread/content/"+d {
		t.Fatalf("asked %v, want /spread/content only", asked)
	}
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, want := range []string{
		`angryduck_worker_mirror_requests_total{node="stream-a",kind="blob",result="stream"} 1`,
		`angryduck_worker_mirror_bytes_total{node="stream-a",source="stream"} 14`,
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("metrics lack %s", want)
		}
	}

	// A manifest isn't asked of an arriving node: a 404, and containerd
	// goes to the registry.
	asked = nil
	resp, err = http.Get(srv.URL + "/v2/team/app/manifests/" + d)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound || len(asked) != 0 {
		t.Fatalf("manifest: status %d, asked %v; want 404 without asking the arriving node", resp.StatusCode, asked)
	}
}
