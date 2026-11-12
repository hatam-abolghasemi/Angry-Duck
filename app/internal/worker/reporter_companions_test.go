package worker

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"angryduck/internal/model"
)

// Reports carry the companions running here with the inventory, and the
// hash of the companion set the worker holds; a set in the ack goes to
// the hook and its hash is echoed from then on.
func TestReporterCarriesCompanionsAndTakesTheSet(t *testing.T) {
	var mu sync.Mutex
	var got []model.WorkerReport
	send := &model.CompanionSet{Hash: "h1", Ready: true, Digests: []string{"sha256:n"}, AppRegistries: []string{"r.io"}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/report") {
			var rep model.WorkerReport
			_ = json.NewDecoder(r.Body).Decode(&rep)
			mu.Lock()
			got = append(got, rep)
			ack := model.ReportAck{Accepted: true, InventoryHash: rep.InventoryHash}
			if rep.CompanionsHash != send.Hash {
				ack.Companions = send
			}
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(ack)
			return
		}
		_, _ = w.Write([]byte("node_filesystem_free_bytes{mountpoint=\"/\",fstype=\"ext4\"} 50\nnode_filesystem_size_bytes{mountpoint=\"/\",fstype=\"ext4\"} 100\n"))
	}))
	defer srv.Close()
	rt := newFakeRuntime()
	rt.local["r.io/app:1"] = true
	rp := NewReporter("n1", "x:1", srv.URL+"/metrics", srv.URL, time.Minute, NewInventory(rt))
	var sets []model.CompanionSet
	rp.OnCompanions(func(s model.CompanionSet) { sets = append(sets, s) })
	last := func() model.WorkerReport { mu.Lock(); defer mu.Unlock(); return got[len(got)-1] }

	rp.reportOnce(0)
	if r := last(); r.CompanionState || r.CompanionsHash != "" {
		t.Fatalf("first report: %+v, want no companion state and no set held", r)
	}
	if len(sets) != 1 || sets[0].Hash != "h1" {
		t.Fatalf("sets %+v, want the ack's set", sets)
	}
	rp.SetCompanions([]model.ImageRef{{Digest: "sha256:n", Name: "docker.io/library/nginx:1", Size: 5}})
	rp.reportOnce(0)
	r := last()
	if !r.CompanionState || len(r.Companions) != 1 || r.Companions[0].Size != 5 || r.CompanionsHash != "h1" {
		t.Fatalf("report %+v, want the companion with its size and the held hash", r)
	}
	if len(sets) != 1 {
		t.Fatalf("the set was sent again: %+v", sets)
	}
	rp.reportOnce(0)
	if r := last(); !r.InventoryOmitted || r.Companions != nil {
		t.Fatalf("unchanged companions must be omitted with the inventory: %+v", r)
	}
}
