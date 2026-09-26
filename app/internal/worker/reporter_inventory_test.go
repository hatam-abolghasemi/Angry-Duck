package worker

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"angryduck/internal/model"
)

// flakyRuntime fails LocalImages while fail is set.
type flakyRuntime struct {
	*fakeRuntime
	mu   sync.Mutex
	fail bool
}

func (f *flakyRuntime) LocalImages() ([]string, error) {
	f.mu.Lock()
	fail := f.fail
	f.mu.Unlock()
	if fail {
		return nil, errors.New("crictl: connection refused")
	}
	return f.fakeRuntime.LocalImages()
}

func TestReporterOmitsUnchangedInventory(t *testing.T) {
	var mu sync.Mutex
	var got []model.WorkerReport
	echo := true // controller understands inventory hashes
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/report") {
			var rep model.WorkerReport
			json.NewDecoder(r.Body).Decode(&rep)
			mu.Lock()
			got = append(got, rep)
			ack := model.ReportAck{Accepted: true}
			if echo {
				ack.InventoryHash = rep.InventoryHash
			}
			mu.Unlock()
			json.NewEncoder(w).Encode(ack)
			return
		}
		w.Write([]byte("node_filesystem_free_bytes{mountpoint=\"/\",fstype=\"ext4\"} 50\nnode_filesystem_size_bytes{mountpoint=\"/\",fstype=\"ext4\"} 100\n"))
	}))
	defer srv.Close()

	rt := &flakyRuntime{fakeRuntime: newFakeRuntime()}
	rt.local["r.io/app:1"] = true
	rt.local["r.io/app@sha256:"+strings.Repeat("a", 64)] = true
	rp := NewReporter("n1", "x:1", srv.URL+"/metrics", srv.URL, time.Minute, NewInventory(rt))

	last := func() model.WorkerReport { mu.Lock(); defer mu.Unlock(); return got[len(got)-1] }

	rp.reportOnce(0)
	if r := last(); r.InventoryOmitted || len(r.Images) != 2 || r.InventoryHash == "" {
		t.Fatalf("first report should carry the inventory: %+v", r)
	}
	rp.reportOnce(0)
	if r := last(); !r.InventoryOmitted || r.Images != nil || r.Repos != nil {
		t.Fatalf("unchanged inventory should be omitted: %+v", r)
	}
	rt.mu.Lock()
	rt.local["r.io/app:2"] = true
	rt.mu.Unlock()
	rp.reportOnce(0)
	if r := last(); r.InventoryOmitted || len(r.Images) != 3 {
		t.Fatalf("changed inventory should be sent: %+v", r)
	}
	// A failed listing keeps the controller's copy instead of emptying it.
	rt.mu.Lock()
	rt.fail = true
	rt.mu.Unlock()
	held := last().InventoryHash
	rp.reportOnce(0)
	if r := last(); !r.InventoryOmitted || r.InventoryHash != held {
		t.Fatalf("failed listing should omit with the held hash: %+v", r)
	}
	rt.mu.Lock()
	rt.fail = false
	rt.mu.Unlock()
	// A controller that doesn't echo (old, or restarted) gets everything.
	mu.Lock()
	echo = false
	mu.Unlock()
	rp.reportOnce(0) // acked hash cleared by this ack
	rp.reportOnce(0)
	if r := last(); r.InventoryOmitted || len(r.Images) != 3 {
		t.Fatalf("non-echoing controller should always get the inventory: %+v", r)
	}
}
