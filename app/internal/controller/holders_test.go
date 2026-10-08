package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"angryduck/internal/model"
)

// /layers/holders lists every fresh holder (no cap), then nodes a running
// spread knows hold the blob ahead of their next inventory.
func TestHoldersListsEveryHolderAndSpreadArrivals(t *testing.T) {
	reg := NewRegistry(time.Minute, time.Minute)
	srv := NewServer(reg, NewRanker(reg, 1, time.Hour, nil, false, false))
	srv.SetToken(rescueToken)
	d := dL1 // a layer of the spread's image below
	for i := 0; i < 6; i++ {
		n := fmt.Sprintf("w%d", i)
		rep := model.WorkerReport{NodeID: n, Address: n + ":18081", Utilization: 0.2, Timestamp: time.Now()}
		if i < 5 {
			rep.Layers = &model.LayerSync{Epoch: n, Seq: 1, Full: true, AddBlobs: []string{d}}
		}
		body, _ := json.Marshal(rep)
		req := httptest.NewRequest(http.MethodPost, "/report", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+rescueToken)
		srv.Handler().ServeHTTP(httptest.NewRecorder(), req)
	}
	// w5's inventory doesn't list the blob yet, but a spread just
	// delivered it there.
	f := &spreadFleet{busy: map[string]bool{}}
	report(reg, "w5", f.worker(t, "w5"), 0.2)
	sp := testSpreader(reg, 1)
	if _, err := sp.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	sp.wg.Wait()
	sp.done(model.SpreadDone{Node: "w5", Job: sp.jobFor(img).id, Digest: d, Path: model.SpreadPathPeer, Result: model.SpreadResultOK})
	srv.SetSpreader(sp)

	get := func(exclude string) []model.Holder {
		req := httptest.NewRequest(http.MethodGet, "/layers/holders?digest="+d+"&exclude="+exclude, nil)
		req.Header.Set("Authorization", "Bearer "+rescueToken)
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		var out []model.Holder
		_ = json.NewDecoder(rec.Body).Decode(&out)
		return out
	}
	hs := get("w0")
	if len(hs) != 5 {
		t.Fatalf("%d holders %v, want all 5 others: 4 by inventory (no cap of 3) and w5 from the spread", len(hs), hs)
	}
	if last := hs[len(hs)-1]; last.NodeID != "w5" || last.Address == "" {
		t.Fatalf("spread arrival = %+v, want w5 with its address, after the inventory holders", last)
	}
	if hs := get("w5"); len(hs) != 5 {
		t.Fatalf("excluding the asker: %d holders, want 5", len(hs))
	}
}
