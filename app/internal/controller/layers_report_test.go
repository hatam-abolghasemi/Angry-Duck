package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"angryduck/internal/layerindex"
	"angryduck/internal/model"
)

func TestReportCarriesLayerSyncAndAck(t *testing.T) {
	reg := NewRegistry(time.Minute, time.Minute)
	srv := NewServer(reg, NewRanker(reg, 1, time.Hour, nil, false, false))
	post := func(rep model.WorkerReport) model.ReportAck {
		body, _ := json.Marshal(rep)
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/report", bytes.NewReader(body)))
		var ack model.ReportAck
		_ = json.NewDecoder(rec.Body).Decode(&ack)
		return ack
	}
	d := fmt.Sprintf("sha256:%064x", 1)
	base := model.WorkerReport{NodeID: "n1", Address: "10.0.0.1:18081", Utilization: 0.5}

	rep := base
	rep.Layers = &model.LayerSync{Epoch: "e", Seq: 1, Full: true, AddBlobs: []string{d}}
	if ack := post(rep); ack.LayersSeq != 1 || ack.LayersResync {
		t.Fatalf("full sync ack = %+v", ack)
	}
	if ack := post(base); ack.LayersSeq != 1 {
		t.Fatalf("report without layers should confirm seq 1, got %+v", ack)
	}
	st := reg.Snapshot()
	if len(st.Workers) != 1 || !st.Workers[0].LayersKnown || st.Workers[0].Blobs != 1 {
		t.Fatalf("status = %+v", st.Workers)
	}

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	out, _ := io.ReadAll(rec.Body)
	if !strings.Contains(string(out), "angryduck_controller_layer_index_digests{} 1") {
		t.Fatalf("metric missing:\n%s", out)
	}
}

type fakeResolver struct {
	layers []layerindex.Layer
	err    error
}

func (f fakeResolver) Layers(context.Context, string, string) ([]layerindex.Layer, error) {
	return f.layers, f.err
}

func TestRankerSeedsNodesHoldingMostBytes(t *testing.T) {
	reg := NewRegistry(time.Minute, time.Minute)
	d := func(n int) string { return fmt.Sprintf("sha256:%064x", n) }
	layers := []layerindex.Layer{{Digest: d(1), ChainID: d(101), Size: 400}, {Digest: d(2), ChainID: d(102), Size: 40}}
	up := func(node string, util float64, blobs, snaps []string) {
		reg.Update(model.WorkerReport{NodeID: node, Address: node + ":1", Utilization: util, Timestamp: time.Now(),
			Layers: &model.LayerSync{Epoch: "e", Seq: 1, Full: true, AddBlobs: blobs, AddSnaps: snaps}})
	}
	up("empty", 0.1, nil, nil)                                                                                     // lacks 440
	up("snaps-base", 0.5, nil, []string{d(101)})                                                                   // lacks 40 (discard=true node)
	up("blob-base", 0.3, []string{d(1)}, nil)                                                                      // lacks 40, emptier
	reg.Update(model.WorkerReport{NodeID: "old-worker", Address: "x:1", Utilization: 0.05, Timestamp: time.Now()}) // no inventory

	rk := NewRanker(reg, 2, time.Hour, nil, true, false)
	rk.SetLayerResolver(fakeResolver{layers: layers}, "linux/amd64", time.Second)
	ranked, missing := rk.rank(img, reg.FreshWorkers())
	var order []string
	for _, w := range ranked {
		order = append(order, w.NodeID)
	}
	if strings.Join(order, ",") != "blob-base,snaps-base,empty,old-worker" || missing["empty"] != 440 {
		t.Fatalf("order %v missing %v", order, missing)
	}

	rk.SetLayerResolver(fakeResolver{err: fmt.Errorf("registry down")}, "linux/amd64", time.Second)
	if ranked, _ := rk.rank(img, reg.FreshWorkers()); ranked[0].NodeID != "old-worker" {
		t.Fatalf("fallback should rank by utilization/locality, got %s", ranked[0].NodeID)
	}
}
