package controller

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"angryduck/internal/model"
)

const dgA = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type retainHarness struct {
	t   *testing.T
	reg *Registry
	rt  *Retainer

	mu     sync.Mutex
	orders map[string][]model.RetainOrder // address -> orders
	status map[string]int                 // address -> status to answer
	copies []string                       // target nodes copied to
}

func newRetainHarness(t *testing.T) *retainHarness {
	h := &retainHarness{t: t, reg: NewRegistry(time.Minute, time.Minute), orders: map[string][]model.RetainOrder{}, status: map[string]int{}}
	h.rt = NewRetainer(h.reg, "tok", RetainerConfig{TTL: 24 * time.Hour, MaxUtilization: 0.70, Copy: true})
	h.rt.send = func(_ context.Context, addr string, o model.RetainOrder) (int, error) {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.orders[addr] = append(h.orders[addr], o)
		if c, ok := h.status[addr]; ok {
			return c, nil
		}
		return http.StatusOK, nil
	}
	h.rt.copyTo = func(_ context.Context, target *workerEntry, _ string, _ []model.RescueSource) error {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.copies = append(h.copies, target.NodeID)
		return nil
	}
	return h
}

// report sends a full report for node: utilization, in-use images,
// retention copies and the blobs it holds.
func (h *retainHarness) report(node string, util float64, inUse []string, retained map[string]int64, blobs ...string) {
	var refs []model.ImageRef
	for _, d := range inUse {
		refs = append(refs, model.ImageRef{Digest: d, Name: "r.io/app:" + d[7:10]})
	}
	h.reg.Update(model.WorkerReport{
		NodeID: node, Address: node + ":1", Utilization: util, Timestamp: time.Now(),
		ImageState: true, InUse: refs, Retained: retained,
		Layers: &model.LayerSync{Epoch: node, Seq: 1, Full: true, AddBlobs: blobs},
	})
}

func (h *retainHarness) sweep() { h.rt.Sweep(context.Background()) }

func (h *retainHarness) sent(node string) []model.RetainOrder {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]model.RetainOrder(nil), h.orders[node+":1"]...)
}

func (h *retainHarness) record() *retention {
	h.rt.mu.Lock()
	defer h.rt.mu.Unlock()
	return h.rt.records[dgA]
}

func TestRetainerKeepsOneCopyOnTheMostFreeHolder(t *testing.T) {
	h := newRetainHarness(t)
	h.report("a", 0.50, []string{dgA}, nil, dgA)
	h.report("b", 0.20, nil, nil, dgA)
	h.report("c", 0.10, nil, nil) // most free, doesn't hold it
	h.report("d", 0.80, nil, nil, dgA)
	h.sweep() // used on a
	if len(h.sent("a")) != 0 || h.record() != nil {
		t.Fatal("nothing to keep while it runs")
	}
	h.report("a", 0.50, nil, nil, dgA) // v2 replaced it
	h.sweep()
	got := h.sent("b")
	if len(got) != 1 || got[0].Digest != dgA {
		t.Fatalf("orders to b: %v, want one (most free holder below 70%%)", got)
	}
	if left := time.Until(time.Unix(got[0].Until, 0)); left < 23*time.Hour || left > 24*time.Hour {
		t.Fatalf("kept for %s, want 24h", left)
	}
	if len(h.sent("a"))+len(h.sent("c"))+len(h.sent("d")) != 0 || len(h.copies) != 0 {
		t.Fatal("exactly one node keeps it")
	}
	out := scrape(t)
	for _, want := range []string{
		`angryduck_controller_image_retention_timestamp_seconds{image="r.io/app:aaa",node="",event="unused"}`,
		`angryduck_controller_image_retention_timestamp_seconds{image="r.io/app:aaa",node="b",event="kept"}`,
		`angryduck_controller_retention_events_total{event="kept"}`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %s in:\n%s", want, out)
		}
	}
	// b confirms; nothing more is sent.
	h.report("b", 0.20, nil, map[string]int64{dgA: got[0].Until}, dgA)
	h.sweep()
	if len(h.sent("b")) != 1 {
		t.Fatalf("orders to b: %v", h.sent("b"))
	}
}

func TestRetainerPausesWhileEveryNodeIsFullThenCatchesUp(t *testing.T) {
	h := newRetainHarness(t)
	h.report("a", 0.90, []string{dgA}, nil, dgA)
	h.report("b", 0.75, nil, nil, dgA)
	h.sweep()
	h.report("a", 0.90, nil, nil, dgA)
	h.sweep()
	if len(h.sent("a"))+len(h.sent("b")) != 0 {
		t.Fatal("no node below 70%: nothing is picked")
	}
	if !strings.Contains(scrape(t), "angryduck_controller_retention_paused 1") {
		t.Fatal("pause not published")
	}
	unusedAt := h.record().unusedAt
	h.report("b", 0.60, nil, nil, dgA) // cleanup freed space
	h.sweep()
	got := h.sent("b")
	if len(got) != 1 {
		t.Fatalf("orders to b: %v, want a catch-up pick", got)
	}
	if want := unusedAt.Add(24 * time.Hour).Unix(); got[0].Until != want {
		t.Fatalf("until %d, want %d: the 24h count from when it went unused", got[0].Until, want)
	}
}

func TestRetainerReleasesWhenTheImageRunsAgain(t *testing.T) {
	h := newRetainHarness(t)
	h.report("a", 0.50, []string{dgA}, nil, dgA)
	h.report("b", 0.20, nil, nil, dgA)
	h.sweep()
	h.report("a", 0.50, nil, nil, dgA)
	h.sweep()
	until := h.sent("b")[0].Until
	h.report("b", 0.20, nil, map[string]int64{dgA: until}, dgA)
	h.sweep()
	h.report("a", 0.50, []string{dgA}, nil, dgA) // rolled back to it
	h.sweep()
	if h.record() != nil {
		t.Fatal("a running image needs no copy")
	}
	got := h.sent("b")
	if len(got) != 2 || got[1].Until != 0 {
		t.Fatalf("orders to b: %v, want a release", got)
	}
	h.sweep() // b hasn't reported yet: the release isn't repeated
	if len(h.sent("b")) != 2 {
		t.Fatalf("release repeated: %v", h.sent("b"))
	}
}

func TestRetainerPicksAgainWhenTheKeeperGoesAway(t *testing.T) {
	h := newRetainHarness(t)
	h.reg.staleAfter = 50 * time.Millisecond
	h.report("a", 0.50, []string{dgA}, nil, dgA)
	h.report("b", 0.20, nil, nil, dgA)
	h.sweep()
	h.report("a", 0.50, nil, nil, dgA)
	h.sweep()
	if len(h.sent("b")) != 1 {
		t.Fatal("b should keep it")
	}
	time.Sleep(80 * time.Millisecond) // b stops reporting
	h.report("a", 0.50, nil, nil, dgA)
	h.sweep()
	if got := h.sent("a"); len(got) != 1 || got[0].Until == 0 {
		t.Fatalf("orders to a: %v, want it to take over", got)
	}
}

func TestRetainerTriesTheNextHolderWhenOneRefuses(t *testing.T) {
	h := newRetainHarness(t)
	h.status["b:1"] = http.StatusNotFound
	h.report("a", 0.50, []string{dgA}, nil, dgA)
	h.report("b", 0.20, nil, nil, dgA)
	h.sweep()
	h.report("a", 0.50, nil, nil, dgA)
	h.sweep()
	if len(h.sent("b")) != 1 || len(h.sent("a")) != 1 {
		t.Fatalf("b refused, a should keep it: b=%v a=%v", h.sent("b"), h.sent("a"))
	}
	if rec := h.record(); rec == nil || rec.node != "a" {
		t.Fatalf("record %+v", rec)
	}
}

func TestRetainerCopiesToANodeWithRoomWhenNoHolderHasAny(t *testing.T) {
	h := newRetainHarness(t)
	h.report("a", 0.90, []string{dgA}, nil, dgA)
	h.report("b", 0.30, nil, nil)
	h.sweep()
	h.report("a", 0.90, nil, nil, dgA)
	h.sweep()
	deadline := time.Now().Add(2 * time.Second)
	for len(h.sent("b")) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	h.mu.Lock()
	copies := append([]string(nil), h.copies...)
	h.mu.Unlock()
	if len(copies) != 1 || copies[0] != "b" {
		t.Fatalf("copies %v, want one to b", copies)
	}
	if got := h.sent("b"); len(got) != 1 || got[0].Until == 0 {
		t.Fatalf("orders to b: %v, want b to keep the copy", got)
	}
}

func TestRetainerDropsWhatNoNodeHolds(t *testing.T) {
	h := newRetainHarness(t)
	h.report("a", 0.50, []string{dgA}, nil, dgA)
	h.sweep()
	h.report("a", 0.50, nil, nil) // removed by hand meanwhile
	h.sweep()
	if h.record() != nil {
		t.Fatal("nothing holds it: nothing to keep")
	}
}

func TestRetainerAdoptsMarksAfterARestartAndReleasesSecondCopies(t *testing.T) {
	h := newRetainHarness(t)
	until := time.Now().Add(10 * time.Hour).Unix()
	h.report("a", 0.50, nil, map[string]int64{dgA: until}, dgA)
	h.report("b", 0.20, nil, map[string]int64{dgA: until}, dgA)
	h.sweep()
	rec := h.record()
	if rec == nil || rec.until.Unix() != until {
		t.Fatalf("record %+v, want the reported mark adopted", rec)
	}
	keeper, other := rec.node, "a"
	if keeper == "a" {
		other = "b"
	}
	if got := h.sent(other); len(got) != 1 || got[0].Until != 0 {
		t.Fatalf("orders to %s: %v, want its second copy released", other, got)
	}
	if len(h.sent(keeper)) != 0 {
		t.Fatalf("the adopted keeper got orders: %v", h.sent(keeper))
	}
}

func TestRetainerIgnoresARestartedWorkerUntilItReportsAgain(t *testing.T) {
	h := newRetainHarness(t)
	h.report("a", 0.50, []string{dgA}, nil, dgA)
	h.report("b", 0.20, nil, nil, dgA)
	h.sweep()
	// a's worker restarts: no image state until its cleanup's first look.
	h.reg.Update(model.WorkerReport{NodeID: "a", Address: "a:1", Utilization: 0.5, Timestamp: time.Now()})
	h.sweep()
	if h.record() != nil || len(h.sent("b")) != 0 {
		t.Fatal("a restart is not the image going unused")
	}
}

func TestRetainerSkipsNodesWithRetentionOff(t *testing.T) {
	h := newRetainHarness(t)
	h.report("a", 0.50, []string{dgA}, nil, dgA)
	h.reg.Update(model.WorkerReport{NodeID: "b", Address: "b:1", Utilization: 0.1, Timestamp: time.Now(),
		Layers: &model.LayerSync{Epoch: "b", Seq: 1, Full: true, AddBlobs: []string{dgA}}})
	h.sweep()
	h.report("a", 0.50, nil, nil, dgA)
	h.sweep()
	if len(h.sent("b")) != 0 {
		t.Fatal("b doesn't run retention: it can't keep a copy")
	}
	if len(h.sent("a")) != 1 {
		t.Fatalf("orders to a: %v", h.sent("a"))
	}
}

func TestRetainerExpires(t *testing.T) {
	h := newRetainHarness(t)
	h.rt.cfg.TTL = time.Millisecond
	h.report("a", 0.50, []string{dgA}, nil, dgA)
	h.sweep()
	h.report("a", 0.50, nil, nil, dgA)
	h.sweep()
	time.Sleep(5 * time.Millisecond)
	h.sweep()
	if h.record() != nil {
		t.Fatal("expired copy still tracked")
	}
}

func TestRetainerSendErrorIsAFailure(t *testing.T) {
	h := newRetainHarness(t)
	h.rt.send = func(context.Context, string, model.RetainOrder) (int, error) { return 0, errors.New("down") }
	h.report("a", 0.50, []string{dgA}, nil, dgA)
	h.sweep()
	h.report("a", 0.50, nil, nil, dgA)
	h.sweep()
	if rec := h.record(); rec == nil || rec.node != "" || rec.failed["a"].IsZero() {
		t.Fatalf("record %+v", rec)
	}
}
