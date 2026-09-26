package worker

import (
	"context"
	"fmt"
	"testing"

	"angryduck/internal/layerindex"
	"angryduck/internal/model"
)

type fakeLister struct {
	blobs, snaps map[string]bool
	calls        int
}

func (f *fakeLister) Digests(context.Context) (map[string]bool, error) {
	f.calls++
	return copyMap(f.blobs), nil
}
func (f *fakeLister) Snapshots(context.Context) (map[string]bool, error) {
	return copyMap(f.snaps), nil
}

func copyMap(m map[string]bool) map[string]bool {
	out := map[string]bool{}
	for k, v := range m {
		out[k] = v
	}
	return out
}

func dg(n int) string { return fmt.Sprintf("sha256:%064x", n) }

// controllerSim answers reports the way controller.Registry.Update does.
type controllerSim struct{ x *layerindex.Index }

func (c *controllerSim) report(node string, s *model.LayerSync) model.ReportAck {
	ack := model.ReportAck{Accepted: true}
	if s != nil {
		ack.LayersSeq, ack.LayersResync = c.x.Apply(node, s)
	} else if _, _, known := c.x.Counts(node); known {
		ack.LayersSeq = c.x.Seq(node)
	}
	return ack
}

func step(t *LayerTracker, c *controllerSim, force bool) *model.LayerSync {
	s := t.Next(context.Background(), force)
	t.Ack(c.report("n1", s), s)
	return s
}

func TestTrackerFullThenDeltasThenQuiet(t *testing.T) {
	l := &fakeLister{
		blobs: map[string]bool{dg(1): true, dg(2): true},
		snaps: map[string]bool{dg(101): true, "4f2a9c": true, "angryduck-view-x": true},
	}
	tr := NewLayerTracker(l, "n1", 0)
	c := &controllerSim{x: layerindex.New()}

	s := step(tr, c, true)
	if s == nil || !s.Full || len(s.AddBlobs) != 2 || len(s.AddSnaps) != 1 {
		t.Fatalf("first sync should be full with 2 blobs and 1 chainID snapshot, got %+v", s)
	}
	if s := step(tr, c, true); s != nil {
		t.Fatalf("nothing changed, nothing should be sent: %+v", s)
	}
	delete(l.blobs, dg(1))
	l.blobs[dg(3)] = true
	s = step(tr, c, true)
	if s == nil || s.Full || len(s.AddBlobs) != 1 || len(s.DelBlobs) != 1 {
		t.Fatalf("expected a one-in one-out delta, got %+v", s)
	}
	if m, _ := c.x.Missing("n1", []layerindex.Layer{{Digest: dg(1), Size: 7}, {Digest: dg(3), Size: 5}}); m != 7 {
		t.Fatalf("controller view wrong: missing=%d, want 7", m)
	}
}

func TestTrackerResyncsAfterControllerRestart(t *testing.T) {
	l := &fakeLister{blobs: map[string]bool{dg(1): true}, snaps: map[string]bool{}}
	tr := NewLayerTracker(l, "n1", 0)
	c := &controllerSim{x: layerindex.New()}
	step(tr, c, true)

	c.x = layerindex.New() // controller restarted, holds nothing
	if s := step(tr, c, true); s != nil {
		t.Fatalf("no change, so this report carries nothing: %+v", s)
	}
	// ...but its ack said "I hold nothing", so the next one is full.
	s := step(tr, c, true)
	if s == nil || !s.Full {
		t.Fatalf("expected a full resync, got %+v", s)
	}
	if b, _, _ := c.x.Counts("n1"); b != 1 {
		t.Fatalf("controller should hold 1 blob again, has %d", b)
	}
}

func TestTrackerRecoversFromLostAck(t *testing.T) {
	l := &fakeLister{blobs: map[string]bool{dg(1): true}, snaps: map[string]bool{}}
	tr := NewLayerTracker(l, "n1", 0)
	c := &controllerSim{x: layerindex.New()}
	step(tr, c, true)

	l.blobs[dg(2)] = true
	lost := tr.Next(context.Background(), true)
	c.report("n1", lost) // applied, but the response never reached the worker

	l.blobs[dg(3)] = true
	s := step(tr, c, true) // delta on the old base: rejected, resync asked
	if s == nil || s.Full {
		t.Fatalf("expected a delta, got %+v", s)
	}
	s = step(tr, c, true)
	if s == nil || !s.Full {
		t.Fatalf("expected a full resync after the rejected delta, got %+v", s)
	}
	if b, _, _ := c.x.Counts("n1"); b != 3 {
		t.Fatalf("controller should hold 3 blobs, has %d", b)
	}
}

func TestTrackerThrottlesFullForOldController(t *testing.T) {
	l := &fakeLister{blobs: map[string]bool{dg(1): true}, snaps: map[string]bool{}}
	tr := NewLayerTracker(l, "n1", 0)
	old := model.ReportAck{Accepted: true} // an old controller never sets layer fields
	s := tr.Next(context.Background(), true)
	tr.Ack(old, s)
	if s == nil || !s.Full {
		t.Fatal("first sync should be full")
	}
	if s := tr.Next(context.Background(), true); s != nil {
		t.Fatal("an unacknowledged full sync must not be resent on every report")
	}
}

func TestTrackerScanInterval(t *testing.T) {
	l := &fakeLister{blobs: map[string]bool{}, snaps: map[string]bool{}}
	tr := NewLayerTracker(l, "n1", 1<<62)
	c := &controllerSim{x: layerindex.New()}
	step(tr, c, false)
	step(tr, c, false)
	if l.calls != 1 {
		t.Fatalf("scanned %d times within the interval, want 1", l.calls)
	}
	step(tr, c, true)
	if l.calls != 2 {
		t.Fatal("a forced report must rescan")
	}
}
