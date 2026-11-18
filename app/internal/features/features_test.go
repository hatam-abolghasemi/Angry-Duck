package features

import (
	"strings"
	"testing"
)

func resolveWith(vals map[string]bool) Flags {
	return resolve(func(key string, def bool) bool {
		if v, ok := vals[key]; ok {
			return v
		}
		return def
	})
}

func TestDefaultsAllOnButStreamUnpack(t *testing.T) {
	f := resolveWith(nil)
	if !(f.GC && f.GCRetention && f.Companion && f.Spread && f.SpreadStream && f.SpreadBottomUp && f.SpreadUnpack && f.SpreadUnpackParallel && f.Propagate && f.Rescue && f.Mirror && f.MirrorResolve && f.Repair && f.LayerInventory && f.InventoryEvents) {
		t.Fatalf("every feature is on by default, got %s", f)
	}
	if f.SpreadUnpackStream {
		t.Fatalf("unpacking while blobs arrive needs privileges: off unless asked for, got %s", f)
	}
	if f := resolveWith(map[string]bool{"SPREAD_UNPACK_STREAM_ENABLED": true}); !f.SpreadUnpackStream {
		t.Fatalf("asked for, it is on: %s", f)
	}
	if f := resolveWith(map[string]bool{"SPREAD_UNPACK_STREAM_ENABLED": true, "SPREAD_UNPACK_ENABLED": false}); f.SpreadUnpackStream || f.SpreadUnpackParallel || len(f.Notes) != 2 {
		t.Fatalf("unpacking on arrival off turns it off, with a note: %s %v", f, f.Notes)
	}
	if len(f.Notes) != 0 {
		t.Fatalf("no notes by default, got %v", f.Notes)
	}
}

func TestParentOffTurnsChildrenOff(t *testing.T) {
	f := resolveWith(map[string]bool{"GC_ENABLED": false, "GC_RETENTION_ENABLED": true})
	if f.GC || f.GCRetention || f.Companion {
		t.Fatalf("GC off must turn retention and companions off, got %s", f)
	}
	if len(f.Notes) != 2 || !strings.Contains(f.Notes[0], "GC_ENABLED=false") {
		t.Fatalf("expected a note per child naming the parent, got %v", f.Notes)
	}
	f = resolveWith(map[string]bool{"SPREAD_ENABLED": false})
	if f.Spread || f.SpreadStream || f.SpreadBottomUp || f.SpreadUnpack || f.SpreadUnpackStream || f.SpreadUnpackParallel {
		t.Fatalf("spread off must turn streaming off, got %s", f)
	}
	f = resolveWith(map[string]bool{"MIRROR_ENABLED": false})
	if f.Mirror || f.MirrorResolve {
		t.Fatalf("mirror off must turn resolve off, got %s", f)
	}
}

func TestChildOffLeavesParentOn(t *testing.T) {
	f := resolveWith(map[string]bool{"GC_RETENTION_ENABLED": false, "SPREAD_STREAM_ENABLED": false})
	if !f.GC || f.GCRetention || !f.Spread || f.SpreadStream {
		t.Fatalf("got %s", f)
	}
	if len(f.Notes) != 0 {
		t.Fatalf("a child turned off by itself needs no note, got %v", f.Notes)
	}
}

func TestEachSwitchIsIndependentOfItsSiblings(t *testing.T) {
	for _, s := range tree {
		// Every switch on (those off by default too), but this one.
		vals := map[string]bool{}
		for _, o := range tree {
			vals[o.env] = true
		}
		vals[s.env] = false
		f := resolveWith(vals)
		var off []string
		for _, o := range tree {
			var probe Flags
			o.set(&probe, true)
			if !flagOn(f, probe) {
				off = append(off, o.env)
			}
		}
		for _, name := range off {
			if name != s.env && !childOf(name, s.env) {
				t.Errorf("%s=false turned off %s too", s.env, name)
			}
		}
		if len(off) == 0 {
			t.Errorf("%s=false turned nothing off", s.env)
		}
	}
}

// flagOn reports whether the single flag set in probe is on in f.
func flagOn(f, probe Flags) bool {
	return (probe.GC && f.GC) || (probe.GCRetention && f.GCRetention) || (probe.Companion && f.Companion) || (probe.Spread && f.Spread) ||
		(probe.SpreadStream && f.SpreadStream) || (probe.SpreadBottomUp && f.SpreadBottomUp) || (probe.SpreadUnpack && f.SpreadUnpack) ||
		(probe.SpreadUnpackStream && f.SpreadUnpackStream) || (probe.SpreadUnpackParallel && f.SpreadUnpackParallel) || (probe.Propagate && f.Propagate) || (probe.Rescue && f.Rescue) ||
		(probe.Mirror && f.Mirror) || (probe.MirrorResolve && f.MirrorResolve) || (probe.Repair && f.Repair) || (probe.LayerInventory && f.LayerInventory) ||
		(probe.InventoryEvents && f.InventoryEvents)
}

func childOf(env, parent string) bool {
	for _, s := range tree {
		if s.env == env {
			return s.parent == parent || (s.parent != "" && childOf(s.parent, parent))
		}
	}
	return false
}
