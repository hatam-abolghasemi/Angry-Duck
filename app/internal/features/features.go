// Package features reads Angry Duck's feature switches once, in one place,
// and applies the rule that a feature is on only when every feature it
// belongs to is on: turning a parent off turns off everything beneath it,
// whatever the children are set to. Both binaries read the same ConfigMap,
// so they resolve the same tree.
//
// The tree (children indented):
//
//	GC_ENABLED                  image cleanup on each node
//	  GC_RETENTION_ENABLED      one retention copy of each image that went unused
//	  COMPANION_ENABLED         sidecar images of app pods kept on every node
//	SPREAD_ENABLED              blob-by-blob spreading
//	  SPREAD_STREAM_ENABLED     serving blobs still arriving
//	  SPREAD_UNPACK_ENABLED     unpacking layers as they arrive
//	PROPAGATE_ENABLED           whole-image propagation
//	RESCUE_ENABLED              rescue of pods stuck on pulls
//	MIRROR_ENABLED              the pull-only registry mirror
//	  MIRROR_RESOLVE_ENABLED    resolving tags, peers' copies when the registry can't
//	IMAGE_REPAIR_ENABLED        in-place repair of broken images
//	LAYER_INVENTORY_ENABLED     layer inventory reports
//	INVENTORY_EVENTS_ENABLED    following containerd's events
//
// A feature that is off starts nothing: no goroutine, no timer, no
// endpoint, no metric series of its own.
package features

import (
	"fmt"
	"strings"

	"angryduck/internal/config"
)

// Flags is the resolved set of switches.
type Flags struct {
	GC              bool
	GCRetention     bool
	Companion       bool
	Spread          bool
	SpreadStream    bool
	SpreadUnpack    bool
	Propagate       bool
	Rescue          bool
	Mirror          bool
	MirrorResolve   bool
	Repair          bool
	LayerInventory  bool
	InventoryEvents bool

	// Notes says which features are off only because a parent is, one
	// line each, for the startup log.
	Notes []string
}

// switchDef is one switch: its variable, default and parent.
type switchDef struct {
	env    string
	def    bool
	parent string // env of the parent, "" for a top-level feature
	set    func(*Flags, bool)
}

var tree = []switchDef{
	{"GC_ENABLED", true, "", func(f *Flags, v bool) { f.GC = v }},
	{"GC_RETENTION_ENABLED", true, "GC_ENABLED", func(f *Flags, v bool) { f.GCRetention = v }},
	{"COMPANION_ENABLED", true, "GC_ENABLED", func(f *Flags, v bool) { f.Companion = v }},
	{"SPREAD_ENABLED", true, "", func(f *Flags, v bool) { f.Spread = v }},
	{"SPREAD_STREAM_ENABLED", true, "SPREAD_ENABLED", func(f *Flags, v bool) { f.SpreadStream = v }},
	{"SPREAD_UNPACK_ENABLED", true, "SPREAD_ENABLED", func(f *Flags, v bool) { f.SpreadUnpack = v }},
	{"PROPAGATE_ENABLED", true, "", func(f *Flags, v bool) { f.Propagate = v }},
	{"RESCUE_ENABLED", true, "", func(f *Flags, v bool) { f.Rescue = v }},
	{"MIRROR_ENABLED", true, "", func(f *Flags, v bool) { f.Mirror = v }},
	{"MIRROR_RESOLVE_ENABLED", true, "MIRROR_ENABLED", func(f *Flags, v bool) { f.MirrorResolve = v }},
	{"IMAGE_REPAIR_ENABLED", true, "", func(f *Flags, v bool) { f.Repair = v }},
	{"LAYER_INVENTORY_ENABLED", true, "", func(f *Flags, v bool) { f.LayerInventory = v }},
	{"INVENTORY_EVENTS_ENABLED", true, "", func(f *Flags, v bool) { f.InventoryEvents = v }},
}

// Load reads every switch from the environment and resolves the tree.
func Load() Flags { return resolve(config.Bool) }

// resolve applies the tree to the values get returns. Parents come before
// their children in tree, so one pass is enough.
func resolve(get func(key string, def bool) bool) Flags {
	var f Flags
	on := make(map[string]bool, len(tree))
	for _, s := range tree {
		v := get(s.env, s.def)
		if v && s.parent != "" && !on[s.parent] {
			f.Notes = append(f.Notes, fmt.Sprintf("%s is off because %s=false", strings.TrimSuffix(s.env, "_ENABLED"), s.parent))
			v = false
		}
		on[s.env] = v
		s.set(&f, v)
	}
	return f
}

// String lists every switch as resolved, for the startup log.
func (f Flags) String() string {
	return fmt.Sprintf("gc=%t gc_retention=%t companion=%t spread=%t spread_stream=%t spread_unpack=%t propagate=%t rescue=%t mirror=%t mirror_resolve=%t repair=%t layer_inventory=%t inventory_events=%t",
		f.GC, f.GCRetention, f.Companion, f.Spread, f.SpreadStream, f.SpreadUnpack, f.Propagate, f.Rescue, f.Mirror, f.MirrorResolve, f.Repair, f.LayerInventory, f.InventoryEvents)
}
