// Package layerindex is the controller's view of which image layers every
// node holds, built from the workers' LayerSync reports.
//
// A layer is present on a node if the node has its compressed blob (in
// containerd's content store) or its unpacked snapshot (named by its
// chainID). Checking both is what makes the index indifferent to
// discard_unpacked_layers: a node that drops blobs after unpacking still
// counts as having the layer through its snapshot. It is the same rule
// blobship uses on a single receiver, lifted to the whole cluster.
//
// Memory: every digest is interned once, cluster-wide, as a 32-byte key
// mapped to a small integer ID with a reference count, and each node holds
// its layers as a bitset over those IDs: one bit per distinct digest in
// the fleet, instead of a map entry per layer per node. Most layers are
// shared between nodes, so N nodes holding the same base layers cost one
// copy of each digest, not N. An ID is freed (and reused) as soon as the
// last node drops the digest, so IDs stay dense and the bitsets stay as
// small as what the fleet actually holds.
package layerindex

import (
	"encoding/hex"
	"math/bits"
	"strings"
	"sync"

	"angryduck/internal/model"
)

type key [32]byte

// parseKey turns "sha256:<64 hex>" into a key. Anything else is refused.
func parseKey(digest string) (key, bool) {
	var k key
	h, ok := strings.CutPrefix(digest, "sha256:")
	if !ok || len(h) != 64 {
		return k, false
	}
	if _, err := hex.Decode(k[:], []byte(h)); err != nil {
		return k, false
	}
	return k, true
}

// idSet is a set of IDs, as a bitset.
type idSet struct {
	bits []uint64
	n    int
}

func (s *idSet) has(id uint32) bool {
	w := int(id >> 6)
	return w < len(s.bits) && s.bits[w]&(1<<(id&63)) != 0
}

// add reports whether id was newly added.
func (s *idSet) add(id uint32) bool {
	w := int(id >> 6)
	if w >= len(s.bits) {
		grown := make([]uint64, w+1+w/4) // headroom for the next new IDs
		copy(grown, s.bits)
		s.bits = grown
	}
	if s.bits[w]&(1<<(id&63)) != 0 {
		return false
	}
	s.bits[w] |= 1 << (id & 63)
	s.n++
	return true
}

// del reports whether id was present.
func (s *idSet) del(id uint32) bool {
	if !s.has(id) {
		return false
	}
	s.bits[id>>6] &^= 1 << (id & 63)
	s.n--
	return true
}

func (s *idSet) len() int { return s.n }

func (s *idSet) each(fn func(id uint32)) {
	for w, word := range s.bits {
		for word != 0 {
			fn(uint32(w<<6 | bits.TrailingZeros64(word)))
			word &= word - 1
		}
	}
}

type nodeLayers struct {
	epoch string
	seq   uint64
	blobs idSet
	snaps idSet
}

// Index is safe for concurrent use.
type Index struct {
	mu    sync.RWMutex
	ids   map[key]uint32
	refs  []int32 // id -> how many node sets hold it
	keys  []key   // id -> key, for releasing a node's whole set
	free  []uint32
	nodes map[string]*nodeLayers
}

// New builds an empty index.
func New() *Index {
	return &Index{ids: make(map[key]uint32), nodes: make(map[string]*nodeLayers)}
}

// acquire returns the ID for k, taking one reference. Caller holds mu.
func (x *Index) acquire(k key) uint32 {
	if id, ok := x.ids[k]; ok {
		x.refs[id]++
		return id
	}
	var id uint32
	if n := len(x.free); n > 0 {
		id, x.free = x.free[n-1], x.free[:n-1]
		x.keys[id], x.refs[id] = k, 1
	} else {
		id = uint32(len(x.keys))
		x.keys = append(x.keys, k)
		x.refs = append(x.refs, 1)
	}
	x.ids[k] = id
	return id
}

// release drops one reference to id. Caller holds mu.
func (x *Index) release(id uint32) {
	if x.refs[id]--; x.refs[id] <= 0 {
		delete(x.ids, x.keys[id])
		x.refs[id] = 0
		x.free = append(x.free, id)
	}
}

// lookup returns the digest's ID without taking a reference. Caller holds mu.
func (x *Index) lookup(digest string) (uint32, bool) {
	k, ok := parseKey(digest)
	if !ok {
		return 0, false
	}
	id, ok := x.ids[k]
	return id, ok
}

// add puts digests into set, taking one reference per digest newly held.
// Caller holds mu.
func (x *Index) add(set *idSet, digests []string) {
	for _, d := range digests {
		k, ok := parseKey(d)
		if !ok {
			continue
		}
		if id, ok := x.ids[k]; ok && set.has(id) {
			continue // already held: no second reference
		}
		set.add(x.acquire(k))
	}
}

// del removes digests from set, dropping their references. Caller holds mu.
func (x *Index) del(set *idSet, digests []string) {
	for _, d := range digests {
		if id, ok := x.lookup(d); ok && set.del(id) {
			x.release(id)
		}
	}
}

// dropAll releases every reference a node's sets hold. Caller holds mu.
func (x *Index) dropAll(n *nodeLayers) {
	n.blobs.each(x.release)
	n.snaps.each(x.release)
	n.blobs, n.snaps = idSet{}, idSet{}
}

// Apply records one LayerSync from node. It returns the version the index
// now holds for the node, and resync=true when a delta didn't apply (the
// worker should send a full sync next).
func (x *Index) Apply(node string, s *model.LayerSync) (seq uint64, resync bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	n := x.nodes[node]
	if s.Full {
		if n == nil {
			n = &nodeLayers{}
			x.nodes[node] = n
		}
		x.dropAll(n)
		n.epoch, n.seq = s.Epoch, s.Seq
		x.add(&n.blobs, s.AddBlobs)
		x.add(&n.snaps, s.AddSnaps)
		return n.seq, false
	}
	if n == nil || n.epoch != s.Epoch || n.seq != s.Base {
		if n != nil {
			return n.seq, true
		}
		return 0, true
	}
	x.del(&n.blobs, s.DelBlobs)
	x.del(&n.snaps, s.DelSnaps)
	x.add(&n.blobs, s.AddBlobs)
	x.add(&n.snaps, s.AddSnaps)
	n.seq = s.Seq
	return n.seq, false
}

// Forget drops everything held for node.
func (x *Index) Forget(node string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if n, ok := x.nodes[node]; ok {
		x.dropAll(n)
		delete(x.nodes, node)
	}
}

// Counts returns how many blobs and snapshots node holds; known is false
// when the node never sent an inventory.
func (x *Index) Counts(node string) (blobs, snaps int, known bool) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	n, ok := x.nodes[node]
	if !ok {
		return 0, 0, false
	}
	return n.blobs.len(), n.snaps.len(), true
}

// Unique is the number of distinct digests held across the fleet.
func (x *Index) Unique() int {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return len(x.ids)
}

// Layer is one layer of an image, as the scoring needs it: its blob
// digest, its chainID and its compressed size.
type Layer struct {
	Digest  string
	ChainID string
	Size    int64
}

// Missing returns the bytes of layers node lacks, counting a layer as
// present if the node holds its blob or its snapshot. known is false when
// the node never sent an inventory; the caller must then not treat the
// node as holding nothing.
func (x *Index) Missing(node string, layers []Layer) (missing int64, known bool) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	n, ok := x.nodes[node]
	if !ok {
		return 0, false
	}
	for _, l := range layers {
		if id, ok := x.lookup(l.ChainID); ok && n.snaps.has(id) {
			continue
		}
		if id, ok := x.lookup(l.Digest); ok && n.blobs.has(id) {
			continue
		}
		missing += l.Size
	}
	return missing, true
}

// Seq returns the inventory version held for node (0: none).
func (x *Index) Seq(node string) uint64 {
	x.mu.RLock()
	defer x.mu.RUnlock()
	if n, ok := x.nodes[node]; ok {
		return n.seq
	}
	return 0
}

// BlobBytes returns the bytes of layers whose compressed blob node holds
// (snapshots don't count). Sources with blobs are preferred: a blob is
// digest-verified on arrival, a snapshot copy is not.
func (x *Index) BlobBytes(node string, layers []Layer) int64 {
	x.mu.RLock()
	defer x.mu.RUnlock()
	n, ok := x.nodes[node]
	if !ok {
		return 0
	}
	var have int64
	for _, l := range layers {
		if id, ok := x.lookup(l.Digest); ok && n.blobs.has(id) {
			have += l.Size
		}
	}
	return have
}

// Holders returns the nodes, among candidates, that hold blob digest.
func (x *Index) Holders(digest string, candidates []string) []string {
	x.mu.RLock()
	defer x.mu.RUnlock()
	id, ok := x.lookup(digest)
	if !ok {
		return nil
	}
	var out []string
	for _, c := range candidates {
		if n, ok := x.nodes[c]; ok && n.blobs.has(id) {
			out = append(out, c)
		}
	}
	return out
}

// Present reports, for each of layers, whether node holds it (its blob or
// its snapshot) and, separately, whether it holds the blob, which is what
// another node can fetch from it. One lock, one pass; out slices are
// reused when big enough. known is false when the node never sent an
// inventory.
func (x *Index) Present(node string, layers []Layer, present, blob []bool) (presentOut, blobOut []bool, known bool) {
	if cap(present) < len(layers) {
		present = make([]bool, len(layers))
	}
	if cap(blob) < len(layers) {
		blob = make([]bool, len(layers))
	}
	present, blob = present[:len(layers)], blob[:len(layers)]
	x.mu.RLock()
	defer x.mu.RUnlock()
	n, ok := x.nodes[node]
	for i, l := range layers {
		present[i], blob[i] = false, false
		if !ok {
			continue
		}
		if id, found := x.lookup(l.Digest); found && n.blobs.has(id) {
			present[i], blob[i] = true, true
			continue
		}
		if id, found := x.lookup(l.ChainID); found && n.snaps.has(id) {
			present[i] = true
		}
	}
	return present, blob, ok
}
