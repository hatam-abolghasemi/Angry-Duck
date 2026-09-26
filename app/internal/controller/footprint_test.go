package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"angryduck/internal/model"
)

func dgT(s string) string { h := sha256.Sum256([]byte(s)); return "sha256:" + hex.EncodeToString(h[:]) }

func heap() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

func heapFleet(nodes int) []model.WorkerReport {
	pool := make([]string, 900)
	for i := range pool {
		pool[i] = fmt.Sprintf("registry.example.com/team%d/service-%d", i%40, i)
	}
	blobPool := make([]string, 20000)
	for i := range blobPool {
		blobPool[i] = dgT(fmt.Sprint("b", i))
	}
	var out []model.WorkerReport
	for n := 0; n < nodes; n++ {
		r := rand.New(rand.NewSource(int64(n)))
		rep := model.WorkerReport{NodeID: fmt.Sprintf("node-%03d", n), Address: "x", Timestamp: time.Now()}
		for _, i := range r.Perm(len(pool))[:300] {
			tag := fmt.Sprintf("%s:1.%d.0", pool[i], i%7)
			rep.Images = append(rep.Images, tag, pool[i]+"@"+dgT(tag))
			rep.Repos = append(rep.Repos, pool[i])
		}
		ls := &model.LayerSync{Epoch: rep.NodeID, Seq: 1, Full: true}
		for _, i := range r.Perm(len(blobPool))[:5000] {
			ls.AddBlobs = append(ls.AddBlobs, blobPool[i])
		}
		for _, i := range r.Perm(len(blobPool))[:4000] {
			ls.AddSnaps = append(ls.AddSnaps, dgT(fmt.Sprint("s", blobPool[i])))
		}
		rep.Layers = ls
		out = append(out, rep)
	}
	return out
}

// TestHeapFootprint guards the controller's memory per node: 150 nodes,
// each with 600 image references from a shared pool and 9000 layers.
// Before interning and bitsets this took about 35 MiB; now about 8.
func TestHeapFootprint(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates a 150-node fleet")
	}
	reps := heapFleet(150)
	// Fresh copies of strings, as JSON decoding would produce per report.
	clone := func(r model.WorkerReport, withLayers, withInv bool) model.WorkerReport {
		c := model.WorkerReport{NodeID: r.NodeID, Address: r.Address, Timestamp: r.Timestamp}
		if withInv {
			for _, s := range r.Images {
				c.Images = append(c.Images, string([]byte(s)))
			}
			for _, s := range r.Repos {
				c.Repos = append(c.Repos, string([]byte(s)))
			}
		}
		if withLayers {
			l := *r.Layers
			c.Layers = &l
		}
		return c
	}
	b0 := heap()
	inv := NewRegistry(time.Minute, time.Minute)
	for _, r := range reps {
		inv.Update(clone(r, false, true))
	}
	b1 := heap()
	lay := NewRegistry(time.Minute, time.Minute)
	for _, r := range reps {
		lay.Update(clone(r, true, false))
	}
	b2 := heap()
	runtime.KeepAlive(reps)
	runtime.KeepAlive(inv)
	runtime.KeepAlive(lay)
	invMiB, layMiB := float64(b1-b0)/(1<<20), float64(b2-b1)/(1<<20)
	t.Logf("image inventories: %.1f MiB, layer index: %.1f MiB (150 nodes)", invMiB, layMiB)
	if invMiB > 5 || layMiB > 10 {
		t.Errorf("controller state grew: inventories %.1f MiB (limit 5), layer index %.1f MiB (limit 10)", invMiB, layMiB)
	}
}
