package controller

import (
	"testing"
	"time"

	"angryduck/internal/model"
)

// A worker that can't reach containerd is left out of everything that
// picks nodes (spread, propagation, rescue, the mirror's holders) until a
// report says containerd is back.
func TestContainerdDownWorkerIsNotFresh(t *testing.T) {
	reg := NewRegistry(time.Minute, time.Minute)
	img := "registry.example/app:v1"
	send := func(node string, down bool) {
		reg.Update(model.WorkerReport{NodeID: node, Address: node + ":18081", Utilization: 0.2,
			Images: []string{img}, ContainerdDown: down, Timestamp: time.Now()})
	}
	fresh := func() map[string]bool {
		out := map[string]bool{}
		for _, w := range reg.FreshWorkers() {
			out[w.NodeID] = true
		}
		return out
	}

	send("a", false)
	send("b", true)
	if f := fresh(); !f["a"] || f["b"] {
		t.Fatalf("fresh = %v, want only a", f)
	}
	if hs := reg.ImageHolderList(img); len(hs) != 1 || hs[0].NodeID != "a" {
		t.Fatalf("holders of %s = %v, want only a", img, hs)
	}
	st := reg.Snapshot()
	if st.FreshCount != 1 {
		t.Fatalf("fresh count = %d, want 1", st.FreshCount)
	}
	for _, w := range st.Workers {
		if w.NodeID == "b" && (w.Fresh || !w.ContainerdDown) {
			t.Fatalf("status of b = %+v, want not fresh, containerd down", w)
		}
	}

	send("b", false)
	if f := fresh(); !f["a"] || !f["b"] {
		t.Fatalf("fresh = %v after b recovered, want a and b", f)
	}
}
