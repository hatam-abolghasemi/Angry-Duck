package controller

import (
	"context"
	"testing"
)

// Nodes still pulling a blob for a spread are listed after every holder,
// marked Arriving, so a mirror streams the blob from them instead of
// sending containerd to the registry for one more copy. Small blobs count
// too, and a node whose stream broke is left out.
func TestHoldersListNodesStillReceivingAfterHolders(t *testing.T) {
	s, f, reg := streamSetup(t, 1)
	if _, err := s.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	pulls := f.take() // registry pulls, accepted by the fake workers
	if len(pulls) == 0 {
		t.Fatal("no registry pulls ordered")
	}
	byDigest := map[string][]string{}
	for _, r := range pulls {
		byDigest[r.Digest] = append(byDigest[r.Digest], r.node)
	}
	for _, d := range []string{dL0, dL1} { // below and above StreamMinBytes
		pulling := byDigest[d]
		if len(pulling) == 0 {
			t.Fatalf("%s isn't being pulled", d)
		}
		got := reg.Holders(d, "", s.Holders(d), s.Arriving(d))
		if len(got) != len(pulling) {
			t.Fatalf("%s: holders %+v, want the %d nodes pulling it", d, got, len(pulling))
		}
		for _, h := range got {
			if !h.Arriving || h.Address == "" || !contains(pulling, h.NodeID) {
				t.Fatalf("%s: %+v, want an arriving node from %v with its address", d, h, pulling)
			}
		}
	}

	// A node whose stream broke isn't a streaming source for this job.
	n := byDigest[dL1][0]
	s.mu.Lock()
	j := s.jobFor0(img)
	if j.noStream == nil {
		j.noStream = map[string]bool{}
	}
	j.noStream[n] = true
	s.mu.Unlock()
	if contains(s.Arriving(dL1), n) {
		t.Fatalf("%s's stream broke: it must not be offered to mirrors", n)
	}

	// Streaming off: nobody is offered.
	s.cfg.Stream = false
	if a := s.Arriving(dL1); len(a) != 0 {
		t.Fatalf("streaming off, arriving %v", a)
	}
}

// A worker that doesn't stream is never listed as arriving, and the asker
// is left out like any holder.
func TestHoldersSkipArrivingWorkersThatDontStream(t *testing.T) {
	_, _, reg := streamSetup(t, 1)
	reg.mu.Lock()
	reg.workers["w2"].Streams = false
	reg.mu.Unlock()
	got := reg.Holders(dL1, "w1", nil, []string{"w1", "w2", "w3", "gone"})
	if len(got) != 1 || got[0].NodeID != "w3" || !got[0].Arriving {
		t.Fatalf("got %+v, want only w3: w1 asks, w2 doesn't stream, gone isn't a worker", got)
	}
}

// jobFor0 is jobFor for callers already holding s.mu.
func (s *Spreader) jobFor0(image string) *sjob { return s.jobs[image] }
