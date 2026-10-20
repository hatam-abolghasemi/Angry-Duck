package controller

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"angryduck/internal/model"
)

// metricValue reads one series' value from /metrics (0 when absent).
func metricValue(t *testing.T, series string) float64 {
	t.Helper()
	for _, line := range strings.Split(scrape(t), "\n") {
		if v, ok := strings.CutPrefix(line, series+" "); ok {
			f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
			if err != nil {
				t.Fatal(err)
			}
			return f
		}
	}
	return 0
}

// The blob is the unit: a node lacking several blobs gets them all at
// once when sources are free, not one per path at a time.
func TestSpreader_NodeReceivesSeveralBlobsAtOnce(t *testing.T) {
	s, f, _ := spreadSetup(t, 10)
	if _, err := s.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	job := s.jobFor(img)
	pulled := map[string]bool{}
	held := map[string]bool{}
	for _, r := range f.take() {
		pulled[r.node] = true
		if !held[r.Digest] {
			held[r.Digest] = true
			s.done(model.SpreadDone{Node: r.node, Job: job.id, Digest: r.Digest, Path: r.Path, Result: model.SpreadResultOK})
		}
	}
	var idle string
	for _, n := range []string{"w1", "w2", "w3", "w4", "w5", "w6"} {
		if !pulled[n] {
			idle = n
		}
	}
	if idle == "" {
		t.Fatal("every node got a registry pull; the test needs one that got nothing")
	}
	s.tick(context.Background())
	s.wg.Wait()
	got := map[string]bool{}
	for _, r := range byPath(f.take(), model.SpreadPathPeer) {
		if r.node == idle {
			if got[r.Digest] {
				t.Fatalf("%s ordered %s from a peer twice at once", idle, r.Digest)
			}
			got[r.Digest] = true
		}
	}
	if len(got) != 3 {
		t.Fatalf("%s got %d blobs from peers at once, want all 3 (sources were free)", idle, len(got))
	}
}

// Free sources are shared out one transfer per node per round: the first
// node in line doesn't take them all.
func TestSpreader_SharesFreeSourcesAcrossNodes(t *testing.T) {
	s, f, _ := spreadSetup(t, 1)
	if _, err := s.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	f.take()
	job := s.jobFor(img)
	for _, holder := range []string{"w1", "w2"} {
		for _, d := range []string{dL0, dL1, dL2} {
			s.done(model.SpreadDone{Node: holder, Job: job.id, Digest: d, Path: model.SpreadPathPeer, Result: model.SpreadResultOK})
		}
	}
	s.tick(context.Background())
	s.wg.Wait()
	perNode := map[string]int{}
	for _, r := range byPath(f.take(), model.SpreadPathPeer) {
		perNode[r.node]++
	}
	if len(perNode) != 2 {
		t.Fatalf("peer orders per node = %v: two free sources (PerSource=1) must go to two nodes", perNode)
	}
}

// A node too full to receive no longer keeps the spread from counting as
// complete; the job stays open, quietly, so it still gets the image once
// cleanup makes room.
func TestSpreader_CompleteWhileAFullNodeWaitsForRoom(t *testing.T) {
	s, f, reg := spreadSetup(t, 10)
	completeBefore := metricValue(t, `angryduck_controller_spreads_total{result="complete"}`)
	supersededBefore := metricValue(t, `angryduck_controller_spreads_total{result="superseded"}`)
	if _, err := s.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	job := s.jobFor(img)
	for round := 0; round < 10; round++ {
		recs := f.take()
		if len(recs) == 0 {
			break
		}
		for _, r := range recs {
			s.done(model.SpreadDone{Node: r.node, Job: job.id, Digest: r.Digest, Path: r.Path, Result: model.SpreadResultOK})
		}
		s.tick(context.Background())
		s.wg.Wait()
	}
	s.tick(context.Background())
	s.wg.Wait()

	if got := metricValue(t, `angryduck_controller_spreads_total{result="complete"}`) - completeBefore; got != 1 {
		t.Fatalf("complete counted %v times, want once every node with room has it", got)
	}
	if !strings.Contains(scrape(t), `angryduck_controller_image_spread_seconds{image="`+img+`",result="complete"}`) {
		t.Fatal("completion not in the image history")
	}
	if s.jobFor(img) == nil {
		t.Fatal("job ended while a full node may still get room")
	}
	var st model.SpreadStatus
	for _, x := range s.Status() {
		if x.Image == img {
			st = x
		}
	}
	if !st.Complete || len(st.Skipped) != 1 || st.Skipped[0] != "full" {
		t.Fatalf("status = complete %v skipped %v, want complete with [full] skipped", st.Complete, st.Skipped)
	}
	out := scrape(t)
	for _, gone := range []string{
		`angryduck_controller_spread_nodes{image="` + img + `"`,
		`angryduck_controller_spread_node_missing_bytes{image="` + img + `",node="full"}`,
		`angryduck_controller_image_spread_seconds{image="` + img + `",result="running"}`,
	} {
		if strings.Contains(out, gone) {
			t.Fatalf("complete spread waiting only on a full node still shows %s", gone)
		}
	}

	// Ticks while it waits count nothing again.
	s.tick(context.Background())
	s.wg.Wait()
	if got := metricValue(t, `angryduck_controller_spreads_total{result="complete"}`) - completeBefore; got != 1 {
		t.Fatalf("complete counted %v times after another tick", got)
	}

	// Cleanup made room: the full node gets the image after all.
	report(reg, "full", reg.worker("full").Address, 0.5)
	s.tick(context.Background())
	s.wg.Wait()
	if len(byPath(f.take(), model.SpreadPathPeer)) == 0 {
		t.Fatal("the node with room now got nothing")
	}

	// A newer tag supersedes it: already counted complete, not again.
	if _, err := s.Start(context.Background(), "registry.example.com/team/app:1.6.0"); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	if got := metricValue(t, `angryduck_controller_spreads_total{result="superseded"}`) - supersededBefore; got != 0 {
		t.Fatalf("a spread already complete counted superseded (%v)", got)
	}
}
