package controller

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"angryduck/internal/model"
)

const dg = "sha256:9467d14fc385f36a8b42cfeca64f90b22f67e1f62a80ec5605c96846f8072bf9"

func TestPeersForReturnsOnlyFreshHoldersExceptTheAsker(t *testing.T) {
	r := NewRegistry(30*time.Second, time.Minute)
	now := time.Now()
	r.Update(model.WorkerReport{NodeID: "a", Address: "10.0.0.1:18081", Digests: []string{dg}, Timestamp: now})
	r.Update(model.WorkerReport{NodeID: "b", Address: "10.0.0.2:18081", Digests: []string{dg}, Timestamp: now})
	r.Update(model.WorkerReport{NodeID: "c", Address: "10.0.0.3:18081", Timestamp: now})
	r.Update(model.WorkerReport{NodeID: "stale", Address: "10.0.0.4:18081", Digests: []string{dg}, Timestamp: now.Add(-time.Hour)})

	got := r.PeersFor(dg, "a", 3)
	if len(got) != 1 || got[0] != "10.0.0.2:18081" {
		t.Fatalf("got %v, want only b (a asked, c lacks it, stale is stale)", got)
	}
	if got := r.PeersFor("sha256:other", "", 3); len(got) != 0 {
		t.Fatalf("got %v for an unknown digest", got)
	}
}

func TestPeersForCapsAndShuffles(t *testing.T) {
	r := NewRegistry(30*time.Second, time.Minute)
	for _, id := range []string{"a", "b", "c", "d", "e", "f"} {
		r.Update(model.WorkerReport{NodeID: id, Address: id + ":1", Digests: []string{dg}, Timestamp: time.Now()})
	}
	firsts := map[string]bool{}
	for i := 0; i < 200; i++ {
		got := r.PeersFor(dg, "", 3)
		if len(got) != 3 {
			t.Fatalf("limit not applied: %v", got)
		}
		firsts[got[0]] = true
	}
	if len(firsts) < 4 {
		t.Fatalf("first candidate barely varies (%v); requesters would pile onto one source", firsts)
	}
}

func TestAnnounceAddsDigestUntilNextReport(t *testing.T) {
	r := NewRegistry(30*time.Second, time.Minute)
	r.Update(model.WorkerReport{NodeID: "a", Address: "a:1", Timestamp: time.Now()})
	snap := r.FreshWorkers() // a reader holding the old map
	if !r.Announce("a", dg) {
		t.Fatal("announce for a known worker should apply")
	}
	if len(r.PeersFor(dg, "", 3)) != 1 {
		t.Fatal("announced digest should be served immediately")
	}
	if _, ok := snap[0].Digests[dg]; ok {
		t.Fatal("announce must not mutate a map an earlier snapshot holds")
	}
	if r.Announce("ghost", dg) {
		t.Fatal("unknown worker must be ignored")
	}
	r.Update(model.WorkerReport{NodeID: "a", Address: "a:1", Timestamp: time.Now()})
	if len(r.PeersFor(dg, "", 3)) != 0 {
		t.Fatal("a full report is authoritative and replaces announced state")
	}
}

func TestPeersAndAnnounceHandlers(t *testing.T) {
	reg := NewRegistry(30*time.Second, time.Minute)
	reg.Update(model.WorkerReport{NodeID: "a", Address: "a:1", Timestamp: time.Now()})
	srv := httptest.NewServer(NewServer(reg, NewRanker(reg, 1, time.Hour, nil, true), 3).Handler())
	defer srv.Close()

	body, _ := json.Marshal(model.Announce{NodeID: "a", Digest: dg})
	resp, err := http.Post(srv.URL+"/announce", "application/json", bytes.NewReader(body))
	if err != nil || resp.StatusCode != http.StatusNoContent {
		t.Fatalf("announce: %v %v", err, resp.StatusCode)
	}
	resp, err = http.Get(srv.URL + "/peers?digest=" + dg + "&node=b")
	if err != nil {
		t.Fatal(err)
	}
	var pr model.PeersResponse
	_ = json.NewDecoder(resp.Body).Decode(&pr)
	if len(pr.Peers) != 1 || pr.Peers[0] != "a:1" {
		t.Fatalf("peers = %v", pr.Peers)
	}
	resp, _ = http.Get(srv.URL + "/peers?digest=nope")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad digest -> %d", resp.StatusCode)
	}
}
