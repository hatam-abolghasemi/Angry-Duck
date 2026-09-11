package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"angryduck/internal/model"
)

func newTestRescuer(t *testing.T, handler http.HandlerFunc) *Rescuer {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	hx, err := NewHostExec("")
	if err != nil {
		t.Fatalf("NewHostExec: %v", err)
	}
	return NewRescuer("test-node", srv.URL, "test-token", hx, "/run/containerd/containerd.sock", "k8s.io")
}

func TestRescuerAttempt_NoSourceFound(t *testing.T) {
	rs := newTestRescuer(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(model.RescueSourceResponse{}) // empty address
	})
	err := rs.Attempt(context.Background(), "repo/app:v1")
	if err == nil || !strings.Contains(err.Error(), "no node currently has") {
		t.Fatalf("err = %v, want a 'no node currently has' error", err)
	}
}

func TestRescuerAttempt_ControllerLookupFails(t *testing.T) {
	rs := newTestRescuer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	err := rs.Attempt(context.Background(), "repo/app:v1")
	if err == nil || !strings.Contains(err.Error(), "rescue source") {
		t.Fatalf("err = %v, want an error mentioning asking for a rescue source", err)
	}
}

func TestRescuerAttempt_SourceUnreachable(t *testing.T) {
	rs := newTestRescuer(t, func(w http.ResponseWriter, r *http.Request) {
		// Point at an address nothing listens on.
		_ = json.NewEncoder(w).Encode(model.RescueSourceResponse{Address: "127.0.0.1:1"})
	})
	err := rs.Attempt(context.Background(), "repo/app:v1")
	if err == nil || !strings.Contains(err.Error(), "dialing rescue source") {
		t.Fatalf("err = %v, want a dial error", err)
	}
}

func TestIsPeerDisconnect(t *testing.T) {
	cases := []struct {
		msg  string
		want bool
	}{
		{"write unix ->@: broken pipe", true},
		{"read: connection reset by peer", true},
		{"use of closed network connection", true},
		{`ctr: failed to ingest "blobs/sha256/...": failed to read expected number of bytes: unexpected EOF`, false},
		{"ctr: content digest sha256:xyz: not found", false},
		{"", false},
	}
	for _, c := range cases {
		if got := isPeerDisconnect(c.msg); got != c.want {
			t.Errorf("isPeerDisconnect(%q) = %v, want %v", c.msg, got, c.want)
		}
	}
}

func TestIsPeerDisconnectCaseInsensitive(t *testing.T) {
	if !isPeerDisconnect("Write: Broken Pipe") {
		t.Fatal("classification must not depend on case")
	}
}

// --- RescueExporter --------------------------------------------------------

func newTestExporter(t *testing.T, token string, maxConcurrent int) (*RescueExporter, *httptest.Server) {
	t.Helper()
	hx, err := NewHostExec("")
	if err != nil {
		t.Fatalf("NewHostExec: %v", err)
	}
	e := NewRescueExporter("test-node", token, hx, "/run/containerd/containerd.sock", "k8s.io", maxConcurrent)
	srv := httptest.NewServer(http.HandlerFunc(e.ServeExport))
	t.Cleanup(srv.Close)
	return e, srv
}

func TestRescueExporter_RejectsBadToken(t *testing.T) {
	_, srv := newTestExporter(t, "correct-token", 1)
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/rescue-export?image=repo/app:v1", nil)
	req.Header.Set(rescueTokenHeader, "wrong-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

func TestRescueExporter_RequiresImage(t *testing.T) {
	_, srv := newTestExporter(t, "correct-token", 1)
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/rescue-export", nil)
	req.Header.Set(rescueTokenHeader, "correct-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestRescueExporter_BusyWhenSlotsFull(t *testing.T) {
	e, srv := newTestExporter(t, "correct-token", 1)
	// Fill the one slot by hand, simulating a transfer already in flight.
	e.slots <- struct{}{}
	defer func() { <-e.slots }()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/rescue-export?image=repo/app:v1", nil)
	req.Header.Set(rescueTokenHeader, "correct-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
}
