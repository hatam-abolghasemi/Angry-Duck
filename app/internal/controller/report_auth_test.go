package controller

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"angryduck/internal/model"
)

const testToken = "0123456789abcdef0123456789abcdef"

// An unauthenticated report must not register a worker: its address is
// where rescue and propagation would send the shared token.
func TestReportRequiresToken(t *testing.T) {
	reg := NewRegistry(time.Minute, time.Minute)
	srv := NewServer(reg, NewRanker(reg, 1, time.Hour, nil, false, false))
	srv.SetToken(testToken)

	post := func(auth string) int {
		body, _ := json.Marshal(model.WorkerReport{NodeID: "evil", Address: "203.0.113.9:18081", Utilization: 0.1})
		req := httptest.NewRequest(http.MethodPost, "/report", bytes.NewReader(body))
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		return rec.Code
	}
	if code := post(""); code != http.StatusUnauthorized {
		t.Fatalf("no token: status %d, want 401", code)
	}
	if n := len(reg.Snapshot().Workers); n != 0 {
		t.Fatalf("rejected report registered %d worker(s)", n)
	}
	if code := post("Bearer " + testToken); code != http.StatusOK {
		t.Fatalf("valid token: status %d, want 200", code)
	}
}

func TestPullOrderCarriesToken(t *testing.T) {
	var got atomic.Value
	w := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Store(r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusAccepted)
	}))
	defer w.Close()
	reg := NewRegistry(time.Minute, time.Minute)
	rk := NewRanker(reg, 1, time.Hour, nil, false, false)
	rk.SetToken(testToken)
	rk.sendPullOrder("n1", w.Listener.Addr().String(), "docker.io/library/nginx:1")
	if v, _ := got.Load().(string); v != "Bearer "+testToken {
		t.Fatalf("Authorization = %q", v)
	}
}

func TestWebhookRequiresToken(t *testing.T) {
	reg := NewRegistry(time.Minute, time.Minute)
	srv := NewServer(reg, NewRanker(reg, 1, time.Hour, nil, false, false))
	post := func(auth string) int {
		req := httptest.NewRequest(http.MethodPost, "/webhook/preheat", bytes.NewReader([]byte(`{"image":"nginx"}`)))
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		return rec.Code
	}
	if code := post(""); code == http.StatusUnauthorized {
		t.Fatal("no webhook token configured: should stay open")
	}
	srv.SetWebhookToken(testToken)
	if code := post(""); code != http.StatusUnauthorized {
		t.Fatalf("missing token: %d, want 401", code)
	}
	if code := post("Bearer " + testToken + "x"); code != http.StatusUnauthorized {
		t.Fatalf("wrong token: %d, want 401", code)
	}
	if code := post("Bearer " + testToken); code == http.StatusUnauthorized {
		t.Fatalf("valid token rejected")
	}
}

// Liveness follows the controller's clock: a worker whose node clock is far
// behind still counts as fresh while it reports, and one far ahead doesn't
// outlive its last report.
func TestReportLivenessUsesReceiptTime(t *testing.T) {
	reg := NewRegistry(30*time.Second, time.Minute)
	srv := NewServer(reg, NewRanker(reg, 1, time.Hour, nil, false, false))
	srv.SetToken(testToken)
	post := func(node string, ts time.Time) {
		body, _ := json.Marshal(model.WorkerReport{NodeID: node, Address: "10.0.0.1:18081", Utilization: 0.1, Timestamp: ts})
		req := httptest.NewRequest(http.MethodPost, "/report", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+testToken)
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("report from %s: status %d", node, rec.Code)
		}
	}
	post("behind", time.Now().Add(-time.Hour))
	post("ahead", time.Now().Add(time.Hour))
	if n := reg.FreshCount(); n != 2 {
		t.Fatalf("fresh workers = %d, want both: their clocks don't matter", n)
	}
	for _, w := range reg.Snapshot().Workers {
		if d := time.Since(w.LastSeen); d < 0 || d > time.Minute {
			t.Fatalf("node %s last seen %s ago, want the receipt time", w.NodeID, d)
		}
	}
}
