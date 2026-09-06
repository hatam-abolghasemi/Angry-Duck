package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCounterVecIncAndExposition(t *testing.T) {
	c := NewCounterVec("test_counter_inc_total", "a test counter", "node", "result")
	c.Inc("node-a", "success")
	c.Inc("node-a", "success")
	c.Inc("node-a", "failure")
	c.Inc("node-b", "success")

	var buf strings.Builder
	c.write(&buf)
	out := buf.String()

	checks := []string{
		`# HELP test_counter_inc_total a test counter`,
		`# TYPE test_counter_inc_total counter`,
		`test_counter_inc_total{node="node-a",result="failure"} 1`,
		`test_counter_inc_total{node="node-a",result="success"} 2`,
		`test_counter_inc_total{node="node-b",result="success"} 1`,
	}
	for _, want := range checks {
		if !strings.Contains(out, want) {
			t.Fatalf("expected exposition output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestHandlerServesAllRegisteredCounters(t *testing.T) {
	a := NewCounterVec("test_handler_a_total", "counter a", "node")
	b := NewCounterVec("test_handler_b_total", "counter b", "node")
	a.Inc("node-x")
	b.Add(3, "node-y")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	Handler().ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, `test_handler_a_total{node="node-x"} 1`) {
		t.Fatalf("expected handler output to include counter a, got:\n%s", body)
	}
	if !strings.Contains(body, `test_handler_b_total{node="node-y"} 3`) {
		t.Fatalf("expected handler output to include counter b, got:\n%s", body)
	}
}
