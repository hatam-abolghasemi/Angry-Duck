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

func TestGaugeVecSetAndReset(t *testing.T) {
	g := NewGaugeVec("test_gauge_value", "a test gauge", "node", "repo")
	g.Set(3, "node-a", "repo-x")
	g.Set(7, "node-a", "repo-y")

	var buf strings.Builder
	g.write(&buf)
	out := buf.String()

	checks := []string{
		`# HELP test_gauge_value a test gauge`,
		`# TYPE test_gauge_value gauge`,
		`test_gauge_value{node="node-a",repo="repo-x"} 3`,
		`test_gauge_value{node="node-a",repo="repo-y"} 7`,
	}
	for _, want := range checks {
		if !strings.Contains(out, want) {
			t.Fatalf("expected exposition output to contain %q, got:\n%s", want, out)
		}
	}

	// Set again for the SAME label combo -> replaces, doesn't add.
	g.Set(1, "node-a", "repo-x")
	buf.Reset()
	g.write(&buf)
	if strings.Contains(buf.String(), `repo-x"} 3`) {
		t.Fatalf("expected Set to replace the previous value, got:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), `test_gauge_value{node="node-a",repo="repo-x"} 1`) {
		t.Fatalf("expected replaced value 1, got:\n%s", buf.String())
	}

	// Reset -> every label combination disappears.
	g.Reset()
	buf.Reset()
	g.write(&buf)
	if strings.Contains(buf.String(), "repo-x") || strings.Contains(buf.String(), "repo-y") {
		t.Fatalf("expected Reset to clear all label combinations, got:\n%s", buf.String())
	}
}

func TestHandlerServesGaugesToo(t *testing.T) {
	g := NewGaugeVec("test_handler_gauge", "gauge", "node")
	g.Set(5, "node-z")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	Handler().ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, `test_handler_gauge{node="node-z"} 5`) {
		t.Fatalf("expected handler output to include the gauge, got:\n%s", body)
	}
}
