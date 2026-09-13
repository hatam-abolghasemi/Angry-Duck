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

func TestHistogramVecObserveAndExposition(t *testing.T) {
	h := NewHistogramVec("test_duration_seconds", "a test histogram", []float64{1, 5, 10}, "node")
	h.Observe(0.5, "node-a")  // falls in le="1"
	h.Observe(3, "node-a")    // falls in le="5"
	h.Observe(3, "node-a")    // falls in le="5"
	h.Observe(100, "node-a")  // falls in le="+Inf" only

	var buf strings.Builder
	h.write(&buf)
	out := buf.String()

	checks := []string{
		`# HELP test_duration_seconds a test histogram`,
		`# TYPE test_duration_seconds histogram`,
		// cumulative: le=1 sees only the 0.5 observation
		`test_duration_seconds_bucket{node="node-a",le="1"} 1`,
		// le=5 is cumulative: the 0.5 PLUS both 3s observations
		`test_duration_seconds_bucket{node="node-a",le="5"} 3`,
		// le=10 unchanged from le=5 since nothing new falls in (5,10]
		`test_duration_seconds_bucket{node="node-a",le="10"} 3`,
		// +Inf includes the 100 observation too
		`test_duration_seconds_bucket{node="node-a",le="+Inf"} 4`,
		`test_duration_seconds_count{node="node-a"} 4`,
		`test_duration_seconds_sum{node="node-a"} 106.5`,
	}
	for _, want := range checks {
		if !strings.Contains(out, want) {
			t.Fatalf("expected exposition output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestHandlerServesGaugesAndHistogramsToo(t *testing.T) {
	g := NewGaugeVec("test_handler_gauge", "gauge", "node")
	h := NewHistogramVec("test_handler_histogram_seconds", "hist", []float64{1}, "node")
	g.Set(5, "node-z")
	h.Observe(0.1, "node-z")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	Handler().ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, `test_handler_gauge{node="node-z"} 5`) {
		t.Fatalf("expected handler output to include the gauge, got:\n%s", body)
	}
	if !strings.Contains(body, `test_handler_histogram_seconds_bucket{node="node-z",le="1"} 1`) {
		t.Fatalf("expected handler output to include the histogram, got:\n%s", body)
	}
}
