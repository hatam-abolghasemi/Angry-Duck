package metrics

import (
	"bytes"
	"strings"
	"testing"
)

func TestHistogramVecRendersCumulativeBuckets(t *testing.T) {
	h := &HistogramVec{
		name: "t_seconds", help: "test", labelNames: []string{"node", "path"},
		bounds: []float64{1, 5}, series: map[string]*histSeries{},
	}
	h.Observe(0.5, "n1", "registry") // le=1
	h.Observe(1, "n1", "registry")   // le=1 (bound is inclusive)
	h.Observe(3, "n1", "registry")   // le=5
	h.Observe(9, "n1", "registry")   // +Inf
	h.Observe(2, "n1", "peer")

	var buf bytes.Buffer
	h.write(&buf)
	out := buf.String()
	for _, want := range []string{
		"# TYPE t_seconds histogram\n",
		`t_seconds_bucket{node="n1",path="registry",le="1"} 2` + "\n",
		`t_seconds_bucket{node="n1",path="registry",le="5"} 3` + "\n",
		`t_seconds_bucket{node="n1",path="registry",le="+Inf"} 4` + "\n",
		`t_seconds_sum{node="n1",path="registry"} 13.5` + "\n",
		`t_seconds_count{node="n1",path="registry"} 4` + "\n",
		`t_seconds_bucket{node="n1",path="peer",le="1"} 0` + "\n",
		`t_seconds_bucket{node="n1",path="peer",le="5"} 1` + "\n",
		`t_seconds_count{node="n1",path="peer"} 1` + "\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestHistogramVecNoLabels(t *testing.T) {
	h := &HistogramVec{name: "u_seconds", help: "x", bounds: []float64{1}, series: map[string]*histSeries{}}
	h.Observe(0.2)
	var buf bytes.Buffer
	h.write(&buf)
	if want := `u_seconds_bucket{le="1"} 1`; !strings.Contains(buf.String(), want) {
		t.Errorf("missing %q in:\n%s", want, buf.String())
	}
}
