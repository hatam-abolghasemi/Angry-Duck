// Package metrics is a minimal, dependency-free metrics registry that
// renders in Prometheus text exposition format. Angry Duck only needs a
// handful of counters, gauges, and one histogram — pulling in the full
// client_golang SDK for that would be a heavy dependency for so little.
// This hand-rolls the exposition format the same way worker/metrics.go
// hand-parses node exporter's output: no library, just the text format
// both ends agree on.
package metrics

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// labelSep separates joined label values in a metric's internal map key.
// Not a character any label value in this codebase can legally contain
// (node IDs, image references, "success"/"failure"), so it can't collide.
const labelSep = "\x1f"

// joinLabels renders labelNames/labelValues as Prometheus label-body text
// ("name=\"value\",name2=\"value2\"", no surrounding braces), shared by
// every metric type below so they format labels identically.
func joinLabels(labelNames, labelValues []string) string {
	var b strings.Builder
	for i, name := range labelNames {
		if i > 0 {
			b.WriteByte(',')
		}
		var val string
		if i < len(labelValues) {
			val = labelValues[i]
		}
		fmt.Fprintf(&b, "%s=%q", name, val)
	}
	return b.String()
}

// metric is anything the registry can serve at /metrics. CounterVec,
// GaugeVec, and HistogramVec all implement it.
type metric interface {
	write(w io.Writer)
}

// CounterVec is a counter split by a fixed, ordered set of label names.
// Safe for concurrent use.
type CounterVec struct {
	name       string
	help       string
	labelNames []string

	mu     sync.Mutex
	counts map[string]int64
}

// NewCounterVec creates a labeled counter and registers it against the
// package-level registry, so it is automatically served by Handler().
// labelNames fixes both the number and the order of label values every
// Inc/Add call must supply.
func NewCounterVec(name, help string, labelNames ...string) *CounterVec {
	c := &CounterVec{
		name:       name,
		help:       help,
		labelNames: labelNames,
		counts:     make(map[string]int64),
	}
	defaultRegistry.register(c)
	return c
}

// Inc increments the counter for the given label values by one.
// labelValues must be supplied in the same order as labelNames.
func (c *CounterVec) Inc(labelValues ...string) {
	c.Add(1, labelValues...)
}

// Add increments the counter for the given label values by n.
func (c *CounterVec) Add(n int64, labelValues ...string) {
	key := strings.Join(labelValues, labelSep)
	c.mu.Lock()
	c.counts[key] += n
	c.mu.Unlock()
}

// write renders this counter's current values in Prometheus text
// exposition format. Keys are sorted first so repeated scrapes produce a
// stable line order, which makes diffing scrape output over time sane.
func (c *CounterVec) write(w io.Writer) {
	c.mu.Lock()
	defer c.mu.Unlock()

	fmt.Fprintf(w, "# HELP %s %s\n", c.name, c.help)
	fmt.Fprintf(w, "# TYPE %s counter\n", c.name)

	keys := make([]string, 0, len(c.counts))
	for k := range c.counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		values := strings.Split(key, labelSep)
		fmt.Fprintf(w, "%s{%s} %d\n", c.name, joinLabels(c.labelNames, values), c.counts[key])
	}
}

// GaugeVec is a point-in-time value split by a fixed, ordered set of
// label names — unlike CounterVec, a value here can go up or down between
// samples, and Reset lets a periodic sampler clear stale label
// combinations (e.g. a repo that's no longer running) before writing this
// tick's snapshot, so they don't linger forever at their last value. Safe
// for concurrent use.
type GaugeVec struct {
	name       string
	help       string
	labelNames []string

	mu     sync.Mutex
	values map[string]float64
}

// NewGaugeVec creates a labeled gauge and registers it against the
// package-level registry, so it is automatically served by Handler().
func NewGaugeVec(name, help string, labelNames ...string) *GaugeVec {
	g := &GaugeVec{
		name:       name,
		help:       help,
		labelNames: labelNames,
		values:     make(map[string]float64),
	}
	defaultRegistry.register(g)
	return g
}

// Set records v for the given label values, replacing whatever was there.
func (g *GaugeVec) Set(v float64, labelValues ...string) {
	key := strings.Join(labelValues, labelSep)
	g.mu.Lock()
	g.values[key] = v
	g.mu.Unlock()
}

// Reset clears every label combination this gauge currently holds. Call
// it before re-populating a fresh periodic sample, so a label combination
// that no longer applies (e.g. a repo with zero running containers this
// tick) disappears from the exposition instead of showing a stale value
// forever.
func (g *GaugeVec) Reset() {
	g.mu.Lock()
	g.values = make(map[string]float64)
	g.mu.Unlock()
}

func (g *GaugeVec) write(w io.Writer) {
	g.mu.Lock()
	defer g.mu.Unlock()

	fmt.Fprintf(w, "# HELP %s %s\n", g.name, g.help)
	fmt.Fprintf(w, "# TYPE %s gauge\n", g.name)

	keys := make([]string, 0, len(g.values))
	for k := range g.values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		values := strings.Split(key, labelSep)
		fmt.Fprintf(w, "%s{%s} %s\n", g.name, joinLabels(g.labelNames, values), formatFloat(g.values[key]))
	}
}

// HistogramVec is a cumulative-bucket histogram split by a fixed, ordered
// set of label names, following the same bucket/sum/count exposition
// shape the Prometheus client libraries produce. Safe for concurrent use.
type HistogramVec struct {
	name       string
	help       string
	labelNames []string
	// bounds is ascending and fixed at construction; a value's bucket is
	// the first bound it's <= to, with an implicit "+Inf" bucket (every
	// observation) after the last configured bound.
	bounds []float64

	mu      sync.Mutex
	entries map[string]*histogramEntry
}

type histogramEntry struct {
	// bucketCounts[i] is the count of observations that fell into bounds[i]
	// specifically (NOT cumulative) — write() accumulates these in order
	// to produce the cumulative counts Prometheus's histogram format
	// requires. One extra trailing slot holds everything above the last
	// configured bound (the "+Inf"-only portion). Their sum at write time
	// IS the total count, so there's no separate counter field to keep in
	// sync.
	bucketCounts []int64
	sum          float64
}

// NewHistogramVec creates a labeled histogram and registers it against
// the package-level registry. bounds are the upper (inclusive) edges of
// every bucket except the implicit trailing "+Inf" one; they're sorted
// ascending internally regardless of the order passed in.
func NewHistogramVec(name, help string, bounds []float64, labelNames ...string) *HistogramVec {
	b := append([]float64(nil), bounds...)
	sort.Float64s(b)
	h := &HistogramVec{
		name:       name,
		help:       help,
		labelNames: labelNames,
		bounds:     b,
		entries:    make(map[string]*histogramEntry),
	}
	defaultRegistry.register(h)
	return h
}

// Observe records one sample of v (e.g. a duration in seconds) for the
// given label values.
func (h *HistogramVec) Observe(v float64, labelValues ...string) {
	key := strings.Join(labelValues, labelSep)
	h.mu.Lock()
	defer h.mu.Unlock()
	e, ok := h.entries[key]
	if !ok {
		e = &histogramEntry{bucketCounts: make([]int64, len(h.bounds)+1)}
		h.entries[key] = e
	}
	idx := len(h.bounds) // default: above every configured bound, the "+Inf"-only slot
	for i, bound := range h.bounds {
		if v <= bound {
			idx = i
			break
		}
	}
	e.bucketCounts[idx]++
	e.sum += v
}

func (h *HistogramVec) write(w io.Writer) {
	h.mu.Lock()
	defer h.mu.Unlock()

	fmt.Fprintf(w, "# HELP %s %s\n", h.name, h.help)
	fmt.Fprintf(w, "# TYPE %s histogram\n", h.name)

	keys := make([]string, 0, len(h.entries))
	for k := range h.entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		e := h.entries[key]
		values := strings.Split(key, labelSep)
		labels := joinLabels(h.labelNames, values)

		var cumulative int64
		for i, bound := range h.bounds {
			cumulative += e.bucketCounts[i]
			fmt.Fprintf(w, "%s_bucket{%s,le=%q} %d\n", h.name, labels, formatFloat(bound), cumulative)
		}
		cumulative += e.bucketCounts[len(h.bounds)] // the "+Inf"-only slot
		fmt.Fprintf(w, "%s_bucket{%s,le=\"+Inf\"} %d\n", h.name, labels, cumulative)
		fmt.Fprintf(w, "%s_sum{%s} %s\n", h.name, labels, formatFloat(e.sum))
		fmt.Fprintf(w, "%s_count{%s} %d\n", h.name, labels, cumulative)
	}
}

// formatFloat renders a float64 the way Prometheus text exposition
// expects (shortest round-trippable decimal — "0.5", "10", "45.6", not
// "4.56e+01" or trailing zeros).
func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// registry holds every metric created via NewCounterVec/NewGaugeVec/
// NewHistogramVec, so Handler can serve all of them from one endpoint
// without each call site having to wire its own HTTP route.
type registry struct {
	mu      sync.Mutex
	metrics []metric
}

var defaultRegistry = &registry{}

func (r *registry) register(m metric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metrics = append(r.metrics, m)
}

func (r *registry) snapshot() []metric {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]metric, len(r.metrics))
	copy(out, r.metrics)
	return out
}

// Handler serves every registered metric in Prometheus text exposition
// format. Mount it at /metrics.
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		for _, m := range defaultRegistry.snapshot() {
			m.write(w)
		}
	})
}
