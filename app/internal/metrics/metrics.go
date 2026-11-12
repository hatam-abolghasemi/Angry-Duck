// Package metrics is a minimal, dependency-free metrics registry that
// renders in Prometheus text exposition format. Angry Duck only needs a
// handful of counters and gauges — pulling in the full client_golang SDK
// for that would be a heavy dependency for so little. This hand-rolls
// the exposition format the same way worker/metrics.go hand-parses node
// exporter's output: no library, just the text format both ends agree
// on.
package metrics

import (
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

// appendLabels appends the label body ("name=\"value\",name2=\"value2\"",
// no surrounding braces) of the series stored under key: labelNames paired
// with key's labelSep-separated values ("" where key has fewer). Every
// metric type below formats labels through it, identically.
func appendLabels(b []byte, labelNames []string, key string) []byte {
	rest, more := key, true
	for i, name := range labelNames {
		if i > 0 {
			b = append(b, ',')
		}
		var val string
		if more {
			if j := strings.Index(rest, labelSep); j >= 0 {
				val, rest = rest[:j], rest[j+len(labelSep):]
			} else {
				val, more = rest, false
			}
		}
		b = append(b, name...)
		b = append(b, '=')
		b = strconv.AppendQuote(b, val)
	}
	return b
}

// appendHeader appends a family's HELP and TYPE lines.
func appendHeader(b []byte, name, help, typ string) []byte {
	b = append(b, "# HELP "...)
	b = append(b, name...)
	b = append(b, ' ')
	b = append(b, help...)
	b = append(b, "\n# TYPE "...)
	b = append(b, name...)
	b = append(b, ' ')
	b = append(b, typ...)
	return append(b, '\n')
}

// appendSeriesStart appends "name{labels} ", the start of a sample line.
func appendSeriesStart(b []byte, name string, labelNames []string, key string) []byte {
	b = append(b, name...)
	b = append(b, '{')
	b = appendLabels(b, labelNames, key)
	return append(b, "} "...)
}

// scrapeChunk is how much of a family is built before it is written out.
const scrapeChunk = 16 << 10

// scrapeBufs pools the buffers families are built in, so a scrape
// formats every line without a fmt call or a string per line.
var scrapeBufs = sync.Pool{New: func() any {
	b := make([]byte, 0, scrapeChunk+1024)
	return &b
}}

// flushIfFull writes b out once it holds a chunk, and returns it emptied.
func flushIfFull(w io.Writer, b []byte) []byte {
	if len(b) < scrapeChunk {
		return b
	}
	_, _ = w.Write(b)
	return b[:0]
}

// sortedKeys returns m's keys in order, for a stable line order across
// scrapes.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// metric is anything the registry can serve at /metrics. CounterVec and
// GaugeVec both implement it.
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

	bp := scrapeBufs.Get().(*[]byte)
	defer scrapeBufs.Put(bp)
	b := appendHeader((*bp)[:0], c.name, c.help, "counter")
	for _, key := range sortedKeys(c.counts) {
		b = appendSeriesStart(b, c.name, c.labelNames, key)
		b = strconv.AppendInt(b, c.counts[key], 10)
		b = flushIfFull(w, append(b, '\n'))
	}
	_, _ = w.Write(b)
	*bp = b[:0]
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

// Delete drops one label combination, so a series that only means
// something while a state lasts (an image being broken, a node being
// busy) disappears when it ends instead of lingering at its last value.
func (g *GaugeVec) Delete(labelValues ...string) {
	key := strings.Join(labelValues, labelSep)
	g.mu.Lock()
	delete(g.values, key)
	g.mu.Unlock()
}

func (g *GaugeVec) write(w io.Writer) {
	g.mu.Lock()
	defer g.mu.Unlock()

	bp := scrapeBufs.Get().(*[]byte)
	defer scrapeBufs.Put(bp)
	b := appendHeader((*bp)[:0], g.name, g.help, "gauge")
	for _, key := range sortedKeys(g.values) {
		b = appendSeriesStart(b, g.name, g.labelNames, key)
		b = appendFloat(b, g.values[key])
		b = flushIfFull(w, append(b, '\n'))
	}
	_, _ = w.Write(b)
	*bp = b[:0]
}

// appendFloat appends a float64 the way Prometheus text exposition
// expects (shortest round-trippable decimal — "0.5", "10", "45.6", not
// "4.56e+01" or trailing zeros).
func appendFloat(b []byte, f float64) []byte {
	return strconv.AppendFloat(b, f, 'g', -1, 64)
}

// registry holds every metric created via NewCounterVec/NewGaugeVec, so
// Handler can serve all of them from one endpoint without each call site
// having to wire its own HTTP route.
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
