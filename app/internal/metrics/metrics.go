// Package metrics is a minimal, dependency-free counter registry that
// renders in Prometheus text exposition format. Angry Duck only needs a
// handful of monotonically increasing, labeled counters (pulls done,
// orders sent, images deleted) — pulling in the full client_golang SDK for
// that would be a dependency for three integers. This hand-rolls the
// exposition format the same way worker/metrics.go hand-parses node
// exporter's output: no library, just the text format both ends agree on.
package metrics

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
)

// labelSep separates joined label values in a counter's internal map key.
// Not a character any label value in this codebase can legally contain
// (node IDs, image references, "success"/"failure"), so it can't collide.
const labelSep = "\x1f"

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
		var labels strings.Builder
		for i, name := range c.labelNames {
			if i > 0 {
				labels.WriteByte(',')
			}
			var val string
			if i < len(values) {
				val = values[i]
			}
			fmt.Fprintf(&labels, "%s=%q", name, val)
		}
		fmt.Fprintf(w, "%s{%s} %d\n", c.name, labels.String(), c.counts[key])
	}
}

// registry holds every CounterVec created via NewCounterVec, so Handler
// can serve all of them from one endpoint without each call site having
// to wire its own HTTP route.
type registry struct {
	mu       sync.Mutex
	counters []*CounterVec
}

var defaultRegistry = &registry{}

func (r *registry) register(c *CounterVec) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counters = append(r.counters, c)
}

func (r *registry) snapshot() []*CounterVec {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*CounterVec, len(r.counters))
	copy(out, r.counters)
	return out
}

// Handler serves every registered counter in Prometheus text exposition
// format. Mount it at /metrics.
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		for _, c := range defaultRegistry.snapshot() {
			c.write(w)
		}
	})
}
