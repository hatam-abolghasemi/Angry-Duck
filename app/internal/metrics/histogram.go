package metrics

import (
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// HistogramVec is a histogram split by a fixed, ordered set of label
// names, with bucket bounds fixed at creation. It renders the standard
// Prometheus triple per series: cumulative `_bucket{le=...}` lines, `_sum`
// and `_count`, so histogram_quantile() works on it. Safe for concurrent
// use.
//
// Series appear with their first observation. Histograms are meant for
// quantiles over rate(), where one missing first sample costs nothing, so
// unlike counters they are not created at zero up front: a histogram
// pre-created for every label combination would add bounds+2 series each.
type HistogramVec struct {
	name       string
	help       string
	labelNames []string
	bounds     []float64 // ascending upper bounds, without +Inf

	mu     sync.Mutex
	series map[string]*histSeries
}

type histSeries struct {
	counts []int64 // per bucket, not cumulative; the last is the +Inf overflow
	sum    float64
	count  int64
}

// NewHistogramVec creates a labeled histogram and registers it against the
// package-level registry, so it is served by Handler(). bounds are the
// bucket upper bounds in ascending order; +Inf is implied.
func NewHistogramVec(name, help string, bounds []float64, labelNames ...string) *HistogramVec {
	b := append([]float64(nil), bounds...)
	sort.Float64s(b)
	h := &HistogramVec{
		name:       name,
		help:       help,
		labelNames: labelNames,
		bounds:     b,
		series:     make(map[string]*histSeries),
	}
	defaultRegistry.register(h)
	return h
}

// Observe records v for the given label values, which must be supplied in
// the same order as labelNames.
func (h *HistogramVec) Observe(v float64, labelValues ...string) {
	key := strings.Join(labelValues, labelSep)
	// First bucket whose bound is >= v; len(bounds) is the +Inf bucket.
	i := sort.SearchFloat64s(h.bounds, v)
	h.mu.Lock()
	s := h.series[key]
	if s == nil {
		s = &histSeries{counts: make([]int64, len(h.bounds)+1)}
		h.series[key] = s
	}
	s.counts[i]++
	s.sum += v
	s.count++
	h.mu.Unlock()
}

func (h *HistogramVec) write(w io.Writer) {
	h.mu.Lock()
	defer h.mu.Unlock()

	bp := scrapeBufs.Get().(*[]byte)
	defer scrapeBufs.Put(bp)
	b := appendHeader((*bp)[:0], h.name, h.help, "histogram")
	for _, key := range sortedKeys(h.series) {
		s := h.series[key]
		var cum int64
		for i := 0; i <= len(h.bounds); i++ {
			cum += s.counts[i]
			b = append(b, h.name...)
			b = append(b, "_bucket{"...)
			b = appendLabels(b, h.labelNames, key)
			if len(h.labelNames) > 0 {
				b = append(b, ',')
			}
			b = append(b, `le="`...)
			if i < len(h.bounds) {
				b = appendFloat(b, h.bounds[i])
			} else {
				b = append(b, "+Inf"...)
			}
			b = append(b, `"} `...)
			b = strconv.AppendInt(b, cum, 10)
			b = flushIfFull(w, append(b, '\n'))
		}
		b = appendSeriesStart(b, h.name+"_sum", h.labelNames, key)
		b = appendFloat(b, s.sum)
		b = flushIfFull(w, append(b, '\n'))
		b = appendSeriesStart(b, h.name+"_count", h.labelNames, key)
		b = strconv.AppendInt(b, s.count, 10)
		b = flushIfFull(w, append(b, '\n'))
	}
	_, _ = w.Write(b)
	*bp = b[:0]
}

// Count is how many observations the series with these label values has
// (0 when it has none yet). For tests and debugging; scrapes use write.
func (h *HistogramVec) Count(labelValues ...string) int64 {
	key := strings.Join(labelValues, labelSep)
	h.mu.Lock()
	defer h.mu.Unlock()
	if s := h.series[key]; s != nil {
		return s.count
	}
	return 0
}
