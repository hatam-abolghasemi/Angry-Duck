package metrics

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// eventTTL is how long a RecentGaugeVec event stays in the exposition after
// it was set: long enough for Prometheus to scrape it, which then keeps the
// history. 0 turns events off: Set and Hold do nothing.
var eventTTL atomic.Int64

func init() { eventTTL.Store(int64(15 * time.Second)) }

// SetEventTTL sets how long events stay in the exposition.
func SetEventTTL(d time.Duration) {
	if d < 0 {
		d = 0
	}
	eventTTL.Store(int64(d))
}

// EventTTL returns the TTL SetEventTTL set.
func EventTTL() time.Duration { return time.Duration(eventTTL.Load()) }

// maxRecentSeries caps each RecentGaugeVec: past it, the series set
// longest ago goes first, so a burst of events can't flood the exposition.
const maxRecentSeries = 5000

// RecentGaugeVec is a GaugeVec for things that happen: an event (Set)
// stays EventTTL, then disappears, so Prometheus keeps the history and
// this process keeps only what happened in the last seconds. A state
// (Hold) stays until it is Set or Deleted.
type RecentGaugeVec struct {
	name       string
	help       string
	labelNames []string

	mu     sync.Mutex
	values map[string]recentValue
	now    func() time.Time
	// expiry drops events past EventTTL even when nothing scrapes this
	// process (no Prometheus yet, a scrape outage): otherwise they would
	// pile up to maxRecentSeries and stay. nil while no event is held.
	expiry *time.Timer
}

type recentValue struct {
	v    float64
	at   time.Time
	held bool
}

// NewRecentGaugeVec creates an expiring labeled gauge and registers it.
func NewRecentGaugeVec(name, help string, labelNames ...string) *RecentGaugeVec {
	g := &RecentGaugeVec{name: name, help: help, labelNames: labelNames, values: map[string]recentValue{}, now: time.Now}
	defaultRegistry.register(g)
	return g
}

// Set records an event: v for the label values, for EventTTL.
func (g *RecentGaugeVec) Set(v float64, labelValues ...string) {
	g.update(labelValues, v, false)
}

// Hold records a state: v for the label values, until Set or Delete.
func (g *RecentGaugeVec) Hold(v float64, labelValues ...string) {
	g.update(labelValues, v, true)
}

func (g *RecentGaugeVec) update(labelValues []string, v float64, held bool) {
	if EventTTL() <= 0 {
		return
	}
	key := strings.Join(labelValues, labelSep)
	g.mu.Lock()
	defer g.mu.Unlock()
	g.values[key] = recentValue{v: v, at: g.now(), held: held}
	if !held && g.expiry == nil {
		g.expiry = time.AfterFunc(EventTTL(), g.expire)
	}
	if len(g.values) > maxRecentSeries {
		g.pruneLocked()
		for len(g.values) > maxRecentSeries {
			oldest, at := "", time.Time{}
			for k, rv := range g.values {
				if oldest == "" || rv.at.Before(at) {
					oldest, at = k, rv.at
				}
			}
			delete(g.values, oldest)
		}
	}
}

// Delete drops one label combination right away.
func (g *RecentGaugeVec) Delete(labelValues ...string) {
	key := strings.Join(labelValues, labelSep)
	g.mu.Lock()
	delete(g.values, key)
	g.mu.Unlock()
}

// expire drops expired events, and comes back while any event is left.
func (g *RecentGaugeVec) expire() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.expiry = nil
	g.pruneLocked()
	for _, rv := range g.values {
		if !rv.held {
			ttl := EventTTL()
			if ttl <= 0 {
				ttl = time.Second
			}
			g.expiry = time.AfterFunc(ttl, g.expire)
			return
		}
	}
}

func (g *RecentGaugeVec) pruneLocked() {
	cut := g.now().Add(-EventTTL())
	for k, rv := range g.values {
		if !rv.held && rv.at.Before(cut) {
			delete(g.values, k)
		}
	}
}

func (g *RecentGaugeVec) write(w io.Writer) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.pruneLocked()

	fmt.Fprintf(w, "# HELP %s %s\n", g.name, g.help)
	fmt.Fprintf(w, "# TYPE %s gauge\n", g.name)
	keys := make([]string, 0, len(g.values))
	for k := range g.values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		values := strings.Split(key, labelSep)
		fmt.Fprintf(w, "%s{%s} %s\n", g.name, joinLabels(g.labelNames, values), formatFloat(g.values[key].v))
	}
}
