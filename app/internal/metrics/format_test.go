package metrics

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The text formatting before it was rewritten without fmt, kept to check
// that scrapes come out byte for byte the same.

func refJoinLabels(labelNames, labelValues []string) string {
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

func refWrite(w io.Writer, name, help, typ string, labelNames []string, values map[string]string) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s %s\n", name, typ)
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Fprintf(w, "%s{%s} %s\n", name, refJoinLabels(labelNames, strings.Split(key, labelSep)), values[key])
	}
}

func TestScrapeTextUnchanged(t *testing.T) {
	odd := []string{"", "plain", `quo"te`, `back\slash`, "new\nline", "tab\t", "ünï©ødé", "\x00ctl", "\xff\xfebad", strings.Repeat("long", 2000), "{}=,"}
	labelSets := [][]string{nil, {"node"}, {"node", "image", "result"}}
	for _, names := range labelSets {
		c := &CounterVec{name: "test_total", help: `help with "quotes" and \ backslash`, labelNames: names, counts: map[string]int64{}}
		g := &GaugeVec{name: "test_gauge", help: "gauge help", labelNames: names, values: map[string]float64{}}
		r := &RecentGaugeVec{name: "test_recent", help: "recent", labelNames: names, values: map[string]recentValue{}, now: time.Now}
		cv, gv, rv := map[string]string{}, map[string]string{}, map[string]string{}
		floats := []float64{0, 1, -1, 0.5, 45.6, 1e21, 1.5e-7, 123456789.125}
		for i := 0; i < 400; i++ {
			// fewer values than names, as many, and more
			n := i % (len(names) + 2)
			vals := make([]string, n)
			for j := range vals {
				vals[j] = odd[(i+j)%len(odd)] + strconv.Itoa(i/len(odd))
			}
			key := strings.Join(vals, labelSep)
			c.counts[key] += int64(i * 7)
			cv[key] = strconv.FormatInt(c.counts[key], 10)
			f := floats[i%len(floats)] * float64(i+1)
			g.values[key] = f
			gv[key] = strconv.FormatFloat(f, 'g', -1, 64)
			r.values[key] = recentValue{v: f, at: time.Now(), held: true}
			rv[key] = gv[key]
		}
		for _, tc := range []struct {
			m    metric
			typ  string
			vals map[string]string
		}{{c, "counter", cv}, {g, "gauge", gv}, {r, "gauge", rv}} {
			var got, want bytes.Buffer
			tc.m.write(&got)
			var name, help string
			switch m := tc.m.(type) {
			case *CounterVec:
				name, help = m.name, m.help
			case *GaugeVec:
				name, help = m.name, m.help
			case *RecentGaugeVec:
				name, help = m.name, m.help
			}
			refWrite(&want, name, help, tc.typ, names, tc.vals)
			if !bytes.Equal(got.Bytes(), want.Bytes()) {
				t.Fatalf("%T with labels %v: scrape text changed (%d vs %d bytes)", tc.m, names, got.Len(), want.Len())
			}
			if len(names) == 3 && got.Len() < 2*scrapeChunk {
				t.Fatalf("test should cover several chunks, wrote %d bytes", got.Len())
			}
		}
	}
}

func TestScrapeTextEmptyFamily(t *testing.T) {
	c := &CounterVec{name: "empty_total", help: "h", labelNames: []string{"a"}, counts: map[string]int64{}}
	var got, want bytes.Buffer
	c.write(&got)
	refWrite(&want, "empty_total", "h", "counter", []string{"a"}, nil)
	if got.String() != want.String() {
		t.Fatalf("got %q, want %q", got.String(), want.String())
	}
}
