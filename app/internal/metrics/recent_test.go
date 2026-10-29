package metrics

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestRecentGaugeVecEventsExpireStatesStay(t *testing.T) {
	defer SetEventTTL(EventTTL())
	SetEventTTL(15 * time.Second)
	now := time.Unix(1000, 0)
	g := NewRecentGaugeVec("test_recent_expires", "a test", "image", "result")
	g.now = func() time.Time { return now }
	g.Set(3, "img:1", "complete")
	g.Hold(7, "img:2", "running")

	var buf strings.Builder
	g.write(&buf)
	if !strings.Contains(buf.String(), `test_recent_expires{image="img:1",result="complete"} 3`) {
		t.Fatalf("missing event:\n%s", buf.String())
	}

	now = now.Add(20 * time.Second)
	buf.Reset()
	g.write(&buf)
	out := buf.String()
	if strings.Contains(out, `image="img:1"`) {
		t.Fatalf("event older than the TTL should be gone:\n%s", out)
	}
	if !strings.Contains(out, `test_recent_expires{image="img:2",result="running"} 7`) {
		t.Fatalf("held state should stay:\n%s", out)
	}

	g.Delete("img:2", "running")
	buf.Reset()
	g.write(&buf)
	if strings.Contains(buf.String(), `image="img:2"`) {
		t.Fatalf("deleted state still shown:\n%s", buf.String())
	}
}

func TestRecentGaugeVecOffAndCapped(t *testing.T) {
	defer SetEventTTL(EventTTL())
	SetEventTTL(0)
	g := NewRecentGaugeVec("test_recent_capped", "a test", "image")
	g.Set(1, "img")
	g.Hold(1, "img")
	if len(g.values) != 0 {
		t.Fatal("events off must record nothing")
	}

	SetEventTTL(time.Hour)
	now := time.Unix(1000, 0)
	g.now = func() time.Time { return now }
	for i := 0; i < maxRecentSeries+10; i++ {
		now = now.Add(time.Millisecond)
		g.Set(1, fmt.Sprintf("img-%d", i))
	}
	if len(g.values) != maxRecentSeries {
		t.Fatalf("got %d series, want the cap %d", len(g.values), maxRecentSeries)
	}
	if _, ok := g.values["img-0"]; ok {
		t.Fatal("the oldest series should go first")
	}
}

func TestRecentEventsExpireWithoutScrapes(t *testing.T) {
	old := EventTTL()
	SetEventTTL(50 * time.Millisecond)
	defer SetEventTTL(old)
	g := &RecentGaugeVec{name: "x", labelNames: []string{"image"}, values: map[string]recentValue{}, now: time.Now}
	g.Set(1, "a")
	g.Hold(2, "b")
	deadline := time.Now().Add(2 * time.Second)
	for {
		g.mu.Lock()
		_, a := g.values["a"]
		_, b := g.values["b"]
		g.mu.Unlock()
		if !a {
			if !b {
				t.Fatal("a held state expired")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("event still held long after its TTL, with no scrape")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
