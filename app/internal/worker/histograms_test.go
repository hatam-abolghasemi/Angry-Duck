package worker

import (
	"testing"
	"time"
)

func TestSizeClass(t *testing.T) {
	for _, c := range []struct {
		n    int64
		want string
	}{
		{0, "small"},
		{sizeSmallMax - 1, "small"},
		{sizeSmallMax, "medium"},
		{sizeMediumMax - 1, "medium"},
		{sizeMediumMax, "large"},
		{5 << 30, "large"},
	} {
		if got := sizeClass(c.n); got != c.want {
			t.Errorf("sizeClass(%d) = %q, want %q", c.n, got, c.want)
		}
	}
}

// The helpers must not panic with the label counts the histograms were
// declared with (a mismatch would only show at scrape time otherwise).
func TestObserveHelpersMatchLabelCounts(t *testing.T) {
	SetMetricsNode("test-node")
	start := time.Now().Add(-time.Second)
	observeBlobFetch("registry", 10<<20, time.Second)
	observeBlobOpen("peer", start)
	observeBlobCommit("api", 1<<20, start)
	observeUnpack("streamed", 100<<20, start)
	observeLayerCommit(1<<20, start)
	observeFinalizePhase("hold", start)
	observeImport("unpack", start)
	observeSince(spreadReadySeconds, start)
	if hnode() != "test-node" {
		t.Errorf("hnode() = %q", hnode())
	}
}
