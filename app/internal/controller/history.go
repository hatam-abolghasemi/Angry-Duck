package controller

import (
	"sort"
	"strings"
	"time"

	"angryduck/internal/metrics"
)

// Per-image history: what happened to each image, node by node. Each is an
// event that stays METRICS_EVENT_TTL_S, long enough to be scraped; Prometheus
// keeps the history (see metrics.RecentGaugeVec). A running spread is a
// state and stays until it ends.
var (
	imageSpreadStarted = metrics.NewRecentGaugeVec(
		"angryduck_controller_image_spread_started_timestamp_seconds",
		"When the image's latest spread started (its preheat), as Unix time.",
		"image",
	)
	imageSpreadSeconds = metrics.NewRecentGaugeVec(
		"angryduck_controller_image_spread_seconds",
		"How long the image's spread took, by result: complete, superseded, expired; running (so far) while it runs.",
		"image", "result",
	)
	imageDelivered = metrics.NewRecentGaugeVec(
		"angryduck_controller_image_delivered_timestamp_seconds",
		"When a node got the image, as Unix time, by how (see angryduck_controller_image_delivery_seconds).",
		"image", "node", "via",
	)
	imageDeliverySeconds = metrics.NewRecentGaugeVec(
		"angryduck_controller_image_delivery_seconds",
		"How long a node took to get the image, from the preheat (or, for a rescue, from when its pull got stuck), by how: present (had it already), peer, registry, peer+registry (spread blobs by path), local (spread with every layer already here), image (whole image from a peer, spread's last resort), seed (pulled whole from the registry), propagate (whole image from a peer), rescue, kubelet (got it without Angry Duck).",
		"image", "node", "via",
	)
	imageReceivedBytes = metrics.NewRecentGaugeVec(
		"angryduck_controller_image_received_bytes",
		"Bytes a node received for the image, by path: registry, peer (spread blobs), image (spread's whole-image fallback), propagate, rescue. Set once the node got the image, or when its spread ended without it.",
		"image", "node", "path",
	)
	imageTransferSeconds = metrics.NewRecentGaugeVec(
		"angryduck_controller_image_transfer_seconds",
		"Seconds a node spent receiving the image, by path, summed over its transfers. Parallel transfers each count. Set with the bytes.",
		"image", "node", "path",
	)
)

func recordSpreadStart(image string, at time.Time) {
	imageSpreadStarted.Set(float64(at.Unix()), image)
	for _, r := range []string{"complete", "superseded", "expired"} {
		imageSpreadSeconds.Delete(image, r)
	}
	imageSpreadSeconds.Hold(0, image, "running")
}

func recordSpreadEnd(image, result string, took time.Duration) {
	imageSpreadSeconds.Delete(image, "running")
	imageSpreadSeconds.Set(took.Seconds(), image, result)
}

func recordDelivery(image, node, via string, since, at time.Time) {
	took := at.Sub(since).Seconds()
	if took < 0 {
		took = 0
	}
	imageDelivered.Set(float64(at.Unix()), image, node, via)
	imageDeliverySeconds.Set(took, image, node, via)
}

func recordTransfer(image, node, path string, bytes int64, seconds float64) {
	if bytes > 0 {
		imageReceivedBytes.Set(float64(bytes), image, node, path)
	}
	if seconds > 0 {
		imageTransferSeconds.Set(seconds, image, node, path)
	}
}

// pathTotals is what a node received for one spread, per path.
type pathTotals map[string]*pathTotal

type pathTotal struct {
	bytes   int64
	seconds float64
}

func (t pathTotals) add(path string, bytes int64, seconds float64) pathTotals {
	if t == nil {
		t = pathTotals{}
	}
	pt := t[path]
	if pt == nil {
		pt = &pathTotal{}
		t[path] = pt
	}
	pt.bytes += bytes
	pt.seconds += seconds
	return t
}

func (t pathTotals) record(image, node string) {
	for path, pt := range t {
		recordTransfer(image, node, path, pt.bytes, pt.seconds)
	}
}

// viaPaths names a spread delivery by the paths its blobs came over.
func viaPaths(paths map[string]bool) string {
	if len(paths) == 0 {
		return "local"
	}
	list := make([]string, 0, len(paths))
	for p := range paths {
		list = append(list, p)
	}
	sort.Strings(list)
	return strings.Join(list, "+")
}
