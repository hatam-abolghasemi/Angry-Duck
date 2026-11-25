package worker

import (
	"sync/atomic"
	"time"

	"angryduck/internal/metrics"
)

// Duration histograms for the stages of getting an image onto a node.
//
// The counters in metrics.go and spread.go say how many and how much;
// these say how long, as a distribution, so a slow registry, a slow disk
// or one unusually slow layer shows up in a quantile instead of vanishing
// into an average. They only record what succeeded: failures and
// cancellations are already counted by the matching counters, and a
// cancelled transfer's duration says nothing about the stage.
//
// Series count is bounded by construction: labels are the node, a small
// fixed vocabulary (path, method, phase) and a three-way size class. Never
// an image, a digest or a layer. Per-image and per-node detail stays in the
// TTL-held event gauges (see docs/observability.md#image-history).
//
// A histogram series appears with its first observation (see
// metrics.HistogramVec); none is created at zero.

// durationBuckets are in seconds. They run from a cached local blob (tens
// of milliseconds) to a multi-GB layer over a slow link (minutes).
var durationBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300}

var (
	blobFetchSeconds = metrics.NewHistogramVec(
		"angryduck_worker_blob_fetch_seconds",
		"Wall time of one successful spread blob transfer into containerd's content store: source connect, download, write and the digest check at commit. By path (registry, peer) and blob size class.",
		durationBuckets, "node", "path", "size_class")

	blobOpenSeconds = metrics.NewHistogramVec(
		"angryduck_worker_blob_open_seconds",
		"Time from asking a source for a blob until it starts answering (headers received, before the first byte is read): registry auth, redirect and queueing, or a peer's response. By path.",
		durationBuckets, "node", "path")

	blobCommitSeconds = metrics.NewHistogramVec(
		"angryduck_worker_blob_commit_seconds",
		"Time containerd took to commit a written blob (digest verification and the rename into the content store). By write mode (api, direct) and blob size class.",
		durationBuckets, "node", "mode", "size_class")

	layerUnpackSeconds = metrics.NewHistogramVec(
		"angryduck_worker_layer_unpack_seconds",
		"Wall time of unpacking one layer into a snapshot, before any commit onto its parent. method: streamed (by this worker while the blob arrived, so it includes waiting for the bytes), landed (by containerd from the landed blob), landed_own (by this worker from the landed blob). Successful unpacks only.",
		durationBuckets, "node", "method", "size_class")

	layerCommitSeconds = metrics.NewHistogramVec(
		"angryduck_worker_layer_commit_seconds",
		"Time to attach an already unpacked layer onto the layer below it and commit its snapshot. Only layers unpacked on their own and committed later.",
		durationBuckets, "node", "size_class")

	snapshotReceiveSeconds = metrics.NewHistogramVec(
		"angryduck_worker_snapshot_receive_seconds",
		"Time to receive one layer as a snapshot from a peer and commit it locally. The tar is streamed straight into the unpack, so transfer and apply are one number here. This is the path for layers a peer holds unpacked but without the blob (containerd's discard_unpacked_layers); it is not in blob_fetch_seconds. size_class is the bytes sent over the wire.",
		durationBuckets, "node", "size_class")

	spreadFinalizeSeconds = metrics.NewHistogramVec(
		"angryduck_worker_spread_finalize_seconds",
		"Time spent in each phase of registering a spread image: drain (waiting for layers still unpacking as they arrive), hold (checking and pinning the layers), import (containerd import and unpack under a temporary name, then publishing the real name). Successful finalizes only.",
		durationBuckets, "node", "phase")

	importSeconds = metrics.NewHistogramVec(
		"angryduck_worker_import_seconds",
		"Time in the two parts of a containerd image import, for every import this worker makes (spread, rescue, repair): register (reading the archive and creating the temporary image) and unpack (unpacking the layers that have no snapshot yet, then publishing the real name).",
		durationBuckets, "node", "phase")

	spreadReadySeconds = metrics.NewHistogramVec(
		"angryduck_worker_spread_ready_seconds",
		"Time from the first transfer or layer check this node made for a spread until its image name was registered, i.e. the node-local time to image ready.",
		durationBuckets, "node")

	unpackSlots = metrics.NewGaugeVec(
		"angryduck_worker_unpack_slots",
		"Layer unpack slots on this node across spreads: in_use now, and max (SPREAD_UNPACK_PARALLEL_MAX). in_use at max while layers wait is serialization.",
		"node", "state")
)

// histNode labels every histogram with the node, as the counters are.
// Set once at startup (SetMetricsNode); histograms are written from code
// that has no node ID of its own (the containerd store).
var histNode atomic.Pointer[string]

// SetMetricsNode sets the node label of the duration histograms.
func SetMetricsNode(id string) { histNode.Store(&id) }

func hnode() string {
	if p := histNode.Load(); p != nil {
		return *p
	}
	return ""
}

// Size classes bound what a histogram's size label can take. They follow
// the cost profile: small layers are dominated by round trips, large ones
// by throughput, which is what a quantile per class should separate.
const (
	sizeSmallMax  = 8 << 20  // below: "small"
	sizeMediumMax = 64 << 20 // below: "medium"; at or above: "large"
)

func sizeClass(n int64) string {
	switch {
	case n < sizeSmallMax:
		return "small"
	case n < sizeMediumMax:
		return "medium"
	default:
		return "large"
	}
}

func observeSince(h *metrics.HistogramVec, start time.Time, labels ...string) {
	h.Observe(time.Since(start).Seconds(), append([]string{hnode()}, labels...)...)
}

func observeBlobFetch(path string, size int64, took time.Duration) {
	blobFetchSeconds.Observe(took.Seconds(), hnode(), path, sizeClass(size))
}

func observeBlobOpen(path string, start time.Time) { observeSince(blobOpenSeconds, start, path) }

func observeBlobCommit(mode string, size int64, start time.Time) {
	observeSince(blobCommitSeconds, start, mode, sizeClass(size))
}

// observeUnpack records one successful layer unpack. size is the layer's
// compressed blob size.
func observeUnpack(method string, size int64, start time.Time) {
	observeSince(layerUnpackSeconds, start, method, sizeClass(size))
}

func observeLayerCommit(size int64, start time.Time) {
	observeSince(layerCommitSeconds, start, sizeClass(size))
}

func observeSnapshotReceive(wireBytes int64, start time.Time) {
	observeSince(snapshotReceiveSeconds, start, sizeClass(wireBytes))
}

func observeFinalizePhase(phase string, start time.Time) {
	observeSince(spreadFinalizeSeconds, start, phase)
}

func observeImport(phase string, start time.Time) { observeSince(importSeconds, start, phase) }
