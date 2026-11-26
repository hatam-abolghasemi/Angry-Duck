package worker

import (
	"context"
	"strings"
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
// Series count is bounded by construction: labels are the node, small
// fixed vocabularies (path, mode, method, compression, stage, kind, caller,
// phase) and a three-way size class. Never an image, a digest or a layer. Per-image and per-node detail stays in the
// TTL-held event gauges (see docs/observability.md#image-history).
//
// The series of every stage this node can go through are created at zero
// at startup (InitTimings): spreads come in bursts, and an image is
// finalized once per node, so a series born holding its first
// observations would never show them in rate() or increase(). Stages of
// features that are off get no series.

// durationBuckets are in seconds. They run from a cached local blob (tens
// of milliseconds) to a multi-GB layer over a slow link (minutes).
var durationBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300}

var (
	blobFetchSeconds = metrics.NewHistogramVec(
		"angryduck_worker_blob_fetch_seconds",
		"Wall time of one successful spread blob transfer into containerd's content store, the whole blob from byte 0: source connect, download, write and the digest check at commit. A transfer that went on from bytes an earlier one left (resumed) isn't in it. By path (registry, peer) and blob size class.",
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
		"Wall time of turning one layer blob into files in a snapshot: decompressing it and writing its files, nothing else (preparing the snapshot and committing it are not in it; commits are layer_commit_seconds). method: landed (containerd, from the landed blob), streamed (this worker, while the blob arrived, so it includes waiting for the bytes), landed_own (this worker, from the landed blob). compression: gzip, zstd, none or other, from the blob's media type. size_class is the compressed blob size. Successful unpacks only.",
		durationBuckets, "node", "method", "compression", "size_class")

	layerUnpackStageSeconds = metrics.NewHistogramVec(
		"angryduck_worker_layer_unpack_stage_seconds",
		"For each layer this worker unpacks itself (methods streamed and landed_own), where its unpack time went, one observation per stage per layer. The stages run side by side, so they overlap and don't add up to the unpack time: the stage closest to the unpack time is the bottleneck. stage: input (waiting for the blob's bytes: the network or peer while it arrives, the disk once landed), decompress (decompressing, waiting on neither side), write (writing files, waiting on neither side). Not available for layers containerd unpacks (method landed). Successful unpacks only.",
		durationBuckets, "node", "stage", "compression")

	layerCommitSeconds = metrics.NewHistogramVec(
		"angryduck_worker_layer_commit_seconds",
		"Time to commit one unpacked layer's snapshot under its chainID. kind: in_place (unpacked onto the layer below, committed right after), onto_parent (unpacked on its own by containerd, committed onto the layer below), attach (unpacked on its own by this worker, its files moved onto the layer below, then committed). Successful commits only.",
		durationBuckets, "node", "kind")

	snapshotReceiveSeconds = metrics.NewHistogramVec(
		"angryduck_worker_snapshot_receive_seconds",
		"Time to receive one layer as a snapshot from a peer and commit it locally. The tar is streamed straight into the unpack, so transfer and apply are one number here. This is the path for layers a peer holds unpacked but without the blob (containerd's discard_unpacked_layers); it is not in blob_fetch_seconds. size_class is the bytes sent over the wire.",
		durationBuckets, "node", "size_class")

	spreadFinalizeSeconds = metrics.NewHistogramVec(
		"angryduck_worker_spread_finalize_seconds",
		"Time spent in the phases of registering a spread image before containerd's import (the import itself is import_seconds{caller=\"spread\"}): drain (waiting for layers still unpacking as they arrived; only when one was), hold (checking and pinning the layers). Successful finalizes only.",
		durationBuckets, "node", "phase")

	importSeconds = metrics.NewHistogramVec(
		"angryduck_worker_import_seconds",
		"Time in each part of a containerd image import this worker makes: register (reading the archive and creating the image under a temporary name), unpack (containerd unpacking the layers that have no snapshot yet; near zero when they were unpacked ahead), publish (pointing the real name at it). caller: spread, rescue, repair. Successful parts only.",
		durationBuckets, "node", "caller", "phase")

	spreadReadySeconds = metrics.NewHistogramVec(
		"angryduck_worker_spread_ready_seconds",
		"Time from the first transfer or layer check this node made for a spread until its image name was registered, i.e. the node-local time to image ready. A spread whose job here was forgotten before it finished (idle for longer than its lease lives) is not recorded.",
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

// compressionOf names a layer blob's compression from its media type, as
// the unpack histograms label it: gzip, zstd, none (a plain tar) or other.
func compressionOf(mediaType string) string {
	switch {
	case strings.HasSuffix(mediaType, "+zstd") || strings.HasSuffix(mediaType, ".zstd"):
		return "zstd"
	case strings.HasSuffix(mediaType, "+gzip") || strings.HasSuffix(mediaType, ".gzip"):
		return "gzip"
	case strings.HasSuffix(mediaType, ".tar"):
		return "none"
	default:
		return "other"
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

// Unpack methods, as layer_unpack_seconds labels them.
const (
	unpackLanded    = "landed"     // containerd, from the landed blob
	unpackStreamed  = "streamed"   // this worker, while the blob arrives
	unpackLandedOwn = "landed_own" // this worker, from the landed blob
)

type unpackMethodKey struct{}

// withUnpackMethod tells the store's stream unpack (ApplyLayerStream,
// ExtractLayerStream) whether the reader it is given is a blob still
// arriving (unpackStreamed, the default) or one that has landed
// (unpackLandedOwn): only the caller knows, and only the store knows where
// the unpack itself starts and ends.
func withUnpackMethod(ctx context.Context, method string) context.Context {
	return context.WithValue(ctx, unpackMethodKey{}, method)
}

func streamUnpackMethod(ctx context.Context) string {
	if m, ok := ctx.Value(unpackMethodKey{}).(string); ok {
		return m
	}
	return unpackStreamed
}

// observeUnpack records one successful layer unpack (decompress and write
// only). Its size class is the layer's compressed blob size.
func observeUnpack(method string, l LayerSpec, took time.Duration) {
	layerUnpackSeconds.Observe(took.Seconds(), hnode(), method, compressionOf(l.MediaType), sizeClass(l.Size))
}

// unpackStages is where one worker-run unpack spent its time (see
// streamInto). Stage times overlap: the stages run side by side.
type unpackStages struct {
	input, decompress, write time.Duration
}

func observeUnpackStages(l LayerSpec, st unpackStages) {
	c := compressionOf(l.MediaType)
	n := hnode()
	layerUnpackStageSeconds.Observe(st.input.Seconds(), n, "input", c)
	layerUnpackStageSeconds.Observe(st.decompress.Seconds(), n, "decompress", c)
	layerUnpackStageSeconds.Observe(st.write.Seconds(), n, "write", c)
}

// Layer commit kinds, as layer_commit_seconds labels them.
const (
	commitInPlace    = "in_place"
	commitOntoParent = "onto_parent"
	commitAttach     = "attach"
)

func observeLayerCommit(kind string, start time.Time) {
	observeSince(layerCommitSeconds, start, kind)
}

func observeSnapshotReceive(wireBytes int64, start time.Time) {
	observeSince(snapshotReceiveSeconds, start, sizeClass(wireBytes))
}

// Import callers, as import_seconds labels them.
const (
	importBySpread = "spread"
	importByRescue = "rescue"
	importByRepair = "repair"
)

type importCallerKey struct{}

// withImportCaller names who an import is for, for import_seconds: the
// store's Import is shared by spread, rescue and repair.
func withImportCaller(ctx context.Context, caller string) context.Context {
	return context.WithValue(ctx, importCallerKey{}, caller)
}

func importCaller(ctx context.Context) string {
	if c, ok := ctx.Value(importCallerKey{}).(string); ok {
		return c
	}
	return "other"
}

func observeImport(ctx context.Context, phase string, start time.Time) {
	observeSince(importSeconds, start, importCaller(ctx), phase)
}

// TimingSeries is what this node can do, as far as the duration
// histograms care: InitTimings creates the series of those stages, and
// only those, at zero.
type TimingSeries struct {
	Spread         bool // blobs from the registry and peers, finalize, ready
	Unpack         bool // layers unpacked as they land (SPREAD_UNPACK_ENABLED)
	StreamUnpack   bool // and while they arrive, by this worker
	ParallelUnpack bool // landed layers side by side (containerd 2.2+)
	DirectWrites   bool // blobs straight into containerd's ingest
	Rescue         bool
	Repair         bool
}

// Compressions a layer series is created for up front: what images are
// made of. "other" appears with its first observation.
var (
	initCompressions   = []string{"gzip", "zstd", "none"}
	streamCompressions = []string{"gzip", "none"} // zstd isn't unpacked by the worker
	sizeClasses        = []string{"small", "medium", "large"}
)

// InitTimings creates at zero the duration series of every stage this
// node can go through (see the top of this file). Call it once, after
// SetMetricsNode, with what the worker turned on.
func InitTimings(t TimingSeries) {
	n := hnode()
	if t.Spread {
		for _, path := range []string{"registry", "peer"} {
			blobOpenSeconds.Init(n, path)
			for _, sc := range sizeClasses {
				blobFetchSeconds.Init(n, path, sc)
			}
		}
		spreadFinalizeSeconds.Init(n, "hold")
		spreadReadySeconds.Init(n)
		initImport(n, importBySpread)
	}
	if t.Spread || t.Rescue || t.Repair {
		modes := []string{"api"}
		if t.DirectWrites {
			modes = append(modes, "direct")
		}
		for _, m := range modes {
			for _, sc := range sizeClasses {
				blobCommitSeconds.Init(n, m, sc)
			}
		}
	}
	if t.Spread && t.Unpack {
		for _, c := range initCompressions {
			for _, sc := range sizeClasses {
				layerUnpackSeconds.Init(n, unpackLanded, c, sc)
			}
		}
		layerCommitSeconds.Init(n, commitInPlace)
		spreadFinalizeSeconds.Init(n, "drain")
		if t.StreamUnpack {
			for _, c := range streamCompressions {
				for _, m := range []string{unpackStreamed, unpackLandedOwn} {
					for _, sc := range sizeClasses {
						layerUnpackSeconds.Init(n, m, c, sc)
					}
				}
				for _, st := range []string{"input", "decompress", "write"} {
					layerUnpackStageSeconds.Init(n, st, c)
				}
			}
			layerCommitSeconds.Init(n, commitAttach)
		}
		if t.ParallelUnpack {
			layerCommitSeconds.Init(n, commitOntoParent)
		}
	}
	if t.Rescue {
		for _, sc := range sizeClasses {
			snapshotReceiveSeconds.Init(n, sc)
		}
		initImport(n, importByRescue)
	}
	if t.Repair {
		initImport(n, importByRepair)
	}
}

func initImport(node, caller string) {
	for _, p := range []string{"register", "unpack", "publish"} {
		importSeconds.Init(node, caller, p)
	}
}
