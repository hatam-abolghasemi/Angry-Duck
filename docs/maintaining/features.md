# Features, end to end

For each feature: where it starts, what state it keeps, how it shows, how it
fails, and the tests that hold it. What it does for a cluster is in the
feature's own page; this is the view from the code.

## Spread (`SPREAD_ENABLED`)

The main path: a pushed image reaches every eligible node blob by blob.

- **Starts:** `POST /webhook/preheat` (`controller/server.go`) →
  `spreadstart.go` reads the manifest and config in the background, with
  retries; a newer tag of the same repo supersedes a start still running.
- **Runs:** `controller/spreader.go` plans per blob: who fetches it from
  the registry (a few seeds, rarest layers first, lowest layers first with
  `SPREAD_BOTTOM_UP_ENABLED`), who gets it from which peer, how deep a
  streaming chain may go. Orders go to `POST /spread/fetch` on workers;
  workers answer `POST /spread/done`. The last step, `POST /spread/finalize`,
  hands over the image's metadata so the worker registers the image.
- **Worker side:** `worker/spread.go` accepts a fetch (or says the blob is
  here, or that it is already coming), writes it into containerd
  (`directwrite.go`), serves it to peers while it still arrives
  (`spreadstream.go`), and unpacks layers as they land
  (`spreadunpack.go`, `spreadparallel.go`, `streamunpack.go`,
  `spreadwhole.go`, `attach.go`).
- **State:** the controller holds each running spread in memory; a spread
  that runs past its time expires. A worker holds a job's blobs under a
  containerd lease until finalize, or until the lease expires.
- **Shows:** Delivery and Image tabs; `angryduck_controller_spread*`,
  `angryduck_worker_spread*`, image history (`image_delivered`, `received_bytes`).
- **Fails:** a node that can't get a blob is left to kubelet (it pulls as it
  would have); a transfer slower than expected is restarted from a faster
  source (`spreadguard.go`); a node fuller than `PROPAGATE_MAX_UTILIZATION`
  gets nothing new.
- **Tests:** `controller/spreader*_test.go`, `worker/spread*_test.go`
  (`spread_e2e_test.go` runs whole spreads over HTTP), and the containerd
  integration tests (`containerd_spread_parallel_integration_test.go`,
  `containerd_stream*_integration_test.go`, `containerd_unpack_integration_test.go`).

### Unpacking as layers land

With `SPREAD_UNPACK_ENABLED`, a layer is unpacked as soon as it and the one
below it are here, so finalize has little left to do. Side by side
(`SPREAD_UNPACK_PARALLEL_ENABLED`) where containerd's snapshotter can rebase
(2.2+), one at a time otherwise. While the blob still arrives
(`SPREAD_UNPACK_STREAM_ENABLED`, opt-in) the worker writes the snapshot
itself: see [stream-unpack](../stream-unpack.md) for what that needs.

A layer is tried once even if the layer below isn't known to this spread:
the layer below is usually on the node already (a shared base). If it isn't,
containerd says so before anything is read (`errParentMissing`), and the
layer waits for it. Results: `angryduck_worker_spread_unpacks_total{result}`.

## Seeds and whole-image propagation (`PROPAGATE_ENABLED`)

The fallback for pushes whose manifest can't be read, and for images a
spread didn't cover.

- **Seeds:** `controller/ranker.go` picks the nodes that hold most of the
  image (and have room) and sends `POST /pull`; `worker/puller.go` pulls
  through CRI, as kubelet would.
- **Propagation:** `controller/propagator.go` copies whole images from node
  to node, `PROPAGATE_PER_SOURCE` at a time from each source.
- **Shows:** Delivery tab, Seeds and Propagation rows.
- **Tests:** `controller/ranker_test.go`, `propagator_test.go`,
  `worker/puller_test.go`.

## Mirror (`MIRROR_ENABLED`)

containerd's own pulls, kubelet's included, ask the worker first.

- **Starts:** the worker writes `hosts.toml` for each registry
  (`MIRROR_REGISTRIES`, and the ones it sees) and listens on
  `MIRROR_LISTEN_ADDR`.
- **Serves:** blobs and manifests this node holds; then a peer's, from the
  holders the controller lists (`GET /layers/holders`), including blobs
  still arriving there; else `404` and containerd goes to the registry.
- **Tags** (`MIRROR_RESOLVE_ENABLED`): the registry is asked first, with a
  short timeout; when it can't answer (down, refused, tag gone), the newest
  copy on the nodes is served (`controller/tags.go`). Real pulls stay with
  containerd.
- **Fails:** a controller that doesn't answer is left alone for a growing
  wait (`MIRROR_CONTROLLER_BACKOFF_MIN_MS` to `_MAX_MS`); a registry that
  doesn't answer, for `MIRROR_RESOLVE_UNREACHABLE_TTL_S`.
- **Shows:** Mirror & Registry tab; `angryduck_worker_mirror_*`.
- **Tests:** `worker/mirror*_test.go`.

## Rescue (`RESCUE_ENABLED`)

A pod stuck on a pull gets the image from a node that has it.

- **Starts:** the controller's pod watch (`kube/podwatch.go`) sees a
  container waiting on `ImagePullBackOff` or `ErrImagePull`;
  `controller/rescuer.go` finds its cause (registry missing the image,
  unreachable, refusing the pod's secret) and a source node.
- **Runs:** `worker/rescue.go` on the stuck node fetches the missing blobs
  from the source; only when no node has the blobs does it rebuild the image
  from a running container's snapshots.
- **Fails:** no source, or the copy fails: counted, retried after
  `RESCUE_RETRY_AFTER_S`, with failures in a row shown per node and image.
- **Shows:** Rescue & Repair tab; `angryduck_controller_rescue*`,
  `angryduck_worker_rescue*`; the Problems annotation.
- **Tests:** `controller/rescuer_test.go`, `worker/rescue_test.go`, the
  integration tests (a full rescue that ships a snapshot).

## Repair (`IMAGE_REPAIR_ENABLED`)

An image registered on a node but unable to start a container is fixed in
place: each broken piece (a blob, its metadata) is fetched on its own, from
peers first, then the registry. No whole-image deletion, no hashing passes.
If both fail, the image stays, is reported, and is retried on the next pass.

- **Code:** `worker/imagerepair.go`; image health in `blobship/health.go`.
- **Shows:** Rescue & Repair tab, Repair row; `angryduck_worker_broken_image`.
- **Tests:** `worker/imagerepair_test.go`,
  `containerd_repair_integration_test.go`.

## Cleanup (`GC_ENABLED`)

- **Code:** `worker/imagegc.go`. An image no container used for
  `GC_UNUSED_FOR_S` goes; above `GC_HIGH_UTILIZATION`, unused images go
  oldest first until `GC_LOW_UTILIZATION`. Images of running or crash-looping
  containers stay. When removals stop freeing space, cleanup stalls instead
  of removing more.
- **Retention copies** (`GC_RETENTION_ENABLED`): `controller/retainer.go`
  labels one copy of each image the cluster stopped running, on the node
  with the most room, for `GC_RETENTION_TTL_S`; the worker never removes it
  before it expires, disk pressure included.
- **Companions** (`COMPANION_ENABLED`): `controller/companions.go` finds
  sidecar images of app pods; workers keep them, removed last under pressure.
- **State:** each worker keeps image usage times in a small file under
  `RESCUE_STATE_DIR`, so a restart doesn't make every image look unused.
- **Shows:** Disk Cleanup tab; the Image tab's *On the nodes* row.
- **Tests:** `worker/imagegc_test.go`, `controller/retainer_test.go`,
  `companions_test.go` in both packages.

## Reports and inventory

- `worker/reporter.go` sends disk use, images and layer inventory deltas;
  `worker/ctdevents.go` follows containerd's events
  (`INVENTORY_EVENTS_ENABLED`) and kicks a report; `worker/layers.go` keeps
  the inventory.
- The controller's `state.go` and `layerindex` hold it. After a controller
  restart, workers resend their full inventory once.
- **Shows:** Overview's node table; Runtime tab, inventory events and the
  layer index.
- **Tests:** `worker/reporter_*_test.go`, `layers*_test.go`,
  `controller/inventory_test.go`, `layers_report_test.go`.
