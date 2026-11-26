# Code map

Where things live. Paths are under `app/`. For the directory tree and build
commands, see [Development](../development.md#layout).

## The two binaries

### Controller: `cmd/controller/main.go`

Reads the configuration and the [feature switches](#feature-switches), builds
the parts that are on, and starts one goroutine per loop:

| Loop | File | What it does |
|---|---|---|
| ranker | `internal/controller/ranker.go` | Picks seed nodes for whole-image preheats (the fallback when a spread can't be planned) and sends `/pull` orders. |
| spreader | `internal/controller/spreader.go` | Plans and runs spreads: which node fetches which blob from where, streaming chains, the slow-chain guard, finalize. The largest file; most delivery behavior is here. |
| propagator | `internal/controller/propagator.go` | Whole-image propagation from node to node, for images a spread didn't cover. |
| rescuer + pod watch | `internal/controller/rescuer.go`, `internal/kube/podwatch.go` | Watches Pending pods, finds stuck pulls, picks a source node, orders a rescue. |
| retainer | `internal/controller/retainer.go` | Picks the node that keeps each image's retention copy, labels it, re-picks when the copy is lost. |
| companions | `internal/controller/companions.go` | Finds sidecar images of app pods and keeps them on every node. |
| origin | `internal/controller/origin.go` | Asks each registry whether it still serves the images the nodes hold. |
| server | `internal/controller/server.go` | HTTP: `/webhook/preheat`, `/report`, `/spread/done`, `/layers/holders`, `/status`, `/metrics`, `/healthz`. See [API](../api.md). |

Shared state:

- `state.go`: the **worker registry**. One `workerEntry` per node, built from
  reports; liveness is the controller's own clock at receipt
  (`WORKER_STALE_AFTER_S`).
- `internal/layerindex`: which node holds which blob and snapshot, from the
  workers' layer inventory deltas. The mirror and every planner ask it.
- `history.go`: per-image events (spread started, delivered, received
  bytes), published as short-lived gauges for Prometheus to keep
  ([image history](../observability.md#image-history)).
- `tags.go`: the newest copy of a tag on the nodes, for mirrors whose
  registry can't answer.
- `wake.go`: coalesces "something finished" signals into one early tick
  (`WAKE_COALESCE_MS`), so loops react to events instead of waiting for
  their interval.

### Worker: `cmd/worker/main.go`

Connects to containerd, builds `ContainerdStore`, then starts what is on:

| Part | File | What it does |
|---|---|---|
| reporter | `internal/worker/reporter.go` | Disk use (statfs on containerd's content store), image list and layer inventory deltas to the controller, every `REPORT_INTERVAL_S` and on every containerd event. |
| inventory watch | `internal/worker/ctdevents.go` | Follows containerd's events; falls back to periodic scans if the stream breaks. |
| puller | `internal/worker/puller.go` | Seed pulls ordered by the controller (`/pull`, `/pull/cancel`), through CRI, as kubelet would pull. |
| spread | `internal/worker/spread.go` and `spread*.go` | Receives blobs from the registry or a peer, serves them to peers, finalizes images. `spreadstream.go` serves blobs still arriving, and the rest of a blob from a byte on (`Range`); `spreadguard.go` decides on restarts from a faster source; `resume.go` has a transfer go on from what a stopped one left (the store's `WriteBlobFrom`), restarts and peer copies take a running transfer's ingest over, and counts the bytes resumed and thrown away; `spreadunpack.go`, `spreadparallel.go` unpack layers as they land, one at a time or side by side; `streamunpack.go` unpacks while the blob still arrives; `spreadwhole.go` has every layer of an image do so at once, and `attach.go` attaches a layer unpacked on its own onto the layer below. `histograms.go` has the duration histograms of every stage; the store records unpack and commit times itself, where each starts and ends, told by its caller through the context whether a stream unpack reads a blob still arriving and who an import is for. |
| mirror | `internal/worker/mirror.go` | The registry mirror containerd asks first (`MIRROR_LISTEN_ADDR`): serves local blobs, then peers' (holders from the controller), resolves tags, writes `hosts.toml`. |
| rescue | `internal/worker/rescue.go` | Copies an image from a peer: missing blobs, or snapshots as a last resort. |
| repair | `internal/worker/imagerepair.go` | Fixes broken images in place, piece by piece, peers first. |
| cleanup | `internal/worker/imagegc.go` | Removes unused images by age and under disk pressure, keeps retention copies and companions. |
| pins | `internal/worker/pins.go` | Snapshots pinned for a retried rescue; cleans up what a crashed worker left. |
| preheat monitor | `internal/worker/preheatmonitor.go` | Counts running containers of preheated repos. |

Underneath:

- `containerdstore.go`: **every call to containerd's own gRPC API** goes
  through it: content, images, snapshots, leases, diffs; and direct reads of
  the content store when it is mounted (`openBlob`). CRI calls (pulls,
  container listings) go through `runtime.go`.
- `directwrite.go`: writes received bytes straight into containerd's ingest
  files, with their page cache handed back as it goes.
- `peerconns.go`: the transport every client to other workers uses (spread,
  repair, rescue, the mirror): idle connections closed after
  `IDLE_CONN_TIMEOUT_S`, each one counted (`peer_connections`), and the API
  server's side counted too. Each feature closes its idle connections when
  its work ends; a new client to peers uses `peerTransport` and does the
  same.
- `pagecache.go`: hands back the page cache that serving blob files brings
  in (`PAGE_CACHE_RELEASE_AFTER_S`).
- `leanlist.go`, `leancri.go`, `listing.go`, `inventory.go`: cheap listings,
  one full listing at a time, shared for a few seconds.
- `runtime.go`: the CRI side (pulls, container listings).
- `containerdcalls.go`: counts every containerd call
  (`angryduck_worker_containerd_calls_total`).

## Shared packages

| Package | Holds |
|---|---|
| `model` | Every wire type between controller and workers. Change both sides together. |
| `blobship` | Shipping one image's missing blobs (and snapshots) from a node that has them; image health; an in-memory store for tests. |
| `registryclient`, `registryauth` | Manifest and config reads, with pull-secret credentials. |
| `imageref` | Image reference normalization (`nginx` → `docker.io/library/nginx:latest`), `Repo()`. |
| `features` | The switch tree. |
| `config` | `.env` loading and typed getters; values that can't be used fall back to the default with a warning. |
| `metrics` | A small Prometheus registry: counters, gauges, "recent" gauges for events, process memory. |
| `memlimit` | GOMEMLIMIT from the cgroup, GOGC 50, freed heap and uncollected garbage handed back after bursts, the cgroup's page cache and file records handed back after stream unpacks (`reclaim.go`, `memory.reclaim`). |
| `sharedtoken` | The bearer token between controller and workers. |
| `kube` | The smallest Kubernetes client: pod list and watch, the worker's DaemonSet. |

## Feature switches

`internal/features/features.go` holds the tree; a child is on only when its
parent is. Turning a switch off must leave **no goroutine, timer, endpoint or
metric series** of that feature behind. `main.go` of each binary is where
that is decided: look for `feat.` there.

```text
GC_ENABLED                        cleanup
  GC_RETENTION_ENABLED            retention copies
  COMPANION_ENABLED               companion images
SPREAD_ENABLED                    blob-by-blob spreading
  SPREAD_STREAM_ENABLED           serving blobs still arriving
  SPREAD_RESUME_ENABLED           going on from what a stopped transfer left
  SPREAD_BOTTOM_UP_ENABLED        lowest layers first
  SPREAD_UNPACK_ENABLED           unpacking as layers land
    SPREAD_UNPACK_STREAM_ENABLED    ... while they arrive (off by default)
    SPREAD_UNPACK_PARALLEL_ENABLED  ... side by side (containerd 2.2+)
PROPAGATE_ENABLED                 whole-image propagation
RESCUE_ENABLED                    rescue
MIRROR_ENABLED                    mirror
  MIRROR_RESOLVE_ENABLED          tag resolution
IMAGE_REPAIR_ENABLED              repair
LAYER_INVENTORY_ENABLED           layer inventory reports
INVENTORY_EVENTS_ENABLED          containerd events
```

## Outside `app/`

| Path | Holds | Kept in step with |
|---|---|---|
| `helm/angryduck/` | The chart. | `k8s/`: CI fails if the default render differs. |
| `k8s/` | Plain manifests. | The chart; image tags = the release. |
| `grafana/angryduck-dashboard.json` | The dashboard (schema v2, Grafana 13). | `docs/observability.md`. |
| `bench/` | The pull-time and chaos bench kit. | Its own README. |
| `docs/` | Everything you read. | The code, in the same commit. |
