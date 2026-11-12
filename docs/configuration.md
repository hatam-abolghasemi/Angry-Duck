# Configuration

Angry Duck is configured entirely through environment variables. In
Kubernetes they come from the ConfigMap (or the chart's `config` values).
Locally they can come from a `.env` file (path set by `ENV_FILE`, default
`.env`), and real environment variables always win over the file.
[`app/.env.example`](../app/.env.example) lists every setting with its
default.

- Durations are integers in seconds. Utilizations are fractions between 0
  and 1.
- An empty or unparsable value means the default.
- Each binary ignores the settings it doesn't use, so both share one
  ConfigMap.

Looking for a starting point rather than a reference? See
[Recipes](recipes.md).

## Feature switches

Every feature has a switch, all on by default. A feature runs only when every
feature it belongs to is on: turn a parent off and everything beneath it is
off too, whatever its own switch says. Both binaries resolve the same tree
from the same ConfigMap and log it at startup (`features: gc=true
gc_retention=true ...`), with a line for each feature that is off only because
its parent is. A feature that is off starts nothing: no goroutine, no timer,
no endpoint, no metric series of its own.

```text
GC_ENABLED                  image cleanup on each node
├─ GC_RETENTION_ENABLED     one retention copy of every image that went unused
└─ COMPANION_ENABLED        sidecar images of app pods kept on every node
SPREAD_ENABLED              blob-by-blob spreading
├─ SPREAD_STREAM_ENABLED    serving blobs to peers while still receiving them
└─ SPREAD_UNPACK_ENABLED    unpacking each layer as soon as it and the layers below are here
PROPAGATE_ENABLED           whole-image propagation
RESCUE_ENABLED              rescue of pods stuck on pulls
MIRROR_ENABLED              the pull-only registry mirror
└─ MIRROR_RESOLVE_ENABLED   resolving tags, the newest node copy when the registry can't
IMAGE_REPAIR_ENABLED        in-place repair of broken images
LAYER_INVENTORY_ENABLED     layer inventory reports
INVENTORY_EVENTS_ENABLED    following containerd's events
```

For example, `GC_ENABLED=false` leaves image cleanup to kubelet: no age rule,
no disk-pressure removal and no retention copies, and workers remove any
retention mark left from before at startup.

Features that order workers around (rescue, propagation, spreading, the
mirror's peer lookups, retention) also need the shared token
(`RESCUE_TOKEN_PATH`); without it they stay off and say so in the log. A
retention copy is copied to a node that doesn't hold the image through rescue,
so with `RESCUE_ENABLED=false` it is only ever kept on a node that already
holds it.

## General

| Variable | Default | Description |
|---|---|---|
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. |
| `METRICS_LABEL_REGISTRY` | `false` | Add a `registry` label to pull metrics. The set of registries is unbounded, so it is off by default. |
| `METRICS_EVENT_TTL_S` | `15` | How long an event metric (a spread ending, a node getting an image, a pull, a repair, a removal) stays on `/metrics` after it happened, for Prometheus to scrape it; Prometheus keeps the history (see [Observability](observability.md#image-history)). Match it to your scrape interval: an event that lives less than a scrape apart can be missed. `0` turns them off. |
| `RESCUE_TOKEN_PATH` | `/etc/angryduck/rescue-token/token` | Shared bearer token file, 32+ characters. Needed by rescue, propagation, spreading, image retention and the mirror. Read once at startup. With a token, `/report`, `/pull` and `/pull/cancel` also require it. |
| `WEBHOOK_TOKEN_PATH` | `/etc/angryduck/webhook-token/token` | Controller only. CI's bearer token for `/webhook/preheat`, 32+ characters. If the file is missing, the webhook is unauthenticated; if it exists but is unusable, the controller exits. |
| `AUTH_MODE` | `enforce` | `enforce` rejects control requests without the token; `warn` allows and logs them. Use `warn` only while upgrading controller and workers, then remove it. |
| `REGISTRY_CREDENTIALS_PATH` | *(empty)* | dockerconfigjson covering every private registry, for seed pulls and manifest reads. |

## Controller

| Variable | Default | Description |
|---|---|---|
| `CONTROLLER_LISTEN_ADDR` | `:8080` | Listen address. |
| `WAKE_COALESCE_MS` | `50` | How long spread, propagation and rescue wait after something they were waiting for happens (a transfer ended, a pod got stuck) before they look again, so events arriving together share one look. Every millisecond here is a freed link sitting idle; each look costs a fraction of a millisecond of CPU per running spread. |
| `WORKER_STALE_AFTER_S` | `30` | A worker silent for this long is ignored until it reports again. |

## Preheat

| Variable | Default | Description |
|---|---|---|
| `RANK_TOP_N` | `5` | Seed nodes per pushed image. |
| `RANK_INTERVAL_S` | `10` | How often pull orders are (re)issued. |
| `TARGET_TTL_S` | `120` | How long failed pull orders keep being replaced, and how long a preheat keeps retrying to read the image's manifest before seeding it whole. |
| `RANK_BY_LAYERS` | `true` | Rank seeds by bytes already held, reading the image's manifest from its registry. |
| `RANK_PLATFORM` | `linux/amd64` | Platform whose manifest is read. |
| `RANK_RESOLVE_TIMEOUT_S` | `5` | Manifest lookup timeout. |
| `RANK_RESOLVE_CACHE_S` | `600` | How long a manifest lookup is reused. |
| `RANK_PREFER_IMAGE_LOCALITY` | `true` | In the fallback ranking, prefer nodes holding any tag of the same repo. |
| `RANK_EXCLUDE_NODE_SUBSTRINGS` | *(empty)* | Comma-separated node name substrings never chosen as seeds. |

## Origin check

Whether the registries still serve the images the nodes hold. See
[Observability](observability.md#origin-check).

| Variable | Default | Description |
|---|---|---|
| `ORIGIN_CHECK_INTERVAL_S` | `3600` | How often the controller asks each held image's registry for its manifest (a `HEAD`, with `REGISTRY_CREDENTIALS_PATH`). Images it didn't serve are asked four times as often. `0` turns the check off. |
| `ORIGIN_CHECK_DELAY_S` | `120` | Wait after the controller starts before the first check. |
| `ORIGIN_CHECK_TIMEOUT_S` | `5` | How long one check waits for the registry, token request included, before counting it `unreachable`. |
| `ORIGIN_CHECK_REGISTRIES` | *(empty)* | Comma-separated substrings; only registry hosts containing one are checked. Empty checks every registry. Useful when the controller can't reach public registries the nodes reach through a mirror. |

## Spread

How pushed images reach every node blob by blob. See
[Propagation](propagation.md).

| Variable | Default | Description |
|---|---|---|
| `SPREAD_ENABLED` | `true` | Spread blob by blob. Needs `RANK_BY_LAYERS=true` and the shared token; otherwise, and for images whose manifest can't be read, pushes are seeded and propagated whole. Read by the controller and the worker. |
| `SPREAD_MAX_REGISTRY_PULLS` | `RANK_TOP_N` | Blob pulls from the registry in flight cluster-wide. A node may run several. |
| `SPREAD_RACE_PER_BLOB` | `2` | Registry pulls of one blob at once: an idle node may race a slow one. A blob whose registry pull is streamed on isn't raced. |
| `SPREAD_RETRY_AFTER_S` | `15` | Backoff after a node fails a transfer; doubles per failure. |
| `SPREAD_BACKOFF_MAX_S` | `600` | Cap for that doubling. |
| `SPREAD_TRANSFER_TIMEOUT_S` | `1800` | One blob transfer, start to finish (worker). The controller forgets a transfer nobody reported on a minute after. |
| `SPREAD_STREAM_ENABLED` | `true` | A node still receiving a large blob serves it on to peers as the bytes arrive ([streaming](propagation.md#streaming-large-blobs)). Read by the controller and the worker; a worker streams only with containerd's content store mounted. |
| `SPREAD_UNPACK_ENABLED` | `true` | A node unpacks each layer as soon as its blob and the layers below it are here, while the rest still arrive, instead of all at once when the image registers ([unpacking](propagation.md#unpacking-as-layers-arrive)). Read by the worker. |
| `SPREAD_STREAM_MIN_MB` | `32` | Only blobs at least this large are streamed; smaller ones go node to node whole, as before. |
| `SPREAD_STREAM_MAX_DEPTH` | `0` | Longest chain of nodes receiving a blob through each other: 1 is the node getting it from the registry or a full copy, 2 a node streaming from it, and so on. `0`: no cap but the number of nodes. `1`: nothing is streamed. |
| `SPREAD_SLOW_FACTOR` | `2` | Slow-chain guard: a transfer running more than this many times its expected time (its size at the typical speed of its path) is late. A late streamed registry pull is raced; a late stream is offered a restart from a holder, which the receiver takes only if it should finish sooner. `0` turns the guard off. |
| `SPREAD_EXPRESS_MAX_MB` | `32` | Blobs up to this size have their own `PROPAGATE_PER_SOURCE` slots on each source, so small layers don't wait behind large transfers. `0`: one budget for all. |

Spread also uses `PROPAGATE_INTERVAL_S`, `PROPAGATE_WINDOW_S`,
`PROPAGATE_PER_SOURCE`, `PROPAGATE_MAX_UTILIZATION` and
`PROPAGATE_EXCLUDE_NODE_SUBSTRINGS` below.

## Propagation

The whole-image fallback, and the limits spread shares.


| Variable | Default | Description |
|---|---|---|
| `PROPAGATE_ENABLED` | `true` | Whole-image propagation, used when an image can't spread blob by blob. Blob-by-blob spreading is controlled by `SPREAD_ENABLED`; turn both off to seed only. |
| `PROPAGATE_INTERVAL_S` | `10` | How often propagations advance. A finished transfer also advances them, after `WAKE_COALESCE_MS`. |
| `PROPAGATE_WINDOW_S` | `0` | How long to keep spreading a push. `0` means until done or superseded. |
| `PROPAGATE_MAX_CONCURRENT` | `8` | Whole-image transfers in flight cluster-wide. Spread has no cluster-wide cap. |
| `PROPAGATE_PER_SOURCE` | `3` | Concurrent sends per source node, spread included. For spread it is the only limit on peer transfers: a receiver takes any number of different blobs at once. One send rarely fills a source's link (the receiver's writes into containerd are usually the limit); three keep a 10G link busy while serving from page cache costs little CPU. Raise it on faster links. |
| `PROPAGATE_MAX_UTILIZATION` | `0.70` | Nodes fuller than this receive nothing new until cleanup makes room. A spread counts as complete without them and stays open for them. Keep it at or below `GC_HIGH_UTILIZATION`. |
| `PROPAGATE_SEED_TIMEOUT_MIN_S` | `120` | A seed still pulling after `max(MIN, FACTOR × the first seed's pull time)` is cancelled, and peers serve it instead. |
| `PROPAGATE_SEED_TIMEOUT_MAX_S` | `600` | The timeout while no seed has finished yet. |
| `PROPAGATE_SEED_TIMEOUT_FACTOR` | `3` | See `PROPAGATE_SEED_TIMEOUT_MIN_S`. |
| `PROPAGATE_EXCLUDE_NODE_SUBSTRINGS` | same as `RANK_EXCLUDE_NODE_SUBSTRINGS` | Node name substrings that never receive pushed images. They still serve what they hold, and rescue still covers them. |

## Rescue

| Variable | Default | Runs on | Description |
|---|---|---|---|
| `RESCUE_ENABLED` | `true` | both | Rescue pods stuck on pulls. |
| `RESCUE_INTERVAL_S` | `15` | controller | How often backoffs are re-checked (in memory). Pending pods are watched; without `watch` permission they are listed this often instead. |
| `RESCUE_RESYNC_INTERVAL_S` | `600` | controller | Full relist of Pending pods behind the watch, as a safety net. |
| `RESCUE_RETRY_AFTER_S` | `120` | controller | First retry delay; doubles after each failure. |
| `RESCUE_BACKOFF_MAX_S` | `3600` | controller | Cap for the retry delay. |
| `RESCUE_TIMEOUT_S` | `600` | controller | How long to wait for one rescue. |
| `RESCUE_MAX_CONCURRENT` | `2` | controller | Rescues in flight cluster-wide. |
| `RESCUE_MAX_SOURCES` | `3` | controller | Source nodes offered per rescue. |
| `RESCUE_NODE_MAX_CONCURRENT` | `2` | worker | Images received at once. |
| `RESCUE_PIN_TTL_S` | `3600` | worker | How long a failed attempt's snapshots stay pinned. |
| `RESCUE_STATE_DIR` | `/var/lib/angryduck` | worker | Node directory for pins and image usage times. |
| `RESCUE_CONTAINERD_NAMESPACE` | `k8s.io` | worker | containerd namespace holding kubelet's images. |
| `RESCUE_PLATFORM` | `linux/<arch>` | worker | Platform to import. |

## Worker

| Variable | Default | Description |
|---|---|---|
| `NODE_ID` | hostname | Node name. Set from `spec.nodeName`. |
| `SELF_ADDRESS` | *(required)* | Address the controller and peers reach this worker at. Set from `status.podIP`. |
| `WORKER_LISTEN_ADDR` | `:18081` | Listen address. With `HOST_NETWORK=true`, an address without a host is bound to `POD_IP` (the node's address), not to every interface of the node. |
| `HOST_NETWORK` | `false` | Set by the DaemonSet when the worker shares the node's network (Helm `worker.hostNetwork`, `true` by default): the worker API binds the node's address only and the mirror the node's loopback. |
| `CONTROLLER_URL` | `http://angryduck-controller:8080` | Controller base URL. |
| `REPORT_INTERVAL_S` | `15` | How often the worker reports. |
| `NODE_EXPORTER_URL` | `http://localhost:9100/metrics` | Source of root filesystem utilization. The DaemonSet sets it to `http://$(HOST_IP):9100/metrics`. |
| `POD_IP`, `POD_NAMESPACE`, `HOST_IP` | from the downward API | Set by the DaemonSet. On the pod network `POD_IP` is the mirror's address in `hosts.toml`; on the node's network it is the node's address, which the worker API binds. |
| `DAEMONSET_NAME` | `angryduck-worker` | The DaemonSet the worker checks on shutdown: gone or being deleted means an uninstall, and the worker cleans up the node. |
| `CONTAINER_RUNTIME` | `containerd` | Only containerd is supported. `crictl` and `ctr`, from older configurations, are accepted and mean the same. |
| `CONTAINER_RUNTIME_ENDPOINT` | `unix:///run/containerd/containerd.sock` | containerd's socket, as mounted into the worker. |
| `CONTAINERD_SNAPSHOTTER` | `overlayfs` | The snapshotter kubelet's images are unpacked with. |
| `CONTAINERD_ROOT` | from `/etc/containerd/config.toml`, else `/var/lib/containerd` | containerd's root directory, as mounted into the worker. Blobs are read directly from its content store when mounted, otherwise through containerd's content API. |
| `HOST_ROOT` | *(empty)* | Prefix for the node paths the worker mounts (state dir, containerd's config and `certs.d`). The manifests mount them at the same paths. Not a chroot. |
| `PAGE_CACHE_RELEASE_AFTER_S` | `30` | Reading a blob file to serve it (to a peer, the mirror or a rescue) caches it, and the kernel charges that page cache to the worker's pod, where it stays in its container memory. Once nothing has read a file for this long, the worker gives back the pages its own reads brought in; pages that were cached already stay. While a blob keeps being served, nothing is dropped. `0` keeps them. |
| `DIRECT_WRITES` | `true` | Write blobs of 1 MiB or more straight into the data file of the write the worker opens through containerd, and let containerd hash and commit it, instead of sending every byte through its content API: about a third of the CPU per blob, worker and containerd together, in half the time. Needs containerd's `ingest` directory mounted writable (the manifests do) and the content store mounted; a probe at startup checks the whole path, and any failure leaves it off, through the API as before. The worker keeps at most 16 MiB of a blob it is writing in page cache (8 MiB, more only for a peer reading behind), and none once written. |
| `LAYER_INVENTORY_ENABLED` | `true` | Report layer blobs and snapshot chainIDs to the controller. |
| `LAYER_SCAN_INTERVAL_S` | `60` | Time between layer scans while containerd's event stream is down (or `INVENTORY_EVENTS_ENABLED=false`). A transfer triggers one at once. |
| `INVENTORY_EVENTS_ENABLED` | `true` | Follow containerd's events (images created, updated or deleted, content deleted): any pull or removal, kubelet's included, reaches the controller in about a second. |
| `LAYER_RESYNC_INTERVAL_S` | `600` | Time between layer scans while the event stream is up: a safety net only. |
| `PREHEAT_ATTRIBUTION_INTERVAL_S` | `300` | How often running containers from preheated repos are sampled. `0` disables it. |
| `PREHEAT_ATTRIBUTION_RETENTION_S` | `360` | How long after a preheat its repo still counts. |

## Mirror

| Variable | Default | Description |
|---|---|---|
| `MIRROR_ENABLED` | `true` | Run the mirror and manage containerd's `hosts.toml` files. Needs containerd's CRI registry `config_path` (see [Mirror](mirror.md#requirements)). |
| `MIRROR_LISTEN_ADDR` | `:18082` | Listen address. With `HOST_NETWORK=true`, an address without a host means `127.0.0.1:<port>`. On the pod network, a loopback address, from older configurations, is turned into `:<port>`. |
| `MIRROR_ADVERTISE_ADDR` | the listen address with `HOST_NETWORK=true`, else `$POD_IP:<port>` | The address written into `hosts.toml`. |
| `MIRROR_CONTAINERD_CONFIG_DIR` | `/etc/containerd/certs.d` | Where `hosts.toml` files are written. containerd's CRI registry `config_path` must point here. |
| `MIRROR_REGISTRIES` | *(empty)* | Registries to mirror even before the node uses them. |
| `MIRROR_RESOLVE_ENABLED` | `true` | Resolve tags too: the registry's answer when it gives one, else the newest copy a node holds (see [Mirror](mirror.md#tags)). |
| `MIRROR_RESOLVE_TIMEOUT_MS` | `1000` | How long the mirror waits for the registry to say what a tag points at, token request included. |
| `MIRROR_RESOLVE_UNREACHABLE_TTL_S` | `30` | After a registry gave no answer, how long this node's mirror goes straight to the newest node copy for its tags. `0` asks every time. |
| `MIRROR_RESOLVE_CONTROLLER_TIMEOUT_MS` | `3000` | How long the mirror waits for the controller's answers about a tag: whether a node holds it, and the newest copy's digest. |
| `MIRROR_RESOLVE_NEWEST_CACHE_S` | `30` | Controller: how long the newest copy's digest found for a tag is reused for every mirror asking. |
| `MIRROR_RESOLVE_NEWEST_TIMEOUT_MS` | `2000` | Controller: how long the holders get to say what a tag points at on each. Keep it below `MIRROR_RESOLVE_CONTROLLER_TIMEOUT_MS`. |

## Image cleanup

An image not in use on the node is removed when it has been unused for
`GC_UNUSED_FOR_S`, or, while the disk is above `GC_HIGH_UTILIZATION`, oldest
first until it is back to `GC_LOW_UTILIZATION`. When an image runs nowhere in
the cluster anymore, one node keeps it as the retention copy for
`GC_RETENTION_TTL_S`, outside both rules. See [Image cleanup](image-cleanup.md).

| Variable | Default | Description |
|---|---|---|
| `GC_ENABLED` | `true` | Clean up images on each node. |
| `GC_INTERVAL_S` | `60` | How often usage is sampled and the disk checked. |
| `GC_UNUSED_FOR_S` | `3600` | Remove images unused this long, whatever the disk usage. |
| `GC_HIGH_UTILIZATION` | `0.70` | Above this, remove unused images oldest first... Keep it below kubelet's `imageGCHighThresholdPercent`. |
| `GC_LOW_UTILIZATION` | `0.40` | ...until the disk is back down to this. |
| `GC_RETENTION_ENABLED` | `true` | Keep one copy of every image that went unused in the cluster. Read by both binaries; off when `GC_ENABLED=false`. |
| `GC_RETENTION_TTL_S` | `86400` | How long the copy is kept, counted from when the image went unused. Removed right after. `0` turns retention off. |
| `GC_RETENTION_MAX_UTILIZATION` | `GC_HIGH_UTILIZATION` | Controller only. Never keep a copy on a node whose disk is at or above this. While every node is, copies wait. |
| `GC_RETENTION_INTERVAL_S` | `30` | Controller only. How often copies are checked for what time alone changes (expiry, a node going away, a node with room again). Reports changing what runs where are acted on at once. |
| `GC_BATCH` | `5` | Images removed per batch. |
| `GC_SETTLE_S` | `10` | Wait after a batch before re-measuring the disk. |
| `GC_PROTECT_SUBSTRINGS` | `pause` | Never remove images whose name contains one of these. |

## Companion images

Images a pod runs beside an app image (sidecars) are spread to every node and
kept there: no age rule, and under disk pressure removed only after every
other unused image. Spreading them needs `PROPAGATE_ENABLED`; without it they
are only kept. See [Companion images](companions.md).

| Variable | Default | Description |
|---|---|---|
| `COMPANION_ENABLED` | `true` | Keep companion images on every node. Read by both binaries; off when `GC_ENABLED=false`. |
| `COMPANION_MAX_MB` | `300` | Controller only. Larger images aren't kept; while set, nor are those whose size no node reported yet. `0`: no limit. |
| `COMPANION_INTERVAL_S` | `30` | Controller only. How often the set is re-evaluated at least. Reports changing what runs where are acted on at once. |

## Image repair

Each worker looks for images that are registered but can't start a
container: a metadata blob (index, manifest, config) is gone or doesn't parse,
or the top layer isn't unpacked and some layer has neither its snapshot nor
its blob on the node. kubelet treats such an image as present and never pulls
it again, so every container create fails.

The worker repairs such an image in place, without removing it. It fetches only
the pieces that are gone, by digest: first from peers that hold them, then
from the image's registry. The image keeps its name throughout. Anything
still here is held under a lease during the repair, so the garbage collector
can't take it meanwhile. If no peer and not the registry has a piece, the
image stays as it is, reported in `angryduck_worker_broken_image`, and the
next check tries again. Images being pulled or received are left alone.

Nothing is hashed to find corruption. A layer blob counts as corrupted only
when a container create failed unpacking it: the controller sees the pod's
`CreateContainerError` and sends the node a repair order carrying kubelet's
error, which names the layer. That one blob is replaced. Peers serve repairs
through their spread endpoint, so a node with `SPREAD_ENABLED=false` serves
none. See [Operations](operations.md#an-image-is-present-but-containers-cant-be-created).

| Variable | Default | Description |
|---|---|---|
| `IMAGE_REPAIR_ENABLED` | `true` | Find and repair images that can't start a container. |
| `IMAGE_REPAIR_INTERVAL_S` | `120` | How often every image is checked. An image's metadata is read once per digest (it never changes); later checks use the worker's shared listings only, with no containerd call of their own. A container failing on a broken image triggers a repair right away instead (sent by the controller), so this only bounds how long a broken image nothing uses goes unnoticed. |
