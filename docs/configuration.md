# Configuration

Angry Duck is configured entirely through environment variables. In
Kubernetes they come from the ConfigMap. Locally they can come from a `.env`
file (path set by `ENV_FILE`, default `.env`), and real environment variables
always win over the file. [`app/.env.example`](../app/.env.example) lists
every setting with comments.

Durations are integers in seconds. Utilizations are fractions between 0 and
1.

## General

| Variable | Default | Description |
|---|---|---|
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. |
| `METRICS_LABEL_REGISTRY` | `false` | Add a `registry` label to pull metrics. The set of registries is unbounded, so it is off by default. |
| `RESCUE_TOKEN_PATH` | `/etc/angryduck/rescue-token/token` | Shared bearer token file, 32+ characters. Needed by rescue, propagation and the mirror. Read once at startup. With a token, `/report`, `/pull` and `/pull/cancel` also require it. |
| `WEBHOOK_TOKEN_PATH` | `/etc/angryduck/webhook-token/token` | Controller only. CI's bearer token for `/webhook/preheat`, 32+ characters. If the file is missing, the webhook is unauthenticated; if it exists but is unusable, the controller exits. |
| `AUTH_MODE` | `enforce` | `enforce` rejects control requests without the token; `warn` allows and logs them. Use `warn` only while upgrading controller and workers, then remove it. |
| `REGISTRY_CREDENTIALS_PATH` | *(empty)* | dockerconfigjson covering every private registry, for seed pulls and manifest reads. |

## Controller

| Variable | Default | Description |
|---|---|---|
| `CONTROLLER_LISTEN_ADDR` | `:8080` | Listen address. |
| `WORKER_STALE_AFTER_S` | `30` | A worker silent for this long is ignored until it reports again. |

## Preheat

| Variable | Default | Description |
|---|---|---|
| `RANK_TOP_N` | `5` | Seed nodes per pushed image. |
| `RANK_INTERVAL_S` | `10` | How often pull orders are (re)issued. |
| `TARGET_TTL_S` | `120` | How long failed pull orders keep being replaced. |
| `RANK_BY_LAYERS` | `true` | Rank seeds by bytes already held, reading the image's manifest from its registry. |
| `RANK_PLATFORM` | `linux/amd64` | Platform whose manifest is read. |
| `RANK_RESOLVE_TIMEOUT_S` | `5` | Manifest lookup timeout. |
| `RANK_RESOLVE_CACHE_S` | `600` | How long a manifest lookup is reused. |
| `RANK_PREFER_IMAGE_LOCALITY` | `true` | In the fallback ranking, prefer nodes holding any tag of the same repo. |
| `RANK_EXCLUDE_NODE_SUBSTRINGS` | *(empty)* | Comma-separated node name substrings never chosen as seeds. |

## Spread

How pushed images reach every node blob by blob. See
[Propagation](propagation.md).

| Variable | Default | Description |
|---|---|---|
| `SPREAD_ENABLED` | `true` | Spread blob by blob. Needs `RANK_BY_LAYERS=true` and the shared token; otherwise, and for images whose manifest can't be read, pushes are seeded and propagated whole. Read by the controller and the worker. |
| `SPREAD_MAX_REGISTRY_PULLS` | `RANK_TOP_N` | Blob pulls from the registry in flight cluster-wide. |
| `SPREAD_RACE_PER_BLOB` | `2` | Registry pulls of one blob at once: an idle node may race a slow one. |
| `SPREAD_RETRY_AFTER_S` | `15` | Backoff after a node fails a transfer; doubles per failure. |
| `SPREAD_BACKOFF_MAX_S` | `600` | Cap for that doubling. |
| `SPREAD_TRANSFER_TIMEOUT_S` | `1800` | One blob transfer, start to finish (worker). The controller frees a slot nobody reported on a minute after. |

Spread also uses `PROPAGATE_INTERVAL_S`, `PROPAGATE_WINDOW_S`,
`PROPAGATE_MAX_CONCURRENT` (peer transfers cluster-wide),
`PROPAGATE_PER_SOURCE`, `PROPAGATE_MAX_UTILIZATION` and
`PROPAGATE_EXCLUDE_NODE_SUBSTRINGS` below.

## Propagation

The whole-image fallback, and the limits spread shares.


| Variable | Default | Description |
|---|---|---|
| `PROPAGATE_ENABLED` | `true` | Spread pushed images to every eligible node. |
| `PROPAGATE_INTERVAL_S` | `10` | How often propagations advance. A finished transfer also advances them, after about a second. |
| `PROPAGATE_WINDOW_S` | `0` | How long to keep spreading a push. `0` means until done or superseded. |
| `PROPAGATE_MAX_CONCURRENT` | `8` | Transfers in flight cluster-wide. |
| `PROPAGATE_PER_SOURCE` | `1` | Concurrent sends per source node. |
| `PROPAGATE_MAX_UTILIZATION` | `0.70` | Nodes fuller than this wait for cleanup. |
| `PROPAGATE_SEED_TIMEOUT_MIN_S` | `120` | Lower bound before a slow seed is cancelled. |
| `PROPAGATE_SEED_TIMEOUT_MAX_S` | `600` | Timeout while no seed has finished. |
| `PROPAGATE_SEED_TIMEOUT_FACTOR` | `3` | Multiple of the first seed's pull time. |
| `PROPAGATE_EXCLUDE_NODE_SUBSTRINGS` | same as `RANK_EXCLUDE_NODE_SUBSTRINGS` | Nodes to skip. |

## Rescue

| Variable | Default | Runs on | Description |
|---|---|---|---|
| `RESCUE_ENABLED` | `true` | both | Rescue pods stuck on pulls. |
| `RESCUE_INTERVAL_S` | `15` | controller | How often Pending pods are listed. |
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
| `WORKER_LISTEN_ADDR` | `:18081` | Listen address. |
| `CONTROLLER_URL` | `http://angryduck-controller:8080` | Controller base URL. |
| `REPORT_INTERVAL_S` | `15` | How often the worker reports. |
| `NODE_EXPORTER_URL` | `http://localhost:9100/metrics` | Source of root filesystem utilization. The DaemonSet sets it to `http://$(HOST_IP):9100/metrics`. |
| `POD_IP`, `POD_NAMESPACE`, `HOST_IP` | from the downward API | Set by the DaemonSet. `POD_IP` is the mirror's address in `hosts.toml`. |
| `DAEMONSET_NAME` | `angryduck-worker` | The DaemonSet the worker checks on shutdown: gone or being deleted means an uninstall, and the worker cleans up the node. |
| `CONTAINER_RUNTIME` | `containerd` | Only containerd is supported. The pre-1.8.6 values `crictl` and `ctr` are accepted and mean the same. |
| `CONTAINER_RUNTIME_ENDPOINT` | `unix:///run/containerd/containerd.sock` | containerd's socket, as mounted into the worker. |
| `CONTAINERD_SNAPSHOTTER` | `overlayfs` | The snapshotter kubelet's images are unpacked with. |
| `CONTAINERD_ROOT` | from `/etc/containerd/config.toml`, else `/var/lib/containerd` | containerd's root directory, as mounted into the worker. Blobs are read directly from its content store when mounted, otherwise through containerd's content API. |
| `HOST_ROOT` | *(empty)* | Prefix for the node paths the worker mounts (state dir, containerd's config and `certs.d`). The manifests mount them at the same paths. Not a chroot. |
| `LAYER_INVENTORY_ENABLED` | `true` | Report layer blobs and snapshot chainIDs to the controller. |
| `LAYER_SCAN_INTERVAL_S` | `60` | Minimum time between layer scans. A pull or transfer triggers one at once. |
| `PREHEAT_ATTRIBUTION_INTERVAL_S` | `300` | How often running containers from preheated repos are sampled. `0` disables it. |
| `PREHEAT_ATTRIBUTION_RETENTION_S` | `360` | How long after a preheat its repo still counts. |

## Mirror

| Variable | Default | Description |
|---|---|---|
| `MIRROR_ENABLED` | `false` | Run the mirror and manage containerd's `hosts.toml` files. |
| `MIRROR_LISTEN_ADDR` | `:18082` | Listen address, on the pod network. A loopback address (the pre-1.8.6 default) is turned into `:<port>`. |
| `MIRROR_ADVERTISE_ADDR` | `$POD_IP:<port>` | The address written into `hosts.toml`. |
| `MIRROR_CONTAINERD_CONFIG_DIR` | `/etc/containerd/certs.d` | containerd's CRI registry `config_path`. |
| `MIRROR_REGISTRIES` | *(empty)* | Registries to mirror even before the node uses them. |

## Image cleanup

| Variable | Default | Description |
|---|---|---|
| `GC_ENABLED` | `true` | Clean up images on each node. |
| `GC_INTERVAL_S` | `60` | How often usage is sampled and the disk checked. |
| `GC_UNUSED_FOR_S` | `21600` | Remove images unused this long, whatever the disk usage. |
| `GC_HIGH_UTILIZATION` | `0.70` | Above this, remove unused images oldest first... |
| `GC_LOW_UTILIZATION` | `0.60` | ...until the disk is back down to this. |
| `GC_ROLLBACK_KEEP` | `3` | Newest unused images per repo kept for rollback. |
| `GC_BATCH` | `5` | Images removed per batch. |
| `GC_SETTLE_S` | `10` | Wait after a batch before re-measuring the disk. |
| `GC_PROTECT_SUBSTRINGS` | `pause` | Never remove images whose name contains one of these. |
