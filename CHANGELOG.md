# Changelog

## 1.8.7

### Performance

- **Snapshot shipping is about 40% faster.** The source reads containerd's
  finished diff straight from the content store it already mounts, instead
  of through the content API in 32 KiB calls, and the receiver stages the
  layer in 1 MiB writes instead of 32 KiB ones. That removes about 20,000
  gRPC round trips per 300 MB layer, which also lowers CPU in the worker and
  containerd. Blob reads through the content API, used when the content
  store isn't mounted, read 1 MiB at a time too. See
  [Sizing](docs/operations.md#sizing).
- **Propagation starts its next round when a transfer finishes**, about a
  second later, instead of at the next `PROPAGATE_INTERVAL_S`. Each doubling
  round no longer costs at least 10 seconds.
- **Rescues held back by `RESCUE_MAX_CONCURRENT` start as soon as a running
  rescue finishes**, instead of at the next `RESCUE_INTERVAL_S`.

### Added

- A Helm chart in `charts/angryduck`, with generated tokens kept across
  upgrades. See [Helm](docs/helm.md).
- [Node settings and limits](docs/node-settings.md): the containerd and
  kubelet settings that decide how much Angry Duck can do, and what it can't.

### Security

- The controller runs as a non-root user (65532) with a read-only root
  filesystem, no capabilities and the default seccomp profile, in the chart
  and in the example manifests.
- [Security](docs/security.md) describes what the shared token allows with
  snapshots, and why it should be treated as a credential for every node's
  image store.

### Fixed

- Docs still described pre-1.8.6 behavior: workers on the node network,
  `ctr` in transfers, no auth on `/pull`, no Kubernetes access for the
  worker. The GitLab CI example didn't send the webhook token.
- Removed unused code and comments that still referred to the `ctr` and
  `crictl` backends.

## 1.8.6

### Security

- The worker no longer needs host privileges. It talks to containerd's API
  and CRI over the socket instead of chrooting into `/proc/1/root` to run the
  node's `crictl`, `ctr` and `tar`. The DaemonSet drops `hostPID`, the
  unconfined AppArmor profile and all ten added capabilities (`SYS_ADMIN`,
  `SYS_PTRACE`, `SYS_CHROOT`, `DAC_*`, `CHOWN`, `FOWNER`, `FSETID`,
  `SETFCAP`, `MKNOD`), and adds `seccompProfile: RuntimeDefault` and
  `readOnlyRootFilesystem`. It mounts the containerd socket, its content
  store (read-only), `config.toml` (read-only), `certs.d` and the state dir.
- Snapshot shipping goes through containerd's diff service on both ends, and
  transfers between 1.8.6 workers carry a SHA-256 checked before the commit.
  Pre-1.8.6 peers keep working in both directions during a rollout. See
  [Transfers](docs/transfers.md#snapshots) for the one difference: extended
  attributes other than `security.capability` don't travel with a snapshot.
- `/report` (controller) and `/pull`, `/pull/cancel` (worker) require the
  shared token when one is configured (`AUTH_MODE`).
- `/webhook/preheat` requires CI's own bearer token from the new
  `angryduck-webhook-token` Secret (`WEBHOOK_TOKEN_PATH`). Without the
  Secret it stays open, with a warning at startup.

- The worker runs on the pod network: no more `hostNetwork`. The mirror
  listens on the pod IP and answers only containerd, which sends a random
  per-pod token from `hosts.toml`. node-exporter is read at the node IP.
  NetworkPolicy now applies to the worker.
- On SIGTERM the worker removes its `hosts.toml` files and releases rescue
  pins. When its DaemonSet is deleted, it also removes temporary snapshots
  and base images and its state files. This needs `get` on its own
  DaemonSet, the worker's only Kubernetes API permission.

### Added

- `angryduck_worker_containerd_calls_total{method}`: every call the worker
  makes to containerd.
- `angryduck_process_memory_bytes{component, kind}` for worker and
  controller: `rss_anon` (memory really held), `rss_file` (code pages),
  `heap_live`, `heap_retained`, `heap_released`, `stacks`.
- Worker and controller return freed heap to the OS once a minute when more
  than 1 MiB is retained.

### Performance

Measured on a node with 304 images and 614 snapshots, next to 1.8.5: CPU at
steady state (worker, its subprocesses and containerd) from ≈0.9% to ≈0.17%
of a core; no more `ctr`/`crictl` processes of up to ~27 MiB each; blob
rescue and the mirror faster; snapshot rescue slower. See
[Sizing](docs/operations.md#sizing).

### Removed

- The `docker` runtime backend and the `ctr`/`crictl` CLI backends. Only
  containerd is supported; `CONTAINER_RUNTIME=crictl` and `ctr` are still
  accepted. `HOST_ROOT` is no longer a chroot.

### Added

- Apache-2.0 [license](LICENSE).
- Release workflow for GitHub: each version tag publishes the controller and
  worker images to `ghcr.io/hatam-abolghasemi` and creates a GitHub Release
  from this changelog. See [Installation](docs/installation.md#1-get-the-images).

### Fixed

- README listed 1.8.4 as the current version.

## 1.8.5

### Fixed

- **`angryduck_worker_preheated_containers_running` was always empty with
  the crictl runtime.** kubelet creates containers from the resolved image
  ID, so `crictl ps` reports a bare `sha256:...` in both `image.image` and
  `imageRef`, and no running container could be matched to a preheated
  repo. The worker now takes the repo from `image.userSpecifiedImage` when
  the runtime provides it, and otherwise resolves the image ID through
  `crictl images`. The `ctr` backend was not affected.

### Changed

- **The Grafana dashboard is reorganized into tabs** (Overview, Preheat,
  Propagation, Rescue, Transfers, Mirror, Cleanup) and now uses Grafana's
  dashboard schema v2. It needs Grafana 13, or Grafana 12 with the dynamic
  dashboards and `kubernetesDashboards` feature toggles; Grafana 10.4 and 11
  can no longer import it. New: a per-node overview table, Registry and Image
  variables, click-to-filter node and image names, annotations for cleanup
  starts, rescue failures and finished propagations, and a Reset filters
  link. Event counts are computed per scrape from a `scrape_interval`
  constant (15s). See [Observability](docs/observability.md#grafana-dashboard).
- Example manifests: mgmt controller CPU request 200m and limit 2, memory
  limit 192Mi (were 20m, 150m and 64Mi); stg worker CPU request 200m and
  limit 2 (were 50m and 1).

### Added

- Docs: [Comparison](docs/comparison.md) with similar tools, and
  [The name](docs/name.md).

## 1.8.4

Resource usage. Measured with the real binaries against a simulated
150-node fleet; the worker figures are per node.

| | 1.8.3 | 1.8.4 |
|---|---|---|
| Worker subprocesses | 174/min, up to 9 at once | 6/min, 1 at a time |
| Worker CPU (own process) | 6.1m | 1.9m |
| Worker memory (own process, peak) | 25Mi | 15Mi |
| Controller CPU | 95m | 13m |
| Controller memory (steady / peak) | 44Mi / 47Mi | 22Mi / 26Mi |

### Changed

- **The mirror no longer lists the content store on every request.**
  Concurrent callers now share one listing, results are shared instead of
  copied per request, and presence is checked with a file lookup. Under a
  pull, 1.8.3 could run several full `ctr content ls` processes at once,
  each 20-40 MB in the worker's pod.
- **Blobs are read directly from containerd's content store** when serving
  the mirror, peers and transfers, instead of one `ctr content get` process
  per blob. Falls back to `ctr` when the store isn't found.
- Background listings (images, containers, content, snapshots) run one at
  a time, with a timeout, and their output is parsed as it streams.
- Workers reuse their image listing for up to a minute between reports.
  Pulls and transfers still report a fresh one right away.
- Disk usage is read with node-exporter's `collect[]=filesystem`, so
  node-exporter skips its other collectors. Endpoints that reject the
  filter get the full page.
- The controller keeps each node's layers as a bitset and interns image
  names shared across nodes: about 8 MiB of state for 150 nodes, down from
  about 35 MiB.
- Workers send their image inventory only when it changes. Controllers and
  workers of different versions keep working together during an upgrade.
- Both binaries default to `GOGC=50`.
- Controller manifests: memory request 32Mi and limit 64Mi (were 16Mi and
  32Mi), which 150 nodes exceeded even after tuning.

### Added

- `CONTAINERD_ROOT`, for containerd installations whose root isn't in the
  node's `config.toml` or under `/var/lib/containerd`.

### Fixed

- A failed image listing on a worker no longer makes the controller think
  the node has no images until the next report.

## 1.8.3

### Changed

- **Image cleanup removes an image when either rule holds**, instead of
  requiring both. Images unused for `GC_UNUSED_FOR_S` are now removed whatever
  the disk usage. Above `GC_HIGH_UTILIZATION`, unused images are removed
  oldest first, however recently they ran, until `GC_LOW_UTILIZATION`.
  Previously a node above the high mark whose images had all run within
  `GC_UNUSED_FOR_S` could not free any space.
- The rollback images kept per repo are now the newest `GC_ROLLBACK_KEEP`
  unused images, regardless of age.
- A failed disk usage reading no longer blocks the age-based cleanup.
- `.env.example` reorganized by component, with stale entries removed.

### Added

- `pressure` value for the `tier` label of
  `angryduck_worker_gc_images_deleted_total` and
  `angryduck_worker_gc_candidates`: images removed early because the disk was
  above the high mark.
- A rebuilt Grafana dashboard at `deploy/grafana/angryduck-dashboard.json`
  covering every metric, with data source, cluster and node variables.
- Documentation split into `docs/`.

### Fixed

- `angryduck_worker_gc_candidates` is now updated on every cleanup tick, not
  only while the disk is above the high mark.
- The help text of `angryduck_worker_rescue_cleanups_total` lists
  `temp_image`.

### Upgrade notes

- Nodes with plenty of free disk will now remove images unused for 6 hours.
  Raise `GC_UNUSED_FOR_S` if you want to keep them longer.
- Alerts on `angryduck_worker_gc_candidates{tier="old"} == 0` should sum all
  tiers instead; see [Observability](docs/observability.md#suggested-alerts).
- The previous dashboard's pull duration panels used
  `angryduck_worker_pull_duration_seconds`, removed in 1.8.2. Replace it with
  the new dashboard.

## 1.8.2

- Layer inventory: each worker reports its blobs and snapshots as deltas.
- Seeds are chosen by bytes already held, reading manifests from any registry
  through its auth challenge.
- Propagation continues until every node has the image, one node at a time per
  source, waiting pods first, with slow seeds cancelled in favor of peers.
- Pull-only peer-to-peer mirror for containerd.
- Blobs-first transfers from several sources, with snapshots as a last resort.
- Node-local image cleanup.
- Leaner pull metrics. Removed `angryduck_worker_pull_duration_seconds`,
  `SPEGEL_IMAGE_SUBSTRING` and `PULL_DURATION_RESET_INTERVAL_S`.

## 1.8.0

- Propagation: pushed images spread to every eligible node from peers.

## 1.7.1

- Rescue backoff, pins kept across retries, startup cleanup, `/status` for
  rescues, more metrics.

## 1.7.0

- Snapshot shipping, so rescue works when no node has the layer blobs.
- Per-layer decisions skip layers already unpacked.

## 1.6.1

- Sources report which blobs they lack instead of refusing.

## 1.6.0

- Rescue rebuilt as a node-to-node transfer of only the missing blobs.

## 1.5.0

- Removed the old image GC and rescue; preheat only.
