# Changelog

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
