# Changelog

## Unreleased

### Changed

- **Example manifests are generic.** `k8s/` no longer carries a staging
  label, an internal ingress host and path, or `imagePullSecrets` for the
  public images. The registry credentials Secret is `registry-pull` (was
  `gitlab-registry-pull`) and is optional for the worker too. The webhook
  path is `/webhook/preheat`, as in the Helm chart. Selectors changed, so
  plain-manifest installs must delete the controller Deployment and worker
  DaemonSet before applying, or keep their own labels.
- **No comments in configuration files.** `app/.env.example`, the chart's
  `values.yaml` and the Dockerfiles are plain; `.env.example` now lists every
  setting with its default. The docs are the reference.

### Docs

- New [Design](docs/design.md) page: why Angry Duck starts at the push, runs
  everything in parallel, reacts to events and pairs distribution with
  cleanup.
- New [Story](docs/story.md) page: the problems that shaped Angry Duck,
  from slow rollouts to registry outages, stuck pulls and full disks. The
  README and the chart now describe it as an image lifecycle manager.
- New [Recipes](docs/recipes.md) page with settings for common clusters and
  goals.
- Security: corrected how seed pulls get credentials, documented the open
  `/status` and `/metrics` endpoints, added a hardening checklist and a
  private reporting channel.
- `PROPAGATE_ENABLED` is described correctly: it controls the whole-image
  fallback; blob-by-blob spreading is `SPREAD_ENABLED`.
- Issue and pull request templates, and a security policy.

## 1.8.10

### Changed

- **Spread has no cluster-wide cap on peer transfers.** Until now at most
  `PROPAGATE_MAX_CONCURRENT` (8) blobs moved between nodes at once across
  the whole cluster, so once a layer had more holders than that, most of
  them sat idle. The per-node slots are now the only limit: each node
  receives one blob from a peer at a time and serves
  `PROPAGATE_PER_SOURCE` at once. Per-node CPU, disk and network load and
  registry traffic are unchanged; transfers in flight grow with the
  holders. `PROPAGATE_MAX_CONCURRENT` keeps applying to the whole-image
  fallback.
- **Reports follow containerd's events.** Workers subscribe to containerd's
  image and content events, so any pull or removal (kubelet's own pulls
  included, which Angry Duck doesn't make) reaches the controller in about
  a second instead of at the next 60-second layer scan. Bursts settle into
  one report. While the stream is up, layer scans only resync every
  `LAYER_RESYNC_INTERVAL_S` (600); while it is down, they run every
  `LAYER_SCAN_INTERVAL_S` as before. One idle gRPC stream per node.
  `INVENTORY_EVENTS_ENABLED=false` turns it off.
- **Rescue watches Pending pods instead of listing them every 15 seconds.**
  A pod stuck pulling is seen as kubelet reports it. The watch resumes where
  it stopped; a full list happens only when the API server no longer has
  that version, after an error, or every `RESCUE_RESYNC_INTERVAL_S` (600).
  The controller's ClusterRole needs `watch` on pods (added to the manifests
  and chart); with only `list`, it lists every `RESCUE_INTERVAL_S` as
  before.
- **Mirror lookups find every holder.** `/layers/holders` returns every
  fresh node holding the blob (it stopped at 3), plus nodes a running spread
  just delivered it to, ahead of their next report. The mirror tries them
  all before answering `404`, doesn't cache an empty answer, and forgets a
  list once none of its holders could serve. A `404` now means no node the
  controller knows of, about a second ago, could serve the blob.
- **Blobs are served with their length, by sendfile.** `/spread/content`
  and the node mirror sent blobs chunked and copied them through
  userspace (32 KiB writes). They now set `Content-Length` and hand the
  blob file to the kernel: a 64 MB blob goes out in a few dozen `sendfile`
  calls instead of thousands of writes. A peer's size check, skipped on
  chunked replies, applies again, and a `HEAD` answers with the size.

### Repository

- Plain manifests moved from `deploy/stg/` to `k8s/`; the `mgmt` example is
  gone. The Grafana dashboard moved from `deploy/grafana/` to `grafana/`.
- The manifests use the published images,
  `ghcr.io/hatam-abolghasemi/angry-duck-{controller,worker}:1.8.10`.

### Fixed

- **The mirror falls through when it can't read a blob it holds.** Reading
  a local blob could fail after the node said it had it (a stale content
  listing, a blob removed meanwhile). The mirror then answered an empty
  `200`, which containerd took as the content: the pull failed with a short
  read, and neither the registry nor another holder was tried. The peer
  endpoint did the same, so the asking node forwarded the empty body to its
  own containerd instead of trying its next holder. Now both treat it as a
  miss: the next holder, then a `404`, the only answer on which containerd
  moves on to the registry. Checked end to end with containerd 2.2.1:
  every case that failed before pulls, from the next peer when there is
  one, and pulls through a healthy mirror are unchanged. Logged as before;
  counted by where the request ended up (`peer` or `miss`) instead of
  `error`.

## 1.8.9

### Fixed

- **Image cleanup stops when removing images frees nothing.** On nodes whose
  disk is filled by running images, volumes and logs, it kept deleting
  rollback images every minute without the disk moving. Now a batch that
  frees less than 0.2% of the disk pauses disk-pressure removals for an
  hour, or until the disk grows by 1%; the 6-hour rule keeps running. The
  "nothing is removable" warning no longer repeats every minute.
- Removals under disk pressure kick the reporter again, so the controller
  sees the freed space right away. A shadowed variable had stopped it.
- `angryduck_worker_gc_stalled{node}`, and *stalled* in the dashboard's
  per-node cleanup timeline.

### Changed

- Dashboard: registry bytes avoided, the node overview, the Transfers and
  Preheat tabs count spread traffic; the Propagation tab is now **Whole-Image
  Fallback**; spread rate legends show mean and max; spread nodes by state
  show the peak per interval.

## 1.8.8

### Changed

- **Pushed images spread blob by blob.** Before, a few seeds pulled the
  whole image from the registry, each of them, and nodes copied it from each
  other only once a seed had all of it. Now:
  - every node fetches one blob from the registry and one from a peer at a
    time;
  - a blob spreads between nodes the moment any node has it, rarest first;
  - the registry is asked only for blobs no node has, by at most
    `SPREAD_MAX_REGISTRY_PULLS` nodes at once, so it serves each blob about
    once instead of once per seed;
  - nobody waits on another node's pull: an idle node may race a slow
    registry pull (`SPREAD_RACE_PER_BLOB`), and a node pulling a blob still
    gets it pushed from a peer as soon as one has it. Whichever copy lands
    first wins; the node cancels the other itself, on the spot;
  - once a node holds every layer, the controller sends it the image's
    metadata, checked against its digests, and the node registers it.

  Blobs waiting for their image are held by a containerd lease that expires
  after an hour, so a crash or an abandoned job leaves nothing behind. Images
  whose manifest can't be read, or `SPREAD_ENABLED=false`, are seeded and
  propagated whole, as in 1.8.7. See [Propagation](docs/propagation.md).

### Added

- Worker endpoints `/spread/fetch`, `/spread/finalize`, `/spread/drop` and
  `/spread/content/<digest>`, controller endpoint `/spread/done`, all behind
  the shared token. See [API](docs/api.md).
- Spread metrics, labeled by image, node and blob only while that image is
  spreading. See [Observability](docs/observability.md#spread).
- `angryduck_controller_rescue_stuck_pods{node,image,in_flight}`: stuck pods
  per node and image.
- Dashboard: a **Spread** tab (spreads running, what each node still lacks,
  every blob moving now with its path, source and size, throughput per
  path), stuck pods per node and image on the Rescue tab.

### Fixed

- **Image cleanup no longer removes the image of a crashlooping pod.** It
  kept only images with a running container, and a crashlooping container is
  exited most of the time. Under disk pressure its image was removed between
  restarts, and kubelet pulled it from the registry again on every restart,
  about every 7 minutes. An image now stays while any container of a pod that
  is still up uses it, running or not. A completed Job's image is still
  removable. See [Image cleanup](docs/image-cleanup.md#never-removed).

### Added

- `angryduck_worker_gc_image_returns{node,image}`: images that came back to a
  node within an hour of cleanup removing them, and how many times, with a
  warning in the worker's log. Only the last hour is kept, so neither memory
  nor series count grows with uptime. The dashboard's Cleanup tab shows it as
  "Images Coming Back After Cleanup".

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
