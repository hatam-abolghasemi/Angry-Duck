# Changelog

## 1.13.2

Companion images now work on clusters like sahand-k8s-stg, where they found
nothing in 1.13.0 and 1.13.1.

### Fixed

- **Containers known only by image ID.** kubelet creates containers by image
  ID, and containerd reports no user-specified name, so no container looked
  like an app image. Workers now judge a container by every name its image has
  on the node, from the cleanup's own listing: no extra call.

### Changed

- **App images are those of the repos CI preheats**, whatever registry they
  come from. A sidecar in the same registry as the apps (an openresty built by
  the platform team, say) is a companion, since CI never preheats it. Until CI
  has preheated anything, there are no companions. `COMPANION_APP_REGISTRIES`
  and the credentials-file detection are gone; the setting is no longer read.
- **The companion set survives restarts.** Each worker saves the set it holds
  (`companions.json` in `/var/lib/angryduck`) and reports its app repos; a
  restarted worker keeps protecting it at once, and a restarted controller
  learns the app repos back from the workers without waiting for a push.
- **Dashboard tables size only columns with predictable values.** Free-text
  columns (node, namespace, component, registry, transfer source) take the
  free width instead of a fixed one, so long names are no longer cut off
  next to empty space. Kind and version pills are wide enough for
  `StatefulSet` and `1.13.2-pprof`.

### Added

- Steady-state benchmarks for the loops a worker and the controller run all
  the time (`go test ./internal/... -run '^$' -bench Steady -benchmem`).

## 1.13.1

A slow registry answer at the moment CI calls the webhook no longer sends the
push down the slow path.

### Changed

- **The webhook answers at once; the manifest is read in the background, with
  retries.** Before, the controller read the pushed image's manifest while CI
  waited, once, for `RANK_RESOLVE_TIMEOUT_S`; a single timeout made it seed
  the image whole: no layer-by-layer spread, nothing for mirrors to stream
  from, and the first wave of a rollout pulling from the registry. On
  sahand-k8s-stg one such run cost the registry 8.8 copies of a 500 MB image,
  against 1.3 for the runs whose read succeeded. Now the read is retried,
  backing off from 1 s to 15 s, for up to `TARGET_TTL_S` (120 s), and the
  image is seeded whole only if it never reads, or at once if the registry
  refuses Angry Duck's credentials. A newer tag of the same repo cancels a
  read still going. The answer carries `"status": "spreading"` and no
  `ordered_nodes` (the seeds are in `/status`); with `SPREAD_ENABLED=false` it
  lists the seeds as before. New metric
  `angryduck_controller_spread_starts_total{result}`: `first`, `retried`,
  `fallback`, `superseded`.
- **Bench:** `chaos worker-kill` finds the seeds in `/status` when the
  webhook's answer doesn't list them.

### Upgrading

- CI that read `ordered_nodes` from the answer gets none while spreading is
  on; the seeds show in `/status` under `spreads`.

## 1.13.0

The first wave of a rollout no longer pulls the same layers from the registry
once per node: mirrors stream them from the nodes still receiving them. Sidecar
images of app pods are found on their own and kept on every node, so a
rescheduled pod never waits for its exporter or proxy. And a benchmark and
chaos kit measures all of it on a test cluster.

### Added

- **Mirrors join spread transfers still running.** When no node holds a blob
  yet but some node is receiving it in a spread (a seed pulling it from the
  registry, or a node getting it from a peer), `/layers/holders` lists that
  node last, marked `"arriving": true`, and the mirror streams the blob from
  its `/spread/content` as the bytes land there, instead of answering `404` and
  sending containerd to the registry for one more copy. Any size counts. Nodes
  whose worker doesn't stream, or whose stream broke in that spread, aren't
  offered; manifests never come this way. New `result="stream"` on
  `angryduck_worker_mirror_requests_total` and `source="stream"` on
  `angryduck_worker_mirror_bytes_total`. On sahand-k8s-stg, a rolling update
  of a 500 MB image with the webhook called right before the rollout cost the
  registry 4.1 copies of the image before this change; the rest of those
  copies were first-wave nodes pulling what the seeds were pulling.
- **Companion images** (`COMPANION_ENABLED`, on by default, under
  `GC_ENABLED`). An image a pod runs beside an app image (from an app
  registry) is a companion: it is spread to every eligible node from peers,
  and image cleanup never removes it for age, only under disk pressure, after
  every other unused image. Workers tell companions apart from the cleanup's
  own container listing, now grouped by pod (no extra containerd call), and
  report each with its size (asked of the runtime once per image). The app
  registries are `COMPANION_APP_REGISTRIES`, or else every registry CI
  preheated from plus those in the credentials file, minus public ones.
  `COMPANION_MAX_MB` (300) caps what is kept. Spreading waits while a push
  spreads, reads no registry, and skips nodes above
  `PROPAGATE_MAX_UTILIZATION`. A restarted controller sends no set until every
  live worker has reported, and a fresh worker holds the age rule until the
  set arrives (5 minutes at most), so protection never lapses. New metrics
  `angryduck_controller_companion_images`, `_companion_bytes`,
  `_companion_nodes`, `_companion_transfers_total`, and the `companion` tier
  on `angryduck_worker_gc_candidates` and `_gc_images_deleted_total`;
  `/status` lists them under `companions`. See
  [Companion images](docs/companions.md).
- **Dashboard.** A *Companion images* row on the Disk Cleanup tab (kept, not
  kept, space, copies missing, copied, removed under pressure; every image and
  where it's missing; copies and removals over time), a *companions* column in
  *Cleanup Status*, the `companion` tier in the tier panels, and `stream` in the
  mirror panels.
- **`bench/`**: `duckbench.sh` benchmarks pulls with Angry Duck's features off,
  on without the webhook, and with preheat (any lead time), for DaemonSet
  rollouts and rolling updates, on fresh random images through a throwaway
  registry; and chaos tests for registry outages (tags resolved from peers,
  one holder via the mirror, rescue), killed workers or controller mid-spread,
  and disk pressure. See `bench/README.md`.

### Changed

- **Mirror hits include streams.** Every hit-ratio and peer-bytes query on the
  dashboard counts `stream` with `peer`.
- **Propagation runs pushes first.** Companion jobs take only the transfer
  slots pushes leave, never supersede a push and are never superseded by one.

### Upgrading

- Old workers ignore `arriving` holders' marker and ask their
  `/mirror/content`, which answers `404` for a blob still arriving: they go on
  to the registry as before.
- Companions need both binaries at 1.13.0. A 1.13.0 worker under an older
  controller holds its age rule for 5 minutes after it starts, then cleans up
  as before.

## 1.12.0

Images are ready sooner after their last layer lands. Nodes unpack each layer
as soon as it and the layers below it are here, instead of all at once at
the end; holders serve three nodes at a time instead of one; a finished
transfer starts the next within 50 ms instead of a second; and workers share
the node's network, so image bytes skip the pod network.

### Added

- **Unpacking as layers arrive** (`SPREAD_UNPACK_ENABLED`, on by default,
  under `SPREAD_ENABLED`). Every fetch order now says where its layer sits in
  the image (its chainID, the chainID below it and its media type). When a
  blob lands and the layer below is unpacked, the worker unpacks it under the
  job's lease, bottom up, one layer at a time per image, while the rest still
  arrive. A layer is committed only if what containerd unpacked chains to
  the chainID the image's config names. Finalize waits for a layer still
  being unpacked, then containerd's unpack skips what is done. A layer the
  node held before the push, and everything above it, is unpacked at
  finalize as before; so is any layer whose early unpack fails. A dropped
  job stops unpacking, and its lease takes the work with it. New metric:
  `angryduck_worker_spread_unpacks_total{node,result}` (`applied`,
  `present`, `failed`), shown on the dashboard's Delivery tab in a new
  *Unpacking on arrival* row: layers by result, and layers unpacked per node.
- **`WAKE_COALESCE_MS`** (50): how long spread, propagation and rescue wait
  after a wake-up (a transfer ended, a pod got stuck) before they look again.
- **Helm `worker.hostNetwork`** (`true`) and the worker's `HOST_NETWORK`.

### Changed

- **Workers share the node's network by default.** Transfers between nodes
  skip the pod network (veth, iptables, any tunnel). The worker API binds the
  node's own address only (`POD_IP`, port `18081`), not every interface of
  the node, and the mirror binds the node's loopback (`127.0.0.1:18082`),
  which is also what `hosts.toml` points at: the address no longer changes
  when a worker restarts, so containerd never dials a dead pod IP. The worker
  keeps every other restriction (no capabilities, no host PID, read-only root
  filesystem). Pod NetworkPolicy no longer covers port `18081`; restrict it
  with a host firewall or your CNI's host policies (see
  [Security](docs/security.md)). `worker.hostNetwork: false` keeps the
  pod-network setup of 1.8.6 to 1.11.1. Ports `18081` and `18082` must be
  free on every node.
- **`PROPAGATE_PER_SOURCE` defaults to 3** (was 1). One send rarely fills a
  holder's link, since the receiver's writes into containerd are usually the
  limit; serving from page cache costs the holder little CPU. It applies to
  spread, whole-image propagation and spread's whole-image fallback alike.
- **A finished transfer starts the next within 50 ms** (was 1 s), for
  spread, propagation and rescue alike.

### Upgrading

- The worker DaemonSet changes to `hostNetwork: true`; Helm and
  `k8s/daemonset-worker.yaml` both carry it. Workers rewrite their
  `hosts.toml` files at startup, so nothing else changes on the nodes.
- Workers before 1.12.0 ignore the new placement in fetch orders and unpack
  at finalize; a 1.12.0 worker under an older controller does the same.

## 1.11.1

Dependency updates only; nothing behaves differently.

### Changed

- **Dependencies:** gRPC 1.85.0 (was 1.82.1), protobuf 1.36.12 (the release;
  was a pre-release of it), `golang.org/x/sys` 0.49.0 (0.47.0), and with them
  OpenTelemetry 1.45.0, `golang.org/x/net` 0.60.0, `x/text` 0.42.0 and
  `x/sync` 0.23.0. Pooled content writes still go through gRPC's codec API,
  and the worker binary is 4 KB larger.

## 1.11.0

Peers share even when the registry can't say what a tag means. The mirror
resolves tags: the registry decides while it answers, and when it can't (a
refused pull secret, a deleted tag, an outage), the newest copy in the cluster
is served, so a node holding the image keeps pods out of `ImagePullBackOff`.
Rescue is left for what the mirror can't serve, and says why each pull
failed. The dashboard has an Impact tab.

### Added

- **The mirror resolves tags.** containerd now asks the mirror what a tag
  points at too (`capabilities = ["pull", "resolve"]` in the `hosts.toml`
  files, which each worker rewrites). Before, the tag lookup always went to
  the registry with the pod's pull secret, so a pull whose lookup failed (a
  refused pull secret, an outage) never reached the mirror, even with every
  blob on a peer: the pod went to `ImagePullBackOff`, and rescue copied the
  image, logged and counted as a rescue. Now that pull is a mirror hit.
  - The registry decides while it can answer: the mirror asks it with a
    `HEAD` and Angry Duck's credentials, waiting at most
    `MIRROR_RESOLVE_TIMEOUT_MS` (1000), and serves that digest from the node
    or a peer, or answers `404` when no node holds it, so a moved tag is never
    served stale.
  - When the registry gives no digest (down, too slow, refusing Angry Duck's
    credentials, or no longer knowing the tag), the newest copy a node holds
    is served. The controller asks the holders what the tag points at and
    since when (`GET /images/tag` on the controller, `GET /mirror/tag` on the
    workers), once per tag for every mirror asking within
    `MIRROR_RESOLVE_NEWEST_CACHE_S` (30), giving them
    `MIRROR_RESOLVE_NEWEST_TIMEOUT_MS` (2000) to answer; the mirror waits
    `MIRROR_RESOLVE_CONTROLLER_TIMEOUT_MS` (3000) for the controller.
  - A tag no node holds is a `404` at once, without asking the registry: one
    request to the controller, then containerd goes to the registry as
    before. A registry that gave no answer is left alone on that node for
    `MIRROR_RESOLVE_UNREACHABLE_TTL_S` (30), so pulls during an outage don't
    each wait for the timeout.
  - The mirror only decides. It sends one `HEAD` per tag and never downloads
    from a registry; whatever only the registry has, containerd still pulls,
    with the pod's pull secret. No new goroutine or timer runs while nothing
    is pulled.
  - `MIRROR_RESOLVE_ENABLED` (on by default) turns it off; it is under
    `MIRROR_ENABLED`. New metric
    `angryduck_worker_mirror_resolves_total{node, origin, result}`: the
    registry's answer (`ok`, `missing`, `denied`, `unreachable`, or
    `unchecked` when no node holds the tag) and where the manifest came from
    (`local`, `peer`, `miss`).
- **Why a pull failed, on rescues.** The controller reads it from kubelet's
  message when the pull first fails and keeps it through the back-off
  messages that don't repeat it: `missing` (the registry doesn't have the
  image), `denied` (the pod's pull secret was refused), `unreachable` (no
  usable answer) or `other`. It is in `/status` (`cause`), in the rescue log
  lines, on `angryduck_controller_rescue_stuck_pods` (a `cause` label), and in
  the new `angryduck_controller_rescues_by_cause_total{cause, result}`,
  cluster-wide so it adds 20 series whatever the number of nodes.
- **`ORIGIN_CHECK_TIMEOUT_S`** (5): how long one origin check waits for the
  registry, token request included. It was a fixed 10 seconds.
- **Dashboard.** *Mirror & Registry* has a *Tag resolves* row: tags resolved
  by the registry's answer, and where their manifests came from. *Rescue &
  Repair* has *Rescue Attempts by Cause* and *Pulls the Mirror Kept from
  Getting Stuck* (tags the registry couldn't resolve, served by a node
  instead), and *Stuck Pulls* shows why each pull failed.
- **Dashboard: an Impact tab.** What Angry Duck changes for the pods and
  what it costs, from kubelet, cAdvisor, kube-state-metrics and node-exporter
  next to Angry Duck's own metrics. Kubelet's numbers don't depend on Angry
  Duck, so a cluster that doesn't run it fills most of the tab too, as a
  baseline to compare against.
  - *Activity*: new image versions, pod starts, kubelet's own pulls, pulls
    per pod start, pulls Angry Duck saved (images it delivered to a node
    that then ran them), container restarts, starts, pulls and restarts per
    minute, and the workloads creating the most pods.
  - *Pulls & pod start*: time spent pulling and its share of pod start,
    kubelet's pull p95, bytes kept off the registry, failed pulls, disk
    pressure time, containers waiting on images, and image delivery against
    all inbound node traffic.
  - *Kubelet image pulls*: pull time as a heatmap and as p50, p90, p99 and
    average, pulls by image size, and pull time per image size and per node.
  - *Disk*: containerd's disk per node; disk pressure, evictions and
    kubelet's image GC, shown only when they happened.
  - *What it costs the node*: Angry Duck's CPU throttling and memory against
    its limits, and how busy each node's disk is.

### Changed

- **The origin check gives up after 5 seconds** (was 10). A registry that
  takes longer to answer a `HEAD` counts as `unreachable` and is asked again
  four times as often, as before.
- Reading why a pull failed costs the rescuer one copy of kubelet's message
  per stuck image: 20 stuck pods on one image take as long as before (about
  6 µs a look) with one allocation more.

### Upgrade notes

- **Pull secrets guard the first pull into the cluster, not the copies.** A
  tag some node holds no longer reaches the registry with the pod's pull
  secret, so a broken pull secret shows only where no node has the image yet
  (as it did with rescue, minus the `ImagePullBackOff` first). Where
  per-namespace registry permissions must hold inside the cluster, set
  `MIRROR_RESOLVE_ENABLED: "false"`. See [Security](docs/security.md).
- `imagePullPolicy: Always` pods are covered during a registry outage now,
  when a node holds the tag; rescue still skips them.
- A tag the registry moved to a digest no node holds yet costs the registry
  two `HEAD`s instead of one: the mirror's, then containerd's own.
- Either side can be upgraded first. A 1.11 mirror whose controller doesn't
  know `/images/tag` leaves every tag to the registry, as before; a 1.11
  controller just gets no answer from holders still on 1.10.
- Alerts or panels grouping `angryduck_controller_rescue_stuck_pods` by all
  its labels get a `cause` label.

## 1.10.0

Rollbacks without keeping old images everywhere: when the cluster stops
running an image, one node, the one with the most room, keeps it for 24
hours, and every other copy goes after an hour. Every feature has an on/off
switch, and turning one off turns off everything beneath it. The mirror is on
by default. Image cleanup does about a fifth of the allocations it did.

### Added

- **Retention copies.** When the last container in the cluster stops using
  an image (a new version replaced it, or its pods were removed), the
  controller picks one node to keep it for `GC_RETENTION_TTL_S` (24h),
  counted from that moment. It picks among nodes below
  `GC_RETENTION_MAX_UTILIZATION` (the cleanup's high mark, 70%) one that
  holds the image, preferring one holding its layer blobs (only blobs can be
  served by the mirror), then the most free disk. If no node with room holds
  it, it is copied (through rescue) to the node with the most room. The
  worker marks the image in containerd,
  `angryduck.io/last-survivor=<expiry>`, on every name, and its cleanup
  leaves it alone, disk pressure included, until the time is up, then
  removes it at once. A rollback in the meantime pulls through the mirror
  from that node and the peers that receive it, or through rescue if the
  registry is down.
  - While every node is at or above the limit, no copy is kept; as soon as
    one is below it, images that went unused meanwhile get theirs, until
    their 24 hours are over.
  - An image that runs again loses its mark. A copy whose node went away or
    no longer has it is picked again elsewhere. A second copy (two marks for
    one image) is unmarked.
  - Nothing is lost on a restart: marks live on the images and workers
    report them, so a restarted controller picks them up. A restarted worker
    keeps its marks, and until its first cleanup look its containers still
    count as using what they used, so a worker rollout doesn't look like
    every image going unused.
  - Workers send the images their containers use (by target digest) and
    their marks in the reports they already send, only when they change;
    the controller acts on them at once, and looks every
    `GC_RETENTION_INTERVAL_S` (30s) for what time alone changes.
- **Feature switches as a tree.** Both binaries resolve every `*_ENABLED`
  switch in one place and log the result at startup. A child is on only if
  its parent is: `GC_ENABLED` → `GC_RETENTION_ENABLED`, `SPREAD_ENABLED` →
  `SPREAD_STREAM_ENABLED`. A feature turned off only by its parent gets a log
  line saying so. With `GC_ENABLED=false` kubelet does all image cleanup: no
  age rule, no disk-pressure removal, no retention, and workers remove any
  retention mark at startup.
- `GC_RETENTION_ENABLED` (`true`), `GC_RETENTION_TTL_S` (`86400`),
  `GC_RETENTION_MAX_UTILIZATION` (`GC_HIGH_UTILIZATION`),
  `GC_RETENTION_INTERVAL_S` (`30`).
- Worker endpoint `POST /images/retain` (shared token), and `retain` as a
  rescue reason.
- `angryduck_worker_gc_retained_images{node}`,
  `angryduck_controller_retention_copies{state}`,
  `angryduck_controller_retention_paused`,
  `angryduck_controller_retention_events_total{event}`, the `retention`
  tier in `angryduck_worker_gc_images_deleted_total` and
  `angryduck_worker_gc_image_removal_seconds`, and, in each image's history,
  `angryduck_controller_image_retention_timestamp_seconds{image,node,event}`.
- **Dashboard: retention.** A *Retention copies* row on the Disk Cleanup tab:
  copies kept and waiting, whether retention is paused, copies kept, used
  again and lost in the range; every copy with its node and when it expires;
  retention events over time; and each copy's history. On the Image tab,
  *The Image on Each Node* shows *kept for rollbacks* on the node keeping the
  image's copy, and *Kept for Rollbacks* (under *Pulls, repairs & cleanup*)
  lists its history.
- `/status`: `retention`, every copy with its node and expiry.
- Recipes: *Leave cleanup to kubelet*; *Rollbacks matter most* now keeps
  copies longer.

### Changed

- **`GC_UNUSED_FOR_S` defaults to `3600`** (was `21600`): an image unused on
  a node for an hour is removed, whatever the disk. The retention copy is
  what keeps rollbacks local. This includes images a spread put on a node
  that never ran them: a preheat that lands more than an hour before its
  rollout is removed before use.
- **`GC_LOW_UTILIZATION` defaults to `0.40`** (was `0.60`): a disk that
  crosses 70% is cleaned down to 40%. Cleanup runs less often but removes
  more each time, in batches as before, with the same stop when a batch
  frees nothing.
- **`MIRROR_ENABLED` defaults to `true`** (was `false`; the manifests and
  chart already set it). It takes effect where containerd's `config_path` is
  set, and costs nothing where it isn't.
- **Cleanup is lighter.** An image's names are grouped and sorted in one
  pass into one array, instead of a slice per image and a sort per repo. On
  a node with 300 images (100 in use), a cleanup look went from 1145 to 236
  allocations and from 243 KB to 137 KB, at about the same CPU time; with
  retention on, the same. Image repair's listing uses the same grouping.
- The controller keeps each node's in-use images (interned, like its image
  list): about 0.5 MiB more at 150 nodes with 60 running images each, and
  0.1 MiB for retention itself. Steady reports still allocate nothing. A
  retention look over 150 nodes takes about 0.5 ms.
- **Dependencies:** containerd's client 1.7.36 (was 1.7.27), gRPC 1.82.1
  (1.65.0), protobuf 1.36, `golang.org/x/sys` 0.47.0, CRI API 0.37.2, OCI
  image-spec 1.1.1. containerd's API module stays at 1.8.0: 1.9 and later
  are for containerd 2 and don't build with the 1.7 client, so Dependabot
  now leaves it alone. Building needs Go 1.26; the images build with Go
  1.27.3. CI and release actions are on their current major versions.
- **Pooled content writes kept working on the new gRPC.** gRPC 1.66 moved
  codecs to a new API and stopped returning the old proto codec, which
  would have quietly turned the worker's pooled encoding of content writes
  off: every 1 MiB write into containerd would again be a fresh allocation,
  and gRPC's own pool zeroes each buffer before use. The pooled encoding now
  uses the new API, and gRPC hands each buffer back once its transport wrote
  it. Measured against 1.9.4 on the same Go, a 1 MiB write costs the worker
  about 6% less CPU and 30% less memory.
- `app/.env.example` lists every setting again (the streaming, slow-chain and
  repair settings were missing); `GC_RETENTION_MAX_UTILIZATION` is left empty
  there, so it follows `GC_HIGH_UTILIZATION`.
- kubelet's `imageMaximumGCAge` should now be unset or above
  `GC_RETENTION_TTL_S` (was `GC_UNUSED_FOR_S`); see
  [Node settings](docs/node-settings.md).
- Dashboard: the `rollback` tier is now `retention`; *Cleanup Status* shows
  the retention copies each node keeps; *Images Coming Up for Removal* and
  *Next Removal In* include retention copies' expiry.

### Removed

- **The rollback tier and `GC_ROLLBACK_KEEP`.** The newest 3 unused images
  per repo on every node are no longer kept; one retention copy per image in
  the cluster replaces them. A worker that still finds `GC_ROLLBACK_KEEP` set
  logs that it is ignored. `tier="rollback"` is gone from
  `angryduck_worker_gc_candidates` and
  `angryduck_worker_gc_images_deleted_total`.

### Fixed

- **Dashboard: no "without Angry Duck" claim.** *Registry Downloads,
  Without vs With Angry Duck* presented every byte delivered to the nodes as
  what the registry would have served without Angry Duck. Spreads reach every
  eligible node, including nodes that never run the image, so that
  overstated the savings. The chart is now *Image Bytes Delivered, All vs
  From the Registry*, *Where Image Bytes Came From* is relabeled the same
  way, and the Overview's *Registry Traffic Avoided* is *Bytes from Peers*.
  The README caption and the Observability docs say what the numbers
  measure, including that registry pulls containerd makes on its own aren't
  counted.

### Upgrade notes

- Disks will hold fewer images: unused images go after an hour instead of
  six, and only one copy of each image that went unused stays, for 24 hours.
  To keep the old pace, set `GC_UNUSED_FOR_S: "21600"` and
  `GC_LOW_UTILIZATION: "0.60"`.
- Remove `GC_ROLLBACK_KEEP` from your values or ConfigMap; it does nothing.
- Retention needs the shared token on both sides (as rescue does), and
  copying to a node that doesn't hold the image needs `RESCUE_ENABLED`.
- Alerts or panels on `tier="rollback"` should use
  `angryduck_worker_gc_retained_images` instead.
- During the rollout, workers still on 1.9.x don't report their running
  images, and can't keep a copy: an image running only on such nodes may get
  a retention copy it doesn't need yet, which is dropped as soon as their
  workers run 1.10.0 and report it running.

## 1.9.4

Runs on arm64 nodes, installs without a clone, and the dashboard tells
the truth about colors and memory. Nothing changed in how the controller
or the workers behave.

### Added

- **arm64 images.** Both images are built for `linux/amd64` and
  `linux/arm64`, so Angry Duck runs on Graviton, Ampere and other arm64
  nodes. CI builds both binaries for arm64 on every change.
- **SBOM and provenance** attached to every released image.
- **A chart check in CI:** `helm lint`, and a render of the chart's defaults
  that must match `k8s/` object for object.
- Dependabot for Go modules, the Dockerfiles' base images and the GitHub
  Actions, monthly.
- A contributing guide and a code of conduct.

### Changed

- **Install the chart without cloning:** the README and the Helm guide use
  the published `oci://ghcr.io/hatam-abolghasemi/charts/angryduck`.
- The Grafana dashboard no longer carries a data source, cluster or
  registries of ours: pick yours on import.
- **Dashboard fixes:** series colors matched by pattern never applied
  (Grafana anchors those patterns), so *present*, *cancelled* and *failed*
  transfers and the mirror results got random colors; they now get their
  meant ones, with *present* grey. *Memory by Kind* shows one process,
  averaged, without the memory already handed back to the OS, which made it
  look like a leak. The pull and start stats have titles that fit.
- Development docs: the full release checklist (`version.go`, chart, tags,
  changelog, wait for CI before tagging), and the code layout's `version` and
  `debugpprof` packages.
- Issue and pull request templates point to `helm/angryduck`, and the bug
  report lists Repair and Origin check.
- Screenshots of the dashboard, from our staging cluster, in the README and
  in [Observability](docs/observability.md).

## 1.9.3

The dashboard explains itself: what didn't happen stays out of sight,
colors mean the same thing everywhere, and tables fit their content.
Nothing changed in the controller or the workers.

### Changed

- **Dashboard: easier to read.** Charts and tables for things that didn't
  happen in the time range hide themselves (seed pulls, propagation, repairs,
  streaming, ...) instead of showing a flat zero, and empty status tables say
  what empty means. Lines no longer break where a series disappears: absent
  is drawn as 0. Table columns are sized to their content, and colors keep
  one meaning everywhere (green fine, amber look, red critical, sky blue in
  progress, teal kept in the cluster, peach registry downloads, violet event
  counts); image names take their registry's color. Chart series have plain
  names (*skipped: no peer has it* instead of `no_source`). Every number has
  a sparkline. Annotations show only on the charts they concern, and flapping
  ones mark once.
- **Dashboard: a Registry filter.** The global *Image* filter is replaced by
  *Registry*; each tab that lists images has its own *Image* filter within
  the picked registries.
- **Dashboard: what Angry Duck saves.** *Where Image Bytes Came From* shows
  the bytes per source next to what the registry would have served without
  Angry Duck, and *Registry Downloads, Without vs With Angry Duck* adds both
  up over the time range. *Workers Up* is compared with the DaemonSet's
  desired count.
- **Dashboard: no phantom rows.** Joined tables (*Delivery per Node*,
  *Spreads*, *Repairs*, ...) listed rows that only a side query produced,
  such as pods of images nobody delivered, with the panel's empty message in
  their cells. They are dropped now. *Delivery Time per Node* (images, average
  and slowest delivery, bytes per path) joins the Image tab, and stream orders
  share a chart with the slow-chain guard.
- **Dashboard: fixed state timelines.** *Cleanup State over Time*, *The Image
  on Each Node* and the containerd event watch drew every bar grey with a
  `-∞+` legend: their values were bucketed by threshold, ignoring the value
  mappings. They now show idle/cleaning/stalled, running/receiving/stuck, and
  so on. The event watch only lists nodes that lost containerd's events.
- **Dashboard: numbers that mean what they say.** *Failed Rescues in Range*
  counts rescues that failed, not the retries for images no peer holds (those
  are in *Rescues by Result* as *skipped: no peer has it*). Live-only numbers
  that read 0 nearly all the time became range numbers: *Spreads in Range*,
  *Spreads Finished in Range*, *Registry Bytes for Spreads*, *Rescued in
  Range*. The per-node mirror hit ratio uses a one-hour window instead of
  jumping between 0 and 100%. *Registry Status* gains a *served* gauge and
  lists the worst registry first.

## 1.9.2

Workers give their memory back: after serving images, their container
memory returns to its idle level instead of staying high.

### Added

- **Dashboard:** a *Streaming & slow chains* row on the Delivery tab:
  bytes each node streamed while still receiving them, stream orders, and
  what the slow-chain guard did (`race`, `replaced`, `kept`). *Spread
  Transfers by Result* explains `upstream`.
- **The memory `kubectl top` shows, as a metric.**
  `angryduck_process_memory_bytes` gains `working_set` (what `kubectl top`
  and `container_memory_working_set_bytes` show) and `page_cache` (file
  cache charged to the container), read from the container's cgroup.
- `angryduck_worker_page_cache_released_bytes_total`: page cache a worker
  handed back after serving blobs.
- **Profiling builds.** Built with `-tags pprof` and run with
  `DEBUG_PPROF_ADDR` set (e.g. `127.0.0.1:6060`), either binary serves Go's
  `/debug/pprof/` there. Release images are built without the tag: the
  endpoints aren't in them.

### Changed

Workers use much less memory, most visibly after serving images: their
container memory no longer stays high after a transfer.

- **Page cache from serving blobs is given back.** Reading a blob file to
  serve it (a spread source, the mirror, a rescue) cached it, charged to the
  worker's pod, where it stayed in its container memory until the node ran
  short. Serving one 120 MB layer twice took a worker from 6.5 MiB to
  126 MiB. Now, once nothing has read a file for
  `PAGE_CACHE_RELEASE_AFTER_S` (30), the worker drops the pages its own
  reads brought in; pages that were cached before stay, and nothing is
  dropped while a blob keeps being served. In a 3-node lab, a node
  re-fetching a 190 MB image from peers left them at 149.8 MiB and
  60.1 MiB with 1.9.1, and at 7.7 MiB and 10.3 MiB 45 seconds later now,
  with the same transfer time. `0` keeps the pages, as before.
- **Smaller worker binary.** Built with gRPC's `grpcnotrace` tag: without
  `golang.org/x/net/trace` and the `html/template` it brought, the binary is
  14.7 MB instead of 19 MB, and its code mapped in memory about 4 MiB less.
- **Content writes reuse their buffers.** Every blob that reaches a node
  through the worker goes to containerd in 1 MiB writes, each encoded into a
  new buffer, and every transfer took a new 1 MiB buffer of its own: a
  190 MB image left about 205 MB of garbage on each receiver. Both are
  reused now (about 15 MB), so the collector runs far less during
  transfers, and the heap stays closer to what's live. Worker CPU for the
  same spread dropped by a third to a half (0.63–0.85 CPU-seconds per node
  with 1.9.1, 0.34–0.57 now).
- **Freed heap goes back to the OS seconds after a burst** (a transfer, a
  request, a pass over the images), not up to a minute later; the
  once-a-minute check stays as a safety net.
- **Leaner idle state.** The layer inventory and image repair's per-image
  record keep digests as 32 raw bytes instead of strings, cached content and
  snapshot listings are let go once past their 15 seconds, and event
  metrics expire after `METRICS_EVENT_TTL_S` even when nothing scrapes the
  process (before, unscraped they piled up to 5,000 series per metric).

## 1.9.1

### Fixed

- **Streaming checks it reads the right containerd.** At startup the
  worker writes a small blob through containerd and looks for its file in
  the mounted content store. If it isn't there (`CONTAINERD_ROOT` pointing
  at another daemon's store), streaming is turned off with a warning
  instead of streams waiting on ingest files that never appear.
- CI: the integration tests are given their containerd's root
  (`ANGRYDUCK_CONTAINERD_ROOT`), so they read its content store, not the
  runner's own containerd.

## 1.9.0

Large layers reach every node much sooner: a node still receiving a blob
serves it on as the bytes arrive, small layers no longer wait behind large
ones, and the registry serves each blob once. In a 6-node lab (400 Mbit
links, a 152 MB image with a 120 MB layer, default settings) the image
reached every node in 10.2 s on average instead of 16.1 s with 1.8.15, with
the registry serving it once every time and no more worker CPU or memory.

### Added

- **Streaming large blobs.** A node receiving a blob of
  `SPREAD_STREAM_MIN_MB` (32) or more, from the registry, a full copy or
  another stream, is a source for it too. The peer gets the bytes as
  containerd writes them, read from containerd's own ingest file: nothing
  is buffered or written twice, and every receiver's containerd checks the
  digest at commit as before. Nodes holding the blob are still preferred, a
  stream only goes to a node with no transfer of that blob yet, never in a
  circle. Chains are capped by `SPREAD_STREAM_MAX_DEPTH`, `0` (default):
  only by the number of nodes.
- **A broken stream is the source's failure.** If the source's copy lands
  over its other path, the stream goes on from it. If its transfer fails,
  the stream is cut, the receiver reports `upstream`, isn't backed off and
  gets the blob elsewhere right away; that source streams no more for the
  spread.
- **Slow-chain guard.** A chain is only as fast as its slowest node, so
  the controller watches for transfers running more than
  `SPREAD_SLOW_FACTOR` (2) times their expected time (the blob's size at
  the typical speed of its path, learned from finished transfers), and
  wakes exactly when the next one would turn late. A late streamed
  registry pull is raced from the registry after all, and every stream
  from the slow node moves to whichever of its copies is further ahead,
  so the chain speeds up without starting over. A late peer transfer (a
  stream behind a slow node, or a copy from a slow holder) is offered a
  restart from another holder; the receiver takes it only if it should
  finish clearly sooner, judging its own link by its fastest finished
  transfer, so a node whose own link is the limit keeps what it has. The
  same judgement applies to the registry-versus-peer race: a node at its
  own link's limit declines a second copy instead of splitting its link.
- **Express slots for small blobs.** Blobs up to `SPREAD_EXPRESS_MAX_MB`
  (32) have their own `PROPAGATE_PER_SOURCE` budget on each source, so a
  30 MB layer isn't stuck behind a 300 MB one on the only free holder.
- `SPREAD_STREAM_ENABLED` (`true`), `SPREAD_STREAM_MIN_MB` (`32`),
  `SPREAD_STREAM_MAX_DEPTH` (`0`), `SPREAD_EXPRESS_MAX_MB` (`32`),
  `SPREAD_SLOW_FACTOR` (`2`).
- `angryduck_worker_spread_streamed_bytes_total`,
  `angryduck_controller_spread_stream_orders_total`,
  `angryduck_controller_spread_slow_total{action}`, and
  `result="upstream"` in `angryduck_controller_spread_transfers_total`.
- `/status`: `stream` on each transfer streamed from a node still
  receiving it.

### Changed

- **No registry race for a streamed blob, unless it is late.** A large
  blob whose registry pull is streamed on is no longer raced by a second
  registry pull: other nodes join the stream instead of splitting the
  registry's bandwidth. The slow-chain guard races it once it is late.
  Blobs nobody streams (small ones, or with streaming off) are raced as
  before. In the lab, 1.8.15 pulled the 120 MB layer twice in 3 of 6 runs.

### Compatibility

- Workers report whether they stream; the controller streams only between
  workers that do. A worker without containerd's content store mounted, or
  older than 1.9.0, neither streams nor is streamed to, so a rollout can go
  in any order. All new wire fields are optional. Spread transfers carry
  an ID that workers echo in their reports; reports without one are
  matched by blob and path as before.

## 1.8.15

The blob is the unit everywhere: a node now receives any number of
different blobs at once and registers different images side by side,
instead of one blob per path and one image at a time. A node too full to
receive no longer keeps a spread from counting as complete.

### Changed

- **Transfers per blob, not per path.** A node fetched one blob from the
  registry and one from a peer at a time, for all images together. Now it
  fetches any number of different blobs at once; the same blob still comes
  at most once per path, so the registry-versus-peer race is unchanged. The
  controller hands out new transfers in rounds, one per node per round, so
  the first node in line doesn't take every free source. Peer traffic is
  bounded only by `PROPAGATE_PER_SOURCE` per sender, registry traffic by
  `SPREAD_MAX_REGISTRY_PULLS` cluster-wide. A worker answers `409` only for
  a blob it is already fetching over that path, so far fewer orders come
  back `rejected`.
- **Images register side by side.** Registering (importing and unpacking)
  ran one image at a time per node, so a small image waited behind large
  unpacks of other images: on staging a 0.7 s registration waited about
  12 s, leaving a pod 7 s from pulling the image itself. Now only finalizes
  of the same image wait for each other. There is no cap across images:
  unpacking is bounded by the CPU and IO containerd gets.
- **Full nodes don't hold a spread open as unfinished.** A spread counts as
  complete, logged and recorded, once every node with room has the image.
  It then stays open quietly, publishing nothing, so a node above
  `PROPAGATE_MAX_UTILIZATION` still receives the image once cleanup makes
  room, until a newer tag supersedes it. Before, one node whose disk was
  filled by something else kept every spread "running" until the next tag,
  and its duration was never recorded. A spread already complete isn't
  counted again as superseded or expired.
- **Skips are logged.** The controller logs once per spread each node it
  skips for a full disk, with its utilization, and lists them when the
  spread completes. Skipped nodes no longer appear in "bytes still missing".

### Added

- `angryduck_worker_spread_finalizing`: images being registered on a node
  right now. `angryduck_worker_spread_active` now counts transfers per path
  instead of being 0 or 1.
- `/status`: `complete` and `skipped` per spread, and `transfers` per node
  (every blob coming in, its path and source). `registry`, `peer` and
  `peer_from` still name the first of each.
- The worker's log line names any wait for another finalize of the same
  image: `registered in 716ms (waited 1.2s for another finalize of it)`.

### Known limits

- Two spreads needing the same blob on the same node at once: the second
  order gets `409` and that node waits 5 s for that spread, as before.

## 1.8.14

Angry Duck now reports what happened to each image: when its spread started
and ended, how and how fast each node got it, its seed pulls and repairs, and
which nodes removed it or will remove it next. The dashboard reads this history
back from Prometheus over the selected time range.

### Added

- **Per-image spread and delivery events (controller).** For every spread and
  whole-image fallback: start time, duration and result (`running`,
  `complete`, `superseded`, `expired`). For every node: when it got the image,
  how long it took and how (`present`, `peer`, `registry`, `peer+registry`,
  `local`, `image`, `seed`, `propagate`, `rescue`, or `kubelet` when it got the
  image without Angry Duck). Bytes received and transfer time per path are
  counted inside the spread and published once per node: when it gets the
  image, or when the spread ends without it.
- **Per-image seed pull, repair and cleanup events (worker).** Each seed pull
  with its duration and result. Each repair with its duration, result, and
  bytes from peers versus the registry. Each image cleanup removed, with its
  tier. A live gauge of what cleanup removes next and in how many seconds
  (rollback-kept images aren't listed).
- **`METRICS_EVENT_TTL_S`** (default `15`, `0` turns events off). Events stay on
  `/metrics` for this long, then disappear; Prometheus keeps the history. A
  spread that is still running keeps its live duration until it ends. Each
  metric is capped at 5,000 series. A TTL shorter than one scrape interval can
  miss events. See [configuration](docs/configuration.md).

### Changed

- **Dashboard.** History tables read events with `last_over_time` over the
  dashboard's time range, so they reach back as far as Prometheus retention.
  What comes up for removal next and running spreads stay live. The spread
  annotation is off by default. The "Deliveries by Method" chart is gone,
  because 15-second events would only show up as spikes. See
  [observability](docs/observability.md).
- **Docs** no longer point to panels and tabs that don't exist ("Snapshot share
  of shipped layers", the "Spread" tab).

### Known limits

- Pulls kubelet makes without Angry Duck show up only as `kubelet`, with no
  bytes.

## 1.8.13

Broken images are repaired in place, piece by piece, instead of removed; the
remaining "already here but not held" race is closed; and the dashboard is
rebuilt around a few questions.

### Changed

- **The image repair fixes images in place; it never removes them.** It
  fetches only the pieces that are gone, by digest: first from peers that hold
  them (the controller's layer index says which; they serve them through
  their spread endpoint), then from the image's registry. Everything still
  here is held under a lease of the repair meanwhile: the highest unpacked
  snapshot and the blob of every layer above it. The image's metadata is then
  re-imported under its own name, which unpacks it and republishes the name
  pointing where it already did. containerd checks every digest as it writes,
  so a bad copy fails and the next source is tried. When no peer and not the
  registry has a piece, the image stays as it is, reported broken, and the
  next check tries again. In 1.8.12 the image was removed one interval after
  being found, every layer was then pulled again from the registry, and
  shared layers could be collected meanwhile.
- **Metadata is repaired too.** A missing or unusable index, manifest or
  config (one that doesn't parse, or isn't the size its parent names) made the
  check give up and call the image fine. Now it counts as broken and is
  fetched like any other piece. An unusable blob is removed first, and
  containerd's garbage collector is run right away so the corrupted file is
  really gone before the good copy is written (containerd adopts the file it
  already has for a digest).
- **Corruption is found by real failures only, never by hashing.** When a
  pod's container fails to start on an image with `CreateContainerError`
  (`failed to extract layer`, `failed to get reader from content store`), the
  controller now sends that node's worker a repair order (`POST /repair`)
  carrying kubelet's error. The worker repairs the image right away. A layer
  whose blob is here but failed to extract (not "not found", not a full disk)
  has just that blob replaced. Before, the controller only reacted to
  ImagePullBackOff, and the repair waited for its next check.
- **No second look before acting.** The repair only adds pieces, so it acts on
  the first verdict confirmed against fresh listings. `IMAGE_REPAIR_INTERVAL_S`
  (120) is now only how often every image is checked, and a check is cheaper:
  an image's metadata is read and parsed once per digest (it never changes),
  and later checks use the worker's shared content and snapshot listings
  only. 1.8.12 read and parsed every image's index, manifest and config on
  every check.
- **A node no longer reports an image it can't run.** While an image is
  broken, its names are left out of the worker's reports, so the controller
  neither skips the node as already served nor picks it as a rescue or
  propagation source.
- **Dashboard rebuilt**: 5 tabs (Overview, Delivery, Rescue & Repair, Mirror &
  Registry, Disk Cleanup) and 28 panels instead of 9 tabs and 69, each with a
  plain-language title and a one-line description. New variables: registry,
  kind, namespace, component and image. The component views come from
  kube-state-metrics: which Deployments, StatefulSets, DaemonSets and CronJobs
  use a stuck, broken or no-longer-served image, and which pods wait on it.
  Annotations for releases rolling out, spreads starting and ending, rescue
  failures, repairs, disk cleanup and registry reachability. Every query was
  run against Prometheus with synthetic data.

### Fixed

- **A rescue holds the layers it counts as already here.** It planned from
  listings that could be a moment old and held nothing until the import, the
  same race 1.8.12 closed for spreads. Since 1.8.12 such a rescue failed
  cleanly, wasting its transfer. Now the highest unpacked snapshot and the
  blobs above it are held under the rescue's lease first; one that turns out
  gone is listed again and shipped.
- **`angryduck_controller_seed_missing_bytes` only shows the latest seeded
  image.** It kept values from older images for nodes not seeded since.

### Added

- `angryduck_worker_broken_image{node,image}` (1 while broken, removed after),
  `angryduck_worker_image_repairs_total{node,result}`,
  `angryduck_worker_repair_bytes_total{node,source}`,
  `angryduck_controller_repair_orders_total{node,result}`, and
  `angryduck_build_info{component,version,node}` (value: start time). The
  controller gets `NODE_NAME` from the downward API for the last one.

### Removed

- `angryduck_worker_broken_images` (use `count by (node)
  (angryduck_worker_broken_image)`) and
  `angryduck_worker_broken_images_removed_total` (nothing is removed any more;
  see `angryduck_worker_image_repairs_total`). The suggested
  `AngryDuckBrokenImage` alert uses the new gauge.

## 1.8.12

Fixes images left registered on a node but impossible to unpack, and repairs
the ones already there.

### Fixed

- **A failed spread finalize or rescue no longer leaves a broken image
  behind.** The controller finalizes a spread from a layer inventory up to a
  scan old, and a layer it counted as on the node could be gone by then:
  kubelet or the cleanup removed the image it came with, and containerd's
  garbage collector freed it. containerd's import registered the image name
  first and the unpack failed after, so the name stayed, pointing at an image
  that can't be unpacked. kubelet then reported the image `already present on
  machine`, never pulled it again, and every container create failed with
  `failed to get reader from content store ... not found`; the controller
  counted the node as holding it, so nothing sent it either. Seen as
  `spread: registering image=... failed: unpacking ...` in the worker log.
  - **Imports are atomic.** The archive's names are swapped for temporary
    ones (`angryduck.local/import/...`) on the way into containerd, the image
    is unpacked under those, and only then does the real name point at it. A
    failed import leaves every real name as it was, including a tag that
    already pointed at a working image.
  - **"Already here" is held.** A spread fetch answered `present` now puts
    the blob under the job's lease, and a finalize holds every layer it
    counts on (the highest unpacked snapshot and the blobs above it) before
    importing. A layer that is gone fails the finalize up front, naming it,
    and the worker reports its inventory right away so the controller fetches
    it instead of finalizing again.
  - **A rescue no longer answers `already present` for an image that can't
    be unpacked**: it receives it again, and the import repoints the name.

### Added

- **Image repair** (`IMAGE_REPAIR_ENABLED`, on by default). Every
  `IMAGE_REPAIR_INTERVAL_S` (120) the worker looks for images whose top layer
  isn't unpacked and some layer has neither its snapshot nor its blob. One
  still broken an interval later, checked again against fresh listings and
  not being pulled or received, is removed with all its names; kubelet pulls
  it on the next container start, and the controller learns the node no
  longer has it. This also clears images broken by earlier versions. It also
  removes temporary import names a killed worker left behind. New metrics:
  `angryduck_worker_broken_images`,
  `angryduck_worker_broken_images_removed_total`,
  `angryduck_worker_temp_images_removed_total`; suggested alert
  `AngryDuckBrokenImage`. See
  [Operations](docs/operations.md#an-image-is-present-but-containers-cant-be-created).

### Changed

- **Angry Duck's own temporary images (`angryduck.local/...`) are no longer
  reported to the controller**, and the cleanup never removes any of them
  (before, only rescue base images were protected).

- **Helm chart moved to `helm/angryduck` and now renders exactly `k8s/`**:
  the same objects, names, labels, settings and defaults, apart from the
  Namespace. It no longer generates tokens or a registry Secret; create them
  as for the manifests. `registryCredentials.existingSecret` is now
  `registryCredentials.secretName`, and the ingress and ServiceMonitors are on
  by default. See [Helm](docs/helm.md).

## 1.8.11

Metrics, the dashboard and the example manifests. Nothing Angry Duck does on
the nodes changed.

### Added

- **Origin check.** The controller keeps asking the registries whether they
  still serve the images the nodes hold: a `HEAD` for each image's manifest,
  with Angry Duck's credentials, every `ORIGIN_CHECK_INTERVAL_S` (3600),
  spread evenly and sent one at a time, starting `ORIGIN_CHECK_DELAY_S`
  (120) after startup, on its own goroutine and HTTP client. Peers, the
  mirror, rescue and cleanup could keep an image alive on the nodes long
  after its tag was deleted or its pull secret rotated, and nobody would
  know until the last copy went. New metrics:
  `angryduck_controller_origin_unavailable{image, status}` (images a
  registry answered `missing` or `denied` for, valued by the copies left),
  `angryduck_controller_origin_images{registry, status}` and
  `angryduck_controller_origin_checks_total{status}`.
  `ORIGIN_CHECK_REGISTRIES` limits it to some registries;
  `ORIGIN_CHECK_INTERVAL_S=0` turns it off. See
  [Observability](docs/observability.md#origin-check).
- **Registry Health tab** in the dashboard, and *Unpullable From Origin* and
  *Nodes Disk-Stalled* on the Overview. Suggested alerts for images gone from
  their registry, last copies and refused credentials.

### Changed

- **Counters start at zero.** Every counter exists from the start for every
  label value it can take; per-node controller counters from the node's first
  report. Before, a series appeared with its first event, which `rate()` and
  `increase()` can't see, and charts started mid-way. About 50 series per
  node, nearly all of which a busy node had after a day anyway.
- **Dashboard: no data is zero.** Ratios (throughput, hit ratio, pull
  duration, snapshot share) are zero when nothing moved instead of a gap;
  gauges that come and go (stuck images, node states, preheated repos,
  copies left) stay at zero between appearances; per-node charts list the
  nodes active in the time range and keep them continuous; *Top Senders*
  ranks over the whole range instead of picking a new ten every step.
- **Dashboard: fixed panels that showed nothing.** The registry pull panels
  on the Preheat tab and the progress table and node states on the
  Whole-Image Fallback tab filtered on `$registry` and `$image`, variables
  that no longer existed. Whole-Image Fallback moved to the last tab.


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
