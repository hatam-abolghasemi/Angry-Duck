# Observability

Both components expose Prometheus metrics on `/metrics`. The manifests include
ServiceMonitors for the Prometheus Operator that scrape every 15 seconds.

## Grafana dashboard

[`grafana/angryduck-dashboard.json`](../grafana/angryduck-dashboard.json)
answers a few questions, one tab each:

| Tab | Answers |
|---|---|
| **Overview** | Is Angry Duck healthy, and is it saving anything? Workers up, images stuck pulling, broken images, registry traffic avoided, nodes cleaning disk, the running version; a table with one row per node; bytes kept off the registry over time. |
| **Delivery** | How images reach nodes: every spread running (nodes per state), what each node still lacks, how long the last spread took, registry versus peer download, seed pulls, finished spreads. |
| **Rescue & Repair** | What is stuck or broken, and who it hits: stuck pulls per node and image, broken images, the pods waiting on them and kubelet's reason, the components (Deployment, StatefulSet, DaemonSet, CronJob) using them, rescues and repairs over time. |
| **Mirror & Registry** | The mirror's hit ratio and requests; what each registry answered for the images nodes hold; images their registry no longer serves, the copies left and the components using them. |
| **Disk Cleanup** | Cleanup per node (idle, cleaning, stalled), images removed by tier, images coming back after removal. |

Every panel has a one-line description (hover its title).

The file uses Grafana's dashboard schema v2, for the tabs. It needs Grafana
13, or Grafana 12 with the `dashboardNewLayouts` (dynamic dashboards) and
`kubernetesDashboards` feature toggles. To import it, go to **Dashboards → New
→ Import**, upload the file and pick your Prometheus data source in the
**Data source** variable.

Variables, from broad to narrow:

- **Data source**: a Prometheus or Mimir data source.
- **Cluster**: values of the `k8s_source_cluster` label, for setups that
  aggregate several clusters. Without that label, leave it on *All*. If your
  cluster label has another name, replace `k8s_source_cluster` in the JSON.
- **Node**: applies to every per-node metric. Clicking a node in a table sets
  it.
- **Registry**: narrows images, and components by the images they run.
- **Kind**, **Namespace**, **Component**: Deployments, StatefulSets,
  DaemonSets and CronJobs, from kube-state-metrics.
- **Image**: the images Angry Duck is busy with right now (spreading, stuck,
  broken, unavailable at their registry, coming back after cleanup), narrowed
  by registry and component. Images no pod uses yet, like a spread ahead of
  its rollout, always show. It applies to the panels about now; counters over
  time carry no image label, by design, to keep the number of series bounded.

The component views need kube-state-metrics in the same data source
(`kube_pod_container_info`, `kube_pod_owner`, `kube_replicaset_owner`,
`kube_job_owner`, `kube_pod_container_status_waiting_reason`). A pod is tied
to its component through its owner: a Deployment through its ReplicaSet, a
CronJob through its Job. Images are matched on the image as written in the
pod spec (`image_spec`), which equals Angry Duck's `image` label for
references with a registry host and a tag (`registry.example.com/team/app:v1`).
Docker Hub short names (`nginx`) don't match.

Annotations, toggled from the dashboard controls: Angry Duck version rolling
out, spreads started and ended, rescue failures, images repaired and repairs
that failed, disk cleanup starting and stopping, a registry becoming
unreachable and reachable again (all on), and process restarts (off). The
**Reset filters** link keeps data source, cluster and time range and clears
the rest.

## Metrics

Every counter exists from the start, at zero, for every label value it can
take (per node as soon as the node reports), so `rate()` and `increase()` see
the first event too.

### Controller

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `angryduck_controller_origin_images` | gauge | `registry`, `status` | Distinct images on fresh nodes, by registry and its answer at the last [origin check](#origin-check): `ok`, `missing`, `denied`, `unreachable`, `unchecked`. |
| `angryduck_controller_origin_unavailable` | gauge | `image`, `status` | Images a registry didn't serve (`missing` or `denied`); the value is how many fresh nodes still hold a copy. Only such images are listed. |
| `angryduck_controller_origin_checks_total` | counter | `status` | Origin checks sent, by answer. |
| `angryduck_controller_pull_orders_total` | counter | `node`, `result`, `registry` | Seed pull orders sent to workers: `success` or `failure`. |
| `angryduck_controller_seed_rankings_total` | counter | `basis` | How seeds were ranked: `layers`, or the `repo` and `utilization` fallbacks. |
| `angryduck_controller_seed_missing_bytes` | gauge | `node` | Bytes of the latest seeded image each seed lacked, i.e. its registry download. |
| `angryduck_controller_seed_timeouts_total` | counter | `node` | Slow seed pulls cancelled in favor of peers. |
| `angryduck_controller_layer_index_digests` | gauge | | Distinct blob digests and snapshot chainIDs across all nodes. |
| `angryduck_controller_propagations_total` | counter | `result` | Finished propagations: `complete`, `superseded` or `expired`. |
| `angryduck_controller_propagation_transfers_total` | counter | `node`, `result` | Transfers ordered while spreading images. |
| `angryduck_controller_propagation_nodes` | gauge | `image`, `state` | Nodes per image: `have`, `missing`, `in_flight`, `seeding`, `skipped`. |
| `angryduck_controller_rescues_total` | counter | `node`, `result` | `success`, `failure`, `no_source`, `no_target`, `pull_policy_always`. |
| `angryduck_controller_rescue_stuck_images` | gauge | `node` | Images each node can't pull right now. |
| `angryduck_controller_rescue_consecutive_failures` | gauge | `node`, `image` | Failed attempts in a row, for pairs with at least one failure. |
| `angryduck_controller_rescues_in_flight` | gauge | | Rescues running now. |
| `angryduck_controller_rescue_stuck_pods` | gauge | `node`, `image`, `in_flight` | Pods stuck per node and image; `in_flight` 1 while a rescue runs for it. Only pairs stuck now. |
| `angryduck_controller_repair_orders_total` | counter | `node`, `result` | Repair orders sent because a container failed to start on a broken image (`CreateContainerError`): `sent`, `failed`, `no_worker`. |

<a id="spread"></a>Spread. Series labeled `image` or `node` exist only while
that image is being spread and drop when it ends, so their number follows
what is moving, not history:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `angryduck_controller_spread_nodes` | gauge | `image`, `state` | Nodes per image: `done`, `finalizing`, `transferring`, `waiting`, `skipped`. |
| `angryduck_controller_spread_layers` | gauge | `image`, `state` | Layers per image: `held` (peers can fetch it), `pulling` (only coming from the registry now), `missing`. |
| `angryduck_controller_spread_node_missing_layers` | gauge | `image`, `node` | Layers each node still lacks. Nodes not done only. |
| `angryduck_controller_spread_node_missing_bytes` | gauge | `image`, `node` | Bytes each node still lacks. Nodes not done only. |
| `angryduck_controller_spread_transfer_bytes` | gauge | `image`, `node`, `path`, `source`, `blob` | One series per transfer running now, its blob's size. At most two per node. |
| `angryduck_controller_spread_transfers_total` | counter | `path`, `result` | Transfers ended: `ok`, `failed`, `cancelled` (lost a race), `present`, `rejected`. |
| `angryduck_controller_spread_bytes_total` | counter | `path` | Bytes received for spreads. `path="registry"` is everything the registry served for them. |
| `angryduck_controller_spread_finalizes_total` | counter | `result` | Nodes registering a spread image. |
| `angryduck_controller_spreads_total` | counter | `result` | Spreads ended: `complete`, `superseded`, `expired`. |
| `angryduck_controller_spread_last_duration_seconds` | gauge | | Preheat to last node registered, for the last complete spread. |

### Worker

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `angryduck_worker_pulls_total` | counter | `node`, `result`, `registry` | Seed pulls: `success`, `failure`, `cancelled`. |
| `angryduck_worker_pull_seconds_total` | counter | `node`, `result`, `registry` | Seconds spent in seed pulls. Divide by `pulls_total` for the average. |
| `angryduck_worker_pulls_in_flight` | gauge | `node` | Seed pulls running now. |
| `angryduck_worker_rescues_total` | counter | `node`, `reason`, `result` | Images fetched from peers. `reason`: `rescue` or `propagate`. `result`: `success`, `failure`, `already_present`. |
| `angryduck_worker_rescue_bytes_total` | counter | `node`, `direction` | Bytes moved between nodes, `served` or `received`. |
| `angryduck_worker_rescue_layers_total` | counter | `node`, `method` | Layers by delivery: `present`, `blob`, `snapshot`. |
| `angryduck_worker_rescue_pinned_snapshots` | gauge | `node` | Snapshots pinned for a retry. |
| `angryduck_worker_rescue_cleanups_total` | counter | `node`, `kind` | Leftovers removed: `temp_snapshot`, `temp_image`, `expired_pin`. |
| `angryduck_worker_gc_stalled` | gauge | `node` | 1 while disk-pressure removals are paused because they freed nothing. |
| `angryduck_worker_spread_transfers_total` | counter | `node`, `path`, `result` | Spread blobs fetched, by `path` (`registry`, `peer`) and `result` (`ok`, `failed`, `cancelled`). |
| `angryduck_worker_spread_bytes_total` | counter | `node`, `path` | Spread bytes received (`registry`, `peer`) or `served` to peers. |
| `angryduck_worker_spread_transfer_milliseconds_total` | counter | `node`, `path` | Time spent transferring. Bytes divided by it is the throughput per path. |
| `angryduck_worker_spread_active` | gauge | `node`, `path` | 1 while a blob is coming in on that path. |
| `angryduck_worker_spread_finalizes_total` | counter | `node`, `result` | Images registered from spread blobs. |
| `angryduck_worker_mirror_requests_total` | counter | `node`, `kind`, `result` | Mirror requests (`blob`, `manifest`): `local`, `peer`, `miss`, `error`. |
| `angryduck_worker_mirror_bytes_total` | counter | `node`, `source` | Bytes the mirror served, `local` or `peer`. |
| `angryduck_worker_mirror_peer_served_bytes_total` | counter | `node` | Bytes served to other nodes' mirrors. |
| `angryduck_worker_inventory_events_total` | counter | `node`, `topic` | containerd events that changed what the node can serve; each burst settles into one inventory report. |
| `angryduck_worker_inventory_watch_up` | gauge | `node` | 1 while the node follows containerd's events, 0 while it falls back to periodic layer scans. |
| `angryduck_worker_gc_images_deleted_total` | counter | `node`, `tier` | Images removed: `old`, `pressure`, `rollback`, or `error`. |
| `angryduck_worker_gc_image_returns` | gauge | `node`, `image` | Images that came back within an hour of cleanup removing them, and how many times. Only images that came back in the last hour, so the series count stays small. |
| `angryduck_worker_gc_candidates` | gauge | `node`, `tier` | Images removable right now: `old`, `pressure`, `rollback`. |
| `angryduck_worker_gc_cleaning` | gauge | `node` | 1 while the disk is above high and not yet back to low. |
| `angryduck_worker_broken_image` | gauge | `node`, `image` | 1 while that image is registered on the node but can't start a container. Removed once it is repaired or no longer registered. |
| `angryduck_worker_image_repairs_total` | counter | `node`, `result` | Broken images repaired in place (`repaired`), or not (`failed`: some piece came from no peer and not from the registry; retried at the next check). |
| `angryduck_worker_repair_bytes_total` | counter | `node`, `source` | Bytes repairs fetched, from a `peer` or the `registry`. |
| `angryduck_worker_temp_images_removed_total` | counter | `node` | Temporary import names a killed worker left behind, removed by the repair. |
| `angryduck_worker_preheated_containers_running` | gauge | `node`, `repo` | Running containers from repos recently preheated on the node. |

| `angryduck_worker_containerd_calls_total` | counter | `node`, `method` | gRPC calls to containerd, by service and method (`Content/List`, `ImageService/PullImage`, ...). The load the worker puts on containerd. |

The `registry` label is filled only with `METRICS_LABEL_REGISTRY=true`.

### Both components

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `angryduck_build_info` | gauge | `component`, `version`, `node` | One series per process: its version, and as the value the Unix time it started. |
| `angryduck_process_memory_bytes` | gauge | `component`, `kind` | Refreshed every 15s. `rss_anon`: memory the process really holds. `rss_file`: its own code, shared and reclaimable by the kernel. `heap_live`: what the Go heap needs right now. `heap_retained`: freed heap kept for reuse. `heap_released`: already returned to the OS. `stacks`: goroutine stacks. |

`rss_anon` is the number to size against; container memory usage
(`container_memory_working_set_bytes`) also counts `rss_file`, which the kernel
can drop and reload at will. Once a minute, if `heap_retained` is above 1 MiB,
the process hands it back to the OS, so after a burst `rss_anon` returns to
near its idle level.

Useful queries:

```promql
# containerd calls per minute, per worker
sum by (node) (rate(angryduck_worker_containerd_calls_total[5m])) * 60
# memory each worker really holds
angryduck_process_memory_bytes{component="worker", kind="rss_anon"}
```

<a id="origin-check"></a>
## Origin check

Peers, the mirror, rescue and cleanup can keep an image alive on the nodes
long after its registry stopped serving it. A pod rescheduled to another node
gets the image from a peer; cleanup removes it from the old node; the next
move copies it again. Meanwhile the tag was deleted, or the pull secret was
rotated, and nothing pulls from the registry, so nobody notices until the
last copy is removed and every pod needing it is stuck.

So the controller keeps asking. For every image reference a fresh node
holds (tags and digests), it sends a `HEAD` for the manifest to the image's
registry with the credentials from `REGISTRY_CREDENTIALS_PATH`, the same ones
Angry Duck pulls with. Each image is asked every `ORIGIN_CHECK_INTERVAL_S`
(an hour), and one the registry didn't serve every quarter of that, so a fix
shows within minutes. The answers:

| Status | Meaning |
|---|---|
| `ok` | The registry serves the manifest. |
| `missing` | The registry says it doesn't exist: the tag, manifest or repository is gone. |
| `denied` | The registry refused Angry Duck's credentials, or wants credentials and none are configured for it. A whole registry turning `denied` usually means its pull secret changed. The controller reads the secret at startup, so after rotating it, restart the controller. |
| `unreachable` | No usable answer: network errors, timeouts, rate limits, server errors. |

What it costs: one `HEAD` (no download) per image per interval, plus a token
request where the registry uses them, sent one at a time and spread evenly
over the interval: 600 images make about ten requests every 30 seconds. Docker
Hub doesn't count `HEAD` requests against its pull limit. The check starts
`ORIGIN_CHECK_DELAY_S` (two minutes) after the controller, runs on its own
goroutine with its own HTTP client and token cache, and only reads a copy of
the inventory, so a slow registry delays nothing but the next check. It
changes nothing on the nodes: cleanup, spread and rescue behave as before.

If the controller can't reach registries the nodes reach through a mirror,
their images show as `unreachable`; list the registries worth checking in
`ORIGIN_CHECK_REGISTRIES`. `ORIGIN_CHECK_INTERVAL_S=0` turns the check off.

## Suggested alerts

```yaml
groups:
  - name: angryduck
    rules:
      - alert: AngryDuckRescueFailing
        expr: angryduck_controller_rescue_consecutive_failures >= 3
        for: 10m
        annotations:
          summary: "Rescue of {{ $labels.image }} on {{ $labels.node }} keeps failing"

      - alert: AngryDuckImageUnavailable
        expr: |
          increase(angryduck_controller_rescues_total{result=~"no_source|no_target"}[30m]) > 0
            and on (node) angryduck_controller_rescue_stuck_images > 0
        annotations:
          summary: "{{ $labels.node }} can't pull an image and no node has it"

      - alert: AngryDuckImageGoneFromRegistry
        expr: max by (image, status) (angryduck_controller_origin_unavailable) > 0
        for: 30m
        annotations:
          summary: "{{ $labels.image }} is {{ $labels.status }} at its registry; {{ $value }} node(s) still hold it"

      - alert: AngryDuckLastCopy
        expr: max by (image) (angryduck_controller_origin_unavailable) == 1
        annotations:
          summary: "Only one node holds {{ $labels.image }}, and its registry doesn't serve it"

      - alert: AngryDuckRegistryDenied
        expr: |
          sum by (registry) (angryduck_controller_origin_images{status="denied"})
            / sum by (registry) (angryduck_controller_origin_images{status=~"ok|missing|denied"}) > 0.5
        for: 15m
        annotations:
          summary: "{{ $labels.registry }} refuses Angry Duck's credentials; was its pull secret rotated?"

      - alert: AngryDuckBrokenImage
        expr: angryduck_worker_broken_image > 0
        for: 15m
        annotations:
          summary: "{{ $labels.image }} on {{ $labels.node }} can't start containers and the repair can't get its missing pieces (no peer has them, and the registry doesn't serve them)"

      - alert: AngryDuckDiskFullNotFromImages
        expr: angryduck_worker_gc_stalled == 1
        for: 2h
        annotations:
          summary: "{{ $labels.node }} is above the cleanup threshold, and removing images doesn't free space"

      - alert: AngryDuckSpreadStalled
        expr: |
          max by (image) (angryduck_controller_spread_nodes{state="waiting"}) > 0
            and max by (image) (angryduck_controller_spread_layers{state="missing"}) > 0
        for: 15m
        annotations:
          summary: "Spread of {{ $labels.image }} has layers nobody holds or pulls"

      - alert: AngryDuckSnapshotHeavy
        expr: |
          sum(rate(angryduck_worker_rescue_layers_total{method="snapshot"}[1d]))
            / sum(rate(angryduck_worker_rescue_layers_total{method=~"blob|snapshot"}[1d])) > 0.5
        annotations:
          summary: "Most transferred layers are snapshots; check discard_unpacked_layers"
```

Useful ad-hoc queries:

```promql
# Share of containerd's content served inside the cluster
sum(rate(angryduck_worker_mirror_requests_total{result=~"local|peer"}[1h]))
  / sum(rate(angryduck_worker_mirror_requests_total[1h]))

# Average seed pull duration per node
sum by (node) (rate(angryduck_worker_pull_seconds_total{result="success"}[1h]))
  / sum by (node) (rate(angryduck_worker_pulls_total{result="success"}[1h]))
```
