# Observability

Both components expose Prometheus metrics on `/metrics`. The manifests include
ServiceMonitors for the Prometheus Operator that scrape every 15 seconds.

## Grafana dashboard

[`deploy/grafana/angryduck-dashboard.json`](../deploy/grafana/angryduck-dashboard.json)
covers every metric, in these tabs:

| Tab | Answers |
|---|---|
| **Overview** | Workers reporting, stuck images, images spreading, mirror hit ratio, registry bytes avoided, nodes cleaning; a per-node overview table; running containers from preheated repos. |
| **Preheat** | Seed orders and pulls by result, node and registry, pull duration, ranking basis, registry download per seed, slow seeds, layer index size. |
| **Spread** | Every image being spread now (nodes per state, layers held versus still only coming from the registry), what each node still lacks in layers and bytes, every blob moving right now (receiver, path, source, size), transfers by path and result, bytes and throughput per path, finished spreads, how long the last one took. |
| **Propagation** | Per-image progress, nodes per state, finished propagations, transfers by result and receiving node. |
| **Rescue** | Stuck images per node, rescue decisions, failing node/image pairs, rescues in flight, peer fetches by reason and result, stuck pods per node and image with whether a rescue is running. |
| **Transfers** | Throughput, top senders, layers by delivery method, snapshot share, pinned snapshots and leftover cleanups. |
| **Mirror** | Requests by result, hit ratio, bytes, misses per node. |
| **Cleanup** | Disk-pressure cleanup per node, images removed by tier, candidates by tier, removals per node, images coming back after cleanup. |

The file uses Grafana's dashboard schema v2, for the tabs and per-tab
variables. It needs Grafana 13, or Grafana 12 with the `dashboardNewLayouts`
(dynamic dashboards) and `kubernetesDashboards` feature toggles. To import it,
go to **Dashboards → New → Import**, upload the file and pick your Prometheus
data source in the **Data source** variable.

Variables:

- **Data source**: any Prometheus data source.
- **Cluster**: values of the `k8s_source_cluster` label, for setups that
  aggregate several clusters into one Prometheus. If your series have no such
  label, leave it on *All*. If your cluster label has another name, replace
  `k8s_source_cluster` in the JSON before importing.
- **Node**: one or more nodes. Applies to every per-node metric, including
  controller metrics labeled with the target node. Clicking a node name in a
  table sets it.
- **Registry** (Preheat tab): registries seed pulls came from. Empty unless
  `METRICS_LABEL_REGISTRY=true`.
- **Image** (Propagation tab): images being propagated. Clicking an image in
  the progress table sets it.

Event counts are computed scrape by scrape, so the hidden `scrape_interval`
constant must match your scrape interval. It is `15s`, as in the included
ServiceMonitors; change it in the JSON if you scrape at another interval.

Annotations, toggled from the dashboard controls: disk cleanup started and
rescue failures (on by default), and finished propagations (off by default).
The **Reset filters** link clears every filter except data source, cluster,
time range and the current tab.

## Metrics

### Controller

| Metric | Type | Labels | Meaning |
|---|---|---|---|
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
| `angryduck_worker_spread_transfers_total` | counter | `node`, `path`, `result` | Spread blobs fetched, by `path` (`registry`, `peer`) and `result` (`ok`, `failed`, `cancelled`). |
| `angryduck_worker_spread_bytes_total` | counter | `node`, `path` | Spread bytes received (`registry`, `peer`) or `served` to peers. |
| `angryduck_worker_spread_transfer_milliseconds_total` | counter | `node`, `path` | Time spent transferring. Bytes divided by it is the throughput per path. |
| `angryduck_worker_spread_active` | gauge | `node`, `path` | 1 while a blob is coming in on that path. |
| `angryduck_worker_spread_finalizes_total` | counter | `node`, `result` | Images registered from spread blobs. |
| `angryduck_worker_mirror_requests_total` | counter | `node`, `kind`, `result` | Mirror requests (`blob`, `manifest`): `local`, `peer`, `miss`, `error`. |
| `angryduck_worker_mirror_bytes_total` | counter | `node`, `source` | Bytes the mirror served, `local` or `peer`. |
| `angryduck_worker_mirror_peer_served_bytes_total` | counter | `node` | Bytes served to other nodes' mirrors. |
| `angryduck_worker_gc_images_deleted_total` | counter | `node`, `tier` | Images removed: `old`, `pressure`, `rollback`, or `error`. |
| `angryduck_worker_gc_image_returns` | gauge | `node`, `image` | Images that came back within an hour of cleanup removing them, and how many times. Only images that came back in the last hour, so the series count stays small. |
| `angryduck_worker_gc_candidates` | gauge | `node`, `tier` | Images removable right now: `old`, `pressure`, `rollback`. |
| `angryduck_worker_gc_cleaning` | gauge | `node` | 1 while the disk is above high and not yet back to low. |
| `angryduck_worker_preheated_containers_running` | gauge | `node`, `repo` | Running containers from repos recently preheated on the node. |

| `angryduck_worker_containerd_calls_total` | counter | `node`, `method` | gRPC calls to containerd, by service and method (`Content/List`, `ImageService/PullImage`, ...). The load the worker puts on containerd. |

The `registry` label is filled only with `METRICS_LABEL_REGISTRY=true`.

### Both components

| Metric | Type | Labels | Meaning |
|---|---|---|---|
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

      - alert: AngryDuckDiskFullNothingRemovable
        expr: |
          angryduck_worker_gc_cleaning == 1
            and on (node) sum by (node) (angryduck_worker_gc_candidates) == 0
        for: 30m
        annotations:
          summary: "{{ $labels.node }} is above the cleanup threshold with nothing removable"

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
