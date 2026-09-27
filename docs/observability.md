# Observability

Both components expose Prometheus metrics on `/metrics`. The manifests include
ServiceMonitors for the Prometheus Operator that scrape every 15 seconds.

## Grafana dashboard

[`deploy/grafana/angryduck-dashboard.json`](../deploy/grafana/angryduck-dashboard.json)
covers every metric, in these tabs:

| Tab | Answers |
|---|---|
| **Overview** | Workers reporting, stuck images, active propagations, mirror hit ratio, registry bytes avoided, nodes cleaning; a per-node overview table; running containers from preheated repos. |
| **Preheat** | Seed orders and pulls by result, node and registry, pull duration, ranking basis, registry download per seed, slow seeds, layer index size. |
| **Propagation** | Per-image progress, nodes per state, finished propagations, transfers by result and receiving node. |
| **Rescue** | Stuck images per node, rescue decisions, failing node/image pairs, rescues in flight, peer fetches by reason and result. |
| **Transfers** | Throughput, top senders, layers by delivery method, snapshot share, pinned snapshots and leftover cleanups. |
| **Mirror** | Requests by result, hit ratio, bytes, misses per node. |
| **Cleanup** | Disk-pressure cleanup per node, images removed by tier, candidates by tier, removals per node. |

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
| `angryduck_worker_mirror_requests_total` | counter | `node`, `kind`, `result` | Mirror requests (`blob`, `manifest`): `local`, `peer`, `miss`, `error`. |
| `angryduck_worker_mirror_bytes_total` | counter | `node`, `source` | Bytes the mirror served, `local` or `peer`. |
| `angryduck_worker_mirror_peer_served_bytes_total` | counter | `node` | Bytes served to other nodes' mirrors. |
| `angryduck_worker_gc_images_deleted_total` | counter | `node`, `tier` | Images removed: `old`, `pressure`, `rollback`, or `error`. |
| `angryduck_worker_gc_candidates` | gauge | `node`, `tier` | Images removable right now: `old`, `pressure`, `rollback`. |
| `angryduck_worker_gc_cleaning` | gauge | `node` | 1 while the disk is above high and not yet back to low. |
| `angryduck_worker_preheated_containers_running` | gauge | `node`, `repo` | Running containers from repos recently preheated on the node. |

The `registry` label is filled only with `METRICS_LABEL_REGISTRY=true`.

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
