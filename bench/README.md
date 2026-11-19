# Bench

Benchmarks and chaos tests that check whether Angry Duck holds up. Run them on
a test cluster, **never production**: they switch Angry Duck's features on and
off, restart its pods, simulate a registry outage and fill a node's disk.

Everything runs against a throwaway registry in its own namespace. The images
are random data, built fresh for every run, so no node has them cached and no
real registry is touched.

## What you need

- `bash`, `jq`, GNU `date`, and `kubectl` with cluster-admin
- SSH to every node as `SSH_USER` (root by default), with keys, no password prompt
- Angry Duck installed in namespace `angryduck`
- a hostname for the bench registry that the nodes can resolve, behind your
  ingress with a trusted certificate. containerd and Angry Duck only speak
  HTTPS to registries, and Angry Duck's mirror skips registries that have a
  hand-written `hosts.toml`, so an insecure registry would skip the mirror.

## Setup

```bash
cp bench/bench.env.example bench/bench.env
```

Set `BENCH_HOST` in `bench/bench.env`. If a load balancer in front only
passes known hostnames, use one it already passes and set
`BENCH_INGRESS_PATH=/v2/bench/`: the bench ingress then takes only that path
on the shared host. Then:

```bash
bench/duckbench.sh setup
```

Setup first checks every untainted node over SSH: containerd's `config_path`
points at `/etc/containerd/certs.d`, and `BENCH_HOST` resolves. It then saves
Angry Duck's ConfigMap to `bench/.state/`, adds `BENCH_HOST` to
`MIRROR_REGISTRIES`, and deploys the registry and a toolbox pod. It finishes
with a smoke test: one node pulls a tiny image through `BENCH_HOST`.

## Benchmark

Each run pushes a new image, rolls out a DaemonSet that uses it on every
untainted node, and records:

| Column | Meaning |
|---|---|
| `wall_s` | From the rollout to the last pod Ready |
| `pull_p50_s`, `pull_p95_s`, `pull_max_s` | Per-node pull time from kubelet's `Pulled` event (0 when the image was already there) |
| `already_present` | Nodes that had the image before their pod asked |
| `registry_mb`, `registry_copies` | Blob bytes the registry served (seeds, spread and kubelet), and that number divided by the image's size |

Scenarios:

| Scenario | Angry Duck |
|---|---|
| `kubelet` | Mirror, spread, propagation and rescue off: kubelet alone |
| `no-webhook` | Everything on, but CI never calls the webhook |
| `preheat` | Everything on, webhook right after the push, rollout `PREHEAT_LEAD` seconds later. Rows are labeled `preheat@<lead>s`, so runs with different leads stay apart |

Shapes (the optional fourth argument, or `SHAPE` in `bench.env`):

| Shape | Workload |
|---|---|
| `ds` (default) | A DaemonSet created on every node at once: the worst case for peer-to-peer, since no node has the image yet when all of them ask |
| `rolling` | A Deployment with one pod per node, running the base image, then updated to the new one `ROLL_UNAVAILABLE` (25%) at a time, as a real rollout goes. Rows are labeled `<scenario>+rolling` |

With `preheat` and no lead (`PREHEAT_LEAD=0`), the first wave's mirrors stream
layers from the seeds still pulling them (1.13.0), instead of each pulling its
own copy from the registry.

Sizes: `small` (1 × 50 MB), `medium` (5 × 100 MB), `large` (8 × 256 MB),
`layers` (30 × 10 MB), `files` (one layer of `FILES_COUNT` files of
`FILE_KB` each, 20,000 × 8 KB by default: a `vendor/` or `node_modules/`
tree, where every other size is one big file per layer).

Run one combination:

```bash
bench/duckbench.sh bench kubelet medium 3
```

The same as a rolling update:

```bash
bench/duckbench.sh bench kubelet medium 3 rolling
```

Run every scenario, shape and size, three times each, then print the report:

```bash
bench/duckbench.sh bench-all 3
```

Switching scenarios restarts Angry Duck, so `bench-all` groups runs by
scenario.

`STREAM_UNPACK=on` or `off` switches unpacking while blobs arrive for the
runs, on the worker DaemonSet (the variable is set there, not in the
ConfigMap); rows get `+stream-on` or `+stream-off`. `on` needs a worker
deployed with Helm's `worker.streamUnpack.enabled`, for its privileges.
`teardown` puts the original back.

```bash
STREAM_UNPACK=off bench/duckbench.sh bench preheat files 3
```

## Resources

Every `bench` run also records what Angry Duck cost, in
`results/resources.csv`: every Angry Duck pod is sampled every
`RES_INTERVAL` (15s), from its own metrics (`angryduck_process_memory_bytes`,
containerd calls) and from its node's kubelet (`/metrics/resource`: CPU
seconds and working set, what `kubectl top` shows). One row per component
(worker, controller) and phase:

| Phase | When | Memory columns |
|---|---|---|
| `idle` | Two samples before the run starts | Last value |
| `run` | From the webhook or rollout until pods are Ready and the spread has settled | Each pod's peak |
| `after_1m` | The first minute after | Last value |
| `after_5m` | Up to `RES_AFTER` (300s) after: memory that isn't given back shows here | Last value |

| Column | Meaning |
|---|---|
| `ws_avg_mb`, `ws_max_mb` | Container working set: the average pod, and the largest |
| `rss_anon_avg_mb` | What the process really holds |
| `heap_live_avg_mb` | What the Go heap needs |
| `page_cache_avg_mb`, `page_cache_max_mb` | File cache charged to the container |
| `slab_avg_mb` | The kernel's records of the files the container wrote or opened (1.14.3 and later, cgroup v2; `NA` before) |
| `cpu_s` | CPU seconds all pods used in the phase |
| `cpu_peak_cores` | The busiest pod's highest rate between two samples |
| `containerd_calls` | Calls to containerd all workers made in the phase |
| `heap_retained_avg_mb`, `stacks_avg_mb` | Freed Go heap not yet returned to the OS, and goroutine stacks |

With `RES_CONTAINERD=1` (the default) the bench also samples containerd's own
cgroup on every node over SSH (cgroup v2), as component `containerd`. With
unpacking while blobs arrive off, containerd decompresses and writes the
files, out of sight of Angry Duck's pods: compare the two components
together, or a comparison only shows who did the work, not how much.

`RES_AFTER=0` skips the after phases: a full `bench-all` is about 90 runs,
and five minutes each adds over seven hours. Raw samples stay in
`results/samples/<run>.txt`.

Two more commands measure resources only, with no rollout:

```bash
bench/duckbench.sh burst medium 5
```

`burst <size> [count]` pushes `count` images and preheats them all at once,
as CI pushing several services together, until every spread settles.

```bash
bench/duckbench.sh idle 60
```

`idle [minutes]` samples once a minute while nothing happens; rows `start`
and `end` show whether memory or CPU creeps.

## Chaos

| Scenario | What happens | Passes when |
|---|---|---|
| `outage-always` | The image is on every node, the registry goes down, and a rollout uses `imagePullPolicy: Always` | Every pod starts (tags resolved from peers) |
| `outage-one-holder` | Spread and propagation off, one node holds the image, the registry goes down, rollout everywhere | Every pod starts, served by peers' mirrors |
| `outage-rescue` | Same, with the mirror off too | Every pod starts after rescue (includes kubelet's backoff) |
| `worker-kill` | The workers on two seeds are killed `KILL_AFTER` seconds into a large spread | The spread settles, the rollout starts everywhere, no broken images. `DEGRADED`: some nodes still had to pull |
| `controller-kill` | The controller is killed mid-spread | Same as `worker-kill` |
| `disk-pressure` | One node's root filesystem is filled to `FILL_TARGET` while a pod runs a bench image there | The running pod's image survives cleanup |

```bash
bench/duckbench.sh chaos outage-always medium
```

```bash
bench/duckbench.sh chaos outage-one-holder medium
```

```bash
bench/duckbench.sh chaos outage-rescue medium
```

```bash
bench/duckbench.sh chaos worker-kill large
```

```bash
bench/duckbench.sh chaos controller-kill large
```

```bash
bench/duckbench.sh chaos disk-pressure small
```

The registry outage empties the selector of the Service behind the ingress,
so the ingress answers 503 while pushes keep working through the internal
Service. Every chaos run puts the registry back and removes its pods when it
ends, even when it fails.

`disk-pressure` SSHes to the chosen node and `fallocate`s `FILL_PATH`
(`/var/lib/angryduck-bench-fill`) up to the target, refusing targets above
0.82, below kubelet's default eviction threshold. It checks first that the
file lands on the root filesystem Angry Duck measures, and removes it when the
run ends, even on failure. While
the node is full, Angry Duck's cleanup may remove unused images there, as it
would anyway.

## Results

```bash
bench/duckbench.sh report
```

Raw rows are in `bench/results/bench.csv`, `bench/results/resources.csv` and
`bench/results/chaos.csv`.
Keep the Grafana dashboard open during runs: the spread, mirror, rescue and
cleanup panels show what the numbers can't.

## Cleaning up

```bash
bench/duckbench.sh teardown
```

Teardown restores Angry Duck's original ConfigMap, restarts it and deletes the
bench namespace. Bench images left on nodes are removed by Angry Duck's
cleanup once they've been unused for `GC_UNUSED_FOR_S`.

Many large runs fill the registry's emptyDir (`REGISTRY_SIZE_LIMIT`, 40Gi by
default). Wipe it between batches with:

```bash
bench/duckbench.sh reset-registry
```
