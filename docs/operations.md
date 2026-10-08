# Operations

## Status

The controller's `GET /status` is the quickest view of what Angry Duck is
doing:

```bash
kubectl -n angryduck port-forward svc/angryduck-controller 8080:8080 &

# Workers and their freshness
curl -s localhost:8080/status | jq '{fresh_count, workers: [.workers[] | {node_id, fresh}]}'

# Every image a node currently can't pull
curl -s localhost:8080/status | jq '.rescues'

# Every image being spread
curl -s localhost:8080/status | jq '.propagations[] | {image, have: (.have|length), missing, in_flight, failing}'
```

`rescues` lists, per stuck node and image, the pods, last result or error,
failures in a row and next attempt. `propagations` lists, per image, the nodes
that have it, are missing it, are receiving it, are still seeding, were
skipped, or are failing.

## Logs

Every decision is logged on one line, prefixed with the component and node.
Useful filters:

| Grep for | Shows |
|---|---|
| `rescuer:` | Stuck pulls found and rescue decisions (controller). |
| `propagator:` | Propagation progress and transfers ordered (controller). |
| `rescue image=` | Transfer plans and results (worker). |
| `image cleanup:` | Cleanup decisions and removed images (worker). |
| `mirror:` | Mirror setup and `hosts.toml` changes (worker). |

Set `LOG_LEVEL=debug` for per-image reporting decisions and the raw disk
utilization math.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| Webhook returns `503` | No fresh workers. Check worker logs, `SELF_ADDRESS`, `NODE_EXPORTER_URL` and that the controller can reach port `18081`. |
| `angryduck_controller_seed_rankings_total` shows `repo` or `utilization` | The controller can't read the manifest. Add the registry to the `REGISTRY_CREDENTIALS_PATH` file. |
| Rescue or propagation never starts | The shared token is missing or shorter than 32 characters, or the controller can't list pods. The controller logs a warning at startup. |
| Workers report no disk usage | node-exporter isn't reachable at the node IP from pods. Check `NODE_EXPORTER_URL` in the DaemonSet. |
| Mirror never used, containerd logs `401` | A stale `hosts.toml` from a worker that died without SIGTERM; the next worker rewrites it within seconds. |
| Worker exits with `connecting to containerd` | The socket isn't mounted at `CONTAINER_RUNTIME_ENDPOINT`. Check the `containerd-sock` hostPath. |
| Most transferred layers are snapshots | containerd has `discard_unpacked_layers = true`. See [Transfers](transfers.md#integrity). |
| Mirror requests are all `miss` | containerd's `config_path` doesn't point at `MIRROR_CONTAINERD_CONFIG_DIR`, or peers have no blobs. |
| `image cleanup: ... nothing is removable` | Every image on the node is running, protected or being pulled. The disk needs attention beyond images. |
| Pod still pending after a successful rescue | kubelet waits for its backoff, up to 5 minutes. Delete the pod to retry now. |

## Sizing

### Controller

Measured with 1.8.4 against a simulated fleet of 150 nodes, each holding
about 300 images and 9,000 layers: ~13m CPU, ~22Mi steady, ~26Mi right after
a restart (manifest: 20m / 150m CPU, 32Mi / 64Mi memory). Memory grows roughly
linearly with the number of nodes and peaks after a restart, when every worker
resends its full layer inventory. Raise the request and limit for much larger
fleets.

### Worker

Measured with 1.8.6 against containerd 2.2 on a node holding 304 images,
614 snapshots and 918 blobs, next to 1.8.5 on the same node:

| | 1.8.5 | 1.8.6 |
|---|---|---|
| CPU at steady state, worker + its subprocesses + containerd | ≈0.9% of a core | ≈0.17% |
| Calls to containerd | 5 full listings a minute, each a new `ctr`/`crictl` process (up to ~27 MiB each) | the same listings at the same intervals, as single gRPC calls |
| Memory really held at idle (`rss_anon`) | ~3 MiB, plus the subprocesses | ~5–7 MiB, no subprocesses |
| Peak during a 300 MB rescue | ~4 MiB + subprocesses | ~10 MiB |
| 300 MB rescue as blobs | ~6 s | ~3–4 s |
| 300 MB rescue as snapshots | ~8 s | ~10–14 s |
| 400 mirror requests | ~4 s | ~2 s |

1.8.7 speeds up snapshot shipping. Measured on one CPU against containerd
2.2.1, with a 150 MiB layer that only exists as a snapshot, the same harness
and machine for both (see [Development](development.md#test)):

| | 1.8.6 | 1.8.7 |
|---|---|---|
| Source export, 5 MiB of layers below | ~3.1 s | ~1.1 s |
| Source export, 300 MiB of layers below | ~5.7 s | ~2.8 s |
| Whole rescue | ~17 s | ~10.5 s |

- The worker's container memory (`kubectl top`) is about 15 MiB higher than
  `rss_anon`: that is its own code, mapped from the binary, which the kernel
  can drop and reload. Size against `angryduck_process_memory_bytes{kind="rss_anon"}`.
- Once a minute, worker and controller hand freed heap back to the OS when
  more than 1 MiB is retained, so after a burst memory returns to near its
  idle level.
- Snapshot shipping (only when no node has a layer's blob, as with
  `discard_unpacked_layers`) has containerd stage each layer once in its
  content store on each side, the price of the worker having no
  capabilities. The source's diff also compares the layer against everything
  below it, so it takes longer the bigger the image underneath.
- Unpacking, diffs and content writes happen inside containerd and are not
  counted in the worker's pod.
- Both binaries default to `GOGC=50` and set `GOMEMLIMIT` to 90% of their
  memory limit. Set `GOGC` in the environment to override.

## Upgrading

1. Build and push the new images.
2. If any node can't reach the registry, [bootstrap the new worker image
   there by hand](#bootstrapping-a-node-by-hand) first.
3. Apply the new manifests. The controller and workers are independent: the
   controller rebuilds its state from worker reports within seconds, and
   workers keep serving peers during a controller restart.

Read the [changelog](../CHANGELOG.md) for setting and metric changes.

## Rotating the token

The token is read once at startup. Update the Secret, then restart the
controller and every worker:

```bash
kubectl -n angryduck create secret generic angryduck-rescue-token \
  --from-literal=token=$(openssl rand -hex 32) --dry-run=client -o yaml | kubectl apply -f -
kubectl -n angryduck rollout restart deploy/angryduck-controller ds/angryduck-worker
```

Transfers fail while old and new pods disagree on the token, and resume once
the rollout completes.

## Bootstrapping a node by hand

A worker can't receive its own image. If a node can't reach the registry, a
new Angry Duck version gets stuck there and the DaemonSet rollout stops at
that node. Copy the worker image from a healthy worker that holds its blobs,
then delete the stuck pod so the DaemonSet recreates it with the image already
local. Workers listen on their pod IPs; find a healthy one with
`kubectl -n angryduck get pods -o wide`.

Run this on the stuck node:

```bash
IMG=ghcr.io/hatam-abolghasemi/angry-duck-worker:1.8.10
SRC=<healthy-worker-pod-ip>:18081
TOKEN=<token from the angryduck-rescue-token secret>

# What the image consists of, and what this node already has
curl -sSf -H "Authorization: Bearer $TOKEN" \
  -d "{\"image\":\"$IMG\",\"platform\":\"linux/amd64\"}" http://$SRC/blobs/plan > /tmp/plan.json
sudo ctr -n k8s.io content ls -q > /tmp/have.txt
MISSING=$(jq -c --rawfile have /tmp/have.txt '[.blobs[].digest] - ($have | split("\n"))' /tmp/plan.json)

# Fetch only the missing blobs and import them
curl -sSf -H "Authorization: Bearer $TOKEN" \
  -d "{\"image\":\"$IMG\",\"platform\":\"linux/amd64\",\"digests\":$MISSING}" http://$SRC/blobs/export \
  | sudo ctr -n k8s.io images import --platform linux/amd64 /dev/stdin
```

Better still, do this before rolling out a new version to a cluster with such
a node.

## Uninstalling

`helm uninstall`, or `kubectl delete -f` on the DaemonSet (or the whole
directory), is enough.
Each worker, on SIGTERM, sees its DaemonSet gone and removes its
`hosts.toml` files, releases rescue pins, removes temporary snapshots and
base images, and deletes its state files. Images rescued or preheated onto
nodes stay: kubelet uses them. The empty `/var/lib/angryduck` directory
stays on each node.

If the worker can't reach the API server on shutdown, it keeps the state
files (a reinstall picks up from there); `hosts.toml` is removed either
way.
