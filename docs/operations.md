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
| `permission denied` on `/proc/1/root` | The node enforces AppArmor and the worker isn't running unconfined. Check `appArmorProfile` in the DaemonSet. |
| Most transferred layers are snapshots | containerd has `discard_unpacked_layers = true`. See [Transfers](transfers.md#integrity). |
| Mirror requests are all `miss` | containerd's `config_path` doesn't point at `MIRROR_CONTAINERD_CONFIG_DIR`, or peers have no blobs. |
| `image cleanup: ... nothing is removable` | Every image on the node is running, protected or being pulled. The disk needs attention beyond images. |
| Worker CPU near its limit | `CONTAINER_RUNTIME=containerd` on a busy node. Switch to `crictl`. |
| Pod still pending after a successful rescue | kubelet waits for its backoff, up to 5 minutes. Delete the pod to retry now. |

## Sizing

Measured with 1.8.4 against a simulated fleet of 150 nodes, each holding
about 300 images and 9,000 layers:

| Component | CPU | Memory | Manifest request / limit |
|---|---|---|---|
| Controller | ~13m | ~22Mi steady, ~26Mi after a restart | 20m / 150m CPU, 32Mi / 64Mi memory |
| Worker (process) | ~2m | ~15Mi | 50m / 1 CPU, 32Mi / 192Mi memory |

- The controller's memory grows roughly linearly with the number of nodes.
  It peaks just after a restart, when every worker resends its full layer
  inventory. Raise its request and limit for much larger fleets.
- The worker's own process is small. On top of it, one `ctr` or `crictl`
  listing runs at a time, each a short-lived process of 20-40 MB counted in
  the worker's pod, and transfers run `ctr images import` and `tar`. The
  limit leaves room for those; the request covers the steady state.
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
local.

Run this on the stuck node:

```bash
IMG=registry.example.com/angryduck/angry-duck-worker:1.8.5
SRC=<healthy-node-ip>:18081
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
