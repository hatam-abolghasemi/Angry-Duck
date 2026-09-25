# 🦆🔪 Angry Duck

**Preheat freshly pushed container images onto a few Kubernetes nodes *before* your rollout starts.**

When CI pushes a new image, Angry Duck picks a couple of good nodes and makes
them pull it from the registry right away. By the time ArgoCD (or any other
deployer) schedules the new pods, the image is already sitting on those
nodes, so the first pods start fast and the registry doesn't get hit by every
node at once.

- **Small and focused.** Two static Go binaries with no external Go
  dependencies. It pulls images and does nothing else.
- **Smart placement.** It prefers nodes that already have an older tag of the
  same repo, because that pull is a small delta. Among those, it picks the
  nodes with the most free disk.
- **Uses the node's own tools.** It runs the node's `crictl`, `ctr` or
  `docker` through a chroot, so no runtime binaries ship in the image.
- **Observable.** It exposes Prometheus metrics on both components, and
  ServiceMonitors are included.

---

## Contents

- [How it works](#how-it-works)
- [What it is not](#what-it-is-not)
- [Requirements](#requirements)
- [Quick start (local)](#quick-start-local)
- [Deploy to Kubernetes](#deploy-to-kubernetes)
- [Hook it into CI](#hook-it-into-ci)
- [Configuration](#configuration)
- [API](#api)
- [Metrics](#metrics)
- [Security considerations](#security-considerations)
- [Image cleanup](#image-cleanup)
- [Development](#development)

---

## How it works

Angry Duck has two parts:

| Component | Runs as | Job |
|---|---|---|
| **Controller** | Deployment (1 replica) | Receives the preheat webhook, keeps track of every node, and decides which nodes pull. |
| **Worker** | DaemonSet (1 per node) | Reports the node's disk usage and local images, and pulls when told to. |

```mermaid
sequenceDiagram
    participant CI as CI pipeline
    participant C as Controller
    participant W as Workers (every node)
    participant R as Registry

    loop every REPORT_INTERVAL_S
        W->>C: POST /report {disk utilization, local repos}
    end
    CI->>R: docker push app:1.2.3
    CI->>C: POST /webhook/preheat {"image": "app:1.2.3"}
    C->>C: rank nodes: same repo present first, then lowest disk use
    C->>W: POST /pull (top RANK_TOP_N nodes only)
    W->>R: pull app:1.2.3
    W->>C: POST /report (right after the pull lands)
```

Here is how nodes are ranked and ordered:

1. Only **fresh** workers count, meaning ones that reported within
   `WORKER_STALE_AFTER_S`. If no worker is fresh, the webhook returns `503`
   rather than guessing.
2. Nodes that match `RANK_EXCLUDE_NODE_SUBSTRINGS` (for example `master`) are
   never picked. They still run a worker and still report.
3. Nodes that already have **some tag of the same repo** come first. You can
   turn this off with `RANK_PREFER_IMAGE_LOCALITY=false`.
4. Ties are broken by **lowest disk utilization**, which comes from
   node-exporter.
5. Exactly `RANK_TOP_N` nodes are ordered per image, and the same image is
   never re-ordered. If an order fails, the controller replaces that node on
   its next tick, until `TARGET_TTL_S` runs out.

## What it is not

- **It is not a P2P image distribution system.** Angry Duck only seeds the
  first few nodes. The rest of the rollout, later scale-outs, and new nodes
  are a job for a tool like [Spegel](https://github.com/spegel-org/spegel).
  The two tools work well together.
- **It is not a registry mirror or proxy.** It doesn't sit in front of
  containerd or intercept pulls.
- **It is not an image garbage collector.** It never deletes images. See
  [Image cleanup](#image-cleanup).

> Earlier versions (≤ 1.4.x) also did image garbage collection and
> `ImagePullBackOff` "rescue". Both were removed so the tool does only one
> thing.

## Requirements

- Kubernetes **1.30+**, because the worker uses the
  `securityContext.appArmorProfile` field.
- containerd with **`crictl`** on every node (recommended). `ctr` and
  `docker` are also supported.
- **node-exporter** running with host networking on every node, on
  `localhost:9100` by default.
- For private registries, a `dockerconfigjson` secret with pull credentials.
- Optional: the Prometheus Operator, if you want the included ServiceMonitors.

## Quick start (Kubernetes)

The manifests in `deploy/` are example environments (`stg`, `mgmt`) that
differ only in labels and the ingress host. Copy one of them and adjust it
for your cluster.

**1. Build and push the images.** Replace `registry.example.com/...` and the
tag with your own.

```bash
cd app
sudo docker build -t registry.example.com/angry-duck-controller:1.5.0 -f Dockerfile.controller .
sudo docker build -t registry.example.com/angry-duck-worker:1.5.0 -f Dockerfile.worker .
sudo docker push registry.example.com/angry-duck-controller:1.5.0
sudo docker push registry.example.com/angry-duck-worker:1.5.0
```

Then update the `image:` fields in `deploy/<env>/deployment-controller.yaml`
and `deploy/<env>/daemonset-worker.yaml`.

**2. Create the namespace and secrets.** The manifests expect two secrets:

- `angryduck-registry-creds` holds the credentials the **worker** uses for
  preheat pulls. It is mounted as a file and referenced by
  `REGISTRY_CREDENTIALS_PATH`.
- `registry-pull-secret` is the `imagePullSecret` that lets Kubernetes pull
  the **Angry Duck images themselves**.

If both live in the same registry, you can point both at one secret.

```bash
sudo kubectl apply -f deploy/stg/namespace.yaml
```

```bash
sudo kubectl -n angryduck create secret docker-registry angryduck-registry-creds --docker-server=registry.example.com --docker-username=<user> --docker-password=<password>
```

```bash
sudo kubectl -n angryduck create secret docker-registry registry-pull-secret --docker-server=registry.example.com --docker-username=<user> --docker-password=<password>
```

**3. Review the config.** Open `deploy/stg/configmap-env.yaml` and check:

- `RANK_EXCLUDE_NODE_SUBSTRINGS`
- `CONTAINER_RUNTIME`
- `NODE_EXPORTER_URL`

**4. Apply everything:**

```bash
sudo kubectl apply -f deploy/stg/
```

This includes the ServiceMonitors. If you don't run the Prometheus Operator,
delete `servicemonitor-*.yaml` first.

**5. Check that the workers are reporting:**

```bash
sudo kubectl -n angryduck get pods -o wide
```

```bash
sudo kubectl -n angryduck port-forward svc/angryduck-controller 8080:8080
```

```bash
curl -s http://localhost:8080/status
```

Every node should show up with `"fresh": true`.

**6. Expose the webhook.** Edit the host in
`deploy/stg/ingress-controller-webhook.yaml`. The ingress exposes only
`/angryduck/webhook/preheat`. See
[Security considerations](#security-considerations) before you make it
reachable.

## Hook it into CI

Call the webhook right after `docker push`. Treat it as best-effort, so a
failed preheat never fails the pipeline.

```bash
curl -fsS -X POST https://angryduck-stg.example.com/angryduck/webhook/preheat -H 'Content-Type: application/json' -d "{\"image\":\"${IMAGE}\"}" || true
```

The response tells you which nodes were ordered:

```json
{"accepted": true, "target_image": "registry.example.com/app:1.2.3", "ordered_nodes": ["node-7", "node-3"]}
```

## Configuration

Everything is set through environment variables. In Kubernetes these come
from the ConfigMap; locally they can come from a `.env` file. Real
environment variables always win over the file. Every option is documented in
[`app/.env.example`](app/.env.example). The ones you're most likely to change
are:

| Variable | Default | What it does |
|---|---|---|
| `RANK_TOP_N` | `2` | How many nodes preheat each pushed image. |
| `RANK_PREFER_IMAGE_LOCALITY` | `true` | Prefer nodes that already have some tag of the same repo. |
| `RANK_EXCLUDE_NODE_SUBSTRINGS` | *(empty)* | Nodes to never target, e.g. `master,control-plane`. |
| `TARGET_TTL_S` | `120` | How long the controller keeps replacing failed orders for an image. |
| `WORKER_STALE_AFTER_S` | `30` | How long before a silent worker is ignored. |
| `REPORT_INTERVAL_S` | `15` | How often each worker reports. |
| `CONTAINER_RUNTIME` | `crictl` | `crictl` (recommended), `containerd` (`ctr`), or `docker`. |
| `CONTAINER_RUNTIME_ENDPOINT` | `unix:///run/containerd/containerd.sock` | The CRI socket, as a path on the **node**. |
| `HOST_ROOT` | `/proc/1/root` | Where the node's root filesystem is visible. Use `/` for local runs. |
| `NODE_EXPORTER_URL` | `http://localhost:9100/metrics` | Where the worker reads disk usage. |
| `REGISTRY_CREDENTIALS_PATH` | *(empty)* | Path to a `dockerconfigjson` for private registries. |
| `METRICS_LABEL_REGISTRY` | `false` | Adds a `registry` label to the pull metrics. |
| `SPEGEL_IMAGE_SUBSTRING` | *(empty)* | Labels pull durations by whether Spegel was running on the node, e.g. `spegel`. |
| `LOG_LEVEL` | `info` | One of `debug`, `info`, `warn`, `error`. |

> **Why `crictl`?** With `ctr`, listing running containers takes one
> subprocess per container. On busy nodes that is enough to push the worker
> past its CPU limit. `crictl` does the same job in a single call.

## API

**Controller** (port `8080`):

| Endpoint | Description |
|---|---|
| `POST /webhook/preheat` | Body: `{"image": "..."}`. Returns `202` with the ordered nodes, or `503` if no worker is fresh. Short names are normalized, so `nginx` becomes `docker.io/library/nginx:latest`. |
| `POST /report` | Called by workers. |
| `GET /status` | Shows every known worker and the current preheat target. |
| `GET /metrics` | Prometheus metrics. |
| `GET /healthz` | Liveness and readiness check. |

**Worker** (port `18081`, on the node's network):

| Endpoint | Description |
|---|---|
| `POST /pull` | Body: `{"image": "...", "ordered_at": "..."}`. Called by the controller. Returns `202` and pulls in the background. |
| `GET /metrics` | Prometheus metrics. |
| `GET /healthz` | Liveness and readiness check. |

## Metrics

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `angryduck_controller_pull_orders_total` | counter | `node`, `result`, `registry` | Pull orders sent to workers, and whether each worker accepted it. |
| `angryduck_worker_pulls_total` | counter | `node`, `result`, `registry` | Pulls actually executed, and whether they succeeded. |
| `angryduck_worker_pull_duration_seconds` | gauge | `node`, `result`, `registry`, `image`, `spegel` | Duration of the most recent preheat pull per repo. It is cleared every `PULL_DURATION_RESET_INTERVAL_S`. |
| `angryduck_worker_preheated_containers_running` | gauge | `node`, `repo` | Running containers whose repo was recently preheated on that node. This is a rough "is preheat paying off" signal. |

`registry` is only filled in when `METRICS_LABEL_REGISTRY=true`, and `spegel`
only when `SPEGEL_IMAGE_SUBSTRING` is set.

## Security considerations

Read these before you deploy.

- **The worker has elevated privileges, but it is not `privileged: true`.**
  It needs `hostPID`, `hostNetwork`, `CAP_SYS_CHROOT` and `CAP_SYS_PTRACE`,
  plus an **unconfined AppArmor profile**. It uses these to chroot into
  `/proc/1/root` and run the node's own binaries. Every other capability is
  dropped.
  - Without the AppArmor setting, reading `/proc/1/root` fails with
    `permission denied` on AppArmor-enforcing nodes such as Ubuntu.
  - This is still a real blast radius. Pin and sign the image.
- **Preheat pulls bypass `imagePullSecrets`.** The worker runs the runtime CLI
  directly, so it only has the credentials in `REGISTRY_CREDENTIALS_PATH`.
- **The endpoints have no built-in authentication.** Protect them yourself:
  - Restrict the webhook ingress to your CI runners, for example with an IP
    allowlist annotation on your ingress controller.
  - Consider a NetworkPolicy so only the controller can reach the workers'
    `/pull`.

## Image cleanup

Angry Duck never deletes images. Cleanup is kubelet's job, because kubelet is
the component that actually decides `DiskPressure`. Two
`KubeletConfiguration` settings cover it:

- `imageGCHighThresholdPercent` / `imageGCLowThresholdPercent` handle
  capacity-based cleanup of the least-recently-used unused images.
- `imageMaximumGCAge` handles time-based cleanup of images unused for longer
  than a set duration. It is on by default since Kubernetes 1.30; check the
  docs for your version.

## Development

```text
app/
├── cmd/controller/       controller entrypoint
├── cmd/worker/           worker entrypoint
└── internal/
    ├── controller/       registry of workers, ranking, HTTP server
    ├── worker/           reporter, puller, runtime backends, host exec
    ├── imageref/         image reference parsing / normalization
    ├── registryauth/     dockerconfigjson credential lookup
    ├── metrics/          tiny Prometheus text-format registry
    ├── memlimit/         sets GOMEMLIMIT from the cgroup limit
    ├── logging/          leveled logging
    ├── config/           .env loader + typed getters
    └── model/            wire types shared by both binaries
deploy/                   example Kubernetes manifests per environment
```

Run the tests:

```bash
cd app && go test ./...
```

`TestHostExec_RealChrootExecution` performs a real `chroot(2)`, so it is
skipped unless you run the tests as root.
