# Angry Duck

A small Go system that pre-pulls a freshly-pushed container image onto a
handful of low-utilization cluster nodes, right after `docker push` and
ahead of (or alongside) the GitOps sync — so that when ArgoCD tells every
replica's node to pull the image, only one node hits the origin registry
and the rest fan out the layers peer-to-peer via [Spegel](https://github.com/spegel-org/spegel).

## Why

Without Angry Duck: ArgoCD syncs, and N replicas scheduled across N nodes
each pull independently from the origin registry — 40 replicas, 40
simultaneous origin pulls, Spegel sitting idle because nothing has the
image yet for peers to fetch from.

With Angry Duck: as soon as the pipeline pushes a new image, the
controller orders 1–3 of the least-utilized nodes to pull it immediately.
By the time Argo actually schedules pods onto other nodes, Spegel can serve
those layers from a peer instead of the origin registry. It turns "N origin
pulls" into "1 origin pull + Spegel fan-out," running in parallel with the
GitOps update rather than blocking it.

It is **not** trying to guarantee the image lands on the exact node a pod
will be scheduled to — with layer sharing and Spegel already in place,
chasing that doesn't pay off.

Disk pressure on nodes is the constraint that shaped every design decision
below: the controller refuses to preheat anything if it has no fresh view
of any node's disk state, and every worker garbage-collects images across
the whole node that aren't in use anymore (with a grace period so a
pre-pull doesn't get GC'd before Argo ever asks for it). This is a node-wide
scan by design — Angry Duck manages disk space for the whole node, not just
its own preheating overhead — which is safe because the "is this image
actually running?" check resolves every reference to its canonical content
digest before comparing, rather than comparing strings directly. Two
confirmed production bugs shaped this: first, JSON was being parsed with a
substring marker that never matched real output, making every image look
permanently unused; second, even with correct parsing, one piece of image
content has multiple valid aliases (a tag, a digest-pinned ref, a bare
digest), a running container only ever reports one of them, and comparing
by raw string spared that one alias while scheduling the image's other
aliases — including its own — for removal. See the comment at the top of
`internal/worker/gc.go` for details; `internal/worker/gc_test.go` includes
a regression test built from the actual digests and reference shapes
captured during that incident. Even so, GC ships with `GC_DRY_RUN=true` by
default — it logs every removal decision without deleting anything until
you explicitly flip it to `false`, so you can verify its judgment against
your real nodes first, especially after any change to the matching logic.

## Architecture

```
CI/CD pipeline                     Angry Duck controller               Angry Duck worker (DaemonSet, 1/node)
─────────────────                  ───────────────────────             ──────────────────────────────────────
docker push  ──POST /webhook/──►   - tracks worker freshness           - every REPORT_INTERVAL_S: scrape
             preheat {image}       - rejects if 0 fresh workers          node-exporter, POST /report
                                    - ranks fresh workers by             {node_id, address, utilization}
                                      utilization (ascending)
                                    - every RANK_INTERVAL_S, orders     - on POST /pull {image}: pull the
                                      lowest RANK_TOP_N workers to        image asynchronously via ctr/
                                      pull the current target image       crictl/docker; remember the order
                                      (POST worker's /pull)                time for GC grace-period protection
                                                                        - every GC_CHECK_INTERVAL_S: compare
                                                                          every local image vs. running
                                                                          containers (matched via real JSON
                                                                          parsing, not string-scraping);
                                                                          remove images unused for
                                                                          GC_MISS_THRESHOLD consecutive
                                                                          checks, unless still within
                                                                          GC_GRACE_PERIOD_S of a pull order
```

ArgoCD's normal sync proceeds unmodified and independently — Angry Duck
never blocks or gates the deploy, it just gets ahead of it.

## Repo layout

```
cmd/controller/         controller entrypoint
cmd/worker/              worker entrypoint (runs as a DaemonSet pod)
internal/model/          shared JSON wire types
internal/config/         dependency-free .env loader + typed getters
internal/controller/     worker registry, ranking loop, HTTP handlers
internal/worker/         node-exporter scraping, container runtime shim,
                         pull handler + GC loop, HTTP handlers
deploy/k8s/              namespace, ConfigMap, controller Deployment/Service,
                         worker DaemonSet, Ingress for the external webhook
Dockerfile.controller
Dockerfile.worker
.env.example             every tunable, documented
```

No third-party Go modules are used — everything is stdlib, so `go build
./...` works offline with no `go mod download`.

## Configuration

Every interval, threshold, and count lives in one place — see
[`.env.example`](.env.example) for the full list with explanations:

| Variable | Default | Meaning |
|---|---|---|
| `LOG_LEVEL` | info | `debug`, `info`, `warn`, or `error` — see [Logging](#logging) below |
| `WORKER_STALE_AFTER_S` | 30 | worker silence timeout before it's excluded from ranking |
| `TARGET_TTL_S` | 120 | how long a preheat target stays active |
| `RANK_INTERVAL_S` | 10 | how often the controller re-ranks and re-orders |
| `RANK_TOP_N` | 2 | how many low-utilization nodes get ordered per rank |
| `REPORT_INTERVAL_S` | 15 | how often a worker pushes its utilization |
| `GC_CHECK_INTERVAL_S` | 60 | how often a worker checks for unused images |
| `GC_MISS_THRESHOLD` | 5 | consecutive unused checks before removal |
| `GC_GRACE_PERIOD_S` | 60 | protection window after a controller-ordered pull |
| `GC_DRY_RUN` | true | log removal decisions without deleting anything |
| `CONTAINER_RUNTIME` | containerd | `containerd` (`ctr`), `crictl`, or `docker` |
| `REGISTRY_CREDENTIALS_PATH` | (empty) | path to a dockerconfigjson file for private-registry pulls — see [Registry credentials](#registry-credentials) below |

Copy `.env.example` to `.env` next to the binary, or inject the same keys
via a k8s ConfigMap (see `deploy/k8s/configmap.yaml`) — real environment
variables always win over `.env` file values.

## Registry credentials

Angry Duck's worker pulls images by shelling out directly to the container
runtime CLI (`ctr images pull`, by default). This is a plain CLI
invocation — it does **not** go through kubelet's CRI plumbing, which is
the only place `imagePullSecrets` actually gets applied. Concretely: a
Pod's `imagePullSecrets` lets *kubelet* authenticate when *kubelet* pulls
that Pod's declared image — that's why Angry Duck's own controller/worker
images pull fine. It does nothing for images the *worker process itself*
decides to pull afterward, because that pull was never routed through
kubelet at all. Without credentials configured, every preheat pull is
attempted anonymously, and fails with a 401/403 for any private registry:

```
ctr: failed to resolve image: failed to authorize: failed to fetch
anonymous token: ... 403 Forbidden
```

To fix this, mount a standard `dockerconfigjson` secret — the same format
`imagePullSecrets` already uses — into the worker container, and point
`REGISTRY_CREDENTIALS_PATH` at it. `deploy/k8s/worker-daemonset.yaml`
already does this, reusing the same `gitlab-docker-registry` secret used
for `imagePullSecrets`. You do **not** need a separate secret per
registry — a `dockerconfigjson`'s `auths` map natively supports multiple
registries in one file:

```json
{
  "auths": {
    "registry.internal-registry.example.com": { "auth": "base64(user:pass)" },
    "some-other-registry.example.com": { "auth": "base64(user:pass)" }
  }
}
```

If the secret you're reusing doesn't already have credentials for a
registry you want to preheat images from, add that registry to its
`auths` map (or point `REGISTRY_CREDENTIALS_PATH` at a different secret
entirely) — Angry Duck reads whatever's mounted there and resolves
credentials by matching the image's registry host.

Leaving `REGISTRY_CREDENTIALS_PATH` unset is fine if every image you'll
ever preheat is public.

## Logging

Every log line is tagged `[DEBUG]`, `[INFO]`, `[WARN]`, or `[ERROR]`, filtered by `LOG_LEVEL`:

- **`debug`** — every per-image GC decision on every check (running/spared,
  grace-period/spared, miss count incrementing), plus the raw
  disk-utilization math the reporter computes each cycle (used/free/size
  bytes, not just the final ratio) and the controller's per-node ranking
  candidates. High volume — use when actively troubleshooting.
- **`info`** (default) — normal operational events: a GC tick's summary
  (`tick start: N local images, M running...` / `tick done: ... removed`),
  each image's miss count progress toward the threshold, pulls starting and
  completing, preheat requests accepted, workers reporting.
- **`warn`** — decisions with real consequences: an image is being removed
  (or would be, under `GC_DRY_RUN`), a preheat request was rejected, a pull
  order failed to reach a worker.
- **`error`** — a command or request failed outright (couldn't list images,
  couldn't reach the controller, image removal failed).

GC's reasoning is deliberately visible at `info` by default — you shouldn't
need `debug` just to see *why* GC is about to remove something; you only
need it to see routine "yes, this is fine" confirmations for every running
image on every tick.

## Running locally

```bash
go build -o bin/angryduck-controller ./cmd/controller
go build -o bin/angryduck-worker ./cmd/worker

# terminal 1
ENV_FILE=.env.example ./bin/angryduck-controller

# terminal 2 (needs NODE_ID/SELF_ADDRESS set; SELF_ADDRESS needs a real
# reachable host:port for the controller to call back)
NODE_ID=node-1 SELF_ADDRESS=localhost:18081 ENV_FILE=.env.example ./bin/angryduck-worker
```

Trigger a preheat the way the pipeline would, right after `docker push`:

```bash
curl -X POST http://localhost:8080/webhook/preheat \
  -H 'Content-Type: application/json' \
  -d '{"image":"registry.example.com/app:1.2.3"}'
```

Check what the controller currently sees:

```bash
curl http://localhost:8080/status
```

## Deploying

1. Build and push both images. Tag/registry convention:
   ```bash
   sudo docker build -t registry.internal-registry.example.com/devops/generic/angry-duck-controller:1.0.7 -f Dockerfile.controller .
   sudo docker build -t registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.0.7 -f Dockerfile.worker .

   sudo docker push registry.internal-registry.example.com/devops/generic/angry-duck-controller:1.0.7
   sudo docker push registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.0.7
   ```
   `deploy/k8s/controller.yaml` and `deploy/k8s/worker-daemonset.yaml` already
   point at `registry.internal-registry.example.com/devops/generic/angry-duck-{controller,worker}:1.0.7`
   — bump the tag there too when you cut a new version.
2. Apply the manifests:
   ```bash
   kubectl apply -f deploy/k8s/namespace.yaml
   kubectl apply -f deploy/k8s/configmap.yaml
   kubectl apply -f deploy/k8s/controller.yaml
   kubectl apply -f deploy/k8s/worker-daemonset.yaml
   ```
3. Your pipeline runs outside the cluster, so apply the Ingress that
   exposes just the webhook path (edit the host and IP allowlist first):
   ```bash
   kubectl apply -f deploy/k8s/ingress.yaml
   ```
   Then point the pipeline at it, right after `docker push`:
   ```bash
   curl -X POST https://angryduck-webhook.internal-registry.example.com/webhook/preheat \
     -H 'Content-Type: application/json' \
     -d "{\"image\":\"$IMAGE_REF\"}"
   ```
   `/report`, `/status`, and `/healthz` are intentionally **not** in the
   Ingress's path rules, so they stay unreachable from outside the cluster
   — only `/webhook/preheat` is exposed, and only from the IP range set in
   `nginx.ingress.kubernetes.io/whitelist-source-range`. Worker-to-
   controller traffic (reports and pull orders) never leaves the pod
   network; it always uses the in-cluster Service (`CONTROLLER_URL` in
   `deploy/k8s/configmap.yaml`), not this Ingress.

Notes on the worker DaemonSet:
- It runs with `hostNetwork: true` and `hostPID: true`, and mounts the
  node's containerd socket, so it needs a privileged security context.
  Adjust this if your cluster's containerd socket path differs.
- `NODE_EXPORTER_URL` defaults to `http://localhost:9100/metrics`, which
  assumes node-exporter also runs hostNetwork on each node (the common
  setup). Point it elsewhere if not.
- The worker's runtime shim (`internal/worker/runtime.go`) shells out to
  `ctr`, `crictl`, or `docker` depending on `CONTAINER_RUNTIME` — pick
  whichever CLI is actually present in the worker's container image (the
  provided `Dockerfile.worker` installs containerd's `ctr`).

## API summary

**Controller**
- `POST /webhook/preheat` — `{"image": "..."}`. Rejects with `503` if zero
  workers have reported recently. Otherwise sets the target and
  immediately orders the lowest-utilization fresh nodes to pull. Short
  Docker-style references are normalized before anything else happens —
  `"nginx"` and `"nginx:latest"` both become
  `"docker.io/library/nginx:latest"` — because `ctr` (the worker's default
  pull mechanism) does not do this expansion itself and fails with a
  confusing `invalid port ":latest" after host` error on short references.
  See `internal/imageref` for the exact rules.
- `POST /report` — worker utilization reports (workers call this
  themselves; you shouldn't need to).
- `GET /status` — debug snapshot of every known worker and the current
  target image.
- `GET /healthz` — liveness/readiness.

**Worker**
- `POST /pull` — `{"image": "...", "ordered_at": "..."}`, called by the
  controller. Pulls asynchronously and returns `202` immediately.
- `GET /healthz` — liveness/readiness.

## Testing

```bash
go test ./...
```

Covers the node-exporter metrics parser (the trickiest bit of pure logic —
matching the PromQL expression's `fstype!~"tmpfs|overlay|fuse.lxcfs"` /
`mountpoint="/"` filters and computing the utilization ratio).
