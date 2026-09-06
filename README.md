# Angry Duck

Angry Duck kills the "40 replicas, 40 origin pulls" problem. It pre-pulls
a freshly-pushed image onto a couple of low-utilization nodes the moment
CI pushes it, so by the time ArgoCD actually schedules pods everywhere
else, [Spegel](https://github.com/spegel-org/spegel) can hand those nodes
the image peer-to-peer instead of every one of them hitting the origin
registry at once.

## The problem

A normal rollout looks like this: ArgoCD syncs, and every node that just
got scheduled a new pod pulls the new image — independently, at the same
moment, straight from the origin registry. Spegel is sitting right there
ready to serve peers, but on a fresh deploy nobody has the image yet, so
there's no peer to serve from. 40 replicas means 40 simultaneous origin
pulls, right when the registry is least equipped to enjoy that.

## What Angry Duck does about it

- **Gets ahead of the deploy, not in its way.** One `curl` call right
  after `docker push` is all a pipeline needs to add. ArgoCD's sync is
  completely untouched — Angry Duck never gates or blocks it, it just
  wins the race to have the image ready first.
- **Picks targets based on real signal, not guesses.** Nodes report disk
  utilization every few seconds; the controller orders the least-loaded
  fresh nodes to pull, and flatly refuses to preheat anything if it has
  no recent view of any node's disk state rather than picking blind.
- **Manages the whole node's disk, not just its own mess.** Every
  worker's GC loop scans and reclaims *any* image that's fallen out of
  use on that node — not only images Angry Duck itself pulled — so image
  disk usage stays flat over time instead of accumulating with every tag
  a pipeline ever pushes.
- **Doesn't guess wrong about what's "in use."** Images are matched by
  their canonical content digest, not by raw name — the same content can
  legitimately show up locally under a tag, a digest-pinned reference,
  and a bare digest, and only comparing by digest keeps GC from treating
  those as separate, disposable images.
- **Knows what it can't know.** A small always-safe allowlist
  (`GC_EXCLUDE_IMAGE_SUBSTRINGS`) covers images like `pause` that
  Kubernetes runs as pod sandboxes rather than regular containers — no
  amount of "is this running?" logic will ever see them as running, so
  they get an explicit pass instead of being cycled through GC forever.
- **Doesn't waste a preheat slot on a node nothing will ever use.**
  Master/control-plane nodes never get scheduled a real workload pod, so
  they always look artificially idle. `RANK_EXCLUDE_NODE_SUBSTRINGS` keeps
  them out of the *ranking*, while they keep running their own GC — the
  disk still gets managed, the preheat slot just goes somewhere useful.
- **Ships safe by default.** GC starts in dry-run, logging exactly what
  it would remove and why, until you've watched it decide correctly
  against your own nodes and flip it on.
- **Costs almost nothing to run.** Pure Go, stdlib only — no dependencies
  to audit, no `go mod download`, small static binaries, small images,
  and worker pods sized in double-digit MiB.

## Why this combo — Angry Duck + Spegel + your pipeline — actually adds up

Each piece is solving a different stage of the same lifecycle, and
together they cover it end to end:

- **At push time**, Angry Duck turns "N nodes about to ask the registry
  for the same layers" into a single, controlled origin pull on 1–3
  nodes — the registry sees one request instead of a stampede, no matter
  how large the fleet is.
- **At rollout time**, Spegel turns that one pull into cluster-wide
  availability for free: every other node ArgoCD schedules onto pulls
  the image from a peer over the LAN instead of the origin, so rollouts
  finish faster and registry egress stays flat regardless of replica
  count.
- **At steady state**, Angry Duck's node-wide GC keeps disk usage from
  quietly climbing forever as your pipeline pushes new tags every day —
  the same system that got the image onto the node in the first place is
  the one making sure old ones don't just pile up.

None of that requires touching ArgoCD, your pipeline's deploy logic, or
Spegel's own configuration — Angry Duck sits entirely outside that path
and only adds one webhook call. Origin bandwidth, rollout latency, and
node disk pressure end up being managed as one connected system instead
of three separate problems nobody owns.

## Architecture

```
CI/CD pipeline                     Angry Duck controller               Angry Duck worker (DaemonSet, 1/node)
─────────────────                  ───────────────────────             ──────────────────────────────────────
docker push  ──POST /webhook/──►   - tracks worker freshness           - every REPORT_INTERVAL_S: scrape
             preheat {image}       - rejects if 0 fresh workers          node-exporter, POST /report
                                    - ranks fresh, eligible workers      {node_id, address, utilization}
                                      by utilization (ascending)
                                    - every RANK_INTERVAL_S, orders     - on POST /pull {image}: pull the
                                      lowest RANK_TOP_N workers to        image asynchronously; remember the
                                      pull the current target image       order time for GC grace-period
                                      (POST worker's /pull)                protection
                                                                        - every GC_CHECK_INTERVAL_S: reclaim
                                                                          any local image unused for
                                                                          GC_MISS_THRESHOLD checks, unless
                                                                          still in its grace period or on the
                                                                          exclude list
```

## Quick start

```bash
go build -o bin/angryduck-controller ./cmd/controller
go build -o bin/angryduck-worker ./cmd/worker

# terminal 1
ENV_FILE=.env.example ./bin/angryduck-controller

# terminal 2 (SELF_ADDRESS needs a real reachable host:port so the
# controller can call this worker back)
NODE_ID=node-1 SELF_ADDRESS=localhost:18081 ENV_FILE=.env.example ./bin/angryduck-worker
```

Trigger a preheat the way a pipeline would, right after `docker push`:

```bash
curl -X POST http://localhost:8080/webhook/preheat \
  -H 'Content-Type: application/json' \
  -d '{"image":"registry.example.com/app:1.2.3"}'
```

`curl http://localhost:8080/status` shows every worker the controller
currently knows about and the active preheat target.

## Configuration

Everything tunable lives in one place — see [`.env.example`](.env.example)
for the full annotated list. The highlights:

| Variable | Default | What it controls |
|---|---|---|
| `RANK_TOP_N` | 2 | how many low-utilization nodes get ordered per rank |
| `RANK_EXCLUDE_NODE_SUBSTRINGS` | (empty) | node names (substrings) to never pick as preheat targets — e.g. `master,control-plane` |
| `GC_MISS_THRESHOLD` | 5 | consecutive unused checks before an image is removed |
| `GC_GRACE_PERIOD_S` | 60 | protects a freshly-preheated image until Argo actually needs it |
| `GC_EXCLUDE_IMAGE_SUBSTRINGS` | (empty) | images (substrings) GC should never remove, checked or not — e.g. `pause,node-exporter` |
| `GC_DRY_RUN` | true | log what GC would remove without deleting anything |
| `CONTAINER_RUNTIME` | crictl | `crictl` (recommended), `containerd`, or `docker` |
| `REGISTRY_CREDENTIALS_PATH` | (empty) | dockerconfigjson for private-registry preheat pulls |

Real environment variables (a k8s ConfigMap, in practice — see
`deploy/stg/configmap.yaml`) always win over `.env` file values.

**On `CONTAINER_RUNTIME`:** stick with `crictl`. It lists every running
container's image in one `crictl ps -o json` call; the `containerd`
backend has to shell out to `ctr containers info <id>` once *per
container*, which is enough subprocess overhead on a busy node to blow
past a worker's own CPU limit. Only use `containerd` if `crictl` genuinely
isn't available.

**On registry credentials:** the worker pulls by shelling out to the
runtime CLI directly, which bypasses kubelet's CRI plumbing entirely — so
a Pod's `imagePullSecrets` (which only kubelet honors) does nothing for
images the worker decides to pull on its own. Mount the same
`dockerconfigjson` secret and point `REGISTRY_CREDENTIALS_PATH` at it;
`deploy/stg/worker-daemonset.yaml` already does this. Leave it unset if
every image you'll preheat is public.

## Deploying

```bash
# 1. Build and push both images
sudo docker build --no-cache -t registry.internal-registry.example.com/devops/generic/angry-duck-controller:1.0.11 -f Dockerfile.controller .
sudo docker build --no-cache -t registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.0.11 -f Dockerfile.worker .
sudo docker push registry.internal-registry.example.com/devops/generic/angry-duck-controller:1.0.11
sudo docker push registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.0.11
# (bump the tag in deploy/stg/controller.yaml and worker-daemonset.yaml too)

# 2. Apply the manifests
kubectl apply -f deploy/stg/namespace.yaml
kubectl apply -f deploy/stg/configmap.yaml
kubectl apply -f deploy/stg/controller.yaml
kubectl apply -f deploy/stg/worker-daemonset.yaml

# 3. Expose just the webhook to the pipeline (edit host/IP allowlist first)
kubectl apply -f deploy/stg/ingress.yaml
```

Then point the pipeline at it, right after `docker push`:

```bash
curl -X POST https://angryduck-webhook.internal-registry.example.com/webhook/preheat \
  -H 'Content-Type: application/json' \
  -d "{\"image\":\"$IMAGE_REF\"}"
```

`/report`, `/status`, and `/healthz` are deliberately **not** in the
Ingress's path rules — only `/webhook/preheat` is reachable from outside
the cluster, and only from the allowlisted IP range. Worker-to-controller
traffic never leaves the pod network.

A few things worth knowing about the worker DaemonSet: it needs
`hostNetwork: true`, `hostPID: true`, and a mounted containerd socket
(hence the privileged security context); `NODE_EXPORTER_URL` assumes
node-exporter also runs `hostNetwork` on each node; and it should run on
every node, master/control-plane included, since GC needs to manage disk
everywhere even where preheat targets are never chosen.

## API summary

**Controller**
- `POST /webhook/preheat` — `{"image": "..."}`. `503` if zero fresh
  workers. Short references get normalized (`nginx` → `docker.io/library/nginx:latest`) before anything else happens.
- `POST /report` — workers call this themselves.
- `GET /status` — every known worker plus the active target.
- `GET /healthz` — liveness/readiness.

**Worker**
- `POST /pull` — `{"image": "...", "ordered_at": "..."}`, called by the
  controller. Pulls asynchronously, returns `202` immediately.
- `GET /healthz` — liveness/readiness.

## Logging

Every line is tagged `[DEBUG]`/`[INFO]`/`[WARN]`/`[ERROR]`, filtered by
`LOG_LEVEL`. `info` (the default) already shows GC's reasoning — tick
summaries, miss-count progress, what got removed and why — without the
per-image "yes, this is fine" noise that `debug` adds on top.

## Testing

```bash
go test ./...
```
