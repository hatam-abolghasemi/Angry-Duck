# Angry Duck

Angry Duck helps a fresh image roll out onto a cluster without every node
hammering the origin registry, and keeps node disks clean afterward.

It does three things, each independent of the others:

| | What | Why |
|---|---|---|
| **Preheat** | The moment CI pushes an image, order a couple of low-utilization nodes to pull it from origin *before* ArgoCD schedules anything. | Spreads out the "everyone pulls at once" spike. |
| **Garbage collection** | Every node's disk is continuously cleaned of images nobody's using. | Keeps preheat pulls (and everything else) from filling the disk. |
| **Pod rescue** | One-shot fix for a pod stuck in `ImagePullBackOff` because origin itself is unreachable from that specific node. | Covers a failure mode nothing else — including Spegel — can see. |

**What this is not:** general P2P image distribution across the fleet. That's
[Spegel](https://github.com/spegel-org/spegel)'s job. Angry Duck doesn't sit
in front of containerd, doesn't intercept every pull, and doesn't run a
standing mirror. Preheat only ever seeds a handful of nodes; rescue fires
once, for one already-failing pod, and gives up rather than retrying.

## Architecture

One controller pod tracks fleet state and makes preheat decisions. One
worker pod (a DaemonSet, one per node) does everything node-local: GC,
preheat pulls, and watching for stuck pods.

```
CI/CD pipeline                  Angry Duck controller                Angry Duck worker (DaemonSet, 1/node)
─────────────────               ───────────────────────              ──────────────────────────────────────
docker push                     - tracks worker freshness            - every REPORT_INTERVAL_S: scrape
     │                          - rejects if 0 fresh workers           node-exporter, list local images once
     └─POST /webhook/preheat──► - ranks by repo locality, then         (shared with GC), POST /report
        {image}                   utilization (ascending)              {utilization, repos, images}
                                - orders top RANK_TOP_N workers      - on POST /pull: pull async; report again
                                  to pull (POST worker /pull)          the moment it lands
                                - GET /rescue-source?image=  ───►    - every GC_CHECK_INTERVAL_S: reclaim
                                  exact holder, if any (from            images unused for GC_MISS_THRESHOLD
                                  memory, at most one)                  checks
                                                                      - every RESCUE_POLL_INTERVAL_S: watch
                                                                        this node's pods for stuck pulls;
                                                                        /rescue-export (peers, token) serves
                                                                        exactly one image reference on request
```

Only `POST /webhook/preheat` is meant to be reachable from outside the
cluster. Everything else stays on the pod network.

## 1. Preheat

The instant a pipeline pushes an image, `RANK_TOP_N` nodes (default 2)
pull it from origin — before any pod actually needs it.

- Pipeline calls `POST /webhook/preheat {"image": "..."}` right after
  `docker push`. ArgoCD's sync path is untouched.
- Every worker reports disk utilization and its local image list every
  `REPORT_INTERVAL_S`. The controller ranks fresh workers: prefer a node
  that already has *some* tag of the same repo (smaller delta to pull),
  then break ties by lowest utilization.
- Top `RANK_TOP_N` nodes get ordered to pull. No re-preheat of the same
  image once seed slots are filled — only a failed order gets replaced.
- Master/control-plane nodes are excluded from *targeting*
  (`RANK_EXCLUDE_NODE_SUBSTRINGS`) but still run GC.
- Preheat only covers those first few nodes. Every other node — the rest
  of a rollout, a later HPA scale-out, a node joining afterward — is
  Spegel's job, not Angry Duck's.

**Node binaries, not image ones:** the worker image ships no
`crictl`/`ctr` of its own. It chroots into the node's real root
(`HOST_ROOT`) and runs whatever the node already has installed.

**Metrics:** `angryduck_worker_pull_duration_seconds` is a *gauge*
holding the most recent preheat pull's duration (labeled by
node/result/registry/image), cleared every `PULL_DURATION_RESET_INTERVAL_S`
so it doesn't linger as a stale value between pulls. For a distribution
across the fleet, use `angryduck_worker_pulls_total`'s rate instead.
`angryduck_worker_preheated_containers_running{node,repo}` separately
samples how many running containers belong to a recently-preheated repo —
a coarse "is preheat pulling its weight" signal. Spegel exposes its own
`spegel_mirror_requests_total` / `spegel_resolve_duration_seconds` on its
own `/metrics` if you need the P2P side of the picture; a one-off
benchmark against kubelet's `Pulled` events is more trustworthy than
either steady-state counter for a real before/after comparison.

## 2. Garbage collection

Every `GC_CHECK_INTERVAL_S`, each worker scans *every* image on its node
(not just ones it preheated) and removes anything unused for
`GC_MISS_THRESHOLD` consecutive checks.

- **"In use" is decided by canonical content digest**, not the raw
  reference string — one piece of content can exist locally under a tag,
  a digest-pinned ref, and a bare digest at once. Comparing by digest
  keeps the others from being wrongly treated as separate, removable
  images.
- `GC_EXCLUDE_IMAGE_SUBSTRINGS` protects things like `pause`, which
  Kubernetes never runs as a regular container, so no "is it running?"
  check would ever save it.
- `GC_GRACE_PERIOD_S` gives a freshly preheated/pulled image a window
  before it's even eligible, so GC can't race a pod that hasn't started.
- `GC_DRY_RUN` defaults to `true`: log what would be removed, delete
  nothing, until you trust the decisions against your own fleet.
- Logging is quiet on a healthy tick — repeated "still unused" lines only
  show at `DEBUG`; `INFO` only logs an image's first unused sighting, its
  actual removal, and a tick summary when something changed.

> **Known interaction with Spegel:** removing a tag whose digest is still
> referenced by another tag is normal and correct GC behavior, but it can
> trigger a Spegel containerd-event error (`manifest ... still exists`).
> See [spegel-org/spegel#1521](https://github.com/spegel-org/spegel/issues/1521)
> if you're chasing P2P pull reliability issues on a cluster running both.

## 3. Pod rescue

Every `RESCUE_POLL_INTERVAL_S`, each worker checks its own node for a
container stuck in `ImagePullBackOff`/`ErrImagePull`. It asks the
controller for **at most one** other node with that *exact* reference,
and makes **exactly one** copy attempt. On failure: log it, wait
`RESCUE_RETRY_INTERVAL_S`, no internal retry loop, no second source.

**Why this exists separately from GC or Spegel:** a pod stuck in
`Init:ImagePullBackOff` never reaches a state GC's `crictl`-based view can
see — kubelet holds it before `CreateContainer` is ever called. This is
usually origin itself being unreachable for one registry/node combo
(network, DNS, auth) — the pull backs off before containerd asks anything
else for help, so nothing watching containerd/CRI, Spegel included, ever
gets a turn.

```
node B (pod stuck here)                              node A (has the image)
────────────────────────                              ───────────────────────
kubelet: container stuck, Waiting.Reason=ImagePullBackOff, image=repo:tag

B's rescue watch: GET controller/rescue-source?image=repo:tag
                  → {"address": "A:18081"} (or empty — nobody has it)

                  GET A:18081/rescue-export?image=repo:tag  ──────────────► A's exporter:
                                                                              token ok,
                  socket fd ─► ctr images import -  ◄══════ raw TCP ═══════  ctr images
                               (node's ctr, chrooted)                        export - repo:tag

                  done — image now present under repo:tag exactly;
                  kubelet's own retry finds it and never touches origin
```

- Matched by **exact reference**, not digest — kubelet already told us
  the precise stuck string, so that's what gets served.
- Full unpacked `ctr images import`, run only after the failed pull
  already gave up — no lock contention with a concurrent containerd pull.
- No image allowlist. Visibility is the safeguard instead:
  `angryduck_worker_pod_image_pull_failures_total{node}` and
  `angryduck_worker_rescue_attempts_total{node,result}` keep a real,
  recurring registry-reachability problem visible in metrics.
- `RESCUE_EXCLUDE_NAMESPACE_SUBSTRINGS` / `RESCUE_EXCLUDE_IMAGE_SUBSTRINGS`
  quiet down long-abandoned feature branches and deliberately-broken
  test/demo resources.
- **Fails soft:** without `RESCUE_PEER_TOKEN` or `ctr`, rescue keeps
  polling and logging/counting stuck pulls — it just can't fix anything.
  Unlike a misconfigured mirror, this never makes a fine pull misbehave.

## Assumptions & concerns

Worth knowing before you deploy this:

- **Trusts digest-based dedup completely.** GC's entire safety model rests
  on comparing canonical content digests correctly. See the Spegel
  interaction note above for one real consequence of another tool making
  a different assumption about tag/digest lifecycle.
- **Privileged by design.** The worker needs `hostPID: true` and a
  privileged security context to chroot into the node's real root and run
  its binaries. This is a wide blast radius if the image itself is ever
  compromised.
- **Bypasses `imagePullSecrets` for preheat.** The worker pulls by
  shelling out to the runtime CLI directly — a pod's own
  `imagePullSecrets` do nothing for images the worker preheats on its
  own. Credentials come from `REGISTRY_CREDENTIALS_PATH` instead.
- **Rescue trusts exact-reference matching only.** It never resolves tags
  to digests itself, so it can't rescue a request that would resolve to
  the same content under a different tag string.
- **GC deletion is real once `GC_DRY_RUN=false`.** There's no undo; watch
  dry-run output on your actual fleet before flipping it.
- **Assumes `crictl`/`ctr`/`docker` are already on the node** in a form
  the chroot can exec. Nothing is bundled or mounted in.

## Requirements

**To run at all:**
- `crictl` (recommended) or `docker` on the node, for image listing/pulls.
- `hostPID: true` + privileged security context on the worker DaemonSet.
- `NODE_EXPORTER_URL` reachable (needs `hostNetwork: true`, node-exporter
  running the same way).
- The worker running on **every** node, masters included — GC manages
  disk everywhere.
- A `dockerconfigjson` mounted, `REGISTRY_CREDENTIALS_PATH` pointed at it,
  for any non-public registry.

**To also run pod rescue** (`RESCUE_ENABLED=true`, the default):
- `ctr` on the node (crictl has no export/import verbs).
- RBAC from `deploy/*/rbac.yaml` applied (a `ServiceAccount` with
  `get`/`list` on `pods`, cluster-wide).
- The `angryduck-rescue` Secret (`RESCUE_PEER_TOKEN`) created before the
  DaemonSet — without it, rescue polls and logs but can't fix anything.

## Quick start

```bash
go build -o bin/angryduck-controller ./cmd/controller
go build -o bin/angryduck-worker ./cmd/worker

# terminal 1
ENV_FILE=.env.example ./bin/angryduck-controller

# terminal 2 (SELF_ADDRESS needs a real reachable host:port so the
# controller can call this worker back)
NODE_ID=node-1 SELF_ADDRESS=localhost:18081 HOST_ROOT=/ ENV_FILE=.env.example ./bin/angryduck-worker
```

Trigger a preheat the way a pipeline would, right after `docker push`:

```bash
curl -X POST http://localhost:8080/webhook/preheat \
  -H 'Content-Type: application/json' \
  -d '{"image":"registry.example.com/app:1.2.3"}'
```

`curl http://localhost:8080/status` shows every known worker plus the
active preheat target.

## Deploying

```bash
# 1. Build and push both images
sudo docker build --no-cache -t registry.internal-registry.example.com/devops/generic/angry-duck-controller:1.4.6 -f Dockerfile.controller .
sudo docker build --no-cache -t registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.4.6 -f Dockerfile.worker .
sudo docker push registry.internal-registry.example.com/devops/generic/angry-duck-controller:1.4.6
sudo docker push registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.4.6
# (bump the tag in deploy/stg/controller.yaml and worker-daemonset.yaml too)

# 2. The rescue token — before the DaemonSet, once per cluster
sudo kubectl -n angryduck create secret generic angryduck-rescue \
  --from-literal=token="$(openssl rand -hex 32)"

# 3. RBAC (rescue's pod-watch needs get/list on pods, cluster-wide)
sudo kubectl apply -f deploy/stg/rbac.yaml

# 4. Apply the rest
sudo kubectl apply -f deploy/stg/namespace.yaml
sudo kubectl apply -f deploy/stg/configmap.yaml
sudo kubectl apply -f deploy/stg/controller.yaml
sudo kubectl apply -f deploy/stg/worker-daemonset.yaml

# 5. Expose just the webhook to the pipeline (edit host/IP allowlist first)
sudo kubectl apply -f deploy/stg/ingress.yaml
```

To turn rescue off on a running cluster: set `RESCUE_ENABLED=false` and
roll the DaemonSet.

## Configuration

Everything tunable lives in [`.env.example`](.env.example). Highlights:

| Variable | Default | Controls |
|---|---|---|
| `RANK_TOP_N` | 2 | how many nodes preheat per pushed image |
| `RANK_PREFER_IMAGE_LOCALITY` | true | prefer a node with some tag of the repo already, over an empty one |
| `RANK_EXCLUDE_NODE_SUBSTRINGS` | (empty) | node names to never pick as preheat targets, e.g. `master,control-plane` |
| `GC_MISS_THRESHOLD` | 5 | consecutive unused checks before removal |
| `GC_GRACE_PERIOD_S` | 60 | protects a freshly-preheated image until it's actually needed |
| `GC_EXCLUDE_IMAGE_SUBSTRINGS` | (empty) | images GC should never remove, e.g. `pause,node-exporter` |
| `GC_DRY_RUN` | true | log-only, no real deletion |
| `CONTAINER_RUNTIME` | crictl | `crictl` (recommended), `containerd`, or `docker` |
| `REGISTRY_CREDENTIALS_PATH` | (empty) | dockerconfigjson for private-registry preheat pulls |
| `HOST_ROOT` | /proc/1/root | where the node's filesystem is seen for chrooted execs |
| `RESCUE_ENABLED` | true | one-shot rescue for stuck pulls |
| `RESCUE_POLL_INTERVAL_S` | 30 | how often to poll this node's pods |
| `RESCUE_RETRY_INTERVAL_S` | 600 | wait before retrying a failed rescue |
| `RESCUE_MAX_CONCURRENT_EXPORTS` | 1 | concurrent `/rescue-export` requests served per node |
| `RESCUE_PEER_TOKEN` | (empty) | shared secret for `/rescue-export` |
| `RESCUE_EXCLUDE_NAMESPACE_SUBSTRINGS` / `RESCUE_EXCLUDE_IMAGE_SUBSTRINGS` | (empty) | ignore matching stuck pods entirely |
| `SPEGEL_IMAGE_SUBSTRING` | (empty) | label pull-duration metric by whether Spegel was running at pull time |
| `PULL_DURATION_RESET_INTERVAL_S` | 15 | how often the pull-duration gauge resets — match your scrape interval |

Real environment variables (a k8s ConfigMap, in practice) always win over
`.env` file values.

## API summary

**Controller**
- `POST /webhook/preheat` — `{"image": "..."}`. `503` if zero fresh
  workers. Short references get normalized (`nginx` →
  `docker.io/library/nginx:latest`).
- `POST /report` — workers call this themselves.
- `GET /rescue-source?image=<repo:tag>&node=<asker>` — one fresh worker
  known to have this exact reference, or empty if nobody does.
- `GET /status` — every known worker plus the active target.
- `GET /healthz`

**Worker**
- `POST /pull` — `{"image": "...", "ordered_at": "..."}`. Pulls async,
  returns `202` immediately.
- `GET /rescue-export?image=<repo:tag>` — needs
  `X-Angryduck-Rescue-Token`. `200` + raw tar, `503` busy, `401`
  bad/missing token, `400` missing image.
- `GET /healthz`

## Resource usage

Nothing here holds unbounded state — everything's bounded by the number
of local images on one node (tens) or nodes in the fleet (a few dozen).
Worker requests/limits: `24Mi`/`128Mi` memory (worst case ≈ one rescue
export + one rescue import + a listing + a preheat pull all overlapping),
`500m` CPU (bursty subprocess execs, not a sustained transfer loop).
Controller: `16Mi`/`32Mi`.

Both binaries read their own cgroup memory limit at startup and apply it
via `GOMEMLIMIT`, so a burst of concurrent pulls doesn't look like a
permanent leak under cgroup accounting.

## Metrics

Worker (`:18081/metrics`): `angryduck_worker_pod_image_pull_failures_total{node}`,
`angryduck_worker_rescue_attempts_total{node,result}` (`success`,
`no_source`, `lookup_failed`, `dial_failed`, `transfer_failed`),
`angryduck_worker_rescue_exports_total{node,result}` (`served`, `busy`,
`unauthorized`, `failed`).

## Logging

Every line tagged `[DEBUG]`/`[INFO]`/`[WARN]`/`[ERROR]`, filtered by
`LOG_LEVEL`. At `info`: GC's first-sighting/removal lines and rescue's
first-sighting/outcome lines — not a repeated line every tick for
something still in cooldown (that's `debug`).

## Testing

```bash
go test ./...
```

`internal/worker` includes `TestHostExec_RealChrootExecution`, which
needs `CAP_SYS_CHROOT` and is skipped (not failed) outside root.
