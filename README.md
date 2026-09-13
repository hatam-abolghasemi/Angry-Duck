# Angry Duck

Angry Duck helps a fresh image roll out onto a cluster without every node
independently hammering the origin registry, and keeps node disks clean
afterward. It does three things:

1. **Preheat** — the moment CI pushes an image, order a couple of
   low-utilization nodes to pull it from origin *before* ArgoCD schedules
   anything.
2. **Garbage collection** — every node's disk is continuously cleaned of
   images nobody's using, so preheat pulls (and everything else) don't
   accumulate forever.
3. **Pod rescue** — a one-shot, narrowly-scoped fix for a pod stuck in
   `ImagePullBackOff` for a reason nothing else can see: origin itself
   unreachable for that specific registry from that specific node.

General peer-to-peer image distribution across the fleet is **Spegel's
job, not this one**. Angry Duck does not sit in front of containerd, does
not intercept every pull, and does not run a standing mirror. Rescue is
deliberately the opposite of that: it only ever fires for a pod already
failing, tries exactly once, and goes quiet on failure instead of
retrying.

One controller pod tracks fleet state and makes the preheat decision. One
worker pod (a DaemonSet, one per node) does everything node-local: it
runs GC, executes preheat pulls, and watches for stuck pods to rescue.

## 1. Preheat

**What it does:** the instant a pipeline pushes an image, `RANK_TOP_N`
nodes (default 2) pull it from origin — before any pod actually needs it.

**How:**
- The pipeline calls `POST /webhook/preheat {"image": "..."}` right after
  `docker push`. Nothing else in the deploy path changes; ArgoCD's sync is
  untouched.
- Every worker reports its disk utilization and local image inventory to
  the controller every `REPORT_INTERVAL_S`. The controller ranks fresh
  workers by that report: it prefers a node that already has *some* tag of
  the same repo (usually a small delta to pull) over an emptier node
  starting fully cold, then breaks ties by lowest utilization.
- The controller orders the top `RANK_TOP_N` nodes to `POST /pull`
  (`CONTAINER_RUNTIME`, e.g. `crictl`, run directly on the node) and stops.
  It won't order anything if it has no fresh view of the fleet, and it
  never re-preheats the same image once its seed slots are filled — only
  a failed order gets replaced.
- Master/control-plane nodes are excluded from preheat targeting
  (`RANK_EXCLUDE_NODE_SUBSTRINGS`) since no real workload ever schedules
  onto them, but they still run GC.

Preheat only covers the first few nodes. Everything else — the other 38
nodes in that rollout, a 3am HPA scale-out, a node that joins later — is
Spegel's job, not Angry Duck's.

Node binaries, not image ones: the worker image contains no
`crictl`/`ctr` of its own. It runs the node's own copies, chrooted into
the node's real root (`HOST_ROOT`, `/proc/1/root` by default) — so
whatever apt or Kubespray installed is exactly what runs, statically or
dynamically linked, always the current version, with no files copied
onto or mounted over the node.

## 2. Garbage collection

**What it does:** every `GC_CHECK_INTERVAL_S`, each worker scans *every*
image on its node — not just ones Angry Duck itself pulled — and removes
any that's gone unused for `GC_MISS_THRESHOLD` consecutive checks.

**How:**
- "In use" is decided by **canonical content digest**, not by raw
  reference string. One piece of content can show up locally under a tag,
  a digest-pinned ref, and a bare digest — a running container's reported
  image is only ever one of those aliases, so comparing by digest is what
  keeps the others from being wrongly treated as separate, removable
  images.
- A small always-safe allowlist (`GC_EXCLUDE_IMAGE_SUBSTRINGS`) covers
  images like `pause`, which Kubernetes runs as a pod sandbox rather than
  a regular container — no "is this running?" check will ever see it as
  running, so it's excluded outright instead of being cycled through GC
  forever.
- A freshly preheated or pulled image gets a `GC_GRACE_PERIOD_S` window
  before it's even eligible, so GC can never race a pod that hasn't
  started yet.
- `GC_DRY_RUN` defaults to `true`: GC logs exactly what it would remove
  and why, without deleting anything, until you've watched it decide
  correctly against your own fleet.
- Logging is deliberately quiet on a healthy tick: the two per-tick
  summary lines and a per-image "still unused" line only appear at
  `DEBUG` once an image has already been flagged once. At the default
  `INFO` level you only see an image's *first* unused sighting, its
  actual removal, and a tick summary when something in it actually
  changed — not a repeated line every interval for as long as one image
  happens to be counting down.

## 3. Pod rescue

**What it does:** every `RESCUE_POLL_INTERVAL_S`, each worker checks its
own node's pods for any container stuck in
`ImagePullBackOff`/`ErrImagePull`. For each one, it asks the controller
for **at most one** other node that already has that *exact* image
reference, and makes **exactly one** attempt to copy it from there. If
that fails for any reason, it logs it and waits `RESCUE_RETRY_INTERVAL_S`
before trying that image again — there is no internal retry loop, no
second source tried, no busy-wait, nothing resembling continuous peer
serving.

**Why this exists as its own small thing:** GC only ever sees
containerd/CRI state (`crictl images`, `crictl ps`). A pod stuck in
`Init:ImagePullBackOff` never reaches that state at all — kubelet holds
it before ever calling `CreateContainer` — so GC has no way to see it,
no matter how correct its own logic is. This is most common when tag
resolution against **origin itself** fails for a specific
registry/node combination (network, DNS, or auth) — the pull backs off
before containerd asks anything else for help, so nothing that watches
containerd/CRI, including Spegel, ever gets a turn.

**How:**
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
                                                                              (chrooted)
                  done — image now present under repo:tag exactly;
                  kubelet's own retry finds it and never touches origin
```
- Matched by **exact reference**, not digest. Rescue only ever runs after
  kubelet has already told us the precise string ("repo:tag") a specific
  pod is stuck on, so it asks for and serves that exact reference by
  name — no manifest-digest resolution, no tag→digest tracking, no
  continuously-updated peer inventory the way a real mirror would need.
  The controller only tracks each node's exact local image list: a small,
  bounded set of strings, replaced wholesale on every report.
- The import is a full, unpacked `ctr images import` — there's no
  concurrent containerd pull holding an unpack lock to deadlock against,
  since the pull that would have started one already gave up before
  rescue ever runs.
- No allowlist gates which images this applies to. The safeguard is
  visibility: `angryduck_worker_pod_image_pull_failures_total{node}`
  counts every stuck-pull sighting regardless of outcome, and
  `angryduck_worker_rescue_attempts_total{node,result}` counts the
  rescue attempt's own outcome, so a fleet with a real, recurring
  registry-reachability problem stays visible in metrics rather than
  hiding behind "well, rescue fixed it."
- `RESCUE_EXCLUDE_NAMESPACE_SUBSTRINGS` / `RESCUE_EXCLUDE_IMAGE_SUBSTRINGS`
  exist because a real fleet accumulates long-abandoned feature-branch
  deployments and deliberately-broken test/demo resources (chaos-testing
  namespaces, policy-testing images) that would otherwise get re-logged
  at `WARN` on every poll, on every node, forever.
- Fails soft: without `RESCUE_PEER_TOKEN` or `ctr` on the node, rescue
  still polls and still counts/logs stuck pulls, it just can't fix
  anything — unlike a misconfigured mirror, this never makes an
  otherwise-fine pull silently misbehave.

## Architecture

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
cluster (`deploy/stg/ingress.yaml` allowlists it alone). `/report`,
`/rescue-source`, `/status` and every worker endpoint stay on the pod
network.

## Requirements

**To run at all:**
- A node binary the worker can shell out to for image listing/pulls:
  `crictl` (recommended — one `crictl ps -o json` call lists every running
  container's image; `ctr` needs one subprocess per container) or
  `docker`.
- `hostPID: true` and a privileged security context on the worker
  DaemonSet, so it can chroot into `/proc/1/root` and run the node's own
  binaries.
- `NODE_EXPORTER_URL` reachable — the worker reads disk/utilization from
  node-exporter, so it needs `hostNetwork: true` and node-exporter running
  the same way.
- The worker running on **every** node, masters included — GC manages
  disk everywhere, even on nodes that never receive a preheat order.
- A `dockerconfigjson` mounted and `REGISTRY_CREDENTIALS_PATH` pointed at
  it, for any registry that isn't public. The worker pulls by shelling out
  to the runtime CLI directly, bypassing kubelet's CRI plumbing — so a
  Pod's own `imagePullSecrets` do nothing for images the worker decides to
  pull on its own.

**To also run pod rescue** (`RESCUE_ENABLED=true`, the default):
- `ctr` present on the node (crictl has no export/import verbs) — every
  containerd node already has it.
- The RBAC in `deploy/*/rbac.yaml` applied — a `ServiceAccount` bound to
  a `ClusterRole` granting `get`/`list` on `pods`, and the DaemonSet's
  `serviceAccountName` set to it. Without this, rescue logs why it can't
  start and the worker otherwise runs normally.
- The `angryduck-rescue` Secret (`RESCUE_PEER_TOKEN`) created before the
  DaemonSet for the export side to actually work — without it, rescue
  still polls and counts/logs stuck pulls, it just can't fix anything.

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

`curl http://localhost:8080/status` shows every worker the controller
currently knows about and the active preheat target.

## Configuration

Everything tunable lives in one place — see [`.env.example`](.env.example)
for the full annotated list. The highlights:

| Variable | Default | What it controls |
|---|---|---|
| `RANK_TOP_N` | 2 | exactly how many nodes preheat per pushed image |
| `RANK_PREFER_IMAGE_LOCALITY` | true | prefer a node that already has some tag of the target repo over an emptier node that doesn't |
| `RANK_EXCLUDE_NODE_SUBSTRINGS` | (empty) | node names (substrings) to never pick as preheat targets — e.g. `master,control-plane` |
| `GC_MISS_THRESHOLD` | 5 | consecutive unused checks before an image is removed |
| `GC_GRACE_PERIOD_S` | 60 | protects a freshly-preheated image until Argo actually needs it |
| `GC_EXCLUDE_IMAGE_SUBSTRINGS` | (empty) | images (substrings) GC should never remove, checked or not — e.g. `pause,node-exporter` |
| `GC_DRY_RUN` | true | log what GC would remove without deleting anything |
| `CONTAINER_RUNTIME` | crictl | `crictl` (recommended), `containerd`, or `docker` |
| `REGISTRY_CREDENTIALS_PATH` | (empty) | dockerconfigjson for private-registry preheat pulls |
| `HOST_ROOT` | /proc/1/root | where the node's filesystem is seen; node binaries run chrooted here |
| `RESCUE_ENABLED` | true | one-shot rescue for pods stuck in ImagePullBackOff |
| `RESCUE_POLL_INTERVAL_S` | 30 | how often to poll this node's pods for stuck pulls |
| `RESCUE_RETRY_INTERVAL_S` | 600 | the entire retry policy — how long to wait before trying a failed rescue again |
| `RESCUE_MAX_CONCURRENT_EXPORTS` | 1 | at most this many /rescue-export requests served at once per node |
| `RESCUE_PEER_TOKEN` | (empty) | shared secret for `/rescue-export`; optional (soft-fail without it), from the `angryduck-rescue` Secret |
| `RESCUE_EXCLUDE_NAMESPACE_SUBSTRINGS` / `RESCUE_EXCLUDE_IMAGE_SUBSTRINGS` | (empty) | ignore stuck pods matching these entirely — no log, no metric, no attempt |
| `METRICS_LABEL_REGISTRY` | false | add a `registry` label (image registry host, "docker.io" for unqualified refs) to the image-related counters; off by default since registry host isn't a bounded set like node — read by both controller and worker |

Real environment variables (a k8s ConfigMap, in practice — see
`deploy/stg/configmap.yaml`) always win over `.env` file values.

## Deploying

```bash
# 1. Build and push both images
sudo docker build --no-cache -t registry.internal-registry.example.com/devops/generic/angry-duck-controller:1.4.3 -f Dockerfile.controller .
sudo docker build --no-cache -t registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.4.3 -f Dockerfile.worker .
sudo docker push registry.internal-registry.example.com/devops/generic/angry-duck-controller:1.4.3
sudo docker push registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.4.3
# (bump the tag in deploy/stg/controller.yaml and worker-daemonset.yaml too)

# 2. The rescue token — before the DaemonSet (once per cluster). Rescue
#    soft-fails without it (see Requirements), but create it up front to
#    avoid a "why isn't rescue fixing anything" surprise later.
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

## API summary

**Controller**
- `POST /webhook/preheat` — `{"image": "..."}`. `503` if zero fresh
  workers. Short references get normalized (`nginx` → `docker.io/library/nginx:latest`) before anything else happens.
- `POST /report` — workers call this themselves.
- `GET /rescue-source?image=<repo:tag>&node=<asker>` — the address of
  one fresh worker known to have this exact reference, or an empty
  address if nobody does. Never a list; never filtered by anything but
  freshness and excluding the asker.
- `GET /status` — every known worker plus the active target.
- `GET /healthz` — liveness/readiness.

**Worker**
- `POST /pull` — `{"image": "...", "ordered_at": "..."}`, called by the
  controller. Pulls asynchronously, returns `202` immediately.
- `GET /rescue-export?image=<repo:tag>` — `X-Angryduck-Rescue-Token`
  required. `200` then the raw image tar straight from `ctr images
  export`; `503` busy; `401` bad/missing token; `400` missing image.
- `GET /healthz` — liveness/readiness.

## Resource usage

Nothing in Angry Duck holds unbounded or multi-megabyte state resident in
memory. Everything that's kept around is bounded by the number of local
images on one node (tens, not thousands) or the number of nodes in the
fleet (a few dozen at most):

- **Worker**: the shared image-listing cache (`Inventory`) holds the
  local ref list and a name→id map from the last `crictl images -o json`
  — a few tens of short strings, comfortably under a megabyte even on a
  node with 100+ local images. GC's per-image miss counters and rescue's
  per-image cooldown timestamps are the same order of magnitude.
- **Controller**: each worker's record is a utilization float, a set of
  bare repo names, and a set of exact image references — replaced
  wholesale on every report (`Update`), never accumulated across reports.
  At 21 nodes × ~90 images × ~80 bytes/string, that's roughly 150 KB
  fleet-wide, not something that scales into megabytes as the cluster
  runs longer.

What actually uses memory is transient, not resident: a preheat pull
(`crictl pull`, ~25 MiB) or a rescue transfer (`ctr export`/`ctr import`,
~30-35 MiB each) is a genuine child process in the worker's cgroup, but
it exists only for the seconds the operation takes and is gone the
instant it returns. Rescue's own concurrency is capped low on purpose
(`RESCUE_MAX_CONCURRENT_EXPORTS`, default 1) since it's a rare repair
path, not a throughput mechanism — there's no reason to provision for
more than one or two of these overlapping at once.

DaemonSet requests/limits reflect this: `24Mi`/`128Mi` for the worker
(worst case ≈ worker 6 + one rescue export ~35 + one rescue import ~35 +
a `crictl` listing ~20 + a preheat pull ~25 ≈ 121 MiB), `16Mi`/`32Mi` for
the controller. CPU is deliberately bursty rather than sustained — short
subprocess execs (`crictl`/`ctr` calls), never a long-running transfer
loop — so the limit only needs to absorb one burst, not throughput; `1`
core was provisioned for the old always-on mirror's concurrent-transfer
model and was reduced to `500m` for the worker and `150m` for the
controller now that neither runs one.

## Metrics

Worker (`:18081/metrics`): `angryduck_worker_pod_image_pull_failures_total{node}`
(every poll-tick observation of a container stuck in
ImagePullBackOff/ErrImagePull, regardless of whether a rescue was
attempted or succeeded — "is this node having pull problems at all"),
`angryduck_worker_rescue_attempts_total{node,result}` (the rescue
attempt's own outcome: `success`, `no_source`, `lookup_failed`,
`dial_failed`, `transfer_failed`), and
`angryduck_worker_rescue_exports_total{node,result}` (this node acting
as a rescue source: `served`, `busy`, `unauthorized`, `failed`).

## Logging

Every line is tagged `[DEBUG]`/`[INFO]`/`[WARN]`/`[ERROR]`, filtered by
`LOG_LEVEL`. At `info` (the default): GC's first-sighting and removal
lines (not every repeated "still unused" tick), and rescue's first
sighting of a stuck container plus the outcome of each rescue attempt —
not a repeated line every poll for a container that's still stuck within
its cooldown window (that goes to `debug`).

## Testing

```bash
go test ./...
```

`internal/worker`'s tests include one (`TestHostExec_RealChrootExecution`)
that performs an actual `chroot(2)` against a fake host root — it needs
`CAP_SYS_CHROOT` and is skipped, not failed, when not running as root.
