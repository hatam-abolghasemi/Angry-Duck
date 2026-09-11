# Angry Duck

Angry Duck kills the "40 replicas, 40 origin pulls" problem: a rollout
where every node that gets scheduled a new pod pulls the same fresh image
independently, straight from the origin registry, all at once.

It does four things:

1. **Preheat** — the moment CI pushes an image, order a couple of
   low-utilization nodes to pull it from origin *before* ArgoCD schedules
   anything.
2. **Peer-to-peer mirror** — every other node that needs the image gets it
   from a node that already has it, over the LAN, instead of from origin.
3. **Garbage collection** — every node's disk is continuously cleaned of
   images nobody's using, so none of the above accumulates forever.
4. **Pod-watch** — a pod stuck in `ImagePullBackOff` for a reason the
   mirror can never see (origin itself unreachable for that tag) gets
   rescued by importing its image from a peer under its exact tag.

One controller pod tracks fleet state and makes the preheat decision. One
worker pod (a DaemonSet, one per node) does everything node-local: it runs
the mirror, runs GC, runs pod-watch, and executes preheat pulls.

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
covered by the mirror below.

## 2. Peer-to-peer mirror

**What it does:** when containerd on any node needs an image, it asks that
node's Angry Duck worker before origin. If a fresh peer already has the
exact digest, the worker pulls the whole image from that peer over the
LAN; containerd never touches origin for the layers.

**How:**
```
node B (needs image)                                    node A (has it)
─────────────────────                                    ────────────────
containerd:
 1. HEAD manifests/<tag> ──────────► origin  (tag → digest, ~300 bytes)
 2. GET manifests/<digest> → 127.0.0.1:18081 (B's worker)  ── held ──┐
                                                                      │
B's worker:  GET controller/peers?digest=  → [A, …]                  │
             GET A:18081/export?digest=  ───────────────────────────►A's worker:
                                                                      │ token ok,
             socket fd ─► ctr images import --no-unpack -  ◄══ TCP ══│ ctr images
                          (node's ctr, chrooted)                     │ export -
             exit 0 → POST controller/announce                       │ (chrooted)
                                                                      │
 ◄────────────────────────────────────────────── 404 ────────────────┘
 3. GET manifests/<digest> ─────────► origin  (small; blobs already local)
 4. every layer already in content store → no fetch → unpack → done
```

- containerd resolves the **tag** against origin itself — the mirror is
  registered `capabilities = ["pull"]` only, no `resolve` — so a moved tag
  can never make a node import a stale image.
- Only the **manifest-by-digest** request is intercepted and held. The
  worker asks the controller who holds that digest (an in-memory lookup,
  no fan-out), orders one to export it, and pipes the TCP socket directly
  into `ctr images import --no-unpack` — image bytes go kernel-socket to
  kernel-socket, the worker process never reads them.
- The import step is `--no-unpack` on purpose: containerd's own pull holds
  an unpack lock on that same digest before fetching it, so an import that
  unpacked would deadlock against the pull it's serving. Unpacking still
  happens once, inside containerd's normal pull, right after.
- Every hold ends in a 404 — success, failure, or nobody has it — so
  containerd always falls through to origin on its own. A dead or wedged
  worker costs a `dial_timeout = "200ms"` connection-refused, not a failed
  pull. A transfer that outruns `MIRROR_HOLD_TIMEOUT_S` is abandoned the
  same way, keeping whatever blobs already landed.
- Multiple pods on one node needing the same digest share one transfer.
  Sources multiply as nodes finish importing and announce themselves; a
  busy source (`MIRROR_MAX_EXPORTS` slots full) answers 503 and the
  requester tries the next candidate.
- The **whole image** crosses the LAN even if the node already has some of
  its layers — no per-layer lookups, no layer serving. That's the
  trade-off for the mechanism being this simple.

Node binaries, not image ones: the worker image contains no `crictl`/`ctr`
of its own. It runs the node's own copies, chrooted into the node's real
root (`HOST_ROOT`, `/proc/1/root` by default) — so whatever apt or
Kubespray installed is exactly what runs, statically or dynamically
linked, always the current version, with no files copied onto or mounted
over the node.

## 3. Garbage collection

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

## 4. Pod-watch

**What it does:** every `POD_WATCH_INTERVAL_S`, each worker checks its own
node's pods for any container stuck in `ImagePullBackOff`/`ErrImagePull`,
and tries to fix it by importing the image from a peer under its exact
tag — no containerd pull involved at all.

**Why this is a different problem from the mirror above:** containerd
resolves a **tag** against origin *before* it ever asks the local mirror
anything (see [Peer-to-peer mirror](#2-peer-to-peer-mirror)). If that
resolution itself fails — DNS, network, auth, or just that specific
registry being unreachable from that specific node — the pull backs off
right there. It never reaches this worker, so the mirror never gets a
turn, no matter how many peers already have the image. `crictl`/GC can't
see this either: kubelet holds a pod in `Init:ImagePullBackOff` before
ever calling `CreateContainer`, so it's invisible to every other
mechanism Angry Duck has.

**How:**
- Needs the RBAC in `deploy/*/rbac.yaml` (`get`/`list` on `pods`,
  cluster-wide — a stuck pod can be in any namespace). It only ever acts
  on pods on its own node, enforced by a `fieldSelector` in the request,
  not by RBAC (Kubernetes has no per-node RBAC scope for pods).
- For each stuck container's image, it asks the controller
  `GET /resolve?tag=...` — "what digest has anyone in the fleet most
  recently observed this tag to mean?" Every worker reports its own local
  tag→digest mappings alongside the digest set it already reports; the
  controller keeps whichever digest was most recently *newly* observed
  for that tag (not just most recently re-reported — see below).
- If a peer has that digest, the worker imports it (fully unpacked — no
  containerd pull is holding an unpack lock to finish it for us the way
  the mirror path relies on) and runs `ctr images tag` to alias it under
  the *exact* reference the stuck pod is waiting on. kubelet's own
  `ImageStatus` check then finds it present on its next retry and never
  calls origin — no need to touch the pod at all.
- **This is a trust decision, not a confirmation** — except when the pod
  is already pinned to an exact digest (kubelet reports a bare
  `sha256:...` when a pod's spec used one directly): there, the digest
  itself already IS the identity being waited on, so it's answered
  exactly like the mirror's own "does a fresh peer have it" question, no
  trust involved. Every other path in Angry Duck ultimately defers to
  origin for what a *tag* means; the tag-resolving path can't, by
  construction — origin is exactly what's unreachable. There is
  deliberately no allowlist restricting which tags this applies to.
  Instead, every fallback attempt logs the digest's age at `WARN`
  regardless of outcome, and two metrics exist specifically so this
  doesn't go unnoticed: `angryduck_worker_pod_image_pull_failures_total{node}`
  (every stuck-pull observation, independent of whether the fix works —
  "is this node having pull problems at all") and
  `angryduck_worker_tag_fallback_total{node,result}` (the rescue
  attempt's own outcome). Watch both.
- **`POD_WATCH_EXCLUDE_NAMESPACE_SUBSTRINGS` / `POD_WATCH_EXCLUDE_IMAGE_SUBSTRINGS`**
  exist because a real fleet accumulates long-abandoned feature-branch
  deployments and deliberately-broken test/demo resources (chaos-testing
  namespaces, policy-testing images) that pod-watch would otherwise
  re-log at `WARN` on every poll, on every node, forever — the first
  rollout here found exactly that. A match is ignored entirely: no log,
  no metric, no fix attempt. Beyond that, a stuck container's own first
  sighting still logs at `WARN`; a repeat sighting of the *same* one
  within `POD_WATCH_RETRY_BACKOFF_S` drops to `DEBUG` (the failure
  counter still increments every time either way — only the repeated log
  line is throttled).
- The controller's tag index only advances a digest's "observed" age when
  the digest for that tag actually *changes* — a node re-reporting the
  same cached mapping on every heartbeat does not make an old fact look
  freshly confirmed. This matters because it's the only signal telling
  you how much to trust a fallback when you can't ask origin.
- Fails soft: with `MIRROR_ENABLED=false` there's no import mechanism
  available, so pod-watch still runs and still counts/logs stuck pulls,
  it just can't repair anything. A retry backoff
  (`POD_WATCH_RETRY_BACKOFF_S`) keeps a genuinely unfixable image from
  being retried every poll interval.

## Architecture

```
CI/CD pipeline                  Angry Duck controller                Angry Duck worker (DaemonSet, 1/node)
─────────────────               ───────────────────────              ──────────────────────────────────────
docker push                     - tracks worker freshness            - every REPORT_INTERVAL_S: scrape
     │                          - rejects if 0 fresh workers           node-exporter, list local images once
     └─POST /webhook/preheat──► - ranks by repo locality, then         (shared with GC and the mirror), POST
        {image}                   utilization (ascending)              /report {utilization, repos, digests}
                                - orders top RANK_TOP_N workers      - on POST /pull: pull async; report again
                                  to pull (POST worker /pull)          the moment it lands
                                - GET /peers?digest=  ──────────►    - every GC_CHECK_INTERVAL_S: reclaim
                                  who has it (from memory,             images unused for GC_MISS_THRESHOLD
                                  shuffled)                            checks
                                - POST /announce: a node just        - /v2/ (mirror, loopback only) and
                                  became a source                      /export (peers, token)
```

Only `POST /webhook/preheat` is meant to be reachable from outside the
cluster (`deploy/stg/ingress.yaml` allowlists it alone). `/report`,
`/peers`, `/announce`, `/status` and every worker endpoint stay on the pod
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

**To also run the peer-to-peer mirror** (`MIRROR_ENABLED=true`):
- `ctr` present on the node (crictl has no export/import verbs) — every
  containerd node already has it.
- containerd's CRI registry `config_path` set to `/etc/containerd/certs.d`
  (needs one containerd restart if it isn't already — Spegel requires the
  same setting, so clusters running Spegel already have it). The worker
  warns at startup if it isn't set; without it, every `hosts.toml` it
  writes is dead text.
- **`discard_unpacked_layers` must be `false`** in containerd's config.
  When `true`, containerd deletes a layer's compressed blob from the
  content store right after unpacking it — which is exactly what the
  mirror needs to still be there to serve to a peer later. With it left
  `true`, exports silently start failing "not found" for images that are
  otherwise running fine. This is a node-level containerd setting, not
  per-tool, so fixing it for Spegel fixes it for Angry Duck too.
- The `angryduck-mirror` Secret (`MIRROR_PEER_TOKEN`) created **before**
  the DaemonSet — workers exit and CrashLoop without it, and pick it up
  cleanly on their next restart once it exists.
- No other tool (Spegel, an Ansible role) already owning a
  `certs.d/<registry>/hosts.toml` for a registry you want Angry Duck to
  cover — containerd uses a registry's own directory *instead of*
  `_default`, so such a file silently shadows the mirror for that
  registry. The worker warns at startup for each one it finds; it never
  touches a file it didn't write.

**To also run pod-watch** (`POD_WATCH_ENABLED=true`, the default):
- The RBAC in `deploy/*/rbac.yaml` applied — a `ServiceAccount` bound to
  a `ClusterRole` granting `get`/`list` on `pods`, and the DaemonSet's
  `serviceAccountName` set to it. Without this, pod-watch logs why it
  can't start and the worker otherwise runs normally.
- Meaningfully useful only alongside the mirror (`MIRROR_ENABLED=true`):
  without it, pod-watch still polls and logs/counts stuck pulls, but has
  no import mechanism to actually fix anything.

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
| `MIRROR_ENABLED` | false | put the worker in front of origin for containerd |
| `MIRROR_REGISTRIES` | `*` | `*` = every registry (via `certs.d/_default`), or an explicit host list |
| `MIRROR_PEER_TOKEN` | (empty) | shared secret for `/export`; required, from the `angryduck-mirror` Secret |
| `MIRROR_HOLD_TIMEOUT_S` | 60 | longest a containerd pull waits on a peer transfer |
| `MIRROR_MAX_EXPORTS` / `MIRROR_MAX_IMPORTS` | 2 / 2 | concurrent ctr transfer processes per node |
| `POD_WATCH_ENABLED` | true | rescue pods stuck in ImagePullBackOff by importing their image from a peer under its exact tag |
| `POD_WATCH_INTERVAL_S` | 30 | how often to poll this node's pods for stuck pulls |
| `POD_WATCH_RETRY_BACKOFF_S` | 300 | how long to wait before retrying a fix that just failed (also throttles repeat WARN logs for the same stuck container) |
| `POD_WATCH_EXCLUDE_NAMESPACE_SUBSTRINGS` / `POD_WATCH_EXCLUDE_IMAGE_SUBSTRINGS` | (empty) | ignore stuck pods matching these entirely — no log, no metric, no fix attempt |

Real environment variables (a k8s ConfigMap, in practice — see
`deploy/stg/configmap.yaml`) always win over `.env` file values.

## Deploying

```bash
# 1. Build and push both images
sudo docker build --no-cache -t registry.internal-registry.example.com/devops/generic/angry-duck-controller:1.3.4 -f Dockerfile.controller .
sudo docker build --no-cache -t registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.3.4 -f Dockerfile.worker .
sudo docker push registry.internal-registry.example.com/devops/generic/angry-duck-controller:1.3.4
sudo docker push registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.3.4
# (bump the tag in deploy/stg/controller.yaml and worker-daemonset.yaml too)

# 2. The peer-transfer token — BEFORE the DaemonSet (once per cluster).
#    If workers start without it they exit with a clear error and
#    CrashLoop; creating the Secret fixes them on their next restart.
sudo kubectl -n angryduck create secret generic angryduck-mirror \
  --from-literal=token="$(openssl rand -hex 32)"

# 3. Apply the manifests
sudo kubectl apply -f deploy/stg/namespace.yaml
sudo kubectl apply -f deploy/stg/configmap.yaml
sudo kubectl apply -f deploy/stg/controller.yaml
sudo kubectl apply -f deploy/stg/worker-daemonset.yaml

# 4. Expose just the webhook to the pipeline (edit host/IP allowlist first)
sudo kubectl apply -f deploy/stg/ingress.yaml
```

To turn the mirror off on a running cluster: set `MIRROR_ENABLED=false`
and roll the DaemonSet. Each worker deletes its `hosts.toml` as it stops,
and containerd goes straight to origin from the next pull on.

## API summary

**Controller**
- `POST /webhook/preheat` — `{"image": "..."}`. `503` if zero fresh
  workers. Short references get normalized (`nginx` → `docker.io/library/nginx:latest`) before anything else happens.
- `POST /report` — workers call this themselves.
- `GET /peers?digest=sha256:…&node=<asker>` — up to `MIRROR_PEER_CANDIDATES`
  fresh workers holding that digest, shuffled.
- `GET /resolve?tag=<name:tag>&node=<asker>` — the fleet's most recently
  observed digest for that tag, and how long it's been the answer. `404`
  if nobody's ever reported it. Used only by pod-watch's fallback path,
  when origin itself can't resolve a tag.
- `POST /announce` — `{"node_id","digest"}`, a worker just imported it.
- `GET /status` — every known worker plus the active target.
- `GET /healthz` — liveness/readiness.

**Worker**
- `POST /pull` — `{"image": "...", "ordered_at": "..."}`, called by the
  controller. Pulls asynchronously, returns `202` immediately.
- `GET /v2/…` — containerd's registry mirror endpoint. Loopback only.
  Always 404 (after a peer import, or at once).
- `GET /export?digest=sha256:…` — `X-Angryduck-Token` required. `200` then
  the raw image tar straight from `ctr images export`; `503` busy; `404`
  not here.
- `GET /healthz` — liveness/readiness.

## Resource usage

Measured against a real containerd (2.2.1, the apt build on Ubuntu 24.04)
moving a 150 MiB, three-layer image between two nodes, with the worker
running from a distroless-style root:

| Process | Peak RSS | CPU | When |
|---|---|---|---|
| angryduck-worker itself | ~6 MiB | 0.01–0.02 s for the whole transfer | always |
| `crictl images -o json` (existing) | ~18–20 MiB | ~0 | once per `REPORT_INTERVAL_S` |
| `crictl pull` (existing, preheat) | ~25 MiB | ~0 | per preheat order |
| `ctr images export` (new, source node) | ~30 MiB | 0.72 s / 150 MiB | per peer served |
| `ctr images import --no-unpack` (new, receiving node) | ~34 MiB | 0.76 s / 150 MiB | per peer import |

A transfer costs ~30–35 MiB and ~5 ms of CPU per MiB on each end, only
while it runs — which is why the DaemonSet limits are memory 192Mi (worst
case ≈ 6 + 2×31 + 2×34 + 20 + 25 ≈ 181 MiB with the default 2 exports + 2
imports) and CPU 1 core (at 200m a 150 MiB image needs ≥3.8 s of throttled
CPU per side, ~40 MiB/s — slower than the LAN). Setting
`MIRROR_MAX_EXPORTS=1` and `MIRROR_MAX_IMPORTS=1` brings the memory worst
case to ~115 MiB at the cost of slower fan-out. Decompression and
unpacking don't move — they happen inside containerd, exactly as for any
pull. The controller holds one digest set per node (a few KB each) and
answers `/peers` from memory.

containerd 1.7's `ctr` runs import client-side rather than through the
transfer service, so expect the same order of magnitude but not identical
numbers — `container_memory_working_set_bytes` for the worker pods during
a stg rollout is the real check. In that 150 MiB test the peer-assisted
pull took 4.7 s end to end, and origin saw exactly one `HEAD` (tag) and
one manifest `GET` — zero layer downloads.

## Metrics

Worker (`:18081/metrics`): `angryduck_worker_mirror_transfers_total{result}`
(`hit` = imported from a peer, `miss` = nobody had it, `failed`, `busy`,
`timeout`), `angryduck_worker_mirror_requests_total{kind,result}`,
`angryduck_worker_mirror_exports_total{result}`, and
`angryduck_worker_mirror_bytes_total{direction}` — `in` is origin traffic
avoided, read from the socket's `TCP_INFO` since the worker never sees the
bytes. Pod-watch: `angryduck_worker_pod_image_pull_failures_total{node}`
(every stuck-pull observation on this node, regardless of whether a fix
was attempted or succeeded) and `angryduck_worker_tag_fallback_total{node,result}`
(the rescue attempt's own outcome — `hit`, `hit_local`, `no_digest`,
`no_peer`, `import_failed`, `tag_failed`, `resolve_error`,
`peer_lookup_failed`). Controller: `angryduck_controller_peer_lookups_total{result}`
and `angryduck_controller_resolve_lookups_total{result}`.

## Logging

Every line is tagged `[DEBUG]`/`[INFO]`/`[WARN]`/`[ERROR]`, filtered by
`LOG_LEVEL`. At `info` (the default) you'll see, per peer-transfer
attempt: the controller offering (or failing to find) peers for a digest,
the worker's own transfer outcome (`hit`/`miss`/`failed`/`busy`/`timeout`)
with timing, and every actual import/export as it happens — plus GC's
tick summaries and what it removed and why. Switch to `debug` for the
full play-by-play of one transfer: the manifest request being held, which
candidate peers were tried and in what order, busy-peer retries, and
local/joined/cancelled outcomes that `info` doesn't surface individually.

## Testing

```bash
go test ./...
```

`internal/worker`'s tests include one (`TestHostExec_RealChrootExecution`)
that performs an actual `chroot(2)` against a fake host root — it needs
`CAP_SYS_CHROOT` and is skipped, not failed, when not running as root.
