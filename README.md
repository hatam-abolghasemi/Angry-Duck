# Angry Duck

Angry Duck kills the "40 replicas, 40 origin pulls" problem. It pre-pulls
a freshly-pushed image onto a couple of low-utilization nodes the moment
CI pushes it, and then — sitting in front of the origin registry as each
node's containerd mirror — hands that image to every other node that asks
for it, whole, peer-to-peer, instead of every one of them hitting the
origin registry at once.

## The problem

A normal rollout looks like this: ArgoCD syncs, and every node that just
got scheduled a new pod pulls the new image — independently, at the same
moment, straight from the origin registry. Spegel is sitting right there
ready to serve peers, but on a fresh deploy nobody has the image yet, so
there's no peer to serve from. 40 replicas means 40 simultaneous origin
pulls, right when the registry is least equipped to enjoy that.

Spegel's answer is to serve individual layers out of every node's content
store. Angry Duck's is coarser and simpler: when containerd on a node
needs an image, the node's worker asks the controller who already has that
exact digest, orders that node to export it, imports it locally, and lets
containerd finish the pull against a content store that already has every
layer. Whole images, identified by name and digest; no per-layer lookups,
no layer serving.

## What Angry Duck does about it

- **Gets ahead of the deploy, not in its way.** One `curl` call right
  after `docker push` is all a pipeline needs to add. ArgoCD's sync is
  completely untouched — Angry Duck never gates or blocks it, it just
  wins the race to have the image ready first.
- **Picks targets based on real signal, not guesses.** Nodes report disk
  utilization *and* their local image inventory every few seconds. Given
  those, the controller prefers a node that already has some older tag of
  the same repo (usually a small delta to pull) over an emptier node
  starting cold, and falls back to the least-utilized fresh nodes when no
  node has the repo at all. It flatly refuses to preheat anything if it
  has no recent view of any node's state rather than picking blind.
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
- **Pulls from peers, whole.** With the mirror on, containerd asks the
  local worker before origin. If any fresh node has the exact digest, the
  worker orders it to `ctr images export` and pipes the stream into this
  node's `ctr images import`; containerd then gets the tiny manifest from
  origin and finds every layer already local. Nobody has it? The worker
  says so instantly and it's an ordinary origin pull. See
  [The peer mirror](#the-peer-mirror).
- **Stays out of the data path, even for its own transfers.** The source
  node's `ctr export` writes straight into the TCP socket, and on the
  receiving node that same socket *is* `ctr import`'s stdin. Image bytes
  never pass through Angry Duck's process — it wires two file descriptors
  together and waits for exit codes.
- **Carries no container tooling of its own.** crictl and ctr are the
  node's binaries — whatever apt or Kubespray installed — run chrooted into
  the node's root. The worker image is a single static binary. See
  [Node binaries](#node-binaries-not-image-binaries).
- **Costs almost nothing to run.** Pure Go, stdlib only — no dependencies
  to audit, no `go mod download`, small static binaries, small images.
  The worker process itself sits at ~6 MiB; see
  [Resource usage](#resource-usage) for what a peer transfer costs.

## Why this combo — preheat + peer mirror + your pipeline — actually adds up

Each piece is solving a different stage of the same lifecycle, and
together they cover it end to end:

- **At push time**, Angry Duck turns "N nodes about to ask the registry
  for the same layers" into a single, controlled origin pull on 1–3
  nodes — the registry sees one request instead of a stampede, no matter
  how large the fleet is.
- **At rollout time**, the peer mirror turns that one pull into
  cluster-wide availability: every other node ArgoCD schedules onto pulls
  the image from a peer over the LAN instead of the origin, and every node
  that finishes becomes a source for the next — so rollouts finish faster
  and registry egress stays flat regardless of replica count. The same
  path covers what preheat can't see coming: a 3am HPA scale-out, a node
  that joins later, a rescheduled pod.
- **At steady state**, Angry Duck's node-wide GC keeps disk usage from
  quietly climbing forever as your pipeline pushes new tags every day —
  the same system that got the image onto the node in the first place is
  the one making sure old ones don't just pile up.

None of that requires touching ArgoCD or your pipeline's deploy logic —
Angry Duck adds one webhook call, and one `hosts.toml` per registry that
it writes itself and removes when it stops. Origin bandwidth, rollout latency, and
node disk pressure end up being managed as one connected system instead
of three separate problems nobody owns.

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
                                  became a source                      /export (peers, token) — below
```

### The peer mirror

```
 node B (needs image)                                              node A (has it)
 ─────────────────────                                             ────────────────
 kubelet → containerd
   1. HEAD manifests/<tag> ─────────────────► origin   (tag → digest, ~300 bytes)
   2. GET manifests/<digest> → 127.0.0.1:18081 (B's worker)   ── held ──┐
                                                                         │
 B's worker:  GET controller/peers?digest=  → [A, …]                     │
              GET A:18081/export?digest=  ─────────────────────────────► A's worker: token ok,
                                                                         │  export slot free →
              socket fd ──► ctr images import --no-unpack -  ◄═══ TCP ═══ ctr images export - <ref>
                            (node's ctr, chrooted)                       │   (node's ctr, chrooted)
              exit 0 → POST controller/announce                          │
                                                                         │
   ◄──────────────────────────────────────────────── 404 ───────────────┘
   3. GET manifests/<digest> ───────────────► origin   (small; blobs already local)
   4. every layer: already in content store → no fetch → unpack → done
```

A few properties fall out of this shape:

- **The mirror only ever sees digests.** `hosts.toml` gives it
  `capabilities = ["pull"]` — no `resolve` — so containerd resolves tags
  against origin itself. A moved tag can never make a node import a stale
  image, and origin's answer is always the one that wins.
- **Every answer is a 404.** After a successful import, after a failed one,
  and instantly when nobody has the image. containerd falls through to
  origin on any mirror error, so a dead or wedged worker costs a
  connection-refused, not a failed pull (`dial_timeout = "200ms"`).
- **One transfer per image per node.** Ten pods on one node needing the
  same image hold ten requests on one transfer.
- **Sources multiply.** A node that finishes importing announces itself,
  so the next requester can pick it. Sources that are busy (every
  `MIRROR_MAX_EXPORTS` slot taken for `MIRROR_QUEUE_WAIT_S`) answer 503 and
  the requester tries the next candidate, then re-asks the controller a
  second later.
- **`--no-unpack` is load-bearing.** containerd's pull takes an unpack lock
  on the manifest digest *before* fetching it — the fetch we are holding.
  An import that unpacked would wait for that lock: pull waits on us, we
  wait on the import, the import waits on the pull. (Found against a real
  containerd; the import sat in `Unpacker.lockBlobDescriptor` with every
  byte received.) Landing blobs only, and letting the pull unpack them
  once, avoids the lock and is the same unpack work any pull does anyway.
- **Bounded, never duplicated.** At `MIRROR_HOLD_TIMEOUT_S` the transfer is
  killed and containerd goes to origin, keeping whatever blobs already
  landed — peer and origin never download the same bytes.
- **The whole image crosses the LAN**, even if the receiving node already
  has some of its layers. That's the trade for having no layer lookups at
  all; LAN bytes are cheap next to origin bytes.

### Node binaries, not image binaries

The worker image contains the worker and nothing else. crictl, ctr (and
docker, if you use that backend) are the node's own, run with each child
process chrooted into the node's root — `HOST_ROOT=/proc/1/root` by
default, which with `hostPID: true` is the node's real root with every
live mount under it.

- **Whatever the node has is what runs.** Kubespray's `/usr/local/bin/ctr`
  or apt's `/usr/bin/ctr`, static or dynamically linked — the child uses
  the node's loader and libc. (apt's ctr *is* dynamic; a copy in a
  distroless image could never run it.)
- **apt works as is.** Nothing is copied onto, mounted over, or pinned from
  the node. A single-file bind mount would keep running the old inode after
  an upgrade; here the next exec simply runs the new file.
- **The node's config applies.** The child reads the node's
  `/etc/crictl.yaml` and sees the node's socket path; it gets a minimal
  environment so the pod's env vars can't override the node's config.
- **Missing binary = loud failure.** The worker exits at startup if the
  node lacks the CLI `CONTAINER_RUNTIME` needs. ctr missing only disables
  the mirror, with a log line saying so.

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
| `RANK_TOP_N` | 2 | how many nodes get ordered per rank |
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
| `MIRROR_REGISTRIES` | (empty) | registries to mirror, e.g. `registry.internal-registry.example.com` |
| `MIRROR_PEER_TOKEN` | (empty) | shared secret for `/export`; required, from the `angryduck-mirror` Secret |
| `MIRROR_HOLD_TIMEOUT_S` | 60 | longest a containerd pull waits on a peer transfer |
| `MIRROR_MAX_EXPORTS` / `MIRROR_MAX_IMPORTS` | 2 / 2 | concurrent ctr transfer processes per node |

Real environment variables (a k8s ConfigMap, in practice — see
`deploy/stg/configmap.yaml`) always win over `.env` file values.

**On `CONTAINER_RUNTIME`:** stick with `crictl`. The mirror uses `ctr`
for export/import either way (crictl has no such verbs), so a node needs
both — which every containerd node already has. It lists every running
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
sudo docker build --no-cache -t registry.internal-registry.example.com/devops/generic/angry-duck-controller:1.3.0 -f Dockerfile.controller .
sudo docker build --no-cache -t registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.3.0 -f Dockerfile.worker .
sudo docker push registry.internal-registry.example.com/devops/generic/angry-duck-controller:1.3.0
sudo docker push registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.3.0
# (bump the tag in deploy/stg/controller.yaml and worker-daemonset.yaml too)

# 2. The peer-transfer token (once per cluster; any 16+ char secret)
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

Then point the pipeline at it, right after `docker push`:

```bash
curl -X POST https://angryduck-webhook.internal-registry.example.com/webhook/preheat \
  -H 'Content-Type: application/json' \
  -d "{\"image\":\"$IMAGE_REF\"}"
```

`/report`, `/peers`, `/announce`, `/status`, and `/healthz` are deliberately **not** in the
Ingress's path rules — only `/webhook/preheat` is reachable from outside
the cluster, and only from the allowlisted IP range. Worker-to-controller
traffic never leaves the pod network.

A few things worth knowing about the worker DaemonSet: it needs
`hostNetwork: true`, `hostPID: true` and a privileged security context —
that is what lets it run the node's binaries through `/proc/1/root`, so no
socket or binary volumes are mounted; `NODE_EXPORTER_URL` assumes
node-exporter also runs `hostNetwork` on each node; and it should run on
every node, master/control-plane included, since GC needs to manage disk
everywhere even where preheat targets are never chosen.

For the mirror, containerd's CRI registry `config_path` must point at
`/etc/containerd/certs.d` (it already does wherever Spegel ran; the worker
warns at startup if not). The worker writes
`certs.d/registry.internal-registry.example.com/hosts.toml` on start and deletes it on
stop; it never touches a `hosts.toml` it didn't write. **While Spegel is
still installed**, that per-registry file takes precedence over Spegel's
`_default` for `registry.internal-registry.example.com`, so those pulls use Angry Duck's
path (or origin) and not Spegel's — a clean comparison. Spegel clears
`certs.d` when it restarts; the worker re-writes its file within 30s.

To turn the mirror off on a cluster: `MIRROR_ENABLED: "false"` and roll the
DaemonSet. Each worker deletes its `hosts.toml` as it stops, and
containerd goes straight to origin from the next pull on.

## API summary

**Controller**
- `POST /webhook/preheat` — `{"image": "..."}`. `503` if zero fresh
  workers. Short references get normalized (`nginx` → `docker.io/library/nginx:latest`) before anything else happens.
- `POST /report` — workers call this themselves.
- `GET /peers?digest=sha256:…&node=<asker>` — up to `MIRROR_PEER_CANDIDATES`
  fresh workers holding that digest, shuffled.
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

What that means:

- **Idle cost is lower than before.** The mirror adds no timers and no
  runtime calls of its own — it reads the image listing the reporter
  already takes, and GC now reuses that same listing instead of running
  its own (`crictl images` 5→4 times a minute).
- **A transfer costs ~30–35 MiB and ~5 ms of CPU per MiB on each end, only
  while it runs.** Those `ctr` children live in the worker pod's cgroup,
  which is why the DaemonSet limits moved: memory 64Mi→192Mi (worst case
  ≈ 6 + 2×31 + 2×34 + 20 + 25 ≈ 181 MiB with the default 2 exports + 2
  imports) and CPU 200m→1 (at 200m a 150 MiB image needs ≥3.8 s of
  throttled CPU per side, ~40 MiB/s — slower than the LAN). Requests are
  unchanged; limits reserve nothing. Setting `MIRROR_MAX_EXPORTS=1` and
  `MIRROR_MAX_IMPORTS=1` brings the memory worst case to ~115 MiB (128Mi
  limit) at the cost of slower fan-out.
- **Decompression and unpacking don't move.** They happen inside
  containerd, exactly as for any pull.
- **The controller** holds one digest set per node (a few KB each) and
  answers `/peers` from memory.

containerd 1.7's `ctr` runs import client-side rather than through the
transfer service, so expect the same order of magnitude but not identical
numbers — `container_memory_working_set_bytes` for the worker pods during a
stg rollout is the real check.

In that test the peer-assisted pull took 4.7 s end to end, and origin saw
exactly one `HEAD` (tag) and one manifest `GET` — zero layer downloads.

## Metrics

Worker (`:18081/metrics`): `angryduck_worker_mirror_transfers_total{result}`
(`hit` = imported from a peer, `miss` = nobody had it, `failed`, `busy`,
`timeout`), `angryduck_worker_mirror_requests_total{kind,result}`,
`angryduck_worker_mirror_exports_total{result}`, and
`angryduck_worker_mirror_bytes_total{direction}` — `in` is origin traffic
avoided, read from the socket's `TCP_INFO` since the worker never sees the
bytes. Controller: `angryduck_controller_peer_lookups_total{result}`.

## Logging

Every line is tagged `[DEBUG]`/`[INFO]`/`[WARN]`/`[ERROR]`, filtered by
`LOG_LEVEL`. `info` (the default) already shows GC's reasoning — tick
summaries, miss-count progress, what got removed and why — without the
per-image "yes, this is fine" noise that `debug` adds on top.

## Testing

```bash
go test ./...
```
