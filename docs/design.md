# Design

Angry Duck manages an image's life on a node, from the push to the cleanup.
It grew out of four problems, in the order we met them
([the story](story.md)):

1. **Deploys waited.** Nodes started downloading only after pods were
   scheduled, all at once.
2. **The registry went down,** and rollouts and rollbacks stopped with it.
3. **Images were stuck next door.** The registry couldn't serve an image that
   a neighbor node already had.
4. **Disks filled.** Usually with logs, sometimes with images, always on the
   same disk.

The goal behind all four is the same: **deploys and rollbacks that are fast,
safe and efficient.** This page explains why each piece looks the way it does.

## Start at the push, not at the rollout

Normally a node starts downloading an image when a pod is scheduled on it.
That is the latest possible moment: the rollout has begun, and every node asks
the registry for the same layers at once.

The earliest moment anyone knows an image will be needed is when CI pushes
it. So that is when Angry Duck starts, and CI tells it with a webhook: one
`curl` line every CI system can run, right after `docker push`.

Why not watch something instead?

- **Registries don't agree on events.** GitLab, Harbor, Nexus and Docker Hub
  each notify differently, if at all. A webhook from CI works with all of them.
- **Watching Deployments is already too late.** By the time a new image shows
  up in a manifest, the rollout is starting.
- **Polling the registry adds delay and load** to the very thing we're trying
  to protect.

## CI never waits

The webhook answers `202` once the controller has read the image's manifest
(a few KB, never the layers, capped by `RANK_RESOLVE_TIMEOUT_S`) and handed
out the first orders. Everything after that happens in the background. CI
calls it with `|| true`, so a failed preheat can never fail a pipeline.

```text
CI:       build ─ push ─ webhook ─ update manifests ─ sync ─ rollout ✓
                           │                                   ▲
cluster:                   └─ spreading layers to every node ──┘ already local
```

The work CI does after the push (updating manifests, a GitOps sync, approvals)
is time the cluster spends distributing the image. The more of the pipeline
runs in parallel, the less of the rollout waits.

## Parallel by default

Inside the cluster, the same rule: nothing waits for something that could
already be moving.

- **Layers, not images.** Each layer spreads on its own. The moment one node
  has a layer, it becomes a source for every other node. No node waits for
  another to finish a whole image.
- **Two lanes per node.** Each node pulls one layer from the registry and
  receives another from a peer at the same time.
- **Rarest first.** The layer with the fewest holders goes first, so new
  sources appear where they are scarcest.
- **Races instead of waits.** When one node is slow pulling a layer, an idle
  node may pull it too. When a peer delivers a layer a node is still pulling
  from the registry, the slower copy is cancelled. Whichever path is faster
  wins.
- **The registry only for what nobody has.** Seeds are the nodes that already
  hold most of the image, so the registry serves little more than the changed
  layers, about once each.

See [Propagation](propagation.md) for the details.

## React to events, don't poll

A loop that checks every 60 seconds adds up to 60 seconds of nothing to every
step. So Angry Duck reacts:

- Workers follow **containerd's events**. Any pull or removal, kubelet's own
  included, reaches the controller in about a second.
- The controller **watches Pending pods**, so a pod stuck in `ImagePullBackOff`
  is seen when kubelet reports it.
- **A finished transfer starts the next one** about a second later, not at the
  next tick.
- Pull orders return at once and run in the background. Transfers stream
  straight into containerd and are never buffered to disk.

Periodic scans still exist, but only as safety nets behind the events
(`LAYER_RESYNC_INTERVAL_S`, `RESCUE_RESYNC_INTERVAL_S`).

## Never in the critical path

A deploy must never depend on Angry Duck.

- If it is down or wrong, containerd pulls from the registry exactly as it
  would without it. The [mirror](mirror.md) answers `404` for anything it
  can't serve, and containerd goes to the registry.
- It doesn't rewrite pod specs, add admission webhooks or plug into
  containerd. Outside its own state directory, the only node files it writes
  are the mirror's `hosts.toml` files, and only when you turn the mirror on.
- The controller keeps everything in memory and rebuilds it from worker
  reports within seconds of a restart. There is nothing to back up.

## When the registry can't help

The registry is the source of truth, but it shouldn't be a single point of
failure for images the cluster already has.

- **Ask it once.** New layers come from the registry about once each; every
  other node gets them from peers. A slow registry slows a few pulls, not the
  whole fleet.
- **Serve containerd from peers.** With the [mirror](mirror.md) on,
  containerd's own pulls, kubelet's included, are answered by the node or a
  neighbor before the registry is asked.
- **Rescue what's stuck.** When a pull fails anyway (an outage, a broken
  route, an image deleted upstream), the controller sees the stuck pod and
  copies the image from a node that has it. kubelet's next retry finds it
  locally.

## Rollbacks are deploys too

A rollback is a deploy of an image the cluster has already seen, so it should
be the fastest deploy of all.

- [Cleanup](image-cleanup.md) keeps every image for hours after it was last
  used, and keeps the newest few of each app (`GC_ROLLBACK_KEEP`) longest,
  even when disks run high.
- If a node lost the image anyway and the registry can't serve it, rescue
  copies it from a node that still has it.

## Cleanup makes distribution safe

Putting every new image on every node is only safe if something keeps disks
healthy. On most nodes, images and container logs share one disk. We can't
make an app log less, but images should never be what tips a node over.
kubelet's image GC acts only under disk pressure, late, and without knowing
which images a rollback needs. So cleanup is part of Angry Duck, not
a separate tool, and the two halves share one view:

- Nodes above `PROPAGATE_MAX_UTILIZATION` receive nothing new until cleanup
  has made room.
- Cleanup removes images unused for 6 hours, and works down to
  `GC_LOW_UTILIZATION` when a disk crosses `GC_HIGH_UTILIZATION`, well before
  kubelet's own threshold.
- When removing images stops freeing space (the disk is full of something
  else), cleanup stops instead of deleting rollback images for nothing.

## Safe by construction

- **The registry stays the source of truth.** Tags always resolve at the
  registry, so a moved tag is never served stale. Only content addressed by
  digest moves between nodes.
- **Verify what can be verified.** Layers travel as blobs, and containerd
  checks every digest. Unpacked snapshots are shipped only as a last resort,
  with their own checksum.
- **Ask containerd, don't impersonate it.** The worker talks to containerd
  over its socket and lets containerd do everything that needs privileges. The
  worker itself has no Linux capabilities. See [Security](security.md).
- **Be gentle.** Transfers are bounded per node, registry pulls cluster-wide,
  retries back off, and cleanup removes images in small batches.

## Explain every decision

Every decision is logged on one line, shown in `/status` and counted in
Prometheus. When a pod starts slowly, you should be able to see why without
reading code. See [Observability](observability.md).

## Small on purpose

Two static Go binaries with no external Go dependencies, no database and no
storage of their own. The controller used about 13m CPU and 22 MiB for a
simulated fleet of 150 nodes; a worker holds about 5 to 7 MiB at idle. See
[Sizing](operations.md#sizing).

One tool rather than four is a choice too. [Comparison](comparison.md#why-one-tool)
explains why.
