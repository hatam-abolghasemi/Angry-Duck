# Architecture

This page is the map: what runs where, what it knows, and how an image moves
from a CI push to cleanup. For why it's built this way, see
[Design](design.md).

Angry Duck has two components. Both are static Go binaries with no external
Go dependencies.

| Component | Runs as | Responsibilities |
|---|---|---|
| **Controller** | Deployment, 1 replica | Receives the preheat webhook, tracks every node's disk usage, images and layers, chooses seed nodes, schedules node-to-node transfers, watches for pods stuck on pulls, and tells mirrors which peer holds a blob. |
| **Worker** | DaemonSet, one per node | Reports its node, pulls when ordered, sends and receives images, runs the node's mirror, and cleans up the node's images. |

## Reporting and state

Each worker posts a report to the controller every `REPORT_INTERVAL_S` (15s),
and right away when containerd reports a pull or removal. A report has:

- root filesystem utilization, read from the local node-exporter;
- every image reference on the node;
- its **layer inventory**: the blob digests and snapshot chainIDs it holds,
  sent as numbered deltas so reports stay small on large nodes.

The controller keeps all state in memory and treats a worker that has been
silent for `WORKER_STALE_AFTER_S` as gone. It has no database or persistent
volume: after a restart it rebuilds its view from the next round of reports,
and each worker resends its full layer inventory once the controller no
longer holds the version its deltas build on.

Workers keep two small files on the node under `RESCUE_STATE_DIR`: snapshots
pinned for a retry, and image usage times for cleanup.

## An image's life

```mermaid
sequenceDiagram
    participant CI
    participant C as Controller
    participant S as Seed workers
    participant W as Other workers
    participant R as Registry

    CI->>R: push app:1.2.3
    CI->>C: POST /webhook/preheat
    C->>R: read manifest and config (a few KB)
    C->>S: POST /spread/fetch: one missing blob each
    S->>R: pull that blob
    S->>C: POST /spread/done
    loop every blob, as soon as any node has it
        C->>W: POST /spread/fetch from a peer
        W->>S: GET /spread/content/<digest>
        W->>C: POST /spread/done
    end
    C->>W: POST /spread/finalize (metadata)
    Note over W: image registered; kubelet finds it locally
```

1. **[Preheat and spread](propagation.md).** Missing blobs are pulled from
   the registry about once each, and every blob spreads from node to node
   the moment any node has it, until every eligible node has registered the
   image. Pushes whose manifest can't be read are [seeded](preheat.md) and
   [propagated](propagation.md#fallbacks) whole instead.
2. **[Mirror](mirror.md).** Any pull containerd still makes, for example on a
   node that joined later or for a third-party image, is served from the node
   or a peer when possible.
3. **[Rescue](rescue.md).** If a pod still gets stuck pulling the image, it is
   copied to that node from a node that has it.
4. **[Image cleanup](image-cleanup.md).** Once nothing has used it on a node
   for 6 hours, or sooner when the disk runs high, the node removes it.

Whole-image propagation and rescue share one
[transfer mechanism](transfers.md); spread moves single blobs by digest.

## Talking to containerd

The worker image contains only the worker binary. Everything it does with
images goes through containerd's socket: its own gRPC API for content,
images, snapshots, leases and diffs, and CRI for seed pulls and container
listings, the calls `crictl` makes. Spread's registry pulls stream single
blobs by digest straight into the content store, with the same pull
secret; a whole-image seed pull is therefore the exact pull
kubelet would make. Work that needs privileges on the node (unpacking
layers, mounting snapshots, writing files with their owners and whiteouts)
happens inside containerd, so the worker has no capabilities and no host
PID namespace; see [Security](security.md).
Listings run one at a time, and each result is shared between callers
for up to 15 seconds. Blob content is read directly from containerd's
content store when it is mounted, and through the content API otherwise.

## Scope

- **It doesn't resolve tags.** The mirror serves content by digest only.
  Registry outages that block tag resolution are covered by rescue.
- **It doesn't replace kubelet's image GC.** kubelet still acts at disk
  pressure as the last line of defense. Angry Duck's cleanup runs well before
  it.
- **It doesn't restart pods.** After a rescue, kubelet's next retry finds the
  image.
