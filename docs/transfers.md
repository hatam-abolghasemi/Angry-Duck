# Node-to-node transfers

[Propagation](propagation.md) and [rescue](rescue.md) move images with the
same mechanism. The controller tells the receiving worker which image to get
and from which peers. The receiver does the rest and fetches only what it
lacks.

```mermaid
sequenceDiagram
    participant T as Receiving worker
    participant S as Source worker

    T->>S: POST /blobs/plan {image, platform}
    S-->>T: every blob and layer of the image, and which ones S has
    T->>T: decide per layer: already here, blob, or snapshot
    opt layers no source has a blob for
        T->>S: POST /snapshots/export {image, chain_id}
        S-->>T: the layer as containerd's diff, gzip on the wire
        T->>T: containerd applies it, committed under the chainID
    end
    T->>S: POST /blobs/export {image, missing digests}
    S-->>T: partial OCI archive: index.json and missing blobs only
    T->>T: containerd import (verifies every digest)
```

## Per-layer decisions

The source walks the image the way containerd does on a pull: index, then the
manifest for the receiver's platform, then config and layers. Attestation
manifests and other platforms are skipped. For each layer, the receiver picks
the cheapest option:

1. **Already here.** The layer's unpacked snapshot, named by its chainID, is
   on the node. Nothing to do.
2. **Blob.** The compressed layer, from the node's own content store or from
   any source that has it. The receiver asks other sources for their plans
   before settling for a snapshot.
3. **Snapshot.** Only when no source has the blob.

## Blobs

Sources read committed blobs straight from containerd's content store
(`CONTAINERD_ROOT`), where they are immutable files named by digest, and
through containerd's content API when the store isn't mounted. Blobs from
several sources are merged into one partial OCI archive and streamed
straight into containerd's import, never buffered to disk.
containerd checks every blob against its digest and size, so a broken
transfer fails the import instead of landing a corrupt image.

## Snapshots

A node can run an image without any of its layer blobs: with
`discard_unpacked_layers = true`, containerd deletes them after unpacking, and
it never downloads a layer whose snapshot already exists. A blob can't be
rebuilt from a snapshot with the same digest, so the layer is rebuilt from
the snapshot instead.

- The source asks containerd's diff service for the snapshot as an OCI layer
  tar: what it adds or changes on top of its parent, with whiteouts for what
  it removes. The source reads the finished diff straight from the mounted
  content store, and fast gzip compresses it on the wire. The receiver
  stages it in its content store in 1 MiB writes and asks containerd to
  apply it onto a new snapshot of the parent, committed under the layer's
  chainID. The import that follows
  finds those chainIDs and skips the layers.
- containerd's differ keeps contents, ownership, modes (setuid included),
  symlinks, hard links and file capabilities (`security.capability`). Like
  Docker's, it drops other extended attributes, such as `user.*`; a layer
  whose files carry those loses them when it travels as a snapshot. Layers
  that travel as blobs are unaffected.
- A snapshot needs its parent unpacked first. When layers below it do have
  blobs, the receiver first imports a small temporary **base image** of
  exactly those layers, so they arrive as verified blobs and the snapshot lands
  on top. The temporary image is removed right afterwards.
- **Mixed versions.** Workers before 1.8.6 ship the overlayfs directory itself
  (GNU tar, whiteouts as device files). A 1.8.6 receiver converts that format;
  a 1.8.6 source serves a pre-1.8.6 receiver in it. So rescue keeps working while
  a rollout is in progress.

## Integrity

Snapshots can't be verified against the layer's digest, because the tar
bytes never match the original layer. Between 1.8.6 workers, the source sends
the SHA-256 of the exact tar it produced as an HTTP trailer, and the receiver
checks it, and gzip's CRC-32, before the commit. From a pre-1.8.6 source there
is only the CRC, the token-authenticated peer and TCP.

> **Recommendation:** keep `discard_unpacked_layers = false` in containerd.
> Verified blobs, about a third of the size of snapshots, can then be used
> whenever possible. The dashboard's *Snapshot share of shipped layers* panel
> shows how often snapshots are needed.

## Interruptions

- Snapshots from a failed attempt stay pinned against containerd's garbage
  collection for `RESCUE_PIN_TTL_S`, so a retry doesn't ship them again. They
  are released right away on success.
- A restarted worker removes leftover temporary snapshots and base images, and
  releases expired pins. `angryduck_worker_rescue_cleanups_total{kind}` counts
  them.
