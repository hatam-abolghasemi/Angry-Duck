# API

## Controller

Port `8080`.

| Endpoint | Auth | Description |
|---|---|---|
| `POST /webhook/preheat` | webhook token, when configured | Body `{"image": "..."}`. Returns `202` at once, or `503` when no worker is fresh. Short names are normalized, so `nginx` becomes `docker.io/library/nginx:latest`. With spreading on, `"status": "spreading"`: the manifest is read in the background, retried for `TARGET_TTL_S`, and the seeds show in `/status`; with spreading off, `ordered_nodes` are the seeds. See [Preheat](preheat.md#triggering-it). |
| `POST /report` | token, when configured | Worker reports. |
| `GET /layers/holders?digest=&exclude=` | token | Nodes holding a blob, for mirrors: by inventory, then those a spread just delivered it to, then those still receiving it in a spread (`"arriving": true`: ask their `/spread/content`, which streams it as it lands). |
| `GET /images/tag?image=&newest=1` | token | For mirrors resolving a tag: `{"holders"}`, how many fresh nodes hold it, and with `newest=1` the `digest` the newest of their copies points at (asked of the holders, reused for `MIRROR_RESOLVE_NEWEST_CACHE_S`). Only with `MIRROR_RESOLVE_ENABLED`. |
| `POST /spread/done` | token | Workers report each spread transfer's end: `{"node", "job", "digest", "path", "result", "bytes", "seconds", "upstream", "id"}`. `upstream` marks a stream that broke because its source's own transfer stopped; `id` is the order's. |
| `GET /status` | none | Workers, the preheat target, stuck pulls (`rescues`), active `spreads`, whole-image `propagations` (companion images marked `"companion": true`), [retention copies](image-cleanup.md#retention-copies) (`retention`: digest, image, node keeping it, when it went unused, until when) and [companion images](companions.md) (`companions`: image, digest, size, nodes running it, kept or why not). |
| `GET /metrics` | none | Prometheus metrics. |
| `GET /healthz` | none | Liveness and readiness. |

Example:

```bash
curl -s -X POST localhost:8080/webhook/preheat -H "Authorization: Bearer ${ANGRYDUCK_WEBHOOK_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{"image": "registry.example.com/app:1.2.3"}'
```

```json
{"accepted": true, "status": "spreading", "target_image": "registry.example.com/app:1.2.3"}
```

## Worker

Port `18081` on the worker's pod IP, which is the node's address when the
worker shares the node's network (the default). Endpoints marked *token* require the
shared token as `Authorization: Bearer <token>`.

| Endpoint | Auth | Called by | Description |
|---|---|---|---|
| `POST /pull` | token, when configured | controller | Body `{"image", "ordered_at"}`. Returns `202` and pulls from the registry in the background. |
| `POST /pull/cancel` | token, when configured | controller | Cancels a slow seed pull. |
| `POST /rescue` | token | controller | Body `{"image", "sources": [{"node_id", "address"}], "reason": "rescue" \| "propagate" \| "retain"}`. Fetches the image from the first source that works and returns the result. |
| `POST /images/retain` | token | controller | Body `{"digest", "until"}`. Marks every name of the image with that target digest as the cluster's retention copy until `until` (Unix seconds), or removes the mark with `until` 0. `200` done, `404` no such image here, `409` retention off on this node. Only with `GC_RETENTION_ENABLED`. |
| `POST /blobs/plan` | token | peer | Body `{"image", "platform"}`. Every blob and layer of the image for that platform, and which ones this node has. |
| `POST /blobs/export` | token | peer | Body `{"image", "platform", "digests"}`. Streams a partial OCI archive with only those blobs. |
| `POST /snapshots/export` | token | peer | Body `{"image", "platform", "chain_id", "format"}`. Streams one layer as a gzip-compressed tar: containerd's OCI layer diff with a SHA-256 trailer for `"format": "oci-layer"`, the overlayfs directory format for pre-1.8.6 receivers. |
| `GET /mirror/content/<digest>` | token | peer mirror | Content for other nodes' mirrors. |
| `GET /mirror/tag?image=` | token | controller | `{"digest", "updated"}`: what the tag points at on this node and when it last changed; `404` if it isn't here. |
| `POST /spread/fetch` | token | controller | Body `{"job", "image", "digest", "size", "path": "registry" \| "peer", "peer", "stream", "id", "replace", "expect_rate"}`; `stream` means the peer is still receiving the blob itself. With `replace`, a transfer of this blob already running over this path is restarted from `peer` only if that should finish clearly sooner at `expect_rate` bytes/s; otherwise `409` with `kept`. Starts fetching one blob (`202`); any number of different blobs run at once. Answers `200` with `present` if it's already here, `409` if this blob is already coming over that path. A `/spread/done` to the controller follows. |
| `POST /spread/finalize` | token | controller | Body `{"job", "image", "top_type", "top_digest", "top_size", "metadata"}`. Registers the image from its metadata blobs, each checked against its digest, once every layer is here. |
| `POST /spread/drop` | token | controller | Body `{"job"}`. Cancels the job's transfers here and releases the blobs held for it. |
| `GET /spread/content/<digest>` | token | peer | One blob, for a peer's spread fetch: committed, or still arriving here, then streamed as it arrives with `Angryduck-Stream: 1` (cut short if this node's own transfer fails). |
| `GET /metrics` | none | Prometheus | Prometheus metrics. |
| `GET /healthz` | none | kubelet | Liveness and readiness. |

The mirror itself listens separately on `MIRROR_LISTEN_ADDR` (`18082`, on
the node's loopback, or the pod IP on the pod network), answers only requests carrying the per-pod token containerd
sends from `hosts.toml`, and implements the read-only part of the OCI distribution
API for containerd.
