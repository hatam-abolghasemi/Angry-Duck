# API

## Controller

Port `8080`.

| Endpoint | Auth | Description |
|---|---|---|
| `POST /webhook/preheat` | webhook token, when configured | Body `{"image": "..."}`. Returns `202` with the seed nodes, or `503` when no worker is fresh. Short names are normalized, so `nginx` becomes `docker.io/library/nginx:latest`. Starts the spread; `ordered_nodes` are the nodes pulling from the registry first. |
| `POST /report` | token, when configured | Worker reports. |
| `GET /layers/holders?digest=&exclude=` | token | Nodes holding a blob, for mirrors. |
| `POST /spread/done` | token | Workers report each spread transfer's end: `{"node", "job", "digest", "path", "result", "bytes", "seconds"}`. |
| `GET /status` | none | Workers, the preheat target, stuck pulls (`rescues`), active `spreads` and whole-image `propagations`. |
| `GET /metrics` | none | Prometheus metrics. |
| `GET /healthz` | none | Liveness and readiness. |

Example:

```bash
curl -s -X POST localhost:8080/webhook/preheat -H "Authorization: Bearer ${ANGRYDUCK_WEBHOOK_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{"image": "registry.example.com/app:1.2.3"}'
```

```json
{"accepted": true, "target_image": "registry.example.com/app:1.2.3", "ordered_nodes": ["node-7", "node-3"]}
```

## Worker

Port `18081` on the worker's pod IP. Endpoints marked *token* require the
shared token as `Authorization: Bearer <token>`.

| Endpoint | Auth | Called by | Description |
|---|---|---|---|
| `POST /pull` | token, when configured | controller | Body `{"image", "ordered_at"}`. Returns `202` and pulls from the registry in the background. |
| `POST /pull/cancel` | token, when configured | controller | Cancels a slow seed pull. |
| `POST /rescue` | token | controller | Body `{"image", "sources": [{"node_id", "address"}], "reason": "rescue" \| "propagate"}`. Fetches the image from the first source that works and returns the result. |
| `POST /blobs/plan` | token | peer | Body `{"image", "platform"}`. Every blob and layer of the image for that platform, and which ones this node has. |
| `POST /blobs/export` | token | peer | Body `{"image", "platform", "digests"}`. Streams a partial OCI archive with only those blobs. |
| `POST /snapshots/export` | token | peer | Body `{"image", "platform", "chain_id", "format"}`. Streams one layer as a gzip-compressed tar: containerd's OCI layer diff with a SHA-256 trailer for `"format": "oci-layer"`, the overlayfs directory format for pre-1.8.6 receivers. |
| `GET /mirror/content/<digest>` | token | peer mirror | Content for other nodes' mirrors. |
| `POST /spread/fetch` | token | controller | Body `{"job", "image", "digest", "size", "path": "registry" \| "peer", "peer"}`. Starts fetching one blob into a free slot (`202`), answers `200` with `present` if it's already here, `409` if that path's slot is busy. A `/spread/done` to the controller follows. |
| `POST /spread/finalize` | token | controller | Body `{"job", "image", "top_type", "top_digest", "top_size", "metadata"}`. Registers the image from its metadata blobs, each checked against its digest, once every layer is here. |
| `POST /spread/drop` | token | controller | Body `{"job"}`. Cancels the job's transfers here and releases the blobs held for it. |
| `GET /spread/content/<digest>` | token | peer | One committed blob, for a peer's spread fetch. |
| `GET /metrics` | none | Prometheus | Prometheus metrics. |
| `GET /healthz` | none | kubelet | Liveness and readiness. |

The mirror itself listens separately on `MIRROR_LISTEN_ADDR` (`:18082`, on
the pod IP), answers only requests carrying the per-pod token containerd
sends from `hosts.toml`, and implements the read-only part of the OCI distribution
API for containerd.
