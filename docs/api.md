# API

## Controller

Port `8080`.

| Endpoint | Auth | Description |
|---|---|---|
| `POST /webhook/preheat` | webhook token, when configured | Body `{"image": "..."}`. Returns `202` with the seed nodes, or `503` when no worker is fresh. Short names are normalized, so `nginx` becomes `docker.io/library/nginx:latest`. Starts propagation. |
| `POST /report` | token, when configured | Worker reports. |
| `GET /layers/holders?digest=&exclude=` | token | Nodes holding a blob, for mirrors. |
| `GET /status` | none | Workers, the preheat target, stuck pulls (`rescues`) and active `propagations`. |
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

Port `18081` on the node network. Endpoints marked *token* require the shared
token as `Authorization: Bearer <token>`.

| Endpoint | Auth | Called by | Description |
|---|---|---|---|
| `POST /pull` | none | controller | Body `{"image", "ordered_at"}`. Returns `202` and pulls from the registry in the background. |
| `POST /pull/cancel` | none | controller | Cancels a slow seed pull. |
| `POST /rescue` | token | controller | Body `{"image", "sources": [{"node_id", "address"}], "reason": "rescue" \| "propagate"}`. Fetches the image from the first source that works and returns the result. |
| `POST /blobs/plan` | token | peer | Body `{"image", "platform"}`. Every blob and layer of the image for that platform, and which ones this node has. |
| `POST /blobs/export` | token | peer | Body `{"image", "platform", "digests"}`. Streams a partial OCI archive with only those blobs. |
| `POST /snapshots/export` | token | peer | Body `{"image", "platform", "chain_id"}`. Streams one layer's snapshot directory as a gzip tar. |
| `GET /mirror/content/<digest>` | token | peer mirror | Content for other nodes' mirrors. |
| `GET /metrics` | none | Prometheus | Prometheus metrics. |
| `GET /healthz` | none | kubelet | Liveness and readiness. |

The mirror itself listens separately on `MIRROR_LISTEN_ADDR` (`:18082`, on
the pod IP), answers only requests carrying the per-pod token containerd
sends from `hosts.toml`, and implements the read-only part of the OCI distribution
API for containerd.
