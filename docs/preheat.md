# Preheat

Preheat puts a new image on a few well-chosen **seed** nodes as soon as CI
pushes it. The seeds are the only nodes that talk to the registry for that
image; [propagation](propagation.md) takes it from there.

## Triggering it

CI calls the controller's webhook right after `docker push`:

```bash
curl -fsS -X POST https://angryduck.example.com/angryduck/webhook/preheat \
  -H "Authorization: Bearer ${ANGRYDUCK_WEBHOOK_TOKEN}" \
  -H 'Content-Type: application/json' -d "{\"image\":\"${IMAGE}\"}" || true
```

The controller answers `202` with the nodes it ordered to pull:

```json
{"accepted": true, "target_image": "registry.example.com/app:1.2.3", "ordered_nodes": ["node-7", "node-3"]}
```

Short names are normalized, so `nginx` becomes
`docker.io/library/nginx:latest`. The call is best-effort: a failed preheat
should never fail a pipeline.

## Choosing seeds

The controller orders exactly `RANK_TOP_N` (default 5) nodes to pull.

1. **Only fresh workers count**: those that reported within
   `WORKER_STALE_AFTER_S`. With none, the webhook returns `503`.
2. **Excluded nodes are skipped.** Node names containing any
   `RANK_EXCLUDE_NODE_SUBSTRINGS` entry, such as `master`, are never chosen.
3. **By layers** (`RANK_BY_LAYERS=true`, the default). The controller reads
   the image's manifest and config from its registry, a few KB and never the
   layers, and compares them with every node's layer inventory. A layer counts
   as present if the node has its blob or its unpacked snapshot, so this works
   whatever `discard_unpacked_layers` is set to. The nodes lacking the fewest
   bytes win, with disk utilization as the tie-breaker.
4. **Fallback.** If the manifest can't be read (no credentials for that
   registry, or a timeout), nodes holding any tag of the same repo come first
   (`RANK_PREFER_IMAGE_LOCALITY`), then the lowest disk utilization.

Failed orders are replaced on the next ranking tick (`RANK_INTERVAL_S`) until
`TARGET_TTL_S` runs out.

## Registry access

The controller follows each registry's `WWW-Authenticate` challenge, Bearer
or Basic, so it works with GitLab, Harbor, Nexus, Docker Hub and other
standard registries without per-registry settings. Credentials come from the
dockerconfigjson at `REGISTRY_CREDENTIALS_PATH`. List every private registry
you push to in that file.

Workers pull through containerd's CRI API directly, not through kubelet, so
kubelet's `imagePullSecrets` don't apply. They use the same credentials file.

## Metrics

| Metric | What to look for |
|---|---|
| `angryduck_controller_pull_orders_total` | Orders by node and result. |
| `angryduck_controller_seed_rankings_total` | `basis="layers"` should dominate; fallbacks mean manifests couldn't be read. |
| `angryduck_controller_seed_missing_bytes` | What each seed had to download for the latest image. |
| `angryduck_worker_pulls_total`, `angryduck_worker_pull_seconds_total` | Seed pull outcomes and time spent. |
| `angryduck_worker_preheated_containers_running` | Whether preheated repos actually run on the seeds. |

## Settings

`RANK_TOP_N`, `RANK_BY_LAYERS`, `RANK_PLATFORM`, `RANK_RESOLVE_TIMEOUT_S`,
`RANK_RESOLVE_CACHE_S`, `RANK_PREFER_IMAGE_LOCALITY`,
`RANK_EXCLUDE_NODE_SUBSTRINGS`, `RANK_INTERVAL_S`, `TARGET_TTL_S`. See
[Configuration](configuration.md#preheat).
