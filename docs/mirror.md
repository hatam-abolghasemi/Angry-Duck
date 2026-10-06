# Mirror

Each worker can run a **pull-only** OCI registry mirror for its node's
containerd. containerd asks the mirror first for every manifest and blob by
digest, for every image, whether or not it was preheated. The mirror answers
from the node or a peer, or tells containerd to go to the registry.

The mirror is off by default (`MIRROR_ENABLED=false`).

## How it works

The mirror listens on the worker's pod IP, port `18082`, and the worker
writes a `hosts.toml` for each registry the node has images from, plus
`MIRROR_REGISTRIES`:

```toml
# managed by angryduck: ...
server = "https://registry.example.com"

[host."http://10.233.64.7:18082"]
  capabilities = ["pull"]
  [host."http://10.233.64.7:18082".header]
    Authorization = "Bearer <random per-pod token>"
```

containerd runs on the host network, which reaches local pods directly. The
token is generated when the worker starts and exists only in these files
(mode `0600`), so containerd is the only client the mirror answers; any other
pod gets `401`. A new pod writes new files with its own IP and token.

On SIGTERM (a rollout, an eviction, an uninstall) the worker removes the
files it wrote, so containerd pulls from registries directly until the next
worker writes them again. If a worker dies without SIGTERM, the files point
at a dead pod IP until the replacement starts; containerd then falls back to
the registry, immediately with Calico, after its dial timeout on CNIs that
don't reject unreachable pod IPs.

For each request, the mirror:

1. serves it from the node's own content store, if present, reading the
   blob file directly (see [`CONTAINERD_ROOT`](configuration.md#worker));
2. otherwise streams it from a peer that the controller's layer index says
   holds it (`GET /layers/holders`);
3. otherwise answers `404`, and containerd pulls from the registry as usual.

containerd verifies every digest, so a peer can't slip in different content.

## Design choices

- **Tags always resolve at the registry.** The mirror has no `resolve`
  capability, so a moved tag such as `latest` is never served stale. During a
  registry outage, kubelet can't resolve the tag, the pull fails, and
  [rescue](rescue.md) takes over.
- **Blobs only.** Peers that hold only snapshots (nodes with
  `discard_unpacked_layers = true`) can't serve mirror requests; containerd
  then uses the registry.
- **Hands off your files.** Only files starting with `# managed by angryduck`
  are written or removed. A registry that already has a hand-written
  `hosts.toml`, such as an insecure internal registry, is left alone and
  logged. Setting `MIRROR_ENABLED=false` removes Angry Duck's files on the
  next worker start.

## Requirements

containerd reads `hosts.toml` files on each pull, without a restart, but only
if its CRI registry `config_path` points at `MIRROR_CONTAINERD_CONFIG_DIR`
(`/etc/containerd/certs.d` by default):

```toml
# /etc/containerd/config.toml, containerd 1.x
[plugins."io.containerd.grpc.v1.cri".registry]
  config_path = "/etc/containerd/certs.d"

# containerd 2.x
[plugins."io.containerd.cri.v1.images".registry]
  config_path = "/etc/containerd/certs.d"
```

The worker logs a warning when `/etc/containerd/config.toml` doesn't mention
it.

## Metrics

| Metric | Meaning |
|---|---|
| `angryduck_worker_mirror_requests_total{kind, result}` | Requests served `local`, from a `peer`, a `miss` (registry), or `error`. |
| `angryduck_worker_mirror_bytes_total{source}` | Bytes served, each one not downloaded from the registry. |
| `angryduck_worker_mirror_peer_served_bytes_total` | Bytes this node served to other nodes' mirrors. |

## Settings

`MIRROR_ENABLED`, `MIRROR_LISTEN_ADDR`, `MIRROR_CONTAINERD_CONFIG_DIR`,
`MIRROR_REGISTRIES`. The mirror needs the shared token for peer fetches. See
[Configuration](configuration.md#mirror).
