# Mirror

Each worker can run a **pull-only** OCI registry mirror for its node's
containerd. containerd asks the mirror first for every tag, manifest and blob,
for every image, whether or not it was preheated. The mirror answers from the
node or a peer, or tells containerd to go to the registry. It never downloads
from a registry itself: that stays containerd's job.

The mirror is on by default (`MIRROR_ENABLED=true`). It needs containerd's
`config_path` (see [Requirements](#requirements)); without it containerd
ignores the `hosts.toml` files and simply pulls from the registry, so turning
the mirror on costs nothing where it can't help yet. Turn it off with
`MIRROR_ENABLED=false`, which also removes Angry Duck's `hosts.toml` files on
the next worker start.

It is also how a rollback reaches the [retention copy](image-cleanup.md#retention-copies)
of an image while the registry is up.

## How it works

The mirror listens on port `18082` of the node's loopback (`127.0.0.1`) when
the worker shares the node's network (the default), or of the worker's pod
IP when it runs on the pod network. The worker writes a `hosts.toml` for each
registry the node has images from, plus `MIRROR_REGISTRIES`:

```toml
# managed by angryduck: ...
server = "https://registry.example.com"

[host."http://127.0.0.1:18082"]
  capabilities = ["pull", "resolve"]
  [host."http://127.0.0.1:18082".header]
    Authorization = "Bearer <random per-pod token>"
```

containerd runs on the host network, so it reaches the loopback, or local
pods, directly. The token is generated when the worker starts and exists only
in these files (mode `0600`), so containerd is the only client the mirror
answers; any other process or pod gets `401`. A new worker writes new files
with its own token (and, on the pod network, its own IP).

On SIGTERM (a rollout, an eviction, an uninstall) the worker removes the
files it wrote, so containerd pulls from registries directly until the next
worker writes them again. If a worker dies without SIGTERM, the files point
at a closed loopback port until the replacement starts, and containerd falls
back to the registry immediately. On the pod network they point at a dead pod
IP: containerd falls back immediately with Calico, after its dial timeout on
CNIs that don't reject unreachable pod IPs.

For each manifest or blob, by digest, the mirror:

1. serves it from the node's own content store, if present, reading the
   blob file directly (see [`CONTAINERD_ROOT`](configuration.md#worker));
2. otherwise streams it from a peer that holds it, trying every holder the
   controller knows (`GET /layers/holders`): those its layer index lists,
   then those a running spread just delivered the blob to. Workers report
   pulls and removals from containerd's events, so the index is about a
   second behind. A holder that can't serve it answers `404`, and the next
   one is tried;
3. otherwise streams it from a peer **still receiving** it in a spread (a
   seed pulling it from the registry, or a node getting it from a peer):
   the bytes go on to containerd as they land there, through the peer's
   `/spread/content`. Many nodes asking for a layer at once then share one
   registry download instead of each pulling its own. Peers whose worker
   doesn't stream, or whose stream broke in this spread, aren't asked;
   manifests never come this way. If that transfer fails midway, the stream
   goes on from another copy of the blob if one is there, else it is cut
   short and containerd's pull fails, to be retried by kubelet;
4. otherwise answers `404`, and containerd pulls from the registry as usual.
   So a `404` means no node the controller knows of could serve the blob,
   whole or still arriving.
   An empty answer from the controller isn't cached, so a blob that lands on
   a peer a moment later is found on the next request.

A node that holds a layer only as an unpacked snapshot can't serve it here:
containerd checks the blob's digest, and a snapshot never rebuilds the same
bytes. If the registry fails too, [rescue](rescue.md) takes over, snapshots
included.

containerd verifies every digest, so a peer can't slip in different content.

### Tags

With `MIRROR_RESOLVE_ENABLED` (on by default), containerd asks the mirror what
a tag points at too. The mirror:

1. asks the controller whether any fresh node holds that exact tag
   (`GET /images/tag`). If none does, it answers `404` straight away, without
   asking the registry, and containerd resolves the tag there as it would
   without the mirror;
2. asks the tag's registry, with a `HEAD` and Angry Duck's credentials,
   waiting at most `MIRROR_RESOLVE_TIMEOUT_MS`. Its answer wins: that digest
   is served from the node or a peer, or, when no node holds it (the tag moved
   to something the cluster doesn't have yet), `404`, and containerd pulls it
   from the registry;
3. when the registry gives no digest (down, too slow, refusing Angry Duck's
   credentials, or no longer knowing the tag), serves the newest copy a node
   holds. The controller asks the holders what the tag points at on each and
   since when (giving them `MIRROR_RESOLVE_NEWEST_TIMEOUT_MS`), once per tag
   for every mirror asking within `MIRROR_RESOLVE_NEWEST_CACHE_S`.

A registry that gave no answer is left alone on that node for
`MIRROR_RESOLVE_UNREACHABLE_TTL_S`, so pulls during an outage don't each wait
for the timeout.

## Design choices

- **The registry decides while it can answer.** A tag resolves to what the
  registry says whenever it says it, so a moved tag such as `latest` is never
  served stale. When it can't, freshness never stops peers from sharing: the
  newest copy in the cluster is served, `imagePullPolicy: Always` pods
  included. `angryduck_worker_mirror_resolves_total{origin}` shows how often
  the registry was `missing`, `denied` or `unreachable` while a peer's copy was
  served.
- **The mirror decides, containerd pulls.** The mirror sends the registry one
  `HEAD` per tag and nothing else. What does come from the registry is
  containerd's own pull, with the pod's pull secret. A tag a node holds no
  longer reaches the registry with the pod's pull secret, so a broken pull
  secret shows only where no node has the image yet.
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
| `angryduck_worker_mirror_requests_total{kind, result}` | Requests served `local`, from a `peer`, as a `stream` from a peer still receiving it, a `miss` (registry), or `error`. |
| `angryduck_worker_mirror_bytes_total{source}` | Bytes served (`local`, `peer`, `stream`), each one not downloaded from the registry. |
| `angryduck_worker_mirror_peer_served_bytes_total` | Bytes this node served to other nodes' mirrors. |
| `angryduck_worker_mirror_resolves_total{origin, result}` | Tags resolved, by the registry's answer (`ok`, `missing`, `denied`, `unreachable`, or `unchecked` when no node holds the tag) and where the manifest came from (`local`, `peer`, `miss`). |

## Settings

`MIRROR_ENABLED`, `MIRROR_LISTEN_ADDR`, `MIRROR_CONTAINERD_CONFIG_DIR`,
`MIRROR_REGISTRIES`, `MIRROR_RESOLVE_ENABLED`, `MIRROR_RESOLVE_TIMEOUT_MS`,
`MIRROR_RESOLVE_UNREACHABLE_TTL_S`, `MIRROR_RESOLVE_CONTROLLER_TIMEOUT_MS`,
`MIRROR_RESOLVE_NEWEST_CACHE_S`, `MIRROR_RESOLVE_NEWEST_TIMEOUT_MS`. The mirror
needs the shared token for peer fetches. See
[Configuration](configuration.md#mirror).
