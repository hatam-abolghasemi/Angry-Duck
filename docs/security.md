# Security

Read this before deploying Angry Duck.

## The worker has no host privileges

The worker runs as root (for the root-owned containerd socket and node
paths) with **no Linux capabilities**, no host PID namespace, the default
seccomp and AppArmor profiles, no privilege escalation and a read-only root
filesystem. It mounts:

| Node path | Access | Why |
|---|---|---|
| `/run/containerd/containerd.sock` | read-write | containerd's API and CRI: every image, layer and snapshot operation. |
| `/var/lib/containerd/io.containerd.content.v1.content` | read-only | Serve blobs without a round trip through the API (optional). |
| `/etc/containerd/config.toml` | read-only | Find containerd's root and check `config_path` for the mirror. |
| `/etc/containerd/certs.d` | read-write | The mirror's `hosts.toml` files. |
| `/var/lib/angryduck` | read-write | Rescue pins and image-usage history. |

Work that needs privileges on the node (unpacking layers, mounting
snapshots, writing files with their owners, modes and whiteouts) happens
inside containerd, which already has them.

The containerd socket is still root-equivalent on the node: anything that
can drive containerd can run a privileged container. That is inherent to a
tool that manages images, and it is the trust the worker keeps. What is
gone is the extra kernel attack surface on top of it. Control who can push
the worker image.

The worker runs on the pod network: nothing listens on node IPs, and
NetworkPolicy applies to it. Its only Kubernetes API permission is `get` on
its own DaemonSet, used on shutdown to tell an uninstall from a rollout.

## Trust boundaries

- **Workers serve image content to each other.** `/blobs/*`, `/snapshots/*`,
  `/spread/content/*` and `/mirror/content/*` hand out layers of any image
  on the node, private images included, to anyone holding the shared token.
  Without a token, rescue, spreading, propagation and the mirror stay off
  rather than serve unauthenticated.
- **Spread content is verified, never trusted.** A blob a worker fetches,
  from the registry or a peer, is committed only if its size and SHA-256
  match the digest the controller read from the registry; a mismatch is
  discarded on the spot. The metadata a finalize carries is checked against
  its digests before anything is imported. Orders are refused unless their
  digest, size and job ID are well formed, and a fetch can't exceed 64 GiB. Traffic is plain HTTP between pod IPs; use a CNI with
  encryption between nodes if the network itself isn't trusted.
- **`/report`, `/pull` and `/pull/cancel` require the shared token** when one
  is configured (`AUTH_MODE`, see [Configuration](configuration.md)).
  `/report` matters most: the address a worker reports is where rescue and
  propagation send the token, so an unauthenticated report could collect it.
  Without a token these endpoints stay open, as before, and rescue,
  propagation and the mirror stay off.
- **The webhook requires CI's own bearer token** (`angryduck-webhook-token`),
  separate from the shared token so a leaked CI variable can't talk to
  workers. Without the Secret it is unauthenticated and the controller logs a
  warning at startup. An IP allowlist on the ingress is a good extra layer.
- **NetworkPolicy applies to the worker.** Ports `18081` (worker API) and
  `18082` (mirror) are on the pod IP. A policy can restrict `18081` to the
  Angry Duck pods and your Prometheus; containerd's traffic to `18082` comes
  from the node itself, which most CNIs (Calico included) always allow.
- **The mirror answers only containerd.** containerd sends a random per-pod
  token from `hosts.toml` (mode `0600`, root-only); any other pod gets `401`.
  The shared token doesn't open it. Peer fetches between mirrors go over the
  token-protected worker port.
- **containerd verifies blobs**, so a peer can't substitute content for a
  digest. Snapshots can't be verified by digest; see
  [Transfers](transfers.md#integrity).
- **The shared token can plant snapshots.** Whoever holds it can serve a
  receiver a snapshot with any content under a real layer's chainID.
  containerd then treats that layer as present and reuses it for every later
  pull of an image with that layer on that node, until the snapshot is
  removed. Treat the token like a credential for every node's image store:
  keep it in the Secret only, rotate it if it may have leaked (see
  [Operations](operations.md#rotating-the-token)), and keep
  `discard_unpacked_layers = false` so snapshots are rarely needed at all.

## Kubernetes permissions

- **Controller**: can list pods cluster-wide, to find stuck pulls, and nothing
  else. It mounts the registry pull secret to read manifests.
- **Worker**: `get` on its own DaemonSet and nothing else, used on shutdown
  to tell an uninstall from a rollout. It writes `hosts.toml` files under
  containerd's config directory and deletes images on its node.

## Credentials

- Seed pulls bypass `imagePullSecrets`: the worker runs the runtime CLI
  directly and only has the credentials in `REGISTRY_CREDENTIALS_PATH`.
- The shared token is read once at startup. Rotating it means updating the
  Secret and restarting the controller and all workers; see
  [Operations](operations.md#rotating-the-token).

## Reporting a vulnerability

Please report security issues privately to the maintainers instead of opening
a public issue.
