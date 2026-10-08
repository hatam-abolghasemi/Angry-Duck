# Security

Angry Duck moves images between your nodes, so it deserves a careful read
before you deploy it. This page says what each part can touch, what protects
it, and what to lock down.

## What the worker can touch

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

It also mounts two Secrets as files: the registry credentials and the shared
token. The webhook token is mounted only in the controller.

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
  Without a token these endpoints are open, and rescue, propagation and the
  mirror stay off.
- **The webhook requires CI's own bearer token** (`angryduck-webhook-token`),
  separate from the shared token so a leaked CI variable can't talk to
  workers. Without the Secret it is unauthenticated and the controller logs a
  warning at startup. An IP allowlist on the ingress is a good extra layer.
- **`/status`, `/metrics` and `/healthz` are open** on both components.
  `/status` lists every node, its images and what is moving where. Keep the
  controller's Service inside the cluster: the Helm chart's ingress and the
  example ingress expose only the webhook path, matched exactly.
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

- **Controller**: can list and watch pods cluster-wide, to find stuck pulls,
  and nothing else. It mounts the registry pull secret to read manifests.
- **Worker**: `get` on its own DaemonSet and nothing else, used on shutdown
  to tell an uninstall from a rollout. It writes `hosts.toml` files under
  containerd's config directory and deletes images on its node.

## Credentials

- Seed pulls and spread's registry fetches go through containerd's CRI and
  content API, not through kubelet, so a pod's `imagePullSecrets` don't apply
  to them. They use only the dockerconfigjson at `REGISTRY_CREDENTIALS_PATH`,
  which the controller also uses to read manifests.
- The shared token is read once at startup. Rotating it means updating the
  Secret and restarting the controller and all workers; see
  [Operations](operations.md#rotating-the-token).

## Hardening checklist

- Let the chart generate both tokens, or create them yourself with at least
  32 random characters. Never run without the shared token in production.
- Restrict the webhook ingress to your CI runners' addresses.
- Add a NetworkPolicy that allows port `18081` only from Angry Duck pods and
  Prometheus.
- Use a CNI that encrypts traffic between nodes if the network isn't trusted.
- Keep `discard_unpacked_layers = false`, so verified blobs travel instead of
  snapshots.
- Pin the worker image by version and control who can push it.

## Reporting a vulnerability

Please don't open a public issue. Report it privately through
[GitHub's security advisory form](https://github.com/hatam-abolghasemi/Angry-Duck/security/advisories/new),
with the version, what an attacker needs, and how to reproduce it. You'll get
an answer there, and a fix is released before the details are published.
