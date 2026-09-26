# Security

Read this before deploying Angry Duck.

## The worker is powerful on its node

The worker is not `privileged: true`, but it runs with `hostPID`,
`hostNetwork`, an unconfined AppArmor profile, and these capabilities, with
all others dropped:

| Capability | Why |
|---|---|
| `SYS_CHROOT`, `SYS_PTRACE` | Chroot into `/proc/1/root` to run the node's own `crictl`, `ctr` and `tar`. |
| `DAC_READ_SEARCH`, `DAC_OVERRIDE`, `CHOWN`, `FOWNER`, `FSETID`, `SETFCAP`, `MKNOD`, `SYS_ADMIN` | Read and write overlayfs snapshot directories faithfully, including ownership, device files and extended attributes. |

Without the unconfined AppArmor profile, reading `/proc/1/root` fails with
`permission denied` on AppArmor-enforcing nodes such as Ubuntu.

The worker reads blob files from containerd's content store directly
(read-only), and also drives containerd through its socket, which is already
root-equivalent on the node, so the extra capabilities don't add a new level
of trust. It is still a real blast radius: **pin the image by digest and sign
it.**

## Trust boundaries

- **Workers serve image content to each other.** `/blobs/*`, `/snapshots/*`
  and `/mirror/content/*` hand out layers of any image on the node, private
  images included, to anyone holding the shared token. Without a token,
  rescue, propagation and the mirror stay off rather than serve
  unauthenticated. Traffic is plain HTTP on the node network.
- **The webhook, `/report`, `/pull` and `/pull/cancel` have no
  authentication.** Restrict the webhook ingress to your CI runners, for
  example with an IP allowlist on your ingress controller, and use a
  NetworkPolicy so only the controller and other workers can reach port
  `18081`.
- **The mirror listens on `127.0.0.1` only** and has no authentication, like
  any local registry mirror: node-local processes can read image content
  through it. Peer fetches between mirrors go over the token-protected worker
  port.
- **containerd verifies blobs**, so a peer can't substitute content for a
  digest. Snapshots can't be verified by digest; see
  [Transfers](transfers.md#integrity).

## Kubernetes permissions

- **Controller**: can list pods cluster-wide, to find stuck pulls, and nothing
  else. It mounts the registry pull secret to read manifests.
- **Worker**: no Kubernetes API access (`automountServiceAccountToken:
  false`). It writes `hosts.toml` files under containerd's config directory
  and deletes images on its node.

## Credentials

- Seed pulls bypass `imagePullSecrets`: the worker runs the runtime CLI
  directly and only has the credentials in `REGISTRY_CREDENTIALS_PATH`.
- The shared token is read once at startup. Rotating it means updating the
  Secret and restarting the controller and all workers; see
  [Operations](operations.md#rotating-the-token).

## Reporting a vulnerability

Please report security issues privately to the maintainers instead of opening
a public issue.
