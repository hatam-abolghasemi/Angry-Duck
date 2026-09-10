# Kubespray + AngryDuck

AngryDuck's worker is designed for Ubuntu nodes bootstrapped by Kubespray with
containerd. The recommended DaemonSet uses the copies Kubespray installed on the node under
`/usr/local/bin` instead of requiring those binaries in the worker image. The
worker code also falls back to `/usr/local/bin/ctr` and `/usr/local/bin/crictl`
inside the container, so older worker images that bundled the tools remain
compatible.

The worker exposes a tiny OCI Distribution endpoint on the node and installs
an `_default/hosts.toml` entry under `/etc/containerd/certs.d`. containerd can
therefore try AngryDuck first for normal kubelet image pulls. A local hit is
served from the node's existing containerd content store; a miss asks a few
repo-local AngryDuck peers; if all peers miss or the mirror is unreachable,
containerd proceeds to the original registry.

## Current Kubespray / containerd 2.1+

containerd 2.1+ uses Transfer Service for CRI image pulls by default. That
path can bypass the full `hosts.toml` mirror behavior, so configure Kubespray
to use the client/local pull path:

```yaml
# inventory/<cluster>/group_vars/all/containerd.yml
containerd_extra_args: |
  [plugins."io.containerd.cri.v1.images"]
    use_local_image_pull = true
```

Kubespray's current containerd template exposes `containerd_extra_args` and
already configures the modern `certs.d` registry `config_path`; the snippet
above is the only extra containerd setting AngryDuck needs. 

Do this **before** bootstrapping/re-running the Kubespray container-engine
role. Do not have AngryDuck rewrite `config.toml` or restart containerd from
inside a worker pod.

For containerd versions older than 2.1, `hosts.toml` registry mirrors are
already on the local-pull path and this setting is not needed.

## Registry mirrors

For AngryDuck to be the `_default` mirror, do not create a more-specific
Kubespray `containerd_registries_mirrors` entry for the same registry namespace
unless you intentionally want that registry to bypass AngryDuck. A registry-
specific `certs.d/<registry>/hosts.toml` takes precedence over `_default`.

If Spegel is also installed, do not let both projects own `_default/hosts.toml`.
Use one local mirror owner or create an explicit ordered configuration instead.

## Worker requirements

The DaemonSet mounts:

```text
/run/containerd/containerd.sock    -> containerd socket
/usr/local/bin/                    -> ctr, crictl, containerd
/etc/containerd/config.toml        -> startup compatibility check
/etc/containerd/certs.d/           -> AngryDuck _default/hosts.toml
```

The node binaries are read-only inside the pod. `certs.d` is writable because
AngryDuck installs only its own `_default/hosts.toml` and refuses to overwrite
an existing file that is not marked as AngryDuck-managed.

The worker Dockerfile is unchanged from the previous version. No base-image or
package-manager change is required for the mirror integration.

## Expected pull path

```text
kubelet
  -> containerd
     -> AngryDuck on the same node
        -> local containerd content, if present
        -> repo-local peer(s), if advertised
        -> 404 / connection failure
     -> original registry
```

AngryDuck does not keep a second image store. It does not inspect or compare
layer graphs. It only resolves a tag to a digest from the node's existing
containerd image metadata and streams individual OCI objects that containerd
itself requested. The P2P decision remains image/repository based; there is no
AngryDuck layer scheduler.

## Resource behavior

The mirror has deliberately bounded work:

- maximum concurrent local content streams defaults to `4` per worker;
- peer candidate lists are cached for 1 second;
- controller requests have a 500 ms timeout;
- peer connection setup is capped at 200 ms and response headers at 500 ms;
- a busy worker does not queue unlimited mirror work;
- manifest bodies are bounded at 4 MiB;
- image preheat still runs asynchronously and uses the ordinary containerd
  pull path, so it benefits from the mirror rather than implementing a second
  image transfer mechanism.

These limits keep AngryDuck out of the critical path when it is busy: a mirror
miss is cheap and containerd goes to the origin instead of waiting on AngryDuck.
Actual CPU/memory cost will still depend on image size and pull concurrency;
benchmark it on representative nodes before increasing the stream limit.
