# Angry Duck

Angry Duck is a lightweight image preheat, peer-mirror, and node-wide image GC
service for Kubernetes clusters running containerd.

Its main job is simple: make the **normal kubelet/containerd image pull faster**
without becoming another image store or another blocking runtime process.

## Pull path

Every worker runs a small OCI Distribution mirror on the node. containerd is
configured to try that mirror first through `_default/hosts.toml`:

```text
kubelet
  -> containerd
     -> AngryDuck :5000
        -> local image/content on this node
        -> repo-local peer(s)
        -> fast miss
     -> original registry
```

A mirror miss is not an error for Kubernetes. AngryDuck returns a normal
registry miss (or is simply unreachable), so containerd continues to the
configured upstream registry.

AngryDuck therefore never needs to sit in front of kubelet waiting for a pull
to finish, and it never takes ownership of the node's image store.

## What the mirror does

For a manifest/tag request AngryDuck first checks the node's existing
containerd image metadata to resolve the reference to its canonical digest.
For a digest request or blob request it uses the requested digest directly.

If the requested object exists locally, AngryDuck streams it from containerd's
existing content store.

If it is not local, AngryDuck asks the controller for a small set of workers
that report the same repository. Each peer is asked for the **exact same OCI
request**. A peer that does not have that digest simply misses, and the next
peer is tried.

There is no layer graph inspection, layer negotiation, tar archive, temporary
image file, or second AngryDuck image database. The image identity used for
peer selection is repository/name plus the requested tag or digest; the exact
content identity is always the containerd digest.

## Preheat

The CI webhook remains useful, but it no longer implements a second P2P pull
mechanism.

```text
CI docker push
      |
      v
POST /webhook/preheat
      |
      v
AngryDuck controller ranks a few good nodes
      |
      v
worker receives /pull asynchronously
      |
      v
containerd pull -> AngryDuck mirror -> peer/origin fallback
```

This means preheat is genuinely useful: it gets the image onto a few nodes
before ArgoCD schedules the workload, and the same mirror is then used by all
other kubelet pulls.

## Wise GC

GC remains node-wide and digest-aware. It considers the actual images on the
node, compares their content identity with currently running containers, and
keeps freshly preheated images alive through the configured grace period.

Because AngryDuck and kubelet/containerd use the same content store, there is
no separate cache to synchronize or garbage-collect.

GC starts in dry-run by default in the worker code path; the example
Kubernetes config can enable real removal explicitly.

## Resource discipline

The mirror is deliberately bounded because it is a node-local acceleration
service, not an image server that should compete with workloads.

Defaults:

| Setting | Default | Purpose |
|---|---:|---|
| `REGISTRY_MAX_CONCURRENT_STREAMS` | `4` | Maximum local content streams per worker |
| `P2P_SOURCE_CANDIDATES` | `3` | Maximum peer candidates considered |
| `REGISTRY_CANDIDATE_CACHE_S` | `1` | Candidate cache TTL |
| `IMAGE_INVENTORY_CACHE_S` | `10` | Shared runtime inventory cache for reporter/GC |

Peer/controller requests have short timeouts. A busy mirror does not build an
unbounded queue: containerd is allowed to fall through to the origin instead.

There is no resident image data in AngryDuck. Local object serving temporarily
uses the existing `ctr content get` command and bounded streaming. The maximum
stream count is intentionally small; increase it only after measuring CPU and
I/O on representative workers.

## Ubuntu + Kubespray

The worker is designed for Kubespray's Ubuntu/containerd nodes.

Kubespray installs the runtime tooling under `/usr/local/bin`, so the worker
mounts these host binaries instead of carrying copies inside its image:

```text
/host/usr/local/bin/ctr
/host/usr/local/bin/crictl
/host/usr/local/bin/containerd
```

The worker image is Debian 13 slim, so `apt` remains available normally.

The worker also mounts:

```text
/run/containerd/containerd.sock
/etc/containerd/config.toml
/etc/containerd/certs.d/
```

and writes only its own `_default/hosts.toml` mirror configuration.

### containerd 2.1+

containerd 2.1+ uses Transfer Service for CRI image pulls by default. To make
`hosts.toml` mirrors apply to normal kubelet pulls, configure Kubespray with:

```yaml
containerd_extra_args: |
  [plugins."io.containerd.cri.v1.images"]
    use_local_image_pull = true
```

See [`deploy/kubespray/README.md`](deploy/kubespray/README.md).

### Kubespray registry mirrors

Do not create a more-specific `certs.d/<registry>/hosts.toml` for registries
that should go through AngryDuck unless that file explicitly includes the
AngryDuck mirror. `_default` is the generic fallback namespace.

Do not have Spegel and AngryDuck both own `_default/hosts.toml` at the same
time.

## Configuration

The main settings are in the environment files/configmaps. Important worker
settings include:

| Variable | Default | Meaning |
|---|---:|---|
| `REGISTRY_MIRROR_ENABLED` | `true` | Enable the node-local OCI mirror |
| `REGISTRY_LISTEN_ADDR` | `:5000` | Mirror HTTP listener |
| `REGISTRY_MAX_CONCURRENT_STREAMS` | `4` | Bound local content streams |
| `REGISTRY_CANDIDATE_CACHE_S` | `1` | Peer candidate cache TTL |
| `P2P_SOURCE_CANDIDATES` | `3` | Peer candidates returned by controller |
| `CTR_PATH` | `/host/usr/local/bin/ctr` | Node `ctr` binary |
| `CRICTL_PATH` | `/host/usr/local/bin/crictl` | Node `crictl` binary |
| `CONTAINERD_PATH` | `/host/usr/local/bin/containerd` | Node containerd binary |
| `CONTAINERD_CONFIG_PATH` | `/host/etc/containerd/config.toml` | Host containerd config |
| `CONTAINERD_CERTS_DIR` | `/host/etc/containerd/certs.d` | Host registry config directory |

`CONTAINER_RUNTIME=crictl` remains the recommended worker runtime for GC and
reporting because one `crictl ps -o json` call can provide running-container
image information without spawning one `ctr containers info` command per
container.

## API

### Controller

- `POST /webhook/preheat` — queue preheat for an image.
- `POST /report` — worker state/inventory report.
- `POST /peer/source` — return a small repo-local peer candidate set.
- `GET /status` — controller state.
- `GET /healthz` — health check.

### Worker control

- `POST /pull` — asynchronous preheat order; responds without waiting for the
  image pull to finish.
- `GET /healthz` — health check.

### Worker registry

The registry listener exposes OCI Distribution-compatible `GET`/`HEAD` paths
under `/v2/`. It is intended for containerd and peer-to-peer mirror traffic,
not for direct human use.

## Build

```bash
cd app
go build -o ../bin/angryduck-controller ./cmd/controller
go build -o ../bin/angryduck-worker ./cmd/worker
go test ./...
go vet ./...
```

The worker runtime image does not copy `ctr`, `crictl`, or `containerd`; those
come from the node's host installation.

## Kubernetes deployment

The ready-to-use DaemonSets are under `deploy/stg/` and `deploy/mgmt/`.

For Kubespray nodes, apply the containerd configuration described in
[`deploy/kubespray/README.md`](deploy/kubespray/README.md) before rolling out
the worker DaemonSet.
