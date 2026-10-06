# Node settings and limits

Angry Duck works with the containerd and kubelet you already run, so their
settings decide how much it can do. This page lists the ones that matter,
how to use it from CI and workloads, and what it can't do.

## containerd

| Setting | Recommended | Why |
|---|---|---|
| `discard_unpacked_layers` | `false` (the default) | With `true`, nodes delete layer blobs once unpacked. Peers can then only send snapshots: slower, not digest-verified, and about three times the bytes of a blob. Every propagation goes that way. |
| `config_path` | includes `/etc/containerd/certs.d` | Needed by the [mirror](mirror.md). containerd 2.x includes it by default. |
| `image_pull_progress_timeout` | `1m` instead of `5m` | When a registry stops answering, kubelet reports `ErrImagePull` only after this timeout, and [rescue](rescue.md) can't start before that. |
| `max_concurrent_downloads` | `3` (default) or more | Layers one pull downloads in parallel, for seeds and for any pull that misses the preheated image. |
| `snapshotter` | `overlayfs` (the default) | The only snapshotter Angry Duck supports. |

Where they live:

```toml
# containerd 2.x (config version 3)
[plugins.'io.containerd.cri.v1.images']
  snapshotter = 'overlayfs'
  discard_unpacked_layers = false
  max_concurrent_downloads = 3
  image_pull_progress_timeout = '1m0s'
  [plugins.'io.containerd.cri.v1.images'.registry]
    config_path = '/etc/containerd/certs.d'

# containerd 2.x pulls through its transfer service, which has its own limit
[plugins.'io.containerd.transfer.v1.local']
  max_concurrent_downloads = 3
```

```toml
# containerd 1.6 and 1.7 (config version 2)
[plugins."io.containerd.grpc.v1.cri"]
  max_concurrent_downloads = 3
  image_pull_progress_timeout = "1m0s"
  [plugins."io.containerd.grpc.v1.cri".containerd]
    snapshotter = "overlayfs"
    discard_unpacked_layers = false
  [plugins."io.containerd.grpc.v1.cri".registry]
    config_path = "/etc/containerd/certs.d"
```

Some managed distributions set `discard_unpacked_layers = true`. Check with
`containerd config dump | grep discard_unpacked_layers` on a node. The
dashboard's *Snapshot share of shipped layers* panel shows the effect.

## kubelet

| Setting | Recommended | Why |
|---|---|---|
| `serializeImagePulls` | `false` | The default `true` pulls one image at a time per node, so one large image delays every other pod there. Angry Duck's own pulls go straight to containerd and aren't queued by kubelet, but pods that miss a preheated image are. |
| `maxParallelImagePulls` | e.g. `5` | Bounds parallel pulls once `serializeImagePulls` is `false`. |
| `imageGCHighThresholdPercent` | above `GC_HIGH_UTILIZATION` (default 85 vs 70) | Angry Duck should clean up first, knowing which images are rollback candidates. kubelet's own GC only starts at this mark. |
| `imageMaximumGCAge` | unset, or above `GC_UNUSED_FOR_S` (6h) | Otherwise kubelet removes rollback images before Angry Duck's 6 hours are up. |

```yaml
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
serializeImagePulls: false
maxParallelImagePulls: 5
imageGCHighThresholdPercent: 85
imageGCLowThresholdPercent: 80
```

## CI and workloads

- **Call the webhook right after the push**, before the manifests change, and
  with `|| true`, so preheat runs alongside the rest of the pipeline and never
  fails it. See [Installation](installation.md#integrate-with-ci).
- **Give every build its own tag** (a version or commit SHA). A tag that moves,
  such as `latest`, makes kubelet ask the registry every time.
- **Use `imagePullPolicy: IfNotPresent`.** With `Always`, kubelet still asks
  the registry for the manifest before it starts a container, so preheated
  layers help but a registry outage still blocks the pod, and rescue can't
  help it. `latest` and untagged images default to `Always`.
- **Keep `PROPAGATE_MAX_UTILIZATION` and `GC_*` consistent.** Propagation
  skips nodes above `PROPAGATE_MAX_UTILIZATION` until cleanup makes room.

## What Angry Duck can't do

- **New layers come from the registry once.** Seeds pull the layers no node
  has yet; everything after that comes from peers.
- **It can't invent an image.** If the registry can't serve it and no node has
  it, as a blob or as a snapshot, nothing can.
- **It doesn't restart pods.** A rescued pod starts on kubelet's next retry.
- **It can't rescue its own worker image**; see
  [Bootstrapping a node by hand](operations.md#bootstrapping-a-node-by-hand).
- **Snapshots lose some extended attributes.** Only `security.capability`
  travels; see [Transfers](transfers.md#snapshots).
- **It is best-effort.** If Angry Duck is down, pulls go to the registry as
  they would without it.
