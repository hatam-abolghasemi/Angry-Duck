# Recipes

The defaults suit a typical cluster of a few dozen nodes with a healthy
private registry. This page covers the setups that differ. Each recipe lists
only the settings that change; mix them as needed.

The snippets are Helm values. With plain manifests, put the same keys in
`k8s/configmap-env.yaml`. Every setting is described in
[Configuration](configuration.md).

## A small cluster

A handful of nodes don't need five seeds: two pull from the registry, and
the rest get the image from them. Registry pulls in flight follow
`RANK_TOP_N` unless you set `SPREAD_MAX_REGISTRY_PULLS`.

```yaml
config:
  RANK_TOP_N: "2"
```

## A large fleet

Hundreds of nodes mean more holders sooner, so let each holder serve more
than one node at a time if your node network has room, and give the
controller more memory: it grows with the number of nodes, and peaks right
after a restart when every worker resends its inventory.

```yaml
controller:
  resources:
    requests: {cpu: 50m, memory: 64Mi}
    limits: {cpu: 500m, memory: 256Mi}

config:
  PROPAGATE_PER_SOURCE: "2"
```

Watch `angryduck_process_memory_bytes` for the controller after a restart and
size from that. See [Sizing](operations.md#sizing).

## A slow or fragile registry

Ask it for less at once, let idle nodes race slow pulls, and serve containerd
from peers whenever possible.

```yaml
config:
  SPREAD_MAX_REGISTRY_PULLS: "2"
  SPREAD_RACE_PER_BLOB: "2"
  MIRROR_ENABLED: "true"
  MIRROR_REGISTRIES: "registry.example.com"
```

Also set containerd's `image_pull_progress_timeout` to `1m`, so a pull that
hangs fails sooner and [rescue](rescue.md) can step in. The mirror needs
containerd's `config_path`; see [Mirror](mirror.md#requirements).

## Small disks

Clean up earlier and keep fewer rollback images. Keep the thresholds in order:
`PROPAGATE_MAX_UTILIZATION` at or below `GC_HIGH_UTILIZATION`, both below
kubelet's `imageGCHighThresholdPercent`.

```yaml
config:
  GC_UNUSED_FOR_S: "7200"
  GC_HIGH_UTILIZATION: "0.60"
  GC_LOW_UTILIZATION: "0.50"
  GC_ROLLBACK_KEEP: "1"
  PROPAGATE_MAX_UTILIZATION: "0.60"
```

If a node shows as *stalled* on the Cleanup tab, images aren't what fills its
disk; see [When removing doesn't help](image-cleanup.md#when-removing-doesnt-help).

## Rollbacks matter most

Keep images longer and keep more versions per app. Make sure kubelet's
`imageMaximumGCAge` is unset or longer than `GC_UNUSED_FOR_S`, or kubelet
removes them first.

```yaml
config:
  GC_UNUSED_FOR_S: "86400"
  GC_ROLLBACK_KEEP: "5"
```

## Images only where they run

Some node pools never run your apps: GPU nodes, ingress nodes, the control
plane. Keep new images off them. They still serve what they hold, and rescue
still covers them.

```yaml
config:
  RANK_EXCLUDE_NODE_SUBSTRINGS: "master,control-plane"
  PROPAGATE_EXCLUDE_NODE_SUBSTRINGS: "master,control-plane,gpu,ingress"
```

`PROPAGATE_EXCLUDE_NODE_SUBSTRINGS` defaults to the seed exclusions, so set it
only when the lists differ.

## Seeds only, no spreading

To warm a few nodes and leave the rest to kubelet (or to the mirror):

```yaml
config:
  SPREAD_ENABLED: "false"
  PROPAGATE_ENABLED: "false"
```

Both are needed: `SPREAD_ENABLED` controls blob-by-blob spreading,
`PROPAGATE_ENABLED` the whole-image fallback.

## No CI integration yet

Angry Duck still helps without the webhook: [rescue](rescue.md) unsticks
pulls, [cleanup](image-cleanup.md) keeps disks and rollback images in shape,
and the [mirror](mirror.md), if enabled, serves layers from peers. Add the
webhook when you're ready, and preheat and spreading start working.

## CI runners inside the cluster

Skip the ingress and call the Service directly:

```bash
curl -fsS -m 10 -X POST http://angryduck-controller.angryduck.svc:8080/webhook/preheat \
  -H "Authorization: Bearer ${ANGRYDUCK_WEBHOOK_TOKEN}" \
  -H 'Content-Type: application/json' -d "{\"image\":\"${IMAGE}\"}" || true
```

## Several private registries

Put every registry in one dockerconfigjson, so seeds can pull and the
controller can read manifests from all of them. Label pull metrics by registry
to see which one is slow.

```bash
kubectl -n angryduck create secret generic registry-pull \
  --type=kubernetes.io/dockerconfigjson \
  --from-file=.dockerconfigjson=config.json
```

```yaml
registryCredentials:
  existingSecret: registry-pull

config:
  METRICS_LABEL_REGISTRY: "true"
```

A registry missing from the file still works through the fallback ranking,
but its images move whole instead of layer by layer.
`angryduck_controller_seed_rankings_total` shows it as `basis="repo"` or
`basis="utilization"`.

## arm64 clusters

The controller reads one platform's manifest per image. Set it to your nodes'
architecture:

```yaml
config:
  RANK_PLATFORM: "linux/arm64"
```

Clusters that mix architectures are only partly covered: preheat and spreading
follow `RANK_PLATFORM`. Exclude the other architecture's nodes with
`PROPAGATE_EXCLUDE_NODE_SUBSTRINGS` if their names allow it, and tell us about
your setup in [Discussions](https://github.com/hatam-abolghasemi/Angry-Duck/discussions).

## Nodes that discard unpacked layers

Some distributions set containerd's `discard_unpacked_layers = true`. Angry
Duck still works: layers then travel as snapshots, which are slower, larger
and not digest-verified, and the mirror can't serve them. If you can, switch it
off; see [Node settings](node-settings.md#containerd). The dashboard's
*Snapshot share of shipped layers* panel shows how often snapshots are needed.

## Adding the shared token to a running install

Workers and controller must agree on the token. While they restart with it,
let requests without it through and log them, then switch back:

```yaml
config:
  AUTH_MODE: "warn"
```

Remove the setting once every pod has restarted. The default, `enforce`,
rejects control requests without the token.

## Investigating a problem

```yaml
config:
  LOG_LEVEL: "debug"
```

Debug logs add per-image reporting decisions and the raw disk math. Go back
to `info` afterwards. See [Operations](operations.md#troubleshooting).
