# Installation

## Requirements

- **Kubernetes 1.30+**.
- **containerd 1.6+** on every node with the default `overlayfs` snapshotter
  and the CRI plugin (as kubelet uses it). No node tools are needed.
- **node-exporter** on every node with host networking, on port `9100`.
  Workers read disk usage from it at the node's IP.
- **Network**: workers reachable from each other and from the controller on
  port `18081` of their pod IPs, and node-exporter reachable from pods at
  the node IP.
- **Registry credentials**: a dockerconfigjson covering every private registry
  you push to.
- Optional: the **Prometheus Operator**, for the included ServiceMonitors, and
  **Grafana**, for the [dashboard](observability.md#grafana-dashboard).

Check the containerd and kubelet settings in
[Node settings and limits](node-settings.md) first: `discard_unpacked_layers`
and `serializeImagePulls` in particular decide how much Angry Duck can do.

## Deploy with Helm

The chart in `charts/angryduck` is the simplest way to install. It generates
the tokens, wires every path and name together, and leaves you the
environment-specific parts: registry credentials, the webhook's ingress host,
and monitoring. See [Helm](helm.md).

## Deploy with plain manifests

`k8s/` holds a complete set of manifests with the default settings. Copy them
and adapt them to your cluster.

### 1. Get the images

Every release publishes both images to GitHub Container Registry, tagged with
the version (`1.8.11`) and `latest`:

```text
ghcr.io/hatam-abolghasemi/angry-duck-controller:1.8.11
ghcr.io/hatam-abolghasemi/angry-duck-worker:1.8.11
```

They are public, so kubelet needs no `imagePullSecrets` for them. If you
build your own into a private registry, add `imagePullSecrets` to
`deployment-controller.yaml` and `daemonset-worker.yaml`.

To build and push your own instead:

```bash
cd app
REGISTRY=registry.example.com/angryduck
docker build -t $REGISTRY/angry-duck-controller:1.8.11 -f Dockerfile.controller .
docker build -t $REGISTRY/angry-duck-worker:1.8.11 -f Dockerfile.worker .
docker push $REGISTRY/angry-duck-controller:1.8.11
docker push $REGISTRY/angry-duck-worker:1.8.11
```

Set the `image:` fields in `deployment-controller.yaml` and
`daemonset-worker.yaml` to match.

### 2. Create the namespace and secrets

| Secret | Used by | Holds |
|---|---|---|
| `registry-pull` | both, mounted as a file | Credentials for seed pulls and manifest reads (`REGISTRY_CREDENTIALS_PATH`). Optional if every image you push is public. |
| `angryduck-rescue-token` | both, mounted as a file | The shared token. Without it, rescue, propagation and the mirror stay off. |
| `angryduck-webhook-token` | controller, mounted as a file | CI's token for the webhook. Without it, the webhook is unauthenticated. |

```bash
kubectl apply -f k8s/namespace.yaml

kubectl -n angryduck create secret docker-registry registry-pull \
  --docker-server=registry.example.com --docker-username=<user> --docker-password=<password>

kubectl -n angryduck create secret generic angryduck-rescue-token \
  --from-literal=token=$(openssl rand -hex 32)

kubectl -n angryduck create secret generic angryduck-webhook-token \
  --from-literal=token=$(openssl rand -hex 32)
```

To cover several private registries, create `registry-pull` from a
dockerconfigjson file listing all of them; see
[Recipes](recipes.md#several-private-registries).

### 3. Review the configuration

Edit `k8s/configmap-env.yaml`. The values most worth checking:

| Setting | Why |
|---|---|
| `RANK_EXCLUDE_NODE_SUBSTRINGS` | Keep control-plane nodes from being chosen as seeds. |
| `CONTAINERD_ROOT` | Only if containerd's root isn't `/var/lib/containerd`; change the DaemonSet's content-store mount to match. |
| `NODE_EXPORTER_URL` | Set in the DaemonSet to the node IP; change the port there if yours differs. |
| `PROPAGATE_MAX_UTILIZATION`, `GC_*` | Keep below kubelet's `imageGCHighThresholdPercent`. |
| `MIRROR_ENABLED` | Enable once containerd's `config_path` is set. |

All settings are described in [Configuration](configuration.md), and
[Recipes](recipes.md) has ready-made sets for common setups.

### 4. Apply

Delete `servicemonitor-*.yaml` first if you don't run the Prometheus Operator.

```bash
kubectl apply -f k8s/
```

### 5. Verify

```bash
kubectl -n angryduck get pods -o wide
kubectl -n angryduck logs deploy/angryduck-controller | grep -E 'rescuer started|propagator started|WARNING'
kubectl -n angryduck port-forward svc/angryduck-controller 8080:8080 &
curl -s localhost:8080/status | jq '{fresh_count, workers: [.workers[] | {node_id, fresh}]}'
```

Every node should show `"fresh": true`, and the controller log should show the
rescuer and propagator starting without warnings.

### 6. Expose the webhook

Set the host and ingress class in `ingress-controller-webhook.yaml`. The
ingress exposes only `/webhook/preheat`. Store the `angryduck-webhook-token` value as a
masked CI variable, `ANGRYDUCK_WEBHOOK_TOKEN`, and send it as a bearer token.
Restricting the ingress to your CI runners as well is a good idea; see
[Security](security.md).

## Integrate with CI

Call the webhook right after `docker push`, before the deploy job updates
the manifests, and never let it fail the pipeline. The call returns within
seconds, and distribution continues while the rest of the pipeline runs.

```bash
curl -fsS -m 10 -X POST https://angryduck.example.com/webhook/preheat \
  -H "Authorization: Bearer ${ANGRYDUCK_WEBHOOK_TOKEN}" \
  -H 'Content-Type: application/json' -d "{\"image\":\"${IMAGE}\"}" || true
```

GitLab CI example:

```yaml
build:
  script:
    - docker build -t "$IMAGE" .
    - docker push "$IMAGE"
    - >
      curl -fsS -m 10 -X POST "$ANGRYDUCK_WEBHOOK"
      -H "Authorization: Bearer $ANGRYDUCK_WEBHOOK_TOKEN"
      -H 'Content-Type: application/json' -d "{\"image\":\"$IMAGE\"}" || true
```

Propagation to the rest of the fleet starts on its own once the seeds have the
image.

## Next steps

- Import the [Grafana dashboard](observability.md#grafana-dashboard) and set
  up the [suggested alerts](observability.md#suggested-alerts).
- Read [Operations](operations.md) before your first upgrade.
