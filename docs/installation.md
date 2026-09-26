# Installation

## Requirements

- **Kubernetes 1.30+**. The worker uses `securityContext.appArmorProfile`.
- **containerd** on every node with the default `overlayfs` snapshotter, and
  `ctr` and GNU `tar` installed on the node. `crictl` is recommended for
  preheat pulls (`CONTAINER_RUNTIME=crictl`).
- **node-exporter** on every node with host networking, reachable at
  `localhost:9100` by default. Workers read disk usage from it.
- **Network**: workers reachable from each other and from the controller on
  port `18081` of the node network.
- **Registry credentials**: a dockerconfigjson covering every private registry
  you push to.
- Optional: the **Prometheus Operator**, for the included ServiceMonitors, and
  **Grafana**, for the [dashboard](observability.md#grafana-dashboard).

Recommended containerd settings:

- `discard_unpacked_layers = false`, so peers can share verified blobs; see
  [Transfers](transfers.md#integrity).
- CRI registry `config_path = "/etc/containerd/certs.d"` if you enable the
  [mirror](mirror.md#requirements).

## Deploy

`deploy/stg` and `deploy/mgmt` are two example environments that differ only
in labels and the ingress host. Copy one and adapt it to your cluster. The
steps below use `stg`.

### 1. Build and push the images

```bash
cd app
REGISTRY=registry.example.com/angryduck
docker build -t $REGISTRY/angry-duck-controller:1.8.4 -f Dockerfile.controller .
docker build -t $REGISTRY/angry-duck-worker:1.8.4 -f Dockerfile.worker .
docker push $REGISTRY/angry-duck-controller:1.8.4
docker push $REGISTRY/angry-duck-worker:1.8.4
```

Set the `image:` fields in `deployment-controller.yaml` and
`daemonset-worker.yaml` to match.

### 2. Create the namespace and secrets

| Secret | Used by | Holds |
|---|---|---|
| `gitlab-docker-registry` | both, as `imagePullSecrets` | Credentials for kubelet to pull the Angry Duck images. |
| `gitlab-registry-pull` | both, mounted as a file | Credentials for seed pulls and manifest reads (`REGISTRY_CREDENTIALS_PATH`). |
| `angryduck-rescue-token` | both, mounted as a file | The shared token. Without it, rescue, propagation and the mirror stay off. |

```bash
kubectl apply -f deploy/stg/namespace.yaml

kubectl -n angryduck create secret docker-registry gitlab-docker-registry \
  --docker-server=registry.example.com --docker-username=<user> --docker-password=<password>

kubectl -n angryduck create secret docker-registry gitlab-registry-pull \
  --docker-server=registry.example.com --docker-username=<user> --docker-password=<password>

kubectl -n angryduck create secret generic angryduck-rescue-token \
  --from-literal=token=$(openssl rand -hex 32)
```

The two registry secrets can hold the same credentials. To cover several
private registries, create `gitlab-registry-pull` from a dockerconfigjson file
listing all of them.

### 3. Review the configuration

Edit `deploy/stg/configmap-env.yaml`. The values most worth checking:

| Setting | Why |
|---|---|
| `RANK_EXCLUDE_NODE_SUBSTRINGS` | Keep control-plane nodes from being chosen as seeds. |
| `CONTAINER_RUNTIME` | `crictl` unless your nodes lack it. |
| `NODE_EXPORTER_URL` | Must reach node-exporter from the host network. |
| `PROPAGATE_MAX_UTILIZATION`, `GC_*` | Keep below kubelet's `imageGCHighThresholdPercent`. |
| `MIRROR_ENABLED` | Enable once containerd's `config_path` is set. |

All settings are described in [Configuration](configuration.md).

### 4. Apply

Delete `servicemonitor-*.yaml` first if you don't run the Prometheus Operator.

```bash
kubectl apply -f deploy/stg/
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

Set the host in `ingress-controller-webhook.yaml`. The ingress exposes only
`/angryduck/webhook/preheat`. The webhook has no authentication of its own, so
restrict it to your CI runners; see [Security](security.md).

## Integrate with CI

Call the webhook right after `docker push`, and never let it fail the
pipeline:

```bash
curl -fsS -X POST https://angryduck.example.com/angryduck/webhook/preheat \
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
      -H 'Content-Type: application/json' -d "{\"image\":\"$IMAGE\"}" || true
```

Propagation to the rest of the fleet starts on its own once the seeds have the
image.

## Next steps

- Import the [Grafana dashboard](observability.md#grafana-dashboard) and set
  up the [suggested alerts](observability.md#suggested-alerts).
- Read [Operations](operations.md) before your first upgrade.
