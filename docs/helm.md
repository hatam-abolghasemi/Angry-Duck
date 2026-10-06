# Helm

The chart in `charts/angryduck` installs the controller, the worker DaemonSet
and everything they need. It wires names, paths and ports together and
generates the tokens, so what's left to you is what depends on your
environment.

## Install

```bash
helm upgrade --install angryduck ./charts/angryduck \
  --namespace angryduck --create-namespace \
  -f my-values.yaml
```

A typical `my-values.yaml`:

```yaml
registryCredentials:
  existingSecret: registry-pull

ingress:
  enabled: true
  className: nginx
  host: angryduck.example.com
  annotations:
    nginx.ingress.kubernetes.io/whitelist-source-range: 10.20.0.0/16

serviceMonitor:
  enabled: true
  labels:
    release: prometheus

config:
  RANK_EXCLUDE_NODE_SUBSTRINGS: "master,control-plane"
  MIRROR_ENABLED: "true"
  MIRROR_REGISTRIES: "registry.example.com"
```

Then check that every node has a fresh worker, as the install notes show:

```bash
kubectl -n angryduck port-forward svc/angryduck-controller 8080:8080 &
curl -s localhost:8080/status | jq '{fresh_count, workers: [.workers[] | {node_id, fresh}]}'
```

## What you set yourself

### Registry credentials

Seeds pull from your registry and the controller reads manifests from it, so
both need credentials for private images. Create a dockerconfigjson Secret
covering every private registry you push to, and name it in the values:

```bash
kubectl -n angryduck create secret docker-registry registry-pull \
  --docker-server=registry.example.com --docker-username=<user> --docker-password=<password>
```

```yaml
registryCredentials:
  existingSecret: registry-pull
```

`registryCredentials.dockerconfigjson` takes the JSON inline instead, for
values files that are themselves encrypted (with SOPS, for example). The
Angry Duck images on GitHub Container Registry are public, so kubelet needs no
`imagePullSecrets` for them.

### The webhook

CI calls `POST /webhook/preheat` on the controller. To reach it from outside
the cluster, enable the ingress and set its host. The ingress exposes only
that one path (`ingress.path`, `/webhook/preheat` by default), so `/status`,
`/report` and `/metrics` stay inside the cluster.

```yaml
ingress:
  enabled: true
  className: nginx
  host: angryduck.example.com
  tls:
    - hosts: [angryduck.example.com]
      secretName: angryduck-tls
```

CI runners inside the cluster can skip the ingress and call
`http://angryduck-controller.angryduck.svc:8080/webhook/preheat`.

Restricting the ingress to your CI runners' addresses, with your ingress
controller's annotation, is a good extra layer.

### Tokens

The chart creates two Secrets, each with a random 64-character token, and
keeps them across upgrades:

| Secret | Used by | Purpose |
|---|---|---|
| `angryduck-token` | controller and workers | The shared token for rescue, propagation, the mirror and reports. Without it they stay off. |
| `angryduck-webhook-token` | controller | CI's bearer token for the webhook. |

Store the webhook token as a masked CI variable:

```bash
kubectl -n angryduck get secret angryduck-webhook-token -o jsonpath='{.data.token}' | base64 -d
```

To manage the tokens yourself, create Secrets with a `token` key of at least
32 characters and name them in `sharedToken.existingSecret` and
`webhookToken.existingSecret`. Do this with **Argo CD, Flux or `helm
template`**: they render without access to the cluster, so the chart can't
see the existing Secret and would generate a new token on every render.

`webhookToken.enabled: false` leaves the webhook unauthenticated. The
controller logs a warning at startup.

### Monitoring

`serviceMonitor.enabled: true` adds ServiceMonitors for the controller and
the workers. Add the labels your Prometheus selects ServiceMonitors by in
`serviceMonitor.labels`. Import the dashboard from
`deploy/grafana/angryduck-dashboard.json`; see
[Observability](observability.md).

## Values

| Value | Default | Description |
|---|---|---|
| `controller.image.repository` | `ghcr.io/hatam-abolghasemi/angry-duck-controller` | Controller image. |
| `controller.image.tag` | the chart's `appVersion` | |
| `worker.image.repository` | `ghcr.io/hatam-abolghasemi/angry-duck-worker` | Worker image. |
| `worker.image.tag` | the chart's `appVersion` | |
| `imagePullSecrets` | `[]` | For kubelet, when the Angry Duck images come from a private registry. |
| `controller.resources`, `worker.resources` | as in the example manifests | See [Sizing](operations.md#sizing). |
| `controller.nodeSelector`, `.tolerations`, `.affinity`, `.priorityClassName` | empty | Controller scheduling. |
| `worker.nodeSelector`, `.affinity`, `.priorityClassName` | empty | Worker scheduling. |
| `worker.tolerations` | `[{operator: Exists}]` | Every node, tainted ones included. |
| `worker.nodeExporterPort` | `9100` | node-exporter's port on the node IP. |
| `worker.containerd.socket` | `/run/containerd/containerd.sock` | containerd's socket on the node. |
| `worker.containerd.root` | `/var/lib/containerd` | containerd's root on the node. Its content store is mounted read-only for direct blob reads. |
| `worker.containerd.config` | `/etc/containerd/config.toml` | containerd's config file on the node. |
| `worker.containerd.certsDir` | `/etc/containerd/certs.d` | Where the mirror writes `hosts.toml`. Must be in containerd's `config_path`. |
| `worker.stateDir` | `/var/lib/angryduck` | Node directory for rescue pins and image-usage history. |
| `sharedToken.existingSecret` | empty | Use this Secret's `token` instead of a generated one. |
| `webhookToken.enabled` | `true` | Require a bearer token on the webhook. |
| `webhookToken.existingSecret` | empty | Use this Secret's `token` instead of a generated one. |
| `registryCredentials.existingSecret` | empty | dockerconfigjson Secret for seed pulls and manifest reads. |
| `registryCredentials.dockerconfigjson` | empty | The same, inline. |
| `ingress.enabled` | `false` | Expose the webhook. |
| `ingress.host` | empty | Required when the ingress is enabled. |
| `ingress.className` | empty | Ingress class. |
| `ingress.path` | `/webhook/preheat` | The only path exposed, matched exactly. |
| `ingress.annotations`, `ingress.tls` | empty | Passed through. |
| `serviceMonitor.enabled` | `false` | Add ServiceMonitors (Prometheus Operator). |
| `serviceMonitor.labels` | `{}` | Extra labels, for your Prometheus's selector. |
| `serviceMonitor.interval` | `15s` | Scrape interval. The dashboard assumes 15s. |
| `config` | the defaults in [Configuration](configuration.md) | Settings for both binaries, as strings. |

The chart sets these settings itself, and ignores them in `config`: the
listen addresses, the token and credential paths, `CONTAINER_RUNTIME`,
`CONTAINER_RUNTIME_ENDPOINT`, `CONTAINERD_ROOT`, `RESCUE_STATE_DIR`,
`MIRROR_CONTAINERD_CONFIG_DIR` and `DAEMONSET_NAME`. `CONTROLLER_URL`
defaults to the controller's Service and can be overridden.

A change to `config` restarts the controller and the workers.

## Upgrading

```bash
helm upgrade angryduck ./charts/angryduck -n angryduck -f my-values.yaml
```

Read the [changelog](../CHANGELOG.md) first. The controller and workers are
independent: the controller rebuilds its state from worker reports within
seconds, and workers keep serving peers during a controller restart. If a node
can't reach the registry,
[bootstrap the new worker image there by hand](operations.md#bootstrapping-a-node-by-hand)
before upgrading.

## Uninstalling

```bash
helm uninstall angryduck -n angryduck
```

Each worker sees its DaemonSet deleted and cleans up its node; see
[Operations](operations.md#uninstalling). The ClusterRole and
ClusterRoleBinding go with the release.
