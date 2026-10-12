# Helm

The chart in `helm/angryduck` renders exactly what `k8s/` holds: the same
objects, names, labels, settings and defaults. Only the Namespace is left to
Helm's `--create-namespace`. Whatever works with the plain manifests works with
the chart, and the other way round.

## Install

Create the Secrets first, as in
[Installation](installation.md#2-create-the-namespace-and-secrets): the chart
references `registry-pull`, `angryduck-rescue-token` and
`angryduck-webhook-token` but doesn't create them.

```bash
helm upgrade --install angryduck ./helm/angryduck \
  --namespace angryduck --create-namespace \
  -f my-values.yaml
```

`values.yaml` is the example setup, the same as `k8s/`. Your values file only
needs what differs. For example, images from a pull-through
cache, a private pull secret, and the webhook under a shared host's path.

```yaml
imagePullSecrets:
  - name: gitlab-docker-registry

controller:
  image:
    repository: nexus.example.com/docker-ghcr/hatam-abolghasemi/angry-duck-controller

worker:
  image:
    repository: nexus.example.com/docker-ghcr/hatam-abolghasemi/angry-duck-worker

registryCredentials:
  secretName: gitlab-registry-pull

ingress:
  className: ingress-nginx
  annotations:
    nginx.ingress.kubernetes.io/use-regex: "true"
    nginx.ingress.kubernetes.io/rewrite-target: /webhook/preheat
  host: api-stg.example.com
  path: /angryduck/webhook/preheat

config:
  MIRROR_REGISTRIES: ""
```

Keys you set in `config` are merged into the defaults, so the snippets in
[Recipes](recipes.md) drop straight in.

Install into `angryduck`, as the manifests do. In another namespace, set
`config.CONTROLLER_URL` to the controller's Service there.

## Values

| Value | Default | Description |
|---|---|---|
| `imagePullSecrets` | `[]` | For kubelet, when the Angry Duck images come from a private registry. |
| `controller.image.repository` | `ghcr.io/hatam-abolghasemi/angry-duck-controller` | Controller image. |
| `controller.image.tag` | the chart's `appVersion` | |
| `controller.resources` | as in `k8s/` | See [Sizing](operations.md#sizing). |
| `worker.image.repository` | `ghcr.io/hatam-abolghasemi/angry-duck-worker` | Worker image. |
| `worker.image.tag` | the chart's `appVersion` | |
| `worker.resources` | as in `k8s/` | See [Sizing](operations.md#sizing). |
| `worker.tolerations` | `[{operator: Exists}]` | Every node, tainted ones included. |
| `registryCredentials.secretName` | `registry-pull` | dockerconfigjson Secret for seed pulls and manifest reads. |
| `registryCredentials.optional` | `true` | Start without the Secret. Set `false` to make a missing Secret block the pods. |
| `ingress.enabled` | `true` | Expose the webhook. |
| `ingress.className` | `nginx` | Ingress class. |
| `ingress.annotations` | `{}` | Passed through. |
| `ingress.host` | `angryduck.example.com` | |
| `ingress.path` | `/webhook/preheat` | The only path exposed, matched exactly. |
| `serviceMonitor.enabled` | `true` | ServiceMonitors for the controller and the workers (Prometheus Operator). |
| `serviceMonitor.labels` | `{release: prometheus}` | The labels your Prometheus selects ServiceMonitors by. |
| `serviceMonitor.interval` | `15s` | Scrape interval. The dashboard assumes 15s. |
| `config` | as in `k8s/configmap-env.yaml` | Settings for both binaries, as strings. See [Configuration](configuration.md). |

Set `serviceMonitor.enabled: false` without the Prometheus Operator.

## Upgrading

```bash
helm upgrade angryduck ./helm/angryduck -n angryduck -f my-values.yaml
```

Read the [changelog](../CHANGELOG.md) first. Like `kubectl apply`, a change to
`config` alone doesn't restart anything: run
`kubectl -n angryduck rollout restart deploy/angryduck-controller ds/angryduck-worker`.
The controller and workers are independent: the controller rebuilds its state
from worker reports within seconds, and workers keep serving peers during a
controller restart. If a node can't reach the registry,
[bootstrap the new worker image there by hand](operations.md#bootstrapping-a-node-by-hand)
before upgrading.

## Uninstalling

```bash
helm uninstall angryduck -n angryduck
```

Each worker sees its DaemonSet deleted and cleans up its node; see
[Operations](operations.md#uninstalling). The ClusterRole and
ClusterRoleBinding go with the release; the Secrets you created stay.
