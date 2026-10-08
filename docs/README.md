# Angry Duck documentation

**Start here**

1. [Design](design.md): what Angry Duck is for and why it works this way.
2. [Installation](installation.md) or [Helm](helm.md): get it running.
3. [Recipes](recipes.md): settings for your kind of cluster.

**How each part works**

- [Preheat](preheat.md): the first nodes get a pushed image right away.
- [Propagation](propagation.md): every other node gets it from peers, layer by layer.
- [Rescue](rescue.md): pods stuck in `ImagePullBackOff` get their image from a neighbor.
- [Mirror](mirror.md): containerd's own pulls are served by peers.
- [Image cleanup](image-cleanup.md): disks stay healthy and rollback images stay put.
- [Transfers](transfers.md): how layers actually move between nodes.
- [Architecture](architecture.md): the controller, the workers and an image's whole life.

**Running it**

- [Node settings and limits](node-settings.md): containerd and kubelet settings that matter.
- [Configuration](configuration.md): every setting with its default.
- [Operations](operations.md): status, logs, troubleshooting, upgrades.
- [Observability](observability.md): metrics, the Grafana dashboard, alerts.
- [Security](security.md): privileges, trust boundaries, hardening.
- [API](api.md): controller and worker endpoints.
- [Development](development.md): building and testing.

**Background**

- [How Angry Duck came to be](story.md): the problems that shaped it, one by one.
- [Comparison](comparison.md): similar tools, and why one tool for the whole image lifecycle.
- [The name](name.md): why a duck, why angry, and why the knife.
