# Angry Duck documentation

**Getting started**

- [Architecture](architecture.md): components, reporting, state, and an image's life.
- [Installation](installation.md): requirements, deployment and CI integration.
- [Configuration](configuration.md): every setting with its default.

**Features**

- [Preheat](preheat.md): seeding pushed images onto the best nodes.
- [Propagation](propagation.md): spreading images from node to node.
- [Rescue](rescue.md): unsticking pods in `ImagePullBackOff`.
- [Transfers](transfers.md): how layers move between nodes.
- [Mirror](mirror.md): a pull-only, peer-backed mirror for containerd.
- [Image cleanup](image-cleanup.md): removing unused images before disks fill.

**Running it**

- [Operations](operations.md): status, logs, troubleshooting, upgrades.
- [Observability](observability.md): metrics, Grafana dashboard, alerts.
- [API](api.md): controller and worker endpoints.
- [Security](security.md): privileges and trust boundaries.
- [Development](development.md): building and testing.

**Background**

- [Comparison](comparison.md): similar tools, and why one tool for the image lifecycle.
- [The name](name.md): why a duck, why angry, and why the knife.
