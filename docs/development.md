# Development

## Layout

```text
app/
├── cmd/
│   ├── controller/        controller entrypoint
│   └── worker/            worker entrypoint
└── internal/
    ├── controller/        worker registry, ranking, propagation, rescue, HTTP server
    ├── worker/            reporter, puller, transfers, mirror, image cleanup, containerd store and CRI runtime
    ├── blobship/          image walk, per-layer decisions, partial OCI archives
    ├── layerindex/        which node holds which blob or snapshot
    ├── registryclient/    manifest and config reads with registry auth challenges
    ├── registryauth/      dockerconfigjson credential lookup
    ├── imageref/          image reference parsing and normalization
    ├── kube/              minimal in-cluster client (list pods, get the worker's DaemonSet)
    ├── sharedtoken/       bearer token shared by controller and workers
    ├── metrics/           small Prometheus text-format registry
    ├── memlimit/          sets GOMEMLIMIT from the cgroup limit
    ├── logging/           leveled logging
    ├── config/            .env loader and typed getters
    └── model/             wire types shared by both binaries
charts/
└── angryduck/             Helm chart
deploy/
├── stg/, mgmt/            example Kubernetes manifests per environment
└── grafana/               Grafana dashboard
docs/                      documentation
```

The worker talks to containerd with containerd's own Go client and CRI API;
those, gRPC and the OCI types are the module's only dependencies.

## Build

```bash
cd app
go build ./cmd/controller ./cmd/worker
docker build -t angry-duck-controller:dev -f Dockerfile.controller .
docker build -t angry-duck-worker:dev -f Dockerfile.worker .
```

Both images are single static binaries on a distroless base.

## Test

```bash
cd app && go test -race ./...
```

- Transfers are tested end to end over HTTP against an in-memory content store
  (`blobship.MemStore`) that mimics containerd's import and unpack behavior.
- Integration tests run against a real containerd (overlayfs and CRI), as
  root, with the `integration` build tag:

  ```sh
  ANGRYDUCK_CONTAINERD=/run/containerd/containerd.sock \
    go test -tags integration ./internal/worker/
  ```

  They cover import, blob reads, a full rescue that ships a snapshot (with
  and without the content store mounted), rescue to and from a pre-1.8.6
  worker (with its exact `tar` commands), and
  CRI pulls from a test registry, and compare the resulting file trees
  (contents, owners, modes, links, xattrs). With `ANGRYDUCK_NO_MOUNT=1` they
  skip the tree comparison, so they can run with every capability dropped:

  ```sh
  go test -tags integration -c -o worker.test ./internal/worker/
  ANGRYDUCK_CONTAINERD=/run/containerd/containerd.sock ANGRYDUCK_NO_MOUNT=1 \
    setpriv --bounding-set=-all --inh-caps=-all -- ./worker.test -test.run 'StoreBasics|RescueShipsSnapshot|CRI'
  ```

- Timing runs for snapshot shipping use the same harness. Sizes come from the
  environment so a run fits the machine:

  ```sh
  ANGRYDUCK_CONTAINERD=/run/containerd/containerd.sock \
  ANGRYDUCK_BENCH_BASE_MB=300 ANGRYDUCK_BENCH_APP_MB=150 \
    go test -tags integration -run TestBench -v ./internal/worker/
  ```

## Helm chart

```bash
helm lint charts/angryduck
helm template angryduck charts/angryduck -n angryduck \
  --set ingress.enabled=true --set ingress.host=angryduck.example.com --set serviceMonitor.enabled=true
```

Keep `version` and `appVersion` in `charts/angryduck/Chart.yaml` equal to the
release.

## Running locally

Copy `app/.env.example` to `app/.env`, set `SELF_ADDRESS` and
`CONTROLLER_URL`, and run the binaries directly. The worker needs a local
containerd and node-exporter.

## Dashboard

The Grafana dashboard is plain JSON. Import it, edit it in Grafana, then export
it with **Share → Export → Save to file** with *Export for sharing externally*
enabled, and replace `deploy/grafana/angryduck-dashboard.json`.
