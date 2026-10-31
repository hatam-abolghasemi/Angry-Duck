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
    ├── debugpprof/        /debug/pprof, only in builds with -tags pprof
    ├── version/           the release version
    ├── config/            .env loader and typed getters
    └── model/             wire types shared by both binaries
helm/
└── angryduck/             Helm chart, the same objects as k8s/
k8s/                       example Kubernetes manifests
grafana/                   Grafana dashboard
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

Both images are single static binaries on a distroless base. The worker is
built with `-tags grpcnotrace`: gRPC without `golang.org/x/net/trace` and the
`html/template` it brings, about 4 MB less binary, all of it otherwise mapped
in every worker's memory.

### Profiling

Built with `-tags pprof`, either binary serves Go's `/debug/pprof/` on
`DEBUG_PPROF_ADDR` when it is set; `DEBUG_MEMPROFILERATE=1` records every
allocation instead of a sample. Release images don't have the tag.

```bash
CGO_ENABLED=0 go build -tags "grpcnotrace pprof" -o angryduck-worker ./cmd/worker
DEBUG_PPROF_ADDR=127.0.0.1:6060 ./angryduck-worker
go tool pprof -top http://127.0.0.1:6060/debug/pprof/heap
```

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
helm lint helm/angryduck
helm template angryduck helm/angryduck -n angryduck
```

The default render must match `k8s/` object for object, apart from the
Namespace. Change both together: CI renders the chart and fails on any
difference. Keep `version` and `appVersion` in
`helm/angryduck/Chart.yaml` equal to the release.

## Running locally

Copy `app/.env.example` to `app/.env`, set `SELF_ADDRESS` and
`CONTROLLER_URL`, and run the binaries directly. The worker needs a local
containerd and node-exporter.

## Dashboard

The Grafana dashboard is plain JSON. Import it, edit it in Grafana, then export
it with **Share → Export → Save to file** with *Export for sharing externally*
enabled, and replace `grafana/angryduck-dashboard.json`. Before committing,
make sure it carries no cluster of yours: the *Data source* and *Cluster*
variables saved empty, `metadata` reduced to `name`, and no internal
registries in the image color mappings.

## CI and releases

GitHub Actions run two workflows from `.github/workflows/`:

- **CI** (`ci.yml`), on every pull request and push to `main`: `gofmt`, `go
  vet` (with and without the `integration` tag), a build of both binaries for
  amd64 and arm64, the unit tests with `-race`, the integration tests against
  a real containerd (the release binaries of the version the nodes run,
  started next to the runner's own, overlayfs and CRI), and `helm lint` plus a
  check that the chart renders exactly `k8s/`.
- **Release** (`release.yml`), on a pushed `vX.Y.Z` tag: checks that the tag,
  `helm/angryduck/Chart.yaml`'s `appVersion`, the image tags in `k8s/` and a
  `## X.Y.Z` section of `CHANGELOG.md` all agree, builds and pushes
  `ghcr.io/<owner>/angry-duck-controller` and `angry-duck-worker` as `X.Y.Z`
  and `latest` for `linux/amd64` and `linux/arm64`, with an SBOM and build
  provenance attached, pushes the chart to `oci://ghcr.io/<owner>/charts`,
  then publishes the GitHub release with that changelog section as its notes.
  It can be re-run for an existing tag from the Actions tab.

It doesn't wait for CI, so tag only a commit whose CI run passed. To release:
bump the version in `app/internal/version/version.go`, `Chart.yaml`
(`version` and `appVersion`), the image tags in `k8s/` and the examples in
`docs/`; turn the changelog's `Unreleased` section into `## X.Y.Z`; merge;
wait for CI on `main`; then:

```bash
git tag -a vX.Y.Z -m "short description" && git push origin vX.Y.Z
```

The tag's message becomes the release title after the dash.
