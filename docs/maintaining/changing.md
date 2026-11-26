# Changing things

Checklists for the changes that come up again and again. Each item is
something that was once forgotten.

## A new setting

1. Read it in `cmd/<binary>/main.go` with the `config` getter that fits:
   `Int`, `Duration` (seconds), `Bool`, `Float`, or `Fraction` for a
   utilization between 0 and 1. A value that can't be used falls back to the
   default with a warning; don't parse it yourself.
2. Default it so that a cluster that never sets it behaves well.
3. Add it to `app/.env.example`, `k8s/configmap-env.yaml` and
   `helm/angryduck/values.yaml` (same key, same default).
4. Document it in `docs/configuration.md`, in the section of its feature.
5. A timeout or limit: a setting, never a constant ([rule 6](rules.md#6-no-magic-numbers)).

## A new feature switch

1. Add it to the tree in `internal/features/features.go`, under its parent.
2. In `main.go`, start nothing when it is off: no goroutine, timer, endpoint
   or metric series.
3. Steps 3 and 4 above, and the feature switches section of
   `docs/configuration.md`.

## A new metric

1. Register it next to the code that sets it (`metrics.NewCounterVec`,
   `NewGaugeVec`, or `NewRecentGaugeVec` for a per-image event), with a help
   text that says exactly what it counts, its unit and its labels' values.
2. Bound its labels ([rule 7](rules.md#7-measurable-and-honest-about-it)).
   Initialize the series a dashboard expects at 0, so "nothing happened"
   reads as 0, not as no data.
3. Add it to the metrics tables in `docs/observability.md`.
4. Show it on the dashboard ([below](#a-dashboard-change)) if it answers a
   question someone will ask.
5. A duration (`metrics.NewHistogramVec`, in `worker/histograms.go`): time
   it where the work starts and ends, usually inside the store, not around
   a caller that also does other things (a caller's span silently takes in a
   prepare, a commit or a retry). Every value of a label must mean the same
   span. Record successes only, after the outcome is known. Don't time the
   same span twice under two names. Create the series that can happen at
   zero at startup (`HistogramVec.Init`; `InitTimings` in the worker), only
   for features that are on: a series born holding its first observations
   never shows them in `rate()` or `increase()`, and events that come once
   per image or in bursts (most of Angry Duck's) would never show at all.
   Quantile queries end in `>= 0`, so a window with no samples shows
   nothing rather than `NaN`.

## A wire type or endpoint

1. Types in `internal/model`; both binaries ship together, but a worker may
   briefly talk to an older or newer controller during a rollout: new fields
   are optional, old ones keep their meaning.
2. Endpoints between controller and workers take the shared token
   (`sharedtoken`). Bound request bodies.
3. Document it in `docs/api.md`.

## A containerd call

1. Through `ContainerdStore` (gRPC API) or `runtime.go` (CRI), never a
   shell-out.
2. Count it (`containerdcalls.go` does for gRPC); make it worth its cost:
   share listings, don't re-list what an event already told you.
3. Run the integration tests against containerd 1.7 and 2.2: CI does both,
   run them locally before touching unpacking, snapshots or leases
   ([Development](../development.md#test)).
4. Think about the worker dying in the middle: what does containerd hold
   then, and who cleans it up?

## A dashboard change

1. Edit in Grafana 13, then export with *Export for sharing externally*
   ([Development](../development.md#dashboard)): data source and cluster
   saved empty, no internal registries in the image colors.
2. Run every query through Prometheus's parser (or Explore) and look at the
   panel at 7 days on a busy cluster: no errors, no empty panels that should
   have data ([rule 10](rules.md#10-the-dashboard-is-designed)).
3. Keep colors and column widths to their rules; a new table follows an
   existing one's style.
4. Update the tab's description in `docs/observability.md`.

## A memory or CPU sensitive change

Anything on a per-blob, per-request or per-event path.

1. Before and after on the same cluster for a day: `rss_anon` and
   `heap_live` per worker, CPU per process, containerd calls per minute
   (Runtime tab).
2. For a closer look, build `<version>-pprof` images (the *pprof images*
   workflow) and profile live.
3. Pull-time and chaos runs: the bench kit in `bench/`.

## Tests

- Unit tests next to the code, with fakes (`fakeruntime_test.go`, the
  `blobship.MemStore`, the fake extractors and appliers). A bug fix starts
  with a test that fails without it.
- `go test -race ./...` passes; `gofmt` and `go vet` (with and without the
  `integration` tag) are clean.
- Timing-dependent tests: run them repeatedly (`-count=20`) before
  committing.

## A release

1. `CHANGELOG.md`: a `## X.Y.Z` section, written for the people running
   it: what changed for them, settings by name with defaults.
2. Bump `app/internal/version/version.go`, `helm/angryduck/Chart.yaml`
   (`version` and `appVersion`), the image tags in `k8s/`, and the version
   in `README.md`, `docs/installation.md`, `docs/helm.md` and
   `docs/operations.md`.
3. Commit in small conventional commits, the bump last
   (`chore(release): X.Y.Z`).
4. Push `main`; wait for CI.
5. `git tag -a vX.Y.Z -m "what it brings"` and push the tag: the release
   workflow checks the versions agree, builds and pushes the images and the
   chart, and publishes the release with the changelog section.
6. Roll it out to a test cluster first and watch it for a few days of real
   use before the next.
