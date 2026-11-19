# Rules

What every change must keep. Each rule says why it exists and how to check a
change against it. A pull request that breaks one needs a very good reason,
written in its description.

## 1. containerd and kubelet come first

Whatever happens to Angry Duck (it crashes, is removed, is uninstalled, is
wrong), containerd and kubelet must keep working, and pods must keep
starting.

- Nothing in containerd's or kubelet's path depends on Angry Duck being up.
  The mirror answers `404` for anything it can't serve, and containerd goes
  to the registry. When the controller doesn't answer, the mirror leaves it
  alone for a while (`MIRROR_CONTROLLER_BACKOFF_*_MS`) instead of making
  every pull wait.
- No admission webhooks, no pod spec rewrites, no containerd plugins. Outside
  the worker's state directory, it writes only the mirror's `hosts.toml`
  files, containerd's ingest files (direct writes, under a lease), and, with
  unpacking while blobs arrive on (opt-in, [stream-unpack](../stream-unpack.md)),
  the snapshot directories containerd prepared for it.
- Host mounts (the content store, snapshots) must leave nothing behind that
  containerd can't handle: writes go into containerd's ingest under a lease,
  are aborted on failure, and temporary snapshots carry a prefix that the
  next worker cleans up (`pins.go`).
- Real pulls stay with containerd. Angry Duck decides and delivers ahead of
  time; it is not in the data path of a pull containerd makes on its own,
  beyond the mirror serving what the nodes hold.

**Check:** kill the worker mid-transfer, mid-unpack and mid-rescue in the
integration tests; uninstall it on a test node and start pods.

## 2. Never in a deploy's critical path

- The webhook answers at once; CI calls it with `|| true`.
- A failed preheat, spread, rescue or repair leaves the node as it was:
  kubelet pulls as it would have.
- Freshness of tags is for reliability, never a reason not to share: when the
  registry can't answer, peers still serve, newest copy first.

## 3. Small, and only when used

- **Worker memory: about 10 MiB, 16 MiB at most** (`rss_anon`, not the
  container limit). If a feature isn't in use right now, its memory isn't
  held: no caches kept "just in case", buffers pooled and handed back,
  freed heap returned after bursts (`memlimit`).
- **CPU: no spikes, no busy loops.** A change must not raise CPU or memory on
  a busy cluster, or raise them barely, with a reason.
- Every call is worth making: fewer containerd calls, listings shared,
  nothing re-read that is already known.

**Check:** `angryduck_process_memory_bytes{kind="rss_anon"}` and CPU per
process on the Runtime tab before and after, on a cluster under real load
for a day. For a closer look, the `pprof images` workflow builds profiling
images of any tag ([Development](../development.md#profiling)).

## 4. Events, not intervals

A loop that checks every N seconds adds N seconds of nothing. Prefer
containerd's events, the pod watch, and "something finished" wake-ups
(`wake.go`). Periodic scans stay only as safety nets behind an event stream.

## 5. Every feature is a switch

Each feature is a true/false switch in the [tree](code-map.md#feature-switches).
Off means nothing: no goroutine, timer, endpoint or metric series. Turning a
parent off turns off everything beneath it.

## 6. No magic numbers

Timeouts, limits and thresholds are settings with a documented default,
not constants in the code. Limits that depend on the cluster (how many
transfers per source, how deep a streaming chain) are derived from the node
count or are settings, never a small fixed number.

## 7. Measurable, and honest about it

- Every decision is a log line, a `/status` field and a metric.
- A metric means exactly what its name and help text say. A count that is
  really an estimate says so.
- Label cardinality is bounded: no unbounded labels on counters; per-image
  or per-node series only while something is happening, or as short-lived
  events ([image history](../observability.md#image-history)).
- A feature isn't done until the dashboard shows it.

## 8. Only verified content moves

Blobs move by digest, and containerd checks each one. Snapshots move only in
rescue, as a last resort, with their own checksum. Blobs are always preferred
over snapshots. A tag is resolved by the registry while it can answer.

## 9. Help every image, not only ideal ones

Angry Duck must work for the images and clusters people actually have: any
layer layout, any registry (GitLab, Nexus, Harbor, Docker Hub), gzip and
zstd, containerd 1.7 and 2.2, `discard_unpacked_layers` on or off. A feature
that only helps images built a certain way is a nice extra, not the design.

## 10. The dashboard is designed

Every panel, color and column width was chosen. When changing it:

- **Colors keep one meaning** ([Observability](../observability.md#grafana-dashboard)).
- **Fixed column widths only for columns with predictable values** (counts,
  durations, states); free text (images, nodes, reasons) stays unsized.
- **Queries must hold on a week of a busy cluster.** A stat "in range" that
  is a range query re-reads the whole range at every point: sums go per
  interval (`increase(x[$__interval])`, summed), everything else is an
  instant query. Joins with kube-state-metrics over `[$__range]` are
  expensive: keep them instant, or bound their window and say so in the
  title.
- **Panels don't hide themselves.** Grafana 13's "hide when no data" hid
  panels that had data; rare rows start collapsed instead, and empty panels
  say so.
- Every query must parse: run each through Prometheus's parser (or Explore)
  before committing.

## 11. Small, readable commits

Conventional commit subjects (`fix(worker): ...`, `feat(controller): ...`,
`docs: ...`, `chore(release): X.Y.Z`), one concern each, straight on `main`.
A release is one annotated tag (`git tag -a vX.Y.Z -m "..."`); its message is
the release title, after the version. A released tag is never moved: fix forward with a new
patch version.
