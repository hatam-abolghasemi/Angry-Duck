# Known limits

What Angry Duck doesn't handle yet, or handles with a known cost. Each says
where it lives, so whoever takes it on starts in the right place. When you
fix one, remove it here.

## One controller, all in memory

The controller is a single replica with no storage. During an upgrade the
Deployment stops the old one before starting the new one (`Recreate`), so
for a few seconds there is none: a preheat webhook in that window gets no
answer (the image is pulled from the registry, as without Angry Duck), and
mirrors send containerd straight to the registry. Running two at once is not
supported: each would plan on its own and give conflicting orders. High
availability would need a leader election and state handover; nothing in the
code prepares for it yet.

## One platform per cluster

The controller plans for one platform, `RANK_PLATFORM` (default
`linux/amd64`). A cluster with arm64 and amd64 nodes would send amd64 layers
to arm64 nodes. Multi-architecture clusters need the platform per node in
the reports and in planning (`controller/spreader.go`, `ranker.go`).

## Cleanup trusts the node's clock

Image usage times are wall-clock times saved in the worker's state file
(`worker/imagegc.go`). A node whose clock jumps forward by hours (a bad
clock corrected by NTP) sees every unused image as long unused and removes
them at once. Running images are safe; the node loses its rollback copies
and re-pulls them. Monotonic time can't be saved across restarts, so a fix
would bound how far a saved time may be trusted.

## Page cache charged to the worker

Workers' container memory staying tens of MiB up for an hour or more was
traced in 1.14.3 to unpacking while blobs arrive: the page cache of the
small files it wrote (fixed: every file's pages are dropped once written)
and the kernel's records of those files (`slab_reclaimable`), which
stayed charged to the worker until the image was removed or the container
neared its limit (fixed: handed back through `memory.reclaim` after each
unpack, where AppArmor allows the worker to mount its cgroup). With that feature off, the bench shows workers flat at about
10–15 MiB through spreads. The Runtime tab's Page cache row and
`slab_reclaimable` show it if it comes back. See
[Debugging](debugging.md#a-workers-memory-stays-up).

## Unpacking while blobs arrive

Opt-in, and needs privileges ([stream-unpack](../stream-unpack.md)). Before
1.14.3, only files of 1 MiB or more had their page cache dropped: a layer of
small files (a `vendor/` tree) left tens of MiB per worker charged to its pod
until the image was removed. The kernel's records of the files written
(`slab_reclaimable`) are handed back through `memory.reclaim` a few seconds
after the last unpack; where the worker can't mount its cgroup (AppArmor,
Kubernetes before 1.30, cgroup v1) they stay until the image is removed or
memory runs short. zstd
layers are unpacked once they land: their decoder holds a large window in
memory. Before 1.14.2, with parallel unpacking on, it never ran (a layer
waited for the layer below to be unpacked by the same spread); watch
`angryduck_worker_spread_unpacks_total{result=~"streamed|stream_failed"}`
on the first clusters running it.

## What the dashboard can't see

- Which blobs each node holds is exported only for transfers running right
  now; per-node blob tables would need a new metric with one series per blob
  and node, too many for Prometheus on a large cluster.
- *Images on Each Node* knows an image's size and arrival only if Angry Duck
  delivered it within the time range.
- Images are matched to pods on the reference as written in the pod spec;
  short Docker Hub names (`nginx:1.27`) don't match Angry Duck's normalized
  `docker.io/library/nginx:1.27`.
- *Busiest Workloads* covers the last 24 hours: it joins every pod of the
  range, too much for one query over a week of a busy cluster.
