# Resource ledger

What an idle worker holds, what it does on its own, and why. [Rule
3](rules.md#3-small-and-only-when-used) and [rule
4](rules.md#4-events-not-intervals) apply: an entry without a reason is a
bug to fix, and a change that adds an entry adds its reason here.

Measured with the bench (`bench/duckbench.sh idle`, `bench`) and `pprof`
images ([Development](../development.md#profiling)). Numbers are per worker,
on a 21-node cluster, unless noted.

## Memory an idle worker holds

| What | Size | Why it's held |
|---|---|---|
| Go runtime: garbage collector bookkeeping, free heap not yet returned, goroutine stacks | ~5 MiB | The cost of a Go process. Freed heap goes back to the OS after work (`memlimit`); stacks are bounded by the goroutines below |
| containerd API type data (protobuf descriptors) | ~1 MiB | Built the first time a containerd call uses each message type, then kept by the protobuf runtime. Avoiding it means not using containerd's client |
| `go-cmp` initialisation | ~0.5 MiB | Never used by Angry Duck: containerd's client package imports `containerd/protobuf`, which imports it. Goes away only with the client |
| HTTP/2 header decoding table | ~0.5 MiB | Needed while talking gRPC to containerd; built once |
| Image list between reports | ~0.5 MiB | Compared against the next listing so unchanged inventory isn't sent |
| Stopped transfers' ingests (`partials`) | 0 when idle; about 200 bytes per blob a transfer stopped part-way | Names the ingest holding bytes already received, so the next transfer of the blob fetches only the rest. Gone when that transfer takes it, the blob lands, the job ends, or after an hour. The bytes themselves stay on disk in containerd's ingest, under the job's lease, as an interrupted pull's do |
| Page cache | 0 when idle | Pages the worker's reads bring in are dropped once a file is idle for `PAGE_CACHE_RELEASE_AFTER_S`; blobs it writes, and files unpacked while blobs arrive, are dropped as they are written |
| Kernel records of files it created (`slab_reclaimable`) | 0 with unpacking while blobs arrive off | With it on, about 1.6 KiB per file while a layer unpacks; handed back a few seconds after the last unpack ends (30 s at most, and never while a request, transfer or unpack is in flight: then at its end) through the cgroup's `memory.reclaim`, logged at info level; once more after the kernel's metadata writeback window (`dirty_expire_centisecs` + `dirty_writeback_centisecs`, read from `/proc/sys/vm`; `RECLAIM_DEADLINE_S` only if they can't be read) when 1 MiB or more is left, since a file's record can't be freed before that (the worker mounts its own cgroup; needs AppArmor allowing mount). Without that access, held until the image is removed or the container nears its limit |

## What an idle worker does on its own

| What | When | Why a timer |
|---|---|---|
| Heartbeat to the controller, with disk usage (`statfs` on containerd's content store) | Every `REPORT_INTERVAL_S` (15s) | Liveness: the controller marks a worker stale after `WORKER_STALE_AFTER_S` of silence |
| Image listing for the report | When containerd's events (or the worker's own pulls) say images changed; a heartbeat reuses the last listing for up to 10 minutes (once a minute while the event stream is down) | Safety net behind the events |
| Layer inventory scan | On events, on reconnect, 30s after a deletion, and every `LAYER_RESYNC_INTERVAL_S` (10 min) | Safety net: containerd's own garbage collection removes blobs without an event |
| Image repair check | 30s after an image or content deletion, on the controller's wakes, and every `IMAGE_REPAIR_INTERVAL_S` (120s) | Kept on purpose as a safety net: files damaged outside containerd announce nothing. Its full listings are a large part of what an idle worker allocates |
| Image cleanup | Disk usage (`statfs`) and the age rule checked every `GC_INTERVAL_S` (60s) on the last listing, in memory; images and running containers listed again only after an image or container event (at most every 30s; the exit of an exec process, such as an exec probe, doesn't count), every 10 minutes as a safety net, and always right before removing anything | Disk filling announces nothing; `statfs` is one system call |
| Rescue pins | One timer for the next pin to expire; nothing while there are none | Time is the event |
| Preheat attribution metric | Every `PREHEAT_ATTRIBUTION_INTERVAL_S` (5 min), but it lists running containers only within `PREHEAT_ATTRIBUTION_RETENTION_S` after a preheat; otherwise it checks an in-memory map and returns | A sample for a dashboard; on an idle node it makes no call |
| Process memory metric | When Prometheus scrapes | Event-driven |
| Returning freed heap to the OS | A few seconds after each burst of work (every loop and request ends with it, probes and scrapes excepted) | Event-driven |

Idle cost measured before these changes (1.14.2, 15 minutes, 21 workers):
69.7 CPU-seconds and 1,985 containerd calls in all, and 54 MB allocated per
worker in 20 minutes, a quarter of it by full content listings on timers.
The 1.14.3 numbers replace these once measured.
