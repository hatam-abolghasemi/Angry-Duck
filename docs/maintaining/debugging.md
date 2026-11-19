# Debugging

From a symptom to what explains it. Each starts on the
[dashboard](../observability.md), goes to the metric behind the panel, then to
the logs and the code. [Operations](../operations.md#troubleshooting) has the
operator's view; this is the maintainer's.

Logs are one line per decision, prefixed with the component, the node and
the part: `angryduck-worker[<node>]: spread: ...`, `mirror:`, `image cleanup:`,
`image repair:`, `retention:`, `layer inventory:`. Grep for the prefix. Set
`LOG_LEVEL=debug` on one worker for the per-blob and per-layer lines.
`/status` on the controller has the current state as JSON.

## A pod started slowly

1. **Impact tab:** *Pod Start vs Time Pulling*. Was it the pull?
2. **Image tab**, pick the tag: *Delivery per Node*, the node's row. `via`
   says how it got there: `kubelet` means Angry Duck didn't deliver it in
   time. `took` against the time CI pushed.
3. Was there a spread at all? *Spreads*: no row means no webhook, or a
   manifest that couldn't be read (controller log, `spread`).
4. The spread ran but the node was late: *Nodes Still Missing* and
   *Transfers Running* (Delivery tab) while it runs; afterwards, the node's
   received bytes per path. A node above `PROPAGATE_MAX_UTILIZATION` gets
   nothing new.
5. Code: `controller/spreader.go` (planning), `worker/spread.go` (the
   transfer), `worker/spreadguard.go` (restarts from a faster source).

## A pod is stuck in ImagePullBackOff

1. **Rescue & Repair tab:** *Stuck Pulls* has the cause (registry missing
   the image, unreachable, refusing the pod's secret) and the rescue's state.
2. *Failures in a row* above 0: the source node, the transfer or the
   snapshot rebuild failed; the worker logs on both nodes say which.
3. No row: the controller's pod watch hasn't seen it (controller logs,
   `angryduck_controller_rescue_stuck_pods`), or rescue is off.
4. Code: `controller/rescuer.go`, `worker/rescue.go`.

## Containers can't be created (CreateContainerError)

The image is registered but broken.
[Operations](../operations.md#an-image-is-present-but-containers-cant-be-created)
covers it. `angryduck_worker_broken_image` lists it; repair fixes it piece
by piece (`worker/imagerepair.go`, `image repair:` logs).

## The mirror misses

1. **Overview:** the node table's *mirror hits*; **Mirror & Registry:**
   *Mirror Requests by Result*.
2. `angryduck_worker_mirror_controller_unreachable` at 1: the controller
   didn't answer in time, and this node's mirror is leaving it alone; misses
   go to the registry until it answers.
3. Misses for blobs no node holds are expected: the registry serves them.
4. Code: `worker/mirror.go` (`lookup`, `askTag`), `controller/tags.go`.

## A disk is full

1. **Disk Cleanup tab:** *Cleanup Status*. *stalled* means removing images
   stopped freeing space: the disk is full of something else (logs,
   volumes), and cleanup won't delete images for nothing.
2. *Images Coming Up for Removal*: what goes next, and when.
3. A retention copy is never removed for disk pressure; it goes when it
   expires. Retention copies only go to nodes below
   `GC_RETENTION_MAX_UTILIZATION`.
4. Code: `worker/imagegc.go`.

## A worker's memory stays up

1. **Runtime tab:** *Memory Held per Process* (`rss_anon`): what the
   process holds. *Page cache* row: file cache counted as its memory, next
   to what its page cache tracker holds.
2. Page cache up, tracker at 0: the pages come from a path the tracker
   doesn't see. Both up: a reader never released its file
   (`worker/pagecache.go`; every `openBlob` caller must release).
3. `rss_anon` up, `heap_live` flat: freed heap not handed back (`memlimit`).
   `heap_live` up: something holds memory; profile it with the pprof images
   ([Development](../development.md#profiling)).

## A worker counts as down

1. **Overview:** *Workers Up*. The controller judges by when the last report
   arrived, on its own clock (`WORKER_STALE_AFTER_S`).
2. Worker logs: `report` lines; a `401` is the shared token
   ([Operations](../operations.md#rotating-the-token)).
3. *Nodes That Lost containerd's Events*: the worker fell back to scans,
   slower to report.

## A setting seems ignored

At startup each binary logs its resolved feature switches (`features:`) and
warns about every value it couldn't use (`config:`), with the default it
used instead. A child switch is off when its parent is.
