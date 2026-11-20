# The ground Angry Duck stands on

Angry Duck works one layer below what most Kubernetes users ever see: inside
containerd, on the node, between kubelet and the disk. This page is the
background a maintainer needs before the rest of these pages make sense. It
explains each piece only as far as Angry Duck depends on it, and says where in
the code that dependency lives. If you already run containerd for a living,
skip to [Where Angry Duck sits](#where-angry-duck-sits).

## A container is a process

A running container is an ordinary Linux process with three things set up
around it:

- **Namespaces** give it its own view: its own process IDs, network,
  hostname and mount table. It can't see the node's other processes.
- **A cgroup** limits and counts what it uses: memory, CPU, I/O. Kubernetes'
  `resources.limits` become cgroup settings; `kubectl top` and the memory
  graphs read the cgroup's counters.
- **A root filesystem** made from the image: the directory it sees as `/`.

Nothing else is special. `ps` on the node shows container processes like any
other. A container stops when its first process (the **init process**) exits.

## An image is a list of tar files

An image is a few JSON documents and a stack of compressed tar archives, all
stored by their content hash (**digest**, `sha256:…`):

| Piece | What it is |
|---|---|
| **Index** (optional) | One manifest per platform (`linux/amd64`, `linux/arm64`). |
| **Manifest** | The image for one platform: its config and its layers, each as a digest and a size. |
| **Config** | The command, environment and user to run, and the **diffIDs**: the digest of each layer *uncompressed*. |
| **Layer** | A `tar.gz` (or `tar.zst`) of the files that layer adds, changes or deletes (a deleted file is a "whiteout" entry). |

Because everything is named by its digest, a blob with a given digest is the
same bytes everywhere: on the registry, on any node, from any peer. This is
what makes Angry Duck possible. A node can take a layer from its neighbor
instead of the registry and check it is exactly the right one by hashing it.

A layer has two digests: the compressed one (in the manifest, what you
download) and the uncompressed one (the diffID, what you unpack). The
**chainID** of a layer combines its diffID with the chainIDs of the layers
below it. It names "this layer on top of exactly these layers", the unit
containerd unpacks to.

The code: `internal/imageref` (names), `internal/registryclient` (manifest and
config reads), `LayerSpec` in `internal/worker` (a layer's digest, diffID and
chainID together).

## Pulling an image

A pull is four steps:

1. **Resolve** the name (`nginx:1.27`) to a manifest digest: ask the registry.
2. **Fetch** the manifest, the config, and every layer blob not already on
   the node.
3. **Unpack** each layer, bottom up, into a directory: the snapshot.
4. **Record** the image: name → manifest digest.

Steps 2 and 3 are where the time goes on a big image. Angry Duck shortens
step 2 (blobs from peers, already there before the pod is scheduled) and,
with [unpacking while blobs arrive](../stream-unpack.md), overlaps step 3 with
step 2.

## containerd

containerd is the daemon on each node that pulls images and runs containers.
kubelet talks to it; so does Angry Duck's worker, through the same socket
(`/run/containerd/containerd.sock`). What matters here:

### Namespaces

containerd keeps separate worlds by namespace. Kubernetes uses `k8s.io`.
Everything Angry Duck reads or writes is in `k8s.io` (`ctr -n k8s.io images
ls` shows what kubelet sees; plain `ctr images ls` shows the `default`
namespace, usually empty).

### Content store

Blobs (manifests, configs, compressed layers) live as files named by digest
under `/var/lib/containerd/io.containerd.content.v1.content/blobs/sha256/`.
A blob being written goes to `ingest/` first and is moved into `blobs/` when
complete and checked (**committed**). The worker mounts both: it serves blobs
to peers straight from these files, and with `DIRECT_WRITES` writes received
bytes straight into an ingest file (`directwrite.go`).

### Snapshotter

Unpacked layers live in the **snapshotter**, normally overlayfs, under
`/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/<n>/fs`.
Each committed snapshot is one layer's files, named by its chainID, with a
parent (the layer below). A running container gets one more, writable
snapshot on top; overlayfs stacks them into the container's `/`.

To unpack a layer, containerd **prepares** an active snapshot on its parent,
extracts the tar into it, and **commits** it under the chainID. Angry Duck's
stream unpack does the same itself while the bytes arrive (`streamunpack.go`,
`attach.go`).

Some nodes set `discard_unpacked_layers`: after unpacking, the compressed
blob is deleted. Then a node has the snapshot but not the blob, which is why
rescue can ship snapshots (`blobship`).

### Metadata, leases and garbage collection

containerd's database (`meta.db`) records images (name → manifest digest),
which content and snapshots exist, and what references what. containerd
**garbage-collects** anything nothing references: a blob no image needs, a
snapshot no image or container needs.

Something being built (a blob half-written, a snapshot half-unpacked) isn't
referenced yet, so it is protected by a **lease**: a named hold that keeps it
alive until the work is done or the lease expires. Angry Duck takes a lease
for every piece it writes, so a worker that dies half way leaves nothing
behind for long: the lease expires and containerd cleans up.

### The image record

An image in containerd is just a name pointing at a manifest digest, plus
labels. Deleting an image deletes the name; the blobs and snapshots go at the
next garbage collection if nothing else uses them. Angry Duck's retention
copies are marked with a label on the image record (`RetainLabel`).

### The diff service

containerd's own way to unpack a layer: "apply this blob onto this snapshot".
It needs the whole blob committed in the content store first. When stream
unpack is off, or a layer arrived whole, the worker asks containerd to do it
this way, and the work (and its memory) is containerd's.

### Tasks: init and exec

A container in containerd is a record; to run, it gets a **task**: its init
process. kubelet can also run more processes inside a running container,
each an **exec**: `kubectl exec`, and every **exec probe**
(`livenessProbe.exec`, `readinessProbe.exec`), which kubelet runs every few
seconds. A busy node runs many execs a minute.

### Events

containerd publishes an event for each change, and clients subscribe with
filters on the topic. The worker subscribes to the ones that change what it
cares about (`ctdevents.go`) and nothing else:

| Topic | Means | Angry Duck |
|---|---|---|
| `/images/create`, `/images/update`, `/images/delete` | An image record changed. | Followed: new inventory report. |
| `/content/delete` | A blob was removed. | Followed: new inventory report. |
| `/containers/create`, `/containers/update`, `/containers/delete` | A container record changed. | Followed by image cleanup. |
| `/tasks/start` | A container's init process started. | Followed by image cleanup. |
| `/tasks/exit` | A process exited: the init process **or an exec**. | Followed by image cleanup, except exec exits (below). |
| `/tasks/create`, `/tasks/delete`, `/tasks/oom`, `/tasks/paused`, `/tasks/resumed`, `/tasks/checkpointed` | Other process lifecycle steps. | Not subscribed. |
| `/tasks/exec-added`, `/tasks/exec-started` | An exec was set up, started. | Not subscribed. |
| `/snapshot/prepare`, `/snapshot/commit`, `/snapshot/remove` | Snapshot changes; fire on every container start and stop. | Not subscribed: a pull's snapshots come with its image event. |
| `/namespaces/…` | containerd namespaces. | Not subscribed. |

`/tasks/exit` is the one topic carrying two different things. Its payload has
`container_id` and `id`: for the init process they are equal, for an exec `id`
is the exec's own ID. A subscription filter can only compare a field with a
fixed value, not two fields with each other, so the worker subscribes to the
topic and drops exec exits as they arrive (`followContainerEvent`). A crash
and a stop are both the init process exiting: both are followed, and image
cleanup doesn't need to tell them apart.

Try it on a node: `ctr events` prints every event as it happens (Ctrl-C to
stop). On a node with exec probes, `/tasks/exec-added`, `/tasks/exec-started`
and `/tasks/exit` scroll by every few seconds.

### Registry mirrors: `hosts.toml`

For each registry, containerd reads `/etc/containerd/certs.d/<registry>/hosts.toml`
to learn which hosts to try, in order, before the registry itself. The worker
writes its own address there first (`mirror.go`), so every pull on the node,
kubelet's included, asks Angry Duck's mirror first and the registry only if
the mirror can't help. This needs `config_path = "/etc/containerd/certs.d"`
in containerd's configuration ([Node settings](../node-settings.md)).

## kubelet and CRI

kubelet doesn't speak containerd's own API. It speaks **CRI** (Container
Runtime Interface), a gRPC API that containerd also serves on its socket:

- **ImageService**: `PullImage`, `ListImages`, `ImageStatus`, `RemoveImage`.
- **RuntimeService**: pod **sandboxes** (`RunPodSandbox`, `ListPodSandbox`:
  a pod's shared network namespace, held by a tiny `pause` container) and
  containers (`CreateContainer`, `ListContainers`, `ExecSync` for exec
  probes).

The worker uses CRI where it must see what kubelet sees: which containers use
which images (`runtime.go`, two calls: ready sandboxes and all containers),
and seed pulls made exactly as kubelet would make them (`puller.go`).
Everything else goes through containerd's own API (`containerdstore.go`).

kubelet has its own image garbage collection (`imageGCHighThresholdPercent`,
default 85%): it deletes unused images when the disk fills. Angry Duck's image
cleanup acts earlier and more carefully ([Image cleanup](../image-cleanup.md)),
so kubelet's rarely has anything to do.

`imagePullPolicy` decides whether kubelet pulls at all: `IfNotPresent` uses
the node's copy (a preheated image starts instantly), `Always` checks the
registry for the tag every time (through the mirror, which resolves tags
too).

## Memory: what the numbers mean

Every graph of "container memory" reads the container's cgroup. On cgroup v2
(every node kernel 5.x and later with systemd), each container has a
directory under `/sys/fs/cgroup/kubepods.slice/…/cri-containerd-<id>.scope/`
with these files:

| File / field | What it is |
|---|---|
| `memory.current` | Everything charged to the cgroup, in bytes. |
| `memory.stat` `anon` | The process's own memory: Go heap, stacks, runtime. What "memory use" usually means. |
| `memory.stat` `file` | **Page cache** charged to this cgroup: file contents the kernel keeps in RAM. Split into `active_file` and `inactive_file`. |
| `memory.stat` `slab_reclaimable` | The kernel's cached records about files (inodes, directory entries). |
| `memory.stat` `kernel`, `percpu`, `sock` | Kernel memory used on the process's behalf. |
| `memory.reclaim` (kernel 5.19+) | Write a size, and the kernel takes back up to that much reclaimable memory from this cgroup now. |

Two rules explain almost every surprise:

1. **Page cache is charged to whoever first touches it**, and stays charged
   there. When the worker writes a layer's files, their cache is the
   worker's, even though only the app container will ever read them. There
   is no way on cgroup v2 to hand pages over to another cgroup. That's why
   the worker drops the cache of files it writes (`pageDropper`), drops the
   cache of blobs it reads after a while (`pagecache.go`), and hands the
   rest back through `memory.reclaim` (`internal/memlimit/reclaim.go`).
2. **Cache is not a leak.** The kernel takes back page cache and
   reclaimable slab on its own whenever memory runs short; it can't cause an
   OOM kill. But it shows up in `memory.current` and looks like one.

Kubernetes' **working set** (`container_memory_working_set_bytes`, what
`kubectl top` shows and kubelet's evictions use) is `memory.current` minus
`inactive_file`. `container_memory_usage_bytes` is `memory.current`
(cache included). Different dashboards use different ones; check which
before comparing.

Go itself: the runtime keeps freed heap for reuse and returns it to the OS
only gradually. After a burst of work the worker hands it back explicitly
(`internal/memlimit/idle.go`). Angry Duck's own view of its memory, split by
kind, is `angryduck_process_memory_bytes` ([Resource ledger](resources.md)).

To look at a worker's cgroup on its node:

```sh
CG=$(find /sys/fs/cgroup -type d -name "*$(crictl ps --name '^worker$' -q | head -1)*" | head -1)
grep -E '^(anon|file|active_file|inactive_file|slab_reclaimable|kernel|percpu) ' $CG/memory.stat
cat $CG/memory.current
```

## Where Angry Duck sits

```
            registry (GitLab, Nexus, Harbor, ...)
                 ▲
                 │ only when no node has it
   ┌─────────────┼───────────────────────── node ──┐
   │  kubelet ──CRI──▶ containerd ◀──API + CRI── worker ◀───▶ workers on
   │                    │   ▲  │                    │         other nodes
   │   hosts.toml: ─────┘   │  └─ content store,    │         (blobs, peer
   │   mirror first         │     snapshots ────────┘          to peer)
   │                        └─ pulls ask the worker's mirror first
   └───────────────────────────────────────────────┘
                 ▲ reports, events          │ orders
                 └──────── controller ──────┘  ◀── preheat webhook (CI)
```

- The **worker** (a DaemonSet, one per node, `hostNetwork`) is a containerd
  client like kubelet: it reads and writes blobs, snapshots and image records
  through the socket, reads the content store's files directly, serves blobs
  to peers, and is the registry mirror containerd asks first.
- The **controller** (one Deployment replica) never touches a node. It
  learns from workers' reports what each node holds, and decides who fetches
  what from where.
- **kubelet and containerd never depend on Angry Duck.** If the worker is
  down, the mirror doesn't answer and containerd falls through to the
  registry; nothing Angry Duck left half done is referenced, and its leases
  expire. This is the first of the [rules](rules.md).

## Words

| Word | Meaning |
|---|---|
| blob | Any content-addressed file: a manifest, a config, a compressed layer. |
| digest | `sha256:` hash naming a blob. |
| diffID | Digest of a layer's uncompressed tar. |
| chainID | A layer together with all the layers below it; names a snapshot. |
| snapshot | One unpacked layer's directory in the snapshotter. |
| ingest | A blob being written, before it is committed. |
| lease | A hold that keeps unfinished content from containerd's garbage collection. |
| task | A container's running init process. |
| exec | Another process run inside a running container (exec probes, `kubectl exec`). |
| sandbox | A pod's shared environment, held by the `pause` container. |
| CRI | Kubernetes' API to the container runtime; containerd serves it. |
| preheat | Putting an image on nodes before a pod needs it (the CI webhook). |
| spread | Angry Duck's peer-to-peer delivery of an image's blobs to many nodes. |
| seed | A node that fetches a blob from the registry for the others. |
| rescue | Rebuilding an image on a node from a peer when no registry can give it. |
| page cache | File contents kept in RAM by the kernel; charged to the cgroup that first touched them. |
| working set | `memory.current` − `inactive_file`: what Kubernetes counts. |
