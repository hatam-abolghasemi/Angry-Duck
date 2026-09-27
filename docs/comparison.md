# Comparison with similar tools

Angry Duck manages an image's whole life on the node: placing it before a
rollout, spreading it across the fleet, rescuing pulls that fail, and removing
it once nothing needs it. Most tools in this space do one of those steps well.
This page compares them feature by feature, and explains why we put the steps
in one tool.

## At a glance

| | Angry Duck | Spegel | Dragonfly | Kraken | kube-fledged | OpenKruise ImagePullJob | Eraser | kubelet image GC | kuik v2 |
|---|---|---|---|---|---|---|---|---|---|
| **Push-triggered preheat** | Yes, webhook from CI | No | Yes, into seed peers | No | Declared list, refreshed on a schedule | Declared job | No | No | No |
| **Image on every node before pods ask** | Yes, propagation | No | No | No | Yes, each node pulls from the registry | Yes, each node pulls from the registry | No | No | No |
| **Peer-to-peer during a pull** | Yes, mirror (opt-in) | Yes | Yes | Yes | No | No | No | No | No |
| **Registry load per new image** | About `RANK_TOP_N` pulls, changed layers only | One per layer, then peers | One per piece via seed peers | Via origin cluster | One per node | One per node | n/a | n/a | n/a |
| **Rescues `ImagePullBackOff` from peers** | Yes, any image | Indirectly, if the pull retries through the mirror | Indirectly, if in cache | Indirectly, if in cache | No | No | No | No | Routes to another registry |
| **Registry outage, image already in cluster** | Pods start, pinned tags with `IfNotPresent` | Digest pulls; tags need `resolveTags` | From cache | From cache | Only where already cached | Only where already pulled | n/a | n/a | If a replica registry is up |
| **Image cleanup** | Yes, age and disk based | No | Its own cache only | Its own cache only | No | No | Yes, list or scanner based | At disk pressure | No |
| **Keeps rollback images** | Yes, `GC_ROLLBACK_KEEP` per repo | n/a | n/a | n/a | n/a | n/a | No | No | n/a |
| **Needs `discard_unpacked_layers = false`** | No, snapshots as last resort | Yes | No | No | No | No | No | No | No |
| **Extra storage or databases** | None | None | Manager with MySQL and Redis, or a lightweight mode without | Origin storage backend | None | None | None | None | Replica registries |
| **Rewrites Pod specs** | No | No | No | No | No | No | No | No | Yes, mutating webhook |
| **Components** | Controller + DaemonSet | DaemonSet | Manager, scheduler, seed peers, clients | Agents, origin, tracker, build-index | Controller + Jobs | kruise-manager + kruise-daemon | Controller + Jobs | Built in | Operator + webhook |

"Indirectly" means the tool helps only if containerd's retry goes through it
and it happens to hold the content. Nothing detects the stuck pod.

## Tool by tool

### Spegel

A stateless peer-to-peer mirror. Every node serves what its containerd already
has, found through a Kademlia DHT. It is small, needs no storage, and is the
closest tool to Angry Duck's mirror.

It is reactive by design: it helps once a pull is under way, never before. It
does not place images, notice stuck pods or clean up. It requires containerd's
`config_path` and `discard_unpacked_layers = false`, which on some
distributions means changing node images and restarting containerd.

### Dragonfly

A general P2P file distribution system that also accelerates images.
Preheating goes through the Manager's Open API into seed peers, and clients
fetch pieces from each other. It scales to very large fleets and covers more
than images.

The cost is weight. A full install adds a Manager backed by MySQL and Redis, a
scheduler, seed peers and a client on every node, and the clients keep their
own piece cache beside containerd's content store. Preheat fills the seed
peers, not every node, so pods still pull at rollout time.

### Kraken

Uber's P2P registry, built for very large clusters. Agents on every host fetch
from each other and from an origin cluster backed by object storage. It is a
registry you run, not an add-on to the one you have.

### kube-fledged and OpenKruise ImagePullJob

Both pull a declared list of images onto selected nodes ahead of time. That
gets images in place before pods ask, but every node pulls from the registry
on its own, which is exactly the load spike a rollout causes. The list is
declared by hand rather than triggered by a push.

### Eraser

Removes non-running images from every node, from an explicit list or based on
a vulnerability scanner. It is a good fit for security-driven cleanup. It does
not know which images are recent rollback candidates, and it does not know what
a preheat tool just placed on the node.

### kubelet image GC

Built in and always there. It acts at disk pressure (85% by default, down to
80%), oldest first, with an optional maximum age. It knows nothing about
rollbacks or preheated images, so it tends to act late and delete the wrong
things: the image a preheat just placed looks exactly like an unused one.

### kuik v2 and pull-through caches

kube-image-keeper v2 rewrites Pod images to the first available alternative
from mirrors or replica registries you declare in CRDs. Harbor, Nexus and
`registry:2` offer pull-through caches. Both answer registry availability at
the registry layer, which is where registry redundancy belongs. Neither
reduces per-node pulls during a rollout, and neither manages node disks.

## Why one tool

An image's life on a node is one loop: push, place, run, recover, clean up.
Each step needs to know what the others did.

- **Seeding and propagation need the layer inventory.** The controller picks
  seeds by which nodes already hold most of an image's layers, and sends only
  the missing layers. Rescue uses the same inventory to find sources.
- **Cleanup must not undo placement.** A separate GC sees a freshly propagated
  image as unused and removes it before the rollout. Angry Duck's cleanup and
  propagation share one view: propagation waits for cleanup on full disks, and
  cleanup keeps rollback images that rescue and rollbacks rely on.
- **Transfers share one budget.** Preheat, propagation and rescue compete for
  the same node bandwidth and disk. One controller bounds them together, and
  gives rescues their own slots so a big rollout can't starve a stuck pod.
- **Tools on one content store step on each other.** Two tools reacting to the
  same containerd events, each with its own idea of what should be there, fail
  in ways neither was tested for. We hit this directly; see below.
- **One place to look.** Every decision is logged on one line, exposed in
  `/status` and counted in Prometheus, instead of spread across four
  controllers with four dashboards.

## Why we moved off Spegel

We ran Spegel alongside Angry Duck on a ~20-node staging cluster, with Angry
Duck handling preheat, rescue and cleanup, and Spegel handling P2P during
pulls. We removed Spegel after these problems:

- **Mirror targets didn't work with our network setup.** Spegel's chart
  registers a `hostPort` mirror target. With Cilium in partial kube-proxy
  replacement, `hostPort` only works with BPF NodePort enabled, which was too
  broad a change for a shared cluster. We patched the vendored chart to drop
  that target.
- **Cross-node pulls stayed slow.** Pull times still ranged from under a
  second to over 45 seconds for the same kinds of workloads, and changing
  `resolveTags` and other settings didn't fix it.
- **It broke on normal image removal.** When a tag was removed while another
  tag still pointed at the same manifest, Spegel's containerd event handler
  errored, and those errors lined up with cluster-wide peer lookup failures
  ([spegel-org/spegel#1521](https://github.com/spegel-org/spegel/issues/1521)).
  Our own cleanup triggered it, which is the point above about tools stepping
  on each other.
- **It covered only one step.** Even when it worked, it helped only during a
  pull. Placement, rescue and cleanup still needed Angry Duck.

These are our results on our cluster, not a verdict on Spegel. It works well
for many people, especially on clusters where its containerd requirements are
already met.

## What Angry Duck doesn't do

- **It isn't a registry or a replicator.** Registry redundancy belongs in the
  registry layer, with replication or a pull-through cache.
- **It can't help with images the cluster has never pulled.** If the registry
  is down for a brand-new image, no peer has it.
- **It skips `imagePullPolicy: Always`**, including `:latest` and untagged
  images, because kubelet goes to the registry even when the image is local.
- **It doesn't lazy-load images.** Formats such as eStargz, Nydus and SOCI
  start containers before the image is fully downloaded. That is orthogonal and
  can be combined.
- **It only works with containerd.**