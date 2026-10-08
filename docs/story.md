# How Angry Duck came to be

Angry Duck didn't start as an image lifecycle manager. It started as one
question, asked during yet another slow rollout, and grew one problem at a
time.

## "Why is nobody fetching it yet?"

Our clusters see hundreds of deploys a day. Every rollout looked the same: CI
built an image and pushed it, then spent a few more seconds updating manifests
and waiting for the sync. Only when pods were finally scheduled did the nodes
start downloading, all of them at once, all asking the registry for the same
layers.

The image had been sitting in the registry the whole time. The cluster just
didn't know it would need it.

So the first idea behind Angry Duck was simple: when CI pushes an image, tell
the cluster right away, with a webhook, and let a few nodes start pulling while
the pipeline is still busy with everything else. The point was
parallelism. Nothing that can start should wait for something else to finish.
That idea still shapes every part of the design.

## The registry went down

Then the registry had a bad day. Rollouts stopped, and so did rollbacks,
because a node can't pull from a registry that isn't there.

The images we needed weren't gone, though. Most of them were already on other
nodes in the same cluster. If preheated images could travel from node to node
instead of from the registry, the registry would matter less on good days and
much less on bad ones. That became propagation: the registry serves each new
layer about once, and nodes share the rest among themselves.

## The image was right next door

Sometimes the registry was up and the image still couldn't be pulled: a
broken route from one node, an image deleted upstream, a tag that no longer
resolved. The pod sat in `ImagePullBackOff`, while the exact image it needed
was on the node next to it.

Nothing in Kubernetes looks for that. So Angry Duck started watching for stuck
pods and copying their image over from a neighbor. That became
[rescue](rescue.md).

## The disk was full of logs

Then nodes hit disk pressure, and images weren't even the reason. Some apps
logged far too much, and on most nodes container logs and images share the
same disk.

As DevOps engineers we can't make an app log less. We can make sure images
never make it worse. kubelet's image GC waits until the disk is already under
pressure, and then deletes images without knowing which ones the next rollback
needs. And putting every new image on every node, which propagation now did,
would only make disks fill faster.

So cleanup became the other half of distribution: remove images nothing has
used for hours, free space early when a disk runs high, keep the last few
images of every app for rollbacks, and don't send new images to a node that is
already full. And when the disk is full of something other than images, stop
deleting rather than throw away rollbacks for nothing. That became
[image cleanup](image-cleanup.md).

## Trying what already existed

We didn't want to build this. We tried [Dragonfly](comparison.md#dragonfly)
first, then [Spegel](comparison.md#spegel), and ran Spegel alongside early
versions of Angry Duck for a while. Neither made us happy. Each solved part of
the problem, and the parts didn't fit together: a separate garbage collector
deletes freshly propagated images as "unused", and two tools reacting to the
same containerd events step on each other. The comparison page tells
[why we moved off Spegel](comparison.md#why-we-moved-off-spegel) in detail.

The problems were really one problem: an image's life on a node, from the push
to the cleanup. It needed one tool that knew every step.

## Building it

Angry Duck was built with Claude as a coding partner. We did the design,
reviewed every change, and tested each release on a real cluster before
publishing it. With that loop, building the tool we actually wanted was
easier than continuing to live without it.

## Why the name

Docker gave us a whale, Kubernetes a ship's wheel and pods of whales. Big
animals, big ships. But the trouble in a rollout happens in small places: one
node, one pull, one full disk.

So we sent ducks. Small, everywhere, out of the way, and ruthless about the
rules. One on every node, with a knife. We also wanted a name people would
remember. The [full explanation](name.md) has more.

## Where it is now

Today Angry Duck preheats pushed images, spreads them layer by layer, serves
containerd's pulls from peers, rescues stuck pods, and keeps disks clean
without losing rollbacks. It is an image lifecycle manager for Kubernetes,
and it's still growing. If your cluster has a problem we haven't met yet,
[tell us](https://github.com/hatam-abolghasemi/Angry-Duck/discussions).
