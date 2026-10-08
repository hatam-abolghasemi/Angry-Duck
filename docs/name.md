# Why "Angry Duck" 🦆🔪

## The harbor

Containers came by sea. Docker gave us a whale carrying them on its back.
Kubernetes gave us a helmsman at the wheel, and it named its smallest unit
after a group of whales: a *pod*. Images arrive from a registry, the harbor
where everything is stored.

It's a lovely picture of big things: the whale, the ship, the harbor. But
anyone who has watched a rollout knows the real trouble happens in the small
places. On one node, one pull, one full disk, one pod stuck waiting.

The big animals don't go there. So we sent ducks.

## The ducks

Every node in the cluster has a duck. That's the worker, a DaemonSet, one duck
per pond. There is also one duck who doesn't swim. That's the controller, who
stands on the dock, watches every pond and shouts orders.

Ducks are small. Each one does small things: pull an image, send a few layers
to a neighbor, delete an old tag, report what's in its pond. None of these
things is ever in anyone's way. A pod never waits for a duck. If a duck is
asleep, containerd goes to the harbor exactly as it always did. CI calls the
duck with `|| true`, because a duck's opinion should never fail a pipeline.

Cute, harmless, non-blocking.

Also completely ruthless.

## Why angry

The whale is patient. The helmsman is patient. kubelet is so patient it waits
until the disk is 85% full before it does anything at all.

The ducks are not patient. They have rules, and they follow them without
waiting for anyone to be in trouble first.

**They don't wait for the rollout.** The moment CI pushes an image, the dock
duck picks the five ponds that already hold most of its layers and sends them
to fetch it. Only those five ever bother the whale. The rest of the fleet gets
the image from each other, before a single pod asks for it.

**They don't ask which node the pod will land on.** Pods get rescheduled.
Nodes drain, spot instances vanish, the scheduler changes its mind. So the
ducks don't guess. The image goes to *every* pond, layer by layer, duck to
duck, each layer swimming on as soon as any duck has it, until the whole
fleet has it. When a pod lands anywhere, its image
is already there. Rescheduling should cost seconds, not a trip to the harbor.

**They don't wait to be asked for help.** A pod stuck in `ImagePullBackOff`
while its image sits one pond over makes a duck very angry. The dock duck
spots it, finds a neighbor that has the image, and has the missing layers
swum across. Nobody files a ticket. kubelet's next retry just works.

**They don't wait for disk pressure.** An image nothing has used for six hours
is gone. Not "at 85%", not "when kubelet panics": six hours, then out. If the
pond gets above 70% anyway, the oldest unused images go first until it's back
under 60%. A duck keeps its pond clean *before* it floods.

**But they are rational about rollbacks.** Ruthless isn't careless. The newest
three unused images of every repo stay the longest, because the most likely
image you'll need next is the one you just replaced. Anything with a running
container is never touched. Rolling back should come from the pond, not from
the harbor.

That's the anger: not noise, just a refusal to wait. Every rule exists
because someone once waited for something that was avoidable.

## Why a duck and not a bigger animal

Because being small is the design.

A whale would be a registry. Big, central, and something everyone depends on.
A goose would stand in the path and hiss: a proxy every pull must go through.
A ship would be a platform with a database and a console.

A duck is none of those. It sits beside containerd, uses the node's own tools,
and gets out of the way. If you remove every duck from the cluster, the
cluster works exactly as it did before. It just waits more.

## And the knife

🔪

The knife is what makes the duck ruthless, and what keeps it careful.

It cuts the whale out of most of the rollout: a handful of pulls instead of
one per node. It cuts transfers down to only the layers a pond is missing, and
containerd checks every digest, so nothing slips in. It cuts unused images off
disks on a schedule, a few at a time, and never what's running or what you
might roll back to.

A small bird, a sharp knife and a short list of rules. That's all it takes to
keep a harbor full of whales moving.

And a name you remember. Somewhere among the whales, the ships and the
harbors, there are small ducks with knives in hand, taking care of things.