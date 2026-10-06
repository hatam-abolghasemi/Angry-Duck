# Propagation

Once a seed has the pushed image, the controller spreads it from node to node
until every eligible node has it (`PROPAGATE_WINDOW_S=0`, the default) or a
newer tag of the same repo supersedes it. Only the seeds ever talk to the
registry.

## How it spreads

Every `PROPAGATE_INTERVAL_S`, and about a second after any transfer finishes,
the controller pairs nodes that lack the image with nodes that have it, and
orders each receiver to fetch it from its peers using the
[transfer mechanism](transfers.md). A finished transfer frees its source and
adds a holder, so the next round starts right away instead of waiting for
the interval.

- **One node at a time per source.** With `PROPAGATE_PER_SOURCE=1`, each
  holder finishes one receiver before starting the next. The first nodes are
  ready as early as possible, and every finished node becomes a source: 5
  holders, then 10, 20, 40.
- **Waiting pods first.** Receivers are served in this order: nodes where a
  pod is already waiting for the image, then nodes lacking the fewest bytes.
  Sources holding the image's blobs are preferred.
- **Bounded load.** At most `PROPAGATE_PER_SOURCE` sends per node and
  `PROPAGATE_MAX_CONCURRENT` transfers cluster-wide. Rescues have their own
  slots.
- **Disk-aware.** Nodes above `PROPAGATE_MAX_UTILIZATION` wait until
  [image cleanup](image-cleanup.md) brings them down, then receive the image.
- **Retries back off** exponentially after failures.
- Nodes matching `PROPAGATE_EXCLUDE_NODE_SUBSTRINGS` (default: the same as
  `RANK_EXCLUDE_NODE_SUBSTRINGS`) are skipped. Rescue still covers them if a
  pod there needs the image.

## Slow seeds

Seeds are left alone while they pull from the registry. A seed still pulling
after

```text
max(PROPAGATE_SEED_TIMEOUT_MIN_S, PROPAGATE_SEED_TIMEOUT_FACTOR × the first seed's pull time)
```

or after `PROPAGATE_SEED_TIMEOUT_MAX_S` while no seed has finished, has its
pull cancelled and gets the image from peers instead. The cancel comes first,
so two writers never fill one content store.

## Watching it

```bash
curl -s localhost:8080/status | jq '.propagations[] | {image, have: (.have|length), missing, in_flight, failing}'
```

`angryduck_controller_propagation_nodes{state}` shows, per image, how many
nodes `have` it, are `missing` it, are `in_flight`, are still `seeding`, or
were `skipped`. `angryduck_controller_propagations_total{result}` counts
finished propagations as `complete`, `superseded` or `expired`.

## Settings

`PROPAGATE_ENABLED`, `PROPAGATE_INTERVAL_S`, `PROPAGATE_WINDOW_S`,
`PROPAGATE_MAX_CONCURRENT`, `PROPAGATE_PER_SOURCE`,
`PROPAGATE_MAX_UTILIZATION`, `PROPAGATE_SEED_TIMEOUT_*`,
`PROPAGATE_EXCLUDE_NODE_SUBSTRINGS`. Propagation needs the shared token. See
[Configuration](configuration.md#propagation).
