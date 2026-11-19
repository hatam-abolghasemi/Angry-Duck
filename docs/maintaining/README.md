# Maintaining Angry Duck

These pages are for the people who change Angry Duck: fixing a bug, adding a
setting, reviewing a pull request, cutting a release. The rest of `docs/`
explains what Angry Duck does and why ([Design](../design.md),
[Architecture](../architecture.md)); these explain where that lives in the
code and what must stay true when you touch it.

Most of the code was written with an AI assistant, change by change, against
rules set by a human maintainer and checked on real clusters. The rules are
written down here so they outlive any one conversation. Where a page names a
file or a function, it is the code as it is at the time of writing; when you
change one, change the page in the same commit.

## Read in this order

1. [Code map](code-map.md): the packages, what each file is for, what each
   binary starts, and which switch turns what off.
2. [Rules](rules.md): what every change must keep: containerd and kubelet
   unharmed, never in a deploy's critical path, small and event-driven,
   measurable. Read this before writing code.
3. [Features](features.md): each feature from its entry point to its
   metrics, the state it keeps, how it fails, and the tests that hold it.
4. [Changing things](changing.md): checklists for a new setting, switch,
   metric, endpoint, dashboard panel or containerd call, and for a release.
5. [Debugging](debugging.md): from a symptom to the panel, metric, log line
   and code that explain it.
6. [Known limits](limits.md): what Angry Duck doesn't handle yet, and what
   to watch.
7. [Resource ledger](resources.md): what an idle worker holds and does on
   its own, and the reason for each.

[Development](../development.md) has the mechanics: building, the test
suites, the integration tests against a real containerd, CI and the release
workflow.

## The five-minute version

- Two binaries. The **controller** (one replica, all state in memory) decides;
  **workers** (one per node) do, through containerd's socket. Workers report;
  the controller orders.
- Everything is a feature switch in one tree
  ([`internal/features`](../../app/internal/features/features.go)); a feature
  that is off starts nothing.
- Images move as blobs, addressed by digest. containerd verifies every one.
  Snapshots move only as a last resort (rescue).
- Every decision is a log line, a `/status` field and a metric. If you can't
  see it on the [dashboard](../observability.md), it isn't done.
- `go test -race ./...` and the integration tests against containerd 1.7
  and 2.2 must pass; CPU and memory must stay where they were. The
  [Rules](rules.md) page says how to check both.
