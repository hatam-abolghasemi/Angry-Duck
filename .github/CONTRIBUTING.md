# Contributing

Thanks for wanting to make Angry Duck better. Bug reports, ideas, docs fixes
and code are all welcome.

## Before you start

- **Questions and ideas** go in
  [Discussions](https://github.com/hatam-abolghasemi/Angry-Duck/discussions).
  For anything bigger than a fix, start there or in an issue, so we agree on
  the approach before you write it.
- **Bugs** go in [Issues](https://github.com/hatam-abolghasemi/Angry-Duck/issues),
  with the version, how you installed it, and the worker and controller logs.
- **Security issues** never go in public: use the
  [private advisory form](https://github.com/hatam-abolghasemi/Angry-Duck/security/advisories/new).

## Making a change

1. Read [Design](../docs/design.md): start early, run in parallel, never
   block, and never make a deploy depend on Angry Duck.
2. Build and test as in [Development](../docs/development.md):
   `cd app && go test -race ./...`, and the integration tests if you touched
   the worker's containerd code.
3. If you change the chart, change `k8s/` the same way. CI fails when the
   chart's default render and `k8s/` differ.
4. Update the docs and add a line to the `Unreleased` section of
   `CHANGELOG.md` when behavior, settings, metrics or the dashboard change.
5. Keep manifests, values and `.env.example` free of comments: explanations
   belong in `docs/`.

## Pull requests

- Title in conventional-commit form: `type(scope): description`, for example
  `fix(rescue): retry when the source node restarts`.
- One concern per pull request, with the why in the description.
- CI must pass: `gofmt`, `go vet`, the unit and integration tests, the arm64
  build and the chart checks.

By contributing you agree that your work is licensed under the
[Apache License 2.0](../LICENSE), and to follow the
[Code of Conduct](CODE_OF_CONDUCT.md).
