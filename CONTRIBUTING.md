# Contributing

Thanks for your interest in improving sandbox-operator!

## Before you start

- Read the [Code of Conduct](CODE_OF_CONDUCT.md).
- For anything beyond a small fix, open an issue first so we can agree on the
  direction before you invest in an implementation.
- The `Sandbox` API and its controller live upstream in
  [kubernetes-sigs/agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox).
  A change to the API, or to how a Pod-backed sandbox is reconciled, belongs
  there; this repository consumes the module.

## Developer setup

Go 1.27+ is required.

```bash
make all         # fmt-check vet test build
make test-race   # unit tests with the race detector
make generate    # NodeInventory CRD + deepcopy (idempotent — commit its output)
make api-docs    # regenerate docs/api.md from api/v1beta1
make lint        # golangci-lint v2 on every target OS
```

`make generate` regenerates the `NodeInventory` CRD and the deepcopy functions
from `api/v1beta1`; its output is committed.

The build-tagged harnesses under `test/` (`l2bench`, `l3bench`,
`envdproxysmoke`) and the package benchmarks in `pkg/scale` and `pkg/e2bcompat`
back the claims in [PERFORMANCE.md](PERFORMANCE.md) and
[docs/scaling-design.md](docs/scaling-design.md). If your change touches a
claimed code path, re-run the relevant harness and update the document rather
than editing numbers by hand. `make vet-tagged` type-checks all three.
`cmd/sandbox-sdk-loadgen` is the persistent-client create-latency driver for a
live cluster; it is bench tooling, not a released binary.

## Pull requests

- Keep changes focused; unrelated refactors belong in their own PR.
- Commit messages: a one-line summary, optionally followed by a body that
  explains *why* the change is needed.
- Tests should encode the intent of the change — if the business rule changes
  and the test still passes, the test is wrong. The store, claim-gateway and
  proxy contracts documented under `docs/` are pinned by tests on purpose; do
  not weaken them to make a change pass.
- CI must be green: `test` (coverage, tagged harnesses, build), `lint`, and
  `build` under `.github/workflows/`.

## Developer Certificate of Origin

Contributions are accepted under the
[Developer Certificate of Origin](https://developercertificate.org/). Sign off
your commits (`git commit -s`) to certify that you have the right to submit
the work under this repository's license.

## License

By contributing you agree that your contributions are licensed under
[AGPL-3.0](LICENSE). The `agents.x-k8s.io` and `extensions.agents.x-k8s.io`
types come from the Apache-2.0 `sigs.k8s.io/agent-sandbox` module and are used
as a dependency; do not copy upstream source into this tree.
