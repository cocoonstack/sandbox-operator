# Roadmap

Direction, not commitment — sequenced by current priority. Issues and PRs that
move any of these forward are welcome.

## Near term

- **Per-pool hypervisor engine axis (`ch` | `fc`).** sandboxd keys warm pools
  by `(template, net, size)` and every sandbox on a node boots with the
  hypervisor cocoon is configured for; there is no engine axis anywhere yet.
  It starts in sandboxd (pool config, `PoolKey`, metrics), then reaches
  `NodeInventory`, `SandboxTemplate`/`SandboxWarmPool` and the warm-pool
  driver so a single node can run mixed-engine pools under operator control.
- **Engine-labeled pool metrics.** Once the engine axis exists, `sandboxd_pool_*`
  needs it as a label and the warm-pool driver needs it in the pool key; today
  both key on template/net/size only.

## Medium term

- **L2 node-local claim gateway.** A per-node gateway that takes claims off
  the apiserver path and records `Bound` afterwards, with orphan adoption, a
  `SubjectAccessReview` authorizer and `ResourceQuota` enforcement on the claim
  path; designed in [scaling-design.md](scaling-design.md), not built.
- **Aggregated-apiserver HA.** Multi-replica scatter-gather with consistent
  claim routing (today: multiple replicas serve reads; claims prefer a single
  writer).
- **Watch as merged per-node streams.** `watch` on `sandboxes.agents.x-k8s.io`
  is served today by re-listing the inventory and diffing against what the
  watcher has seen; serve it from live per-node inventory streams instead.

- **Checkpoints that survive node loss.** A checkpoint currently lives on the
  node that took it: peer healing moves a record to a node that cannot reach
  it, but nothing replicates one, so losing that node's disk loses the
  checkpoint (see [snapshot-placement.md](snapshot-placement.md)).
  Making them durable needs asynchronous replication to N peers plus a
  placement policy that tracks replica sets and repairs under-replication.
  **Not built, and deliberately so** — checkpoints are branch points for agent
  workloads, not backups, and replication would cost write amplification on
  every capture. Until this lands, promote anything that must outlive its node
  to a template and distribute it as one.

## Longer term

- **Million-sandbox validation.** Exercise the full L0–L3 design at fleet
  scale and publish the methodology alongside the existing benchmarks.
- **Upstream alignment.** Track `sigs.k8s.io/agent-sandbox` releases — the
  module this repository's APIs come from — and contribute the gaps the L3 path
  exposes (subresource verbs, warm capacity as node-local intent) back upstream.
