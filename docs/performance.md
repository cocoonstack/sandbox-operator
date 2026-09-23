# Performance

Two kinds of numbers live here. The **current** sections measure what this
repository ships today: the L3 aggregated apiserver, its warm-pool driver, and
the sandboxd tier they drive. The **retired** section at the end was measured
against the forked agent-sandbox controllers this repository carried until
commit `0719d33`; those controllers are gone, replaced by upstream's own, so
their numbers describe code that is no longer here.

## Fleet supply: 50 000 microVMs from one `kubectl patch`

**Method.** A single `SandboxWarmPool` patched `replicas: 0 → 50000` on **20
homogeneous bare-metal nodes** (384 vCPU / 1.5 TiB / local NVMe, 2 500 microVMs
per node). `status.readyReplicas` polled at 1 s; each node's fill cross-checked
by node-side telemetry at 5 s. Sandboxes are real Cloud-Hypervisor/KVM microVMs
restored from a golden snapshot.

**Result.** Full supply in **10–15 s** (node telemetry) / 15.7 s (CR
wall-clock) — an effective **3 300–5 000 microVMs/s** (CR steady-state
3 654/s), at **99 MB net RAM per microVM**.

![0 to 50000 fill, three rounds](images/perf-50k-fill-rounds.png)

Three rounds isolate where the speed comes from — same target, same driver
command, same measurement script:

| round | nodes | recovery path | fill time |
|---|---|---|---|
| 1 | 26 | eager copy | 172.7 s |
| 2 | 26 | mmap CoW (incl. one HDD-backed node) | 290.1 s |
| 3 | 20 | mmap CoW, homogeneous NVMe | **12 ± 3 s** |

Round 2 is the honest counter-example: on the single HDD-backed node, mmap CoW
*regressed* fill from ~140 s to ~295 s while every NVMe node in the same round
improved — the optimization's sign is set by the storage medium. Round 3 drops
the six heterogeneous nodes and lands the clean 12 ± 3 s curve.

Because sandbox objects are synthesized from `NodeInventory` rather than stored,
etcd carried **~2 writes/s** across the whole 50 k run (20 inventories every
30 s plus a little `status`), independent of sandbox count; the claim path
writes nothing to etcd at all.

### Memory ledger

Per-VM footprint, same golden and pool, from `/proc/<pid>/smaps` and a
`MemAvailable` node delta:

| recovery | CH RSS | private | shared | net / VM | per node (1 923 VMs) |
|---|---|---|---|---|---|
| eager copy | 359 MB | 353 | 6 | 358 MB | 672 G |
| mmap CoW | 163 MB | 96 | 67 | **99 MB** | **186 G** |

![memory footprint, eager copy versus mmap CoW](images/perf-memory-mmap-cow.png)

### Scaling law

Supply time is `T(S, N) ≈ T₀ + S / (N · r)` with a node-local constant
`r ≈ 200–270/s` and a control-plane constant `T₀ ≈ 5–6 s`. r is constant
because every input to supply — golden image, 256-way refill budget, SQLite
metadata — is node-local, and the control plane touches each node with one O(1)
`PUT /v1/pools` every 5 s; nodes are zero-coupled, so cluster rate = N · r.

| scenario | N | S | s = S/N | RAM/node | predicted T | status |
|---|---|---|---|---|---|---|
| this run (round 3) | 20 | 50 000 | 2 500 | 242 G | ≈15 s | **measured 15.7 s (CR) / 10–15 s (node)** |
| single-cluster ceiling | 20 | 100 000 | 5 000 | ≈495 G | ≈25 s | >50% RAM headroom; 4 000/node co-residency measured |
| linear extrapolation | 200 | 1 000 000 | 5 000 | ≈495 G | ≈25 s | control plane O(N): etcd ≈7 writes/s, driver 200 PUT/5 s |

![supply rate is linear in node count](images/perf-scaling-law.png)

## sandboxd hot-pool tier (deployed, measured end-to-end)

The `sandboxd` sub-millisecond claim is reachable **through the Kubernetes API**:
a sandbox Pod routed to a [`vk-sandbox`](https://github.com/cocoonstack/vk-sandbox)
virtual node is served from that node's hot pool. Kubernetes stays the
record-of-intent plane; the claim transaction runs on the node.

Measured on the 26-node MY fleet (each node: co-located `vk-cocoon` +
`vk-sandbox` + `sandboxd`; warm pool of **125 golden microVMs**,
`warm=5`/node, template `sandbox/rt:24.04` distributed **P2P** node-to-node):

| metric | result |
|---|---|
| **Sandbox `create` → `Ready`** | **p50 < 1 s**, p95 / p99 / max **1 s** (warm claim) |
| submit 100 `Sandbox` CRs | 2.9 s |
| delivered | 98 / 100 warm (the 2 misses were `sandboxd` cold-provision on a single over-scheduled node, not the control plane) |
| routing | **100 % landed on the `sandboxd` plane; 0 on `vk-cocoon`** |
| apiserver under the burst | APF in-queue **0** throughout, **zero** new flow-control rejections, the `vke-list-limit` priority level **0 / 79** seats — no LIST-seat wedge at 100 concurrent creates (L0 cache-fed reads hold) |
| isolation | cocoon-managed microVMs **unchanged** across the run (distinct image, `firecracker` hypervisor, `sbx-*` VMs — never cocoon's Cloud-Hypervisor VMs or image paths) |

The end-to-end `create → Ready` is dominated by the Kubernetes round-trip
(admission → reconcile → schedule → status propagation), sub-second at this
scale; the underlying `sandboxd` ownership transfer itself is **0.2–0.7 ms** and
the `vk-sandbox` gateway overhead **~0.04 ms** (`test/l2bench`).

This run predates the upstream import: the Pod that reached the `vk-sandbox`
node was produced by the forked controller plus the Pod mutator, both since
removed. The node-side numbers are unaffected — the same Pod is now written by
hand, per the [pod-template contract](runtime-backends.md) — but the
control-plane half is upstream's code and unmeasured here.

## Upstream controller: warm claim on the sandboxd tier

Measured on 2026-09-22 when the forked controllers were replaced by upstream's:
two bare-metal hosts (384 cores, 1.5 TB each), an isolated control plane
(kube-apiserver v1.37.0 + etcd), one `vk-sandbox` virtual node per host over
sandboxd v0.1.13 with 30 warm `rt:24.04` microVMs each. One harness for both
arms: a `SandboxTemplate` carrying the sandboxd pod-template contract, a
`SandboxWarmPool` of 40, then 40 `SandboxClaim`s one at a time, latency measured
from the claim create to the watch event that shows `status.sandboxStatus.name`.
The arms ran interleaved on the same cluster (F1, U1, F2, U2), each on its own
CRD set.

| controller | pool fill (40) | claim p50 | p95 | p99 | max | warm hits |
|---|---|---|---|---|---|---|
| fork `0719d33` | 2 s | 52.4 ms | 102.9 ms | 104.4 ms | 105.4 ms | 40/40 |
| upstream v1.0.3 | 2 s | 53.8 ms | 69.4 ms | 72.0 ms | 75.2 ms | 40/40 |
| fork `0719d33` | 2 s | 44.5 ms | 69.7 ms | 73.8 ms | 75.7 ms | 40/40 |
| upstream v1.0.3 | 2 s | 48.4 ms | 71.0 ms | 72.0 ms | 72.6 ms | 40/40 |

While the pool holds warm members, upstream's claim controller sits inside the
fork's run-to-run spread. The same round re-ran the L3 half before and after the
import on that cluster — the aggregated list across both nodes, the warm-pool
driver setting both nodes' targets, every lifecycle verb over the Kubernetes and
e2b surfaces (`examples/lifecycle`), and an e2b create on one host reached
through `sandbox-envd-proxy` on the other — with identical results, and
`test/l3bench` reports the same 3000 sandboxes from 8 etcd objects on both
builds.

### When claims drain the pool

Measured on 2026-09-23 on the same two hosts: a `SandboxWarmPool` of 40, then a
burst of 200 `SandboxClaim`s at create parallelism 20, timed per claim from its
create to its `Ready` condition, with every Sandbox's and Pod's stages watched
on one clock. Arms interleaved, upstream v1.0.3 against the fork at `0719d33`.

The first bursts found two limits outside the controllers, and both arms hit them
alike:

- **The default scheduler packs one virtual node.** Identical vk-sandbox nodes
  score within a point of each other, and every Pod of a burst landed on one of
  them. A hostname `topologySpreadConstraint` in the pod template spreads them.
- **virtual-kubelet caps each node at 10 Pod syncs a second.** The library's
  default workqueue limiter holds Pod creates and status pushes to 10 a second
  after a burst of 100. Scheduled-to-Pod-IP took 1–6 s, and refill ran at about
  10 a second per node, whatever sandboxd could deliver. vk-sandbox now runs
  those queues at its kube client's budget
  ([vk-sandbox#19](https://github.com/cocoonstack/vk-sandbox/pull/19)).

Spread on, 150 warm microVMs per node:

| vk-sandbox pod queues | controller | claim → Ready p50 / p95 / max | Pod scheduled → Pod IP p50 / p95 | refill |
|---|---|---|---|---|
| 10/s (library default) | upstream v1.0.3 | 0.49–0.50 / 7.9–8.4 / 8.8–9.0 s | 1.0–1.3 / 6.0–6.4 s | 23/s |
| 10/s (library default) | fork `0719d33` | 1.84–1.85 / 4.2–4.7 / 5.1–5.4 s | 0.9–1.0 / 5.0 s | 26–27/s |
| client budget (200/s) | upstream v1.0.3 | 0.48–0.63 / 1.1–1.9 / 1.6–2.1 s | 28–29 / 76–80 ms | 103–104/s |
| client budget (200/s) | fork `0719d33` | 1.79–1.83 / 2.3–2.5 / 2.4–2.5 s | 25–26 / 45–49 ms | 76–82/s |

With the node side fixed, upstream's controller is ahead on every percentile.
Before that, its tail was longer: it binds a claim only to a warm Sandbox whose
Pod IP it has seen, waits up to 2 s for one, then creates its own, and a slow
node side makes that fallback fire. Its median is better throughout, because a
claim it binds is ready.

Per claim, both controllers write about the same: 21–24 apiserver writes and
18–21 etcd puts, upstream 3–9% above the fork, plus 1.4–2.3 times the
claim-controller reconciles, the more the longer claims wait for a warm Sandbox
(it retries every 100 ms while a candidate has no Pod IP). Where a claim's
writes go, measured at 200 claims against a pool of 40:

| writes per claim | count | what |
|---|---|---|
| Events | 4.0 | the scheduler's `Scheduled`, the claim controller's `SandboxAdopted`, virtual-kubelet's `ProviderCreateSuccess` and `ProviderDeleteSuccess` |
| SandboxClaim | 4.1 | the observability annotation, the assigned-sandbox annotation, status, the first-ready annotation |
| Sandbox | 4.5 | the adoption, and the replacement member's status transitions |
| Pod | 3.4 | create, binding, virtual-kubelet's status push |
| creates and deletes | 5.0 | the claim and the Sandbox created, the claim, its Sandbox and its Pod deleted |

Four of them are optional and change no latency: upstream's
`--disable-claim-events` and `--disable-claim-observability-annotations` drop
one write each, and vk-sandbox's `--disable-pod-events` drops the two
virtual-kubelet events. The claim controller's two-phase adoption (the
annotation before the Sandbox patch) and the status transitions are the model's
own cost; the L3 path has none of it.

## Data plane

k8s Pod `exec` is **not** available to `vk-cocoon` microVMs on a managed cluster
(the control plane cannot reach virtual-node kubelets over the microVM network);
the microVM data plane is `cocoon vm exec` / silkd (in-VM agent), validated in
test evidence. For the sandboxd tier the data plane is the e2b path —
`sandbox-envd-proxy` into the guest's `envd` — and `test/envdproxysmoke` is its
hardware harness. The portable standard-kubelet backend uses ordinary Pod exec.

## Reproduce

The harnesses that remain in this repository are build-tagged, one tag per
directory, and write their evidence as JSON:

```bash
# L2: node-local claim gateway overhead and orphan-binding convergence
go run -tags l2bench ./test/l2bench -out /tmp/l2-gateway.json

# L3: aggregation contract and the O(pools+nodes) object-count invariant
go run -tags l3bench ./test/l3bench -out /tmp/l3-aggregation.json

# store and lookup scaling
go test -run '^$' -bench . ./pkg/scale ./pkg/e2bcompat

# envd-proxy against a live sandbox (see envd-proxy.md for the node half)
go run -tags envdproxysmoke ./test/envdproxysmoke \
  -node <owner> -sandbox <id> -token <token> -port 49983
```

`make vet` type-checks all three tagged harnesses.

## Retired: the CRD-path fork controllers (measured at `0719d33`)

Everything below was measured against the forked `Sandbox`, `SandboxClaim`,
`SandboxWarmPool` and `SandboxTemplate` controllers this repository shipped as
`cmd/sandbox-operator`, together with their `test/poolbench`, `test/scalebench`
and `test/e2e` harnesses. All of it was deleted when the APIs moved to the
upstream module. **These numbers are not claims about upstream's controller and
are not reproducible from this tree.** They are kept because the design
discussion in [scaling-design.md](scaling-design.md) refers to them.

| | |
|---|---|
| Cluster | 27 virtual-kubelet (`vk-cocoon`) nodes, 384 cores / 1.5 TiB each; managed Kubernetes v1.26 |
| Operator | the forked `sandbox-operator`, `--sandbox-concurrent-workers=16`, `--sandbox-warm-pool-concurrent-workers=8`, `--kube-api-qps=200` |
| Sandbox | `agents.x-k8s.io/v1beta1` Sandbox, `runtime: vk-cocoon`, Ubuntu microVM (2 vCPU / 8 GiB, hugepage-backed on demand) |
| Driver | `test/poolbench` — controller-runtime client; watch-driven claim timing |

**Warm-pool claim latency.** A `SandboxClaim` adopted a pre-booted microVM, so
latency was a Kubernetes round-trip independent of boot cost:

| pool size | p50 | p95 | warm hits |
|---|---|---|---|
| 10 | 35 ms | 40 ms | 100% |
| 200 | 33 ms | 39 ms | 100% |

**Claim latency vs. concurrency and scale.**

| pool | concurrency | p50 | p95 | note |
|---|---|---|---|---|
| 200 | 1 | 33 ms | 39 ms | serial — the comparable single-start number |
| 200 | 5 | 53 ms | 183 ms | mild contention |
| 200 | 20 | 316 ms | 454 ms | 20 simultaneous claims + their replenishment |
| ~2300 | 1 | 516 ms | 554 ms | apiserver LIST + operator informer cache of 2500 objects |

Beyond ~2000 concurrent sandboxes the centralized control plane (apiserver list
throughput plus the controller's informer cache) became the bottleneck. That
observation is what the L3 design answers, and it is why the aggregated
apiserver exists.

**Scale.** A single `SandboxWarmPool` scaled to 2500 reached **readyReplicas =
2303 / 2500** concurrent real microVMs (cross-checked via per-node
`cocoon vm list`), at CR creation ~36/s and microVM boot ~27/s, with 0 operator
restarts and a clean scale-to-0 with 0 stuck finalizers. It needed a
`topologySpreadConstraint` on hostname: virtual-kubelet nodes under-report
utilization, so the default scheduler packs one node while leaving others idle.

**Cold boot** on that path was 26–32 s (full OCI microVM boot) — the reason a
warm pool exists at all.
