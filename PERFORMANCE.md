# Performance

All numbers below are **measured on a live cluster through the Kubernetes API**
(no mocks, no direct data-plane calls) using the reproducible drivers in
[`test/`](test/). Sandboxes are **real Cloud-Hypervisor/KVM microVMs** provisioned
through the `vk-cocoon` runtime.

## Test environment

| | |
|---|---|
| Cluster | 27 virtual-kubelet (`vk-cocoon`) nodes, 384 cores / 1.5 TiB each; managed Kubernetes v1.26 |
| Operator | `sandbox-operator`, `--sandbox-concurrent-workers=16`, `--sandbox-warm-pool-concurrent-workers=8`, `--kube-api-qps=200` |
| Sandbox | `agents.x-k8s.io/v1beta1` Sandbox, `runtime: vk-cocoon`, Ubuntu microVM (2 vCPU / 8 GiB, hugepage-backed on demand) |
| Driver | `test/poolbench` — controller-runtime client; watch-driven claim timing |

## Headline: warm-pool claim latency (real microVM)

A `SandboxWarmPool` keeps microVMs pre-booted; a `SandboxClaim` adopts one. The
claim is **control-plane only** — the microVM is already running — so latency is
a Kubernetes round-trip, independent of the microVM boot cost.

| pool size | p50 | p95 | warm hits |
|---|---|---|---|
| 10 | **35 ms** | 40 ms | 100% |
| 200 | **33 ms** | 39 ms | 100% |

**~33 ms p50, and flat as the pool grows.** This is below e2b's published ~150 ms
sandbox start, on a real microVM.

### Comparison to e2b

e2b's headline "~150 ms" is a snapshot-resume start
([e2b.dev](https://e2b.dev), vendor-published — verify before quoting). Matching
tiers rather than headlines:

| tier | sandbox-operator (this repo) | cocoonstack/sandbox `sandboxd` | e2b (published) |
|---|---|---|---|
| **warm claim** (pre-booted) | **33 ms** p50 (measured, k8s control plane) | **0.2–0.7 ms** (node-local VM ownership transfer) | — |
| **clone / snapshot resume** | n/a (uses cold boot) | 45–75 ms | ~150 ms |
| **cold boot** (kernel start) | 26–32 s (full OCI microVM boot) | 200–350 ms | — |
| substrate | real CH/KVM microVM | real FC/CH microVM | Firecracker microVM |
| control plane | **Kubernetes CRDs, any k8s SDK** | node-local daemon + Go/Python SDK | proprietary hosted SDK |
| self-hosted | yes (AGPL-3.0) | yes | no |

Two honest caveats:

1. **Cold boot is slow** (full OCI microVM boot, 26–32 s) — the warm pool exists
   precisely to keep that off the request path. cocoon's snapshot-**clone** tier
   (45–75 ms) is faster but lives in the `sandboxd` stack, not this operator.
2. **The 33 ms claim degrades under extreme fan-out** (see below). The number
   that beats e2b *at any scale* is the `sandboxd` sub-millisecond claim, because
   it never touches a centralized control plane.

### Claim latency vs. concurrency and scale

| pool | concurrency | p50 | p95 | note |
|---|---|---|---|---|
| 200 | 1 | 33 ms | 39 ms | serial — the comparable single-start number |
| 200 | 5 | 53 ms | 183 ms | mild contention |
| 200 | 20 | 316 ms | 454 ms | 20 simultaneous claims + their replenishment |
| ~2300 | 1 | 516 ms | 554 ms | apiserver LIST + operator informer cache of 2500 objects |

The operator's warm claim beats e2b up to ~1000-sandbox pools. Beyond ~2000
concurrent sandboxes the **centralized Kubernetes control plane** (apiserver list
throughput + the operator's informer cache) becomes the bottleneck. This is
inherent to a k8s-native design; the `sandboxd` node-local pool does not degrade.

## Scale: thousands of concurrent microVMs

A single `SandboxWarmPool` scaled to 2500:

- **readyReplicas = 2303 / 2500** concurrent real microVMs (operator status;
  cross-checked via per-node `cocoon vm list`).
- **CR creation ~36/s**, **microVM boot ~27/s** at scale.
- **0 operator restarts**; production microVMs on the same cluster **unaffected**
  (their hugepage pool held steady the entire run).
- Clean scale-to-0 with **0 stuck finalizers**.

Reaching this scale required a `topologySpreadConstraint` (hostname) in the Pod
template: virtual-kubelet nodes under-report utilization, so the default
scheduler packs one node while leaving others idle; the spread constraint
rebalances (an idle node went from 0 to ~127 microVMs). At very high per-node
density the cocoon-net DHCP path is the next limiter (a small `WaitingForIP`
tail).

## sandboxd hot-pool tier (deployed, measured end-to-end)

The `sandboxd` sub-millisecond claim is no longer only a data-plane number: it is
now reachable **through the same Kubernetes API and this operator**, via
`runtime: sandboxd`. The operator's `podruntime` mutator pins such a Sandbox to a
[`vk-sandbox`](https://github.com/cocoonstack/vk-sandbox) virtual
node (one per host, co-located with `vk-cocoon`), which serves the claim from the
node-local `sandboxd` hot pool. Kubernetes stays the record-of-intent plane; the
claim transaction runs on the node.

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
(admission → operator reconcile → schedule → status propagation), sub-second at
this scale; the underlying `sandboxd` ownership transfer itself is **0.2–0.7 ms**
and the `vk-sandbox` gateway overhead **~0.04 ms** (see the operator's
`pkg/scale` L2 gateway bench). This is the operator's answer to the "cold boot is
slow" caveat above: the hot tier now serves warm claims at node-local speed while
`kubectl get sandboxes` keeps working.

## Fleet scale: 50 000 microVMs from one `kubectl patch`

The 100-sandbox run above is the deployed hot tier at small scale. The same
stack, driven by one CRD write, was measured to **50 000** concurrent microVMs.

**Method.** A single `SandboxWarmPool` patched `replicas: 0 → 50000` on **20
homogeneous bare-metal nodes** (384 vCPU / 1.5 TiB / local NVMe, 2 500 microVMs
per node). `status.readyReplicas` polled at 1 s; each node's fill cross-checked
by node-side telemetry at 5 s. Sandboxes are real Cloud-Hypervisor/KVM microVMs
restored from a golden snapshot.

**Result.** Full supply in **10–15 s** (node telemetry) / 15.7 s (CR
wall-clock) — an effective **3 300–5 000 microVMs/s** (CR steady-state
3 654/s), at **99 MB net RAM per microVM**.

![0 to 50000 fill, three rounds](docs/images/perf-50k-fill-rounds.png)

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

### Memory ledger

Per-VM footprint, same golden and pool, from `/proc/<pid>/smaps` and a
`MemAvailable` node delta:

| recovery | CH RSS | private | shared | net / VM | per node (1 923 VMs) |
|---|---|---|---|---|---|
| eager copy | 359 MB | 353 | 6 | 358 MB | 672 G |
| mmap CoW | 163 MB | 96 | 67 | **99 MB** | **186 G** |

![memory footprint, eager copy versus mmap CoW](docs/images/perf-memory-mmap-cow.png)

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

![supply rate is linear in node count](docs/images/perf-scaling-law.png)

Because sandbox objects are synthesized from `NodeInventory` rather than stored,
etcd carried **~2 writes/s** across the whole 50 k run (20 inventories every
30 s plus a little `status`), independent of sandbox count; the claim path
writes nothing to etcd at all.

## Data plane

k8s Pod `exec` is **not** available to `vk-cocoon` microVMs on a managed cluster
(the control plane cannot reach virtual-node kubelets over the microVM network);
the microVM data plane is `cocoon vm exec` / silkd (in-VM agent), validated in
test evidence. The portable standard-kubelet backend uses ordinary Pod exec.

## Reproduce

```bash
# warm-pool claim latency (real microVM), pinned to a labeled node pool
go run -tags poolbench ./test/poolbench \
  -runtime vk-cocoon -image <cocoon-oci-image> \
  -node-selector <pool-label>=<v> -snapshot-policy never \
  -pool 200 -claims 40 -claim-conc 1

# core + extensions E2E (12 scenarios) against a real cluster
KUBECONFIG=<vke> go run -tags e2e ./test/e2e -ns <ns> -out /tmp/e2e-results.json
```
