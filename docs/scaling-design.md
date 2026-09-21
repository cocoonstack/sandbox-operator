# Scaling design — decentralized sandbox scheduling on Kubernetes semantics

Modal's ["1M concurrent sandboxes"](https://modal.com/blog/scaling-to-1-million-concurrent-sandboxes-in-seconds)
post argues Kubernetes cannot reach that scale: scheduling is `O(n×p)` and
serialized, every Pod causes multiple etcd writes, etcd is not shardable within a
keyspace, and kubelet heartbeats impose an `O(nodes)` write floor. Their answer is
to **leave Kubernetes entirely** — a fleet of stateless schedulers over in-memory
worker state published to a Redis stream, direct scheduler→worker RPC, and *no
datastore on the sandbox-creation critical path*.

Their diagnosis is correct. Their conclusion is not the only option. What Modal
actually removed is a **centralized transaction path**, not API semantics — and
Kubernetes already separates those two things in its own design: kubelet **static
Pods** (the node acts first, the apiserver records after), the **coordination.k8s.io
Lease** (a dedicated tiny object for heartbeats instead of full Node writes), and
the **metrics.k8s.io aggregation layer** (a virtual resource served by
scatter-gathering live node state, with *zero* etcd storage). Our thesis:

> **Keep Kubernetes as the record-of-intent and policy plane; push the
> transaction plane down to the node — behind CRDs, RBAC, and watch, so
> `kubectl get sandboxes` never stops working.**

We stage this as four layers. L0 and L1 are shipped. L2 has a concrete,
benchmarked gateway and orphan reconciler, but still needs deployment hardening
before it is a supported mode. L3 is implemented by the aggregated-apiserver
binary, manifests, scatter-gather store, and `NodeInventory` publisher. Its
remaining routing follow-up is called out below.

```mermaid
flowchart LR
    subgraph L0["L0 — API hygiene (shipped)"]
        L0a["cache-fed reads<br/>diff-before-write<br/>LIST off etcd"]
    end
    subgraph L1["L1 — ownership transfer (this repo)"]
        L1a["claim = Update + merge Patch<br/>O(nodes) pool status<br/>one leader-elected operator"]
    end
    subgraph L2["L2 — node-local claim gateway core"]
        L2a["gateway → sandboxd<br/>sub-ms delivery<br/>async Bound record"]
    end
    subgraph L3["L3 — aggregated apiserver (implemented)"]
        L3a["scatter-gather node inventory<br/>etcd stores intent only<br/>O(sandboxes)→O(pools+nodes)"]
    end
    L0 --> L1 --> L2 --> L3
```

### L0 — API hygiene (shipped)

The prerequisite, delivered in the `vk-cocoon` provider (2026-07-17): every
periodic read is served from a node-scoped informer cache, every write is
diffed first, and no control-loop `LIST` hits etcd (list at `ResourceVersion=0`,
or a field-selected node-local lister). This is the qualifier — without it any
scale test wedges the apiserver first. Measured on an idle virtual-kubelet node
afterward: **0.2 req/s, zero LIST** (lease renew + node-status patch only). The
root cause it fixed: Kubernetes APF prices `LIST` seats by the **total object
count** of the resource, so at 2500 pods even a tiny per-node list goes
max-width and saturates a dedicated priority level — client QPS caps cannot fix
a seat-seconds problem, only removing the lists can.

### L1 — claim is ownership transfer, not scheduling (implemented)

A warm claim is not a create. The Pod is already scheduled, bound, image-pulled,
and booted; a `SandboxClaim` only needs to **transfer ownership** of one
pre-warmed `Sandbox` — the exact semantics Kubernetes already ships for
`PersistentVolumeClaim → PersistentVolume` binding (`Phase: Bound`). Nothing on
the claim path needs the scheduler, kubelet bind, or image pull.

**Mechanisms**

1. **Claim fast-path — pop, adopt, record.** `getCandidate` pops one
   `warm ∧ unclaimed` Sandbox from the in-memory queue (node-spread pick); the
   claim records the adoption with an `Update` on the SandboxClaim and binds
   the Sandbox with a merge `Patch` under a `resourceVersion` precondition. A
   loser that raced the same Sandbox moves to the next candidate; a pass that
   ends on a conflict requeues instead, and the next pass completes the
   adoption its claim already records. The CRD path is two apiserver
   writes per claim; the sub-millisecond figures below come from the node-local
   gateway (L2), not from this path.
2. **Pool status from the informer cache, not etcd.** `readyReplicas` is
   recomputed each reconcile from the pool's Sandboxes read through the indexed
   cache without deep copies — an in-memory scan, never a `LIST` against the
   apiserver.
3. **One leader-elected operator.** The reconcilers share one manager under a
   single `coordination.k8s.io` leader lease. Pools are independent workqueue
   keys, not shards spread across replicas; per-pool sharding is not
   implemented.

**Kubernetes-semantics mapping**

| Modal mechanism | L1 in pure Kubernetes |
|---|---|
| stateless scheduler fleet | single leader-elected operator + Lease |
| worker accepts/rejects placement | optimistic PATCH with `resourceVersion` precondition |
| no datastore on create path | claim = ownership PATCH of a pre-warmed object (like PVC→PV `Bound`) |
| async result write | Sandbox status/conditions written after the fast-path returns |

**Failure modes**

| Scenario | Behavior | Breaks k8s semantics? |
|---|---|---|
| Two claims race one warm Sandbox | `resourceVersion` PATCH conflict; loser adopts the next candidate | No — standard optimistic concurrency |
| Warm pool exhausted | Claim stays `Pending` until replenish (unchanged) | No |
| The leader operator dies mid-claim | Lease expiry → another replica resumes; claim is idempotent | No |
| Stale informer picks an already-claimed Sandbox | PATCH precondition fails → next candidate | No |

**Acceptance:** claim p50 stays near-constant from a 100-pool to a 2000+-pool
(measured 0.644 ms → 0.646 ms); the pod-exclusivity invariant (one Sandbox, at
most one claim) holds under concurrent claims.

### L2 — node-local claim gateway (implemented core; deployment hardening pending)

L1 still round-trips the apiserver. L2 takes the claim off the central path
entirely for the runtimes that have a node-local warm pool (`sandboxd`), while
keeping the `SandboxClaim` object as the durable record.

**Mechanism.** The implemented `ClaimGateway` is intended to run on each
virtual-kubelet node in front of `sandboxd`. A claim request reaches the node
gateway directly; `sandboxd` hands over an already-running microVM in
**0.2–0.7 ms** and returns connection info immediately. The `SandboxClaim` is
marked `Bound` **asynchronously** — the record follows the action, exactly as
kubelet static Pods record to the apiserver after the container is already
running. The repository contains the concrete gateway and orphan reconciler;
packaging it as a supported DaemonSet remains roadmap work.

**Authorization stays central.** The gateway runs a `SubjectAccessReview`
before delivery; only ownership transfer moves to the node.

```go
// ClaimGateway is the node-local fast path for warm-pool claims.
// A claim is served by the node that already holds a warm microVM; the
// SandboxClaim object is reconciled to Bound asynchronously afterward.
type ClaimGateway interface {
    // Claim transfers ownership of a node-local warm sandbox to the caller,
    // returning connection info. It performs SubjectAccessReview inline;
    // it does NOT block on writing the SandboxClaim.
    Claim(ctx context.Context, req ClaimRequest) (Assignment, error)
    // Release returns a sandbox to the node-local pool (or tears it down).
    Release(ctx context.Context, assignment Assignment) error
}
```

**Failure modes**

| Scenario | Behavior | Breaks k8s semantics? |
|---|---|---|
| Gateway crashes after delivery, before recording `Bound` | Orphan binding → audit-only orphan GC + adopt reconciles the record (the VM is never destroyed on pod-level state — see the delete-authorization contract) | No — eventual consistency |
| Node has no warm VM | Returns the explicit fallback signal; a deployment must route that request through the L1 Kubernetes path | No |

**Acceptance:** claim p50 sub-millisecond on the sandboxd tier; orphan-binding
rate converges to 0 via GC; `kubectl get sandboxclaims` still shows every claim.

### L3 — aggregated apiserver: etcd stores intent, not sandboxes (implemented)

A million `Sandbox` objects in etcd is a dead end (churn alone blows the ~8 GB
keyspace). The Kubernetes-native fix is the aggregation layer: serve
`sandboxes.agents.x-k8s.io` from an **aggregated apiserver** (an `APIService`)
that **scatter-gathers** live node inventory on read. etcd stores only *intent* —
one `SandboxWarmPool` spec expressing a million desired replicas, plus one
`inventory` object per node (`O(nodes)`). Object count drops from
`O(sandboxes)` to `O(pools + nodes)`.

Each virtual-kubelet node already knows its own VMs (L0 made that cache the
source of truth), so the aggregated server assembles a `SandboxList` by fanning
out to node inventories — the exact pattern `metrics.k8s.io` uses to serve
`PodMetrics` with zero etcd storage. `kubectl get sandboxes`, RBAC, field
selectors, and `watch` (currently implemented by polling and diffing the
cache-fed `NodeInventory` view) all keep working; users never see that storage
decentralized.

```go
// SandboxStore backs the aggregated apiserver for sandboxes.agents.x-k8s.io.
// It holds NO per-sandbox etcd objects: List/Get/Watch use the cache-fed node
// inventories, and Create/Delete claim or release through a node-local RPC.
type SandboxStore interface {
    List(ctx context.Context, opts ListOptions) (*SandboxList, error)   // fan-out to node inventories
    Get(ctx context.Context, ns, name string) (*Sandbox, error)         // fan-out; materialize only the match
    Watch(ctx context.Context, opts ListOptions) (watch.Interface, error) // poll and diff inventory
}

// NodeInventory is the one O(nodes) etcd object per node: the durable
// summary of that node's live sandboxes, server-side-applied on a slow
// cadence. The per-sandbox truth lives in the node, not etcd.
type NodeInventory struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata,omitempty"`
    Node    string           `json:"node"`
    Entries []InventoryEntry `json:"entries"` // {name, id, phase, template, claimRef, addr, deadline}
}
```

**Failure modes**

| Scenario | Behavior | Breaks k8s semantics? |
|---|---|---|
| Node partitioned from aggregated server | Its sandboxes briefly absent from `List` (eventual consistency, same as an informer lag) | No |
| A node `inventory` object lost | Rebuilt from the node's own live state on next publish | No |
| Aggregated server restart | Stateless; rebuilds from node fan-out | No |
| Client reads before the owning node republishes inventory | The sandbox is briefly absent; retry until the next publish. Direct authoritative routing is the remaining follow-up below. | No — the read surface is explicitly eventually consistent |

**Acceptance:** 1M sandbox *intent* costs `O(nodes)` etcd objects; `kubectl get
sandboxes` returns the fanned-out list; per-sandbox `Get` only materializes the
matching entry. Strong read-after-write and `O(1)` lookup remain follow-up work.

### L3 routing: why node choice is sampled, not maximized

The operator schedules off `NodeInventory`, which each node republishes on a
5–30 s cadence. Picking the node with the highest advertised warm count would
therefore send *every* claim in a refresh window to whichever node looked best
in that one snapshot — draining it while its peers stay idle. The repo's own
100-burst run showed the signature: 98/100 warm, and both misses were
cold-provision on a single over-scheduled node.

Node choice is instead power-of-two-choices: sample two candidates that
advertise warm capacity for the requested pool and take the warmer one. That
keeps the bias toward warm capacity while spreading a burst across the fleet,
and a stale pick still costs at most one gossip redirect inside sandboxd, so
correctness is unchanged.

The watch path makes the opposite trade. Re-deriving the fleet view is
`O(nodes × sandboxes)` — measured at 124 ms for the list and 173 ms for a full
poll tick at 200 nodes and 50k sandboxes — yet the poll cadence is fixed on
purpose: a widened interval would let a sandbox created and deleted inside the
gap produce neither an Added nor a Deleted event. One watcher per fleet is the
supported shape.

### L3 remaining follow-up: read after write without published inventory

Single-sandbox lookup no longer builds the cluster-wide `SandboxList`.
`SandboxStore.Get` and the e2b claim-id resolver fan out across the cache-fed
per-node inventories, cancel on the first match, and materialize only that
entry. This removes the previous `O(total sandboxes)` `Sandbox` allocation, but
two limitations remain: the lookup cannot see a claim until the owning node
publishes it, and a miss still scans up to every inventory entry.

**It is stale for one publish interval.** A sandbox is live on its node the
moment `Claim` returns, but it does not appear in the read view until that node
republishes its `NodeInventory` (default 30 s). Measured against a 20-node
fleet: `p50` 29.0 s on the e2b surface, 28.6 s through the aggregated API. A
lifecycle verb issued inside that window answers `404`. Callers work around it
by polling until visible — which is what `examples/lifecycle` does — so
"create, then immediately pause" costs half a minute of polling.

**A first lookup is still `O(total inventory entries)` CPU.** An owning-node
index now answers a repeat lookup from one node's inventory: measured at 200
nodes × 2000 sandboxes, `Get` fell from 2.49 ms / 36 MB to 39 µs / 217 KB per
call. A first lookup, or one whose index entry was evicted, still compares
entries across the fleet.

Both fall out of the same omission: `Claim` already returns the node and the
claim id — the e2b create response even hands the node back to the client as
`clientID` — and a lifecycle verb needs nothing else. The plan keeps that
routing information instead of re-deriving it:

- **A. Claim-time index.** Implemented: the store records the owning node when
  a claim is made and when a sweep resolves one, and consults it before fanning
  out. It is bounded by generation swap rather than per-entry recency
  bookkeeping, so memory is a fixed budget rather than a function of load.
- **B. Authoritative fan-out on a miss.** A different replica, or an evicted
  entry, falls back to asking the nodes directly — the authoritative route the
  risk table already prescribes. Bounded by node count, off the read path for
  anything older than one publish interval.

Neither touches etcd. **Publishing inventory on change was considered and
rejected:** `NodeInventory` carries one 105 B entry per live sandbox
(measured), so a node holding 2500 of them is a 263 KB object. Re-applying that
on a 2 s debounce costs 52.6 MB/s of large-object server-side-apply traffic
across 400 nodes at 1 M sandboxes, against 3.5 MB/s for the current 30 s
cadence — and it would still leave the `O(total inventory entries)` lookup in
place.

**Acceptance:** a lifecycle verb succeeds on a sandbox claimed milliseconds ago;
resolving one sandbox allocates `O(1)`, not `O(total sandboxes)`; index memory
is capped independently of fleet size.

### How this differs from Modal

Modal buys throughput by leaving Kubernetes: a proprietary SDK and a proprietary
control plane. Every layer here keeps `kubectl` / CRDs / RBAC / the ecosystem
intact. The one-line framing:

> **Modal proved 1M needs a decentralized transaction plane. We show the
> decentralized transaction plane can hide behind Kubernetes semantics.**

| | Modal | sandbox-operator |
|---|---|---|
| Scheduling | stateless fleet, in-memory worker state | single leader-elected operator + Lease (L1) |
| Create critical path | direct scheduler→worker RPC, no datastore | ownership PATCH (L1) → node-local gateway (L2) |
| State of record | Redis stream (async) | Kubernetes objects; node inventory in etcd is `O(nodes)` (L3) |
| Sandbox storage | proprietary | aggregated apiserver, etcd stores intent only (L3) |
| Client interface | proprietary SDK | any Kubernetes client — unchanged |
| Scale ceiling | no practical limit | decoupled from etcd object count at L3 |

### Measured performance

Every acceptance claim above is backed by a reproducible benchmark committed under
`test/` — the evidence is regenerated by the harness, never hand-written. Numbers
are labelled by substrate: **algorithmic complexity** is proven on a fake apiserver
(so it isolates the scaling term, not machine speed), while **absolute latency on
real microVMs** is measured on a single `vk-cocoon` node (384 vCPU / 1.5 TB bare metal).

| Layer | Acceptance claim | Measured | Substrate / harness |
|---|---|---|---|
| **L1** | claim p50 stays near-constant as the pool grows | fast-path p50 **0.644 ms → 0.646 ms** from N=100 to N=2000 (**1.003×**); a full-`LIST` selection over the same fixtures degrades **15.7×** (1.3 → 20 ms) | fake apiserver + real reconciler — `test/scalebench` |
| **L1** | warm claim on real microVMs | claim→Bound p50 **129 ms**, p95 926 ms, p99 935 ms; 100/100 warm hits, 0 failures. Pool fills 100 microVMs in 62 s (boot p50 47 s) | the microVM node, 100 concurrent claims — `test/poolbench` |
| **L2** | sub-millisecond node-local claim | gateway overhead p50 **0.039 ms**, p95 0.053 ms (sandboxd delivery itself is 0.2–0.7 ms by contract); 200/200 orphan bindings reconciled, **0** VM destroys | httptest sandboxd + fake recorder — `test/l2bench` |
| **L3** | etcd stores intent only, `kubectl` unchanged | **3000** sandboxes served through client-go List/Get/Watch from **8** etcd objects (3 nodes + 5 pools) — **0** per-sandbox objects, 3 server-side-apply writes | in-process aggregated apiserver — `test/l3bench` |
| **e2e** | admission→claim→release→cleanup, zero leak | 100 real microVMs: four-way cross-check 100/100/100/100, 100/100 claims bound, **0 leaked**, production workloads on the same node unaffected | the microVM node, full stack — `test/e2ebench` |
| **sandboxd tier (deployed)** | hot-pool warm claim via k8s, apiserver flat under load | 100 `Sandbox` (`runtime: sandboxd`) create→Ready **p50 < 1 s** (warm), 98/100, submitted in 2.9 s; **100 %** routed to the sandboxd plane; apiserver LIST 37 ms/7 ms, **0 APF rejections, in-queue 0**; cocoon microVMs untouched | 26-node fleet, `vk-sandbox` + sandboxd — the scale benches under `test/` |

Two honest caveats. The sub-millisecond L1/L2 figures measure algorithmic cost and
gateway overhead on fake substrates; real end-to-end latency additionally pays the
apiserver round-trip, sandboxd delivery (0.2–0.7 ms), and informer convergence. And
the real-microVM claim p95 (926 ms, ~7× the p50) is single-node
optimistic-concurrency contention under 100 simultaneous claims — exactly the tail the
node-local claim gateway (L2) takes off the apiserver path.
