# sandbox-operator

A Kubernetes operator and aggregated apiserver for **fast, warm-poolable agent
sandboxes backed by real microVMs**. It implements the
[kubernetes-sigs/agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox)
API in full — `Sandbox`, `SandboxTemplate`, `SandboxWarmPool`, `SandboxClaim`,
v1alpha1 and v1beta1 with conversion webhooks — and is driven entirely through
the standard Kubernetes API, with no proprietary SDK. A pre-warmed sandbox is
acquired in **~33 ms at p50**, and each sandbox is a genuine
Cloud-Hypervisor/KVM microVM rather than a shared-kernel container.

```
any Kubernetes client (kubectl / client-go / controller-runtime)
        |                                        e2b SDK (E2B_API_URL)
        v                                                |
kube-apiserver + agent-sandbox CRDs                       v
        |                                     sandbox-apiserver (aggregated)
        v                                                |
sandbox-operator                                          |
Sandbox / SandboxTemplate / SandboxWarmPool / SandboxClaim
        |
        v
warm pool: N pre-booted microVMs
        |
        +-- claim: adopt one, control-plane only (~33 ms) --> delivered sandbox
        |
        +-- runtime: standard   -> ordinary Pod on a standard kubelet node
        +-- runtime: vk-cocoon  -> Cocoon microVM on a virtual-kubelet node
        +-- runtime: sandboxd   -> node-local sandboxd hot pool (0.2-0.7 ms)
```

## The claim model

A cold `Sandbox` creates a Pod on demand; with the microVM backend that Pod
boots a VM, which costs tens of seconds. The warm path keeps that off the
request path: a `SandboxWarmPool` pre-provisions N Ready microVMs, and a
`SandboxClaim` **adopts** one — the VM is already booted, so the claim is an
`Update` on the claim plus a merge `Patch` on the Sandbox, with no scheduler,
no kubelet bind and no image pull on the claim path. The pool
replenishes in the background.

That is the same ownership-transfer shape Kubernetes already ships for
`PersistentVolumeClaim → PersistentVolume` binding, which is why the whole model
stays expressible in ordinary CRDs: `kubectl get sandboxes` keeps working, RBAC
and audit keep applying, and any Kubernetes client is a valid client.

## Runtime backends

Runtime selection is per-Sandbox, via the Pod-template annotation
`sandbox.cocoonstack.io/runtime`, falling back to the operator's
`--default-runtime`:

- **`standard`** (the default) — an ordinary Pod on a standard kubelet node.
  Portable to any conformant cluster; no special substrate required.
- **`vk-cocoon`** — a hardware-isolated Cloud-Hypervisor/KVM guest, materialized
  on a [vk-cocoon](https://github.com/cocoonstack/vk-cocoon) virtual-kubelet node
  by [Cocoon](https://github.com/cocoonstack/cocoon).
- **`sandboxd`** — routes the Pod to a
  [vk-sandbox](https://github.com/cocoonstack/vk-sandbox) virtual node, which
  serves the claim from the node-local `sandboxd` hot pool of
  [cocoonstack/sandbox](https://github.com/cocoonstack/sandbox). The ownership
  transfer itself is 0.2–0.7 ms.

The adapter only fills in the scheduling fields a backend needs, and rejects —
never overwrites — a conflicting explicit value.

## Scaling design: L0 through L3

Reaching a million sandboxes means removing the **centralized transaction
path**, not the API semantics. The thesis is to keep Kubernetes as the
record-of-intent and policy plane and push the transaction plane down to the
node, behind CRDs, RBAC and watch. Four layers:

| layer | what it does | status |
|---|---|---|
| **L0** — API hygiene | cache-fed reads, diff-before-write, no control-loop `LIST` against etcd; the qualifier that stops APF seat exhaustion at scale | shipped |
| **L1** — ownership transfer | claim = queue pop + `Update` + `Patch`; pool status from the informer cache; one leader-elected operator, no per-pool sharding | implemented here |
| **L2** — node-local claim gateway | the concrete gateway fronts `sandboxd`, delivers a running microVM in 0.2–0.7 ms, records `Bound` asynchronously; authorization stays central | core implemented and benchmarked; supported DaemonSet packaging/hardening remains roadmap work |
| **L3** — aggregated apiserver | `sandboxes` served by scatter-gathering per-node `NodeInventory`; etcd stores intent only, so object count drops from `O(sandboxes)` to `O(pools + nodes)` | implemented by `sandbox-apiserver`, its deployment/APIService manifests, and the cache-fed store |

The measured consequence: one `kubectl patch` taking a `SandboxWarmPool` from 0
to **50 000** microVMs on 20 bare-metal nodes reaches full supply in **10–15 s**
at **99 MB net RAM per microVM**, while etcd sees ~2 writes/s across the whole
run — independent of sandbox count. Full methodology, per-round sampling and the
memory ledger are in
[PERFORMANCE.md](https://github.com/cocoonstack/sandbox-operator/blob/master/PERFORMANCE.md).

The same design has one visible cost: the read view is synthesized from
`NodeInventory` published on a ~30 s cadence, so `list`/`get` are eventually
consistent and a just-created sandbox is briefly invisible. Callers poll.

## Guides

- [API reference](api.md) — the generated reference for every type in
  `agents.x-k8s.io` (v1alpha1, v1beta1) and `extensions.agents.x-k8s.io`
  (v1alpha1, v1beta1)
- [Operator configuration](configuration.md) — every flag: runtime and API
  surface, controller concurrency, webhook and leader election, observability,
  and a Helm/Deployment patch example
- [Runtime backends](runtime-backends.md) — why standard kubelet is the rollout
  default, the deterministic selection rules and their conflict cases, and the
  full annotation contract each backend supplies
- [e2b-compatible API](e2b-compat.md) — serving the e2b REST surface from the
  aggregated apiserver so an unmodified e2b SDK claims from these warm pools:
  flags, endpoint mapping, and the limits worth knowing
- [Lifecycle verbs](lifecycle.md) — pause, resume, fork and snapshot as
  subresources, plus a runnable walk-through over both API surfaces.

- [Scaling design](scaling-design.md) — how claims stay off etcd and what the
  per-node control plane owns.

- [Snapshot placement](snapshot-placement.md) — where a checkpoint lives, how a
  branch reaches it from another node (local hit, probe + redirect, peer heal),
  why shared filesystems are ruled out, and the durability this does *not* give

## Repository

Source and issue tracker:
[github.com/cocoonstack/sandbox-operator](https://github.com/cocoonstack/sandbox-operator).
Part of the [cocoonstack](https://cocoonstack.github.io/) MicroVM platform.
