# sandbox-operator

The L3 half of agent sandboxes on Kubernetes: an aggregated apiserver that
serves `sandboxes.agents.x-k8s.io` from live node inventory, the warm-pool
driver that fills those nodes, and the e2b-compatible control and data planes in
front of them. The `Sandbox` API itself is
[kubernetes-sigs/agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox),
consumed as a Go module; this repository stores no copy of it and ships no
controller of its own.

## What is in the box

| Component | What it is |
|---|---|
| `cmd/sandbox-apiserver` | Aggregated apiserver for `sandboxes.agents.x-k8s.io/v1beta1`: scatter-gather reads, node-local claim/release on create/delete, the pause/resume/fork/snapshot subresources, the optional e2b REST surface, and the in-process `SandboxWarmPool` driver |
| `cmd/sandbox-envd-proxy` | The e2b data plane: one public entry point that carries `files`, `commands` and `pty` into a sandbox's guest port |
| `nodeinventories.sandbox.cocoonstack.io` | The one CRD this repository owns: per-node summary of live sandboxes, warm capacity and the node's sandboxd address |
| `api/v1beta1` | `NodeInventory` plus the lifecycle subresource payloads |
| `pkg/scale`, `pkg/sandboxd`, `pkg/e2bcompat`, `pkg/envdproxy` | The store, the sandboxd client, the e2b translation layer, the proxy |

## The three paths

```
any Kubernetes client (kubectl / client-go / controller-runtime)
        |                                        e2b SDK (E2B_API_URL)
        v                                               |
kube-apiserver + agent-sandbox CRDs                     v
        |                                   sandbox-apiserver (aggregated)
        +-- upstream controller -> Pod           |            |
        |     routed by the pod-template         |            +-- e2b REST
        |     contract to a virtual node         |
        v                                        v
   vk-sandbox / vk-cocoon  ------------>  sandboxd node-local warm pools
                                                 ^
                                  sandbox-envd-proxy (files / commands / pty)
```

The Pod path and the L3 path both end at the same node-local warm pool; they
differ in what Kubernetes stores. On the Pod path etcd holds one object per
sandbox. On the L3 path it holds one `NodeInventory` per node and the pool's
intent, so object count is `O(pools + nodes)` however many sandboxes are live —
which is why one `SandboxWarmPool` patch could take 20 nodes to 50 000 running
microVMs while etcd saw ~2 writes/s.

The cost of that design is a list view assembled from inventory nodes
republish on a ~30 s cadence: `list` and `watch` are eventually consistent. A
lookup by claim id — the e2b surface, the envd proxy — for a sandbox the
inventory does not list yet asks the nodes directly; a lookup by name does so
on the apiserver replica that served the create.

## Guides

- [Using the API](usage.md) — claiming through the aggregated apiserver, the
  pool key a create derives, warm capacity, and what the Pod path does instead
- [Configuration](configuration.md) — both binaries' flags, the chart values,
  and the two install shapes
- [Runtime backends](runtime-backends.md) — the explicit pod-template contract
  for vk-sandbox and vk-cocoon, and what fails now that no mutator fills it in
- [Lifecycle verbs](lifecycle.md) — pause, resume, fork and snapshot as
  subresources, plus a runnable walk-through over both API surfaces
- [e2b-compatible API](e2b-compat.md) — serving the e2b REST surface so an
  unmodified e2b SDK claims from these warm pools: flags, endpoint mapping, and
  the limits worth knowing
- [envd-proxy](envd-proxy.md) — the data-plane half of that surface: one public
  entry point that carries `files`, `commands` and `pty` into the right
  sandbox's guest port, over vsock and without exposing a node
- [Scaling design](scaling-design.md) — how claims stay off etcd and what the
  per-node control plane owns
- [Snapshot placement](snapshot-placement.md) — where a checkpoint lives, how a
  branch reaches it from another node, and the durability this does *not* give
- [API reference](api.md) — the generated reference for
  `sandbox.cocoonstack.io/v1beta1`. The `agents.x-k8s.io` and
  `extensions.agents.x-k8s.io` types are upstream's; their reference is in
  [agent-sandbox's docs](https://github.com/kubernetes-sigs/agent-sandbox/blob/main/docs/api.md)
- [Performance](performance.md) — the fleet-supply and claim numbers, how they
  were measured, and how to reproduce them
- [Security model](security.md) — trust boundaries and how to report a
  vulnerability
- [Roadmap](roadmap.md) — what comes next, by priority

## Repository

Source and issue tracker:
[github.com/cocoonstack/sandbox-operator](https://github.com/cocoonstack/sandbox-operator).
Part of the [cocoonstack](https://cocoonstack.github.io/) MicroVM platform.
