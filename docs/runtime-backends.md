# Runtime backends: the pod-template contract

On the Pod path, upstream's agent-sandbox controller turns a `Sandbox` into a
Pod from `spec.podTemplate` (or from a `SandboxTemplate`'s blueprint). Nothing in
this repository mutates that Pod. Everything a microVM node needs — the node
selector, the toleration and the provider's annotations — is written by the
template author, verbatim, and reaches the provider unchanged.

Three backends serve a sandbox Pod today.

| | standard kubelet | sandboxd (vk-sandbox) | vk-cocoon |
|---|---|---|---|
| node selector | none, or your own | `sandbox.cocoonstack.io/runtime: sandboxd` | `cocoonstack.io/pool: <pool>` |
| toleration | none | `virtual-kubelet.io/provider`, `Exists`, `NoSchedule` | same key |
| required annotations | none | `sandbox.cocoonstack.io/template` | `vm.cocoonstack.io/name` |
| isolation | the cluster's RuntimeClass (Kata, gVisor) | a pre-booted microVM claimed from the node's warm pool | a Cocoon microVM booted or cloned for this Pod |
| delivery | Pod scheduling and image pull | node-local ownership transfer, 0.2–0.7 ms | VM boot or snapshot clone |

The two virtual nodes advertise their labels and taint themselves: vk-sandbox
from `--node-labels` (default `sandbox.cocoonstack.io/runtime=sandboxd`) plus the
taint `virtual-kubelet.io/provider=sandboxd:NoSchedule`, vk-cocoon from
`VK_NODE_POOL` (default pool `default`) plus
`virtual-kubelet.io/provider=cocoon:NoSchedule`. Neither provider reads
`spec.runtimeClassName`: it names a CRI handler, and a virtual node runs no
container runtime.

## sandboxd

```yaml
apiVersion: agents.x-k8s.io/v1beta1
kind: Sandbox
metadata:
  name: sandboxd-example
spec:
  podTemplate:
    metadata:
      annotations:
        sandbox.cocoonstack.io/runtime: sandboxd
        sandbox.cocoonstack.io/template: ghcr.io/cocoonstack/sandbox/rt:24.04
        sandbox.cocoonstack.io/net: none
        sandbox.cocoonstack.io/size: small
    spec:
      nodeSelector:
        sandbox.cocoonstack.io/runtime: sandboxd
      tolerations:
        - key: virtual-kubelet.io/provider
          operator: Exists
          effect: NoSchedule
      containers:
        - name: agent
          image: ghcr.io/cocoonstack/sandbox/rt:24.04
```

| Annotation | Effect |
|---|---|
| `sandbox.cocoonstack.io/runtime` | must be `sandboxd`; absent is read as `sandboxd`, any other value fails the create |
| `sandbox.cocoonstack.io/template` | **required** — the sandboxd pool template. `CreatePod` fails without it |
| `sandbox.cocoonstack.io/net` | pool network lane (`none`, `egress`); empty takes sandboxd's default |
| `sandbox.cocoonstack.io/size` | pool size class (`small`, `medium`, `large`); empty takes sandboxd's default |
| `sandbox.cocoonstack.io/ttl-seconds` | claim lease; absent means 86400 (sandboxd's maximum for an ordinary claim), an explicit `0` takes sandboxd's five-minute default, and a negative or non-integer value fails the create |
| `sandbox.cocoonstack.io/claim-id` | written back by the provider: the claim id backing the Pod |

The container image is not the claim axis — `template` is. Keep the two equal
unless the pool is built from a different reference, or the Pod claims from a
pool nobody provisioned. The pool key the warm-pool driver provisions is
`(template, net, size)`; a Pod naming a key with no warm capacity gets a typed
`CreatePod` failure and stays `Pending`.

Three things set how fast a drained warm pool refills
([performance](performance.md#when-claims-drain-the-pool)):

- **Spread the Pods.** vk-sandbox nodes look identical to the scheduler, which
  then packs one of them. Label the pool's Pods and add a hostname
  `topologySpreadConstraint`, as
  [the example](https://github.com/cocoonstack/sandbox-operator/blob/master/examples/sandboxd/template-warmpool-claim.yaml)
  does.
- **Run a vk-sandbox whose pod queues follow `--kube-api-qps`**
  ([vk-sandbox#19](https://github.com/cocoonstack/vk-sandbox/pull/19)). The
  virtual-kubelet default holds each node to 10 Pod creates and status pushes a
  second.
- **Size both pools.** `SandboxWarmPool` replicas cover the burst you expect,
  and each node's sandboxd keeps enough warm microVMs to refill them.

The full provider contract, including what happens when a Pod is deleted while
its `Sandbox` lives, is in
[vk-sandbox's pod contract](https://github.com/cocoonstack/vk-sandbox/blob/master/docs/pod-contract.md).

## vk-cocoon

```yaml
apiVersion: agents.x-k8s.io/v1beta1
kind: Sandbox
metadata:
  name: cocoon-example
spec:
  podTemplate:
    metadata:
      annotations:
        vm.cocoonstack.io/name: sandbox-default-cocoon-example
        cocoonset.cocoonstack.io/managed: "true"
        cocoonset.cocoonstack.io/mode: run
        cocoonset.cocoonstack.io/image: ubuntu-dev-base
        cocoonset.cocoonstack.io/os: linux
    spec:
      nodeSelector:
        cocoonstack.io/pool: default
      tolerations:
        - key: virtual-kubelet.io/provider
          operator: Exists
          effect: NoSchedule
      containers:
        - name: agent
          image: ubuntu-dev-base
```

| Annotation | Effect |
|---|---|
| `vm.cocoonstack.io/name` | **required** — the VM's name on the node. `CreatePod` fails without it, and a Pod that reuses the name of a live VM adopts that VM instead of booting one |
| `cocoonset.cocoonstack.io/managed` | `"true"` lets cocoon create the VM; an unmanaged Pod must carry a pre-assigned `vm.cocoonstack.io/id` and `vm.cocoonstack.io/ip` |
| `cocoonset.cocoonstack.io/mode` | `run` boots the image, `clone` clones a snapshot |
| `cocoonset.cocoonstack.io/image` | the image in the Cocoon image library the VM boots from |
| `cocoonset.cocoonstack.io/os` | guest OS family (`linux`, `windows`, `macos`); empty means `linux` |

vk-cocoon reads more annotations than these — storage, network, snapshot policy,
probe port, hypervisor backend. They are listed in
[cocoon-common's key set](https://github.com/cocoonstack/cocoon-common/blob/master/meta/keys.go)
and documented in [vk-cocoon](https://cocoonstack.github.io/vk-cocoon/).

Two constraints have no equivalent on the sandboxd path:

- **The VM name is per sandbox, so a `SandboxTemplate` cannot supply it.** A
  template's pod template is copied verbatim, with no per-sandbox
  substitution, so every Sandbox in a warm pool would carry one
  `vm.cocoonstack.io/name` — and the second Pod would adopt the first Pod's VM
  instead of booting its own. Write the annotation per Sandbox; there is no
  vk-cocoon warm-pool example for this reason.
- **cocoon-webhook gates the cocoon toleration.** Where that webhook runs, a
  Pod carrying `virtual-kubelet.io/provider` or `vm.cocoonstack.io/name` is
  denied unless it is owned by a `CocoonSet` *and* its creator is listed in the
  webhook's `POD_CREATORS` (default: the cocoon-operator service account). A
  Sandbox Pod is neither, so that cluster needs the agent-sandbox controller's
  service account added to `POD_CREATORS`.

## What replaced the mutator

Until the upstream import this repo shipped a Pod mutator that filled these
fields in and rejected conflicting ones at admission. It is gone, and so is
admission-time rejection. A wrong pod template now fails later, and differently:

| Mistake | What happens |
|---|---|
| No node selector, or one matching no node | The scheduler never binds the Pod; it stays `Pending` with an unschedulable condition |
| Selector set, toleration missing | The virtual node's `NoSchedule` taint keeps the Pod off it; still `Pending` |
| `spec.nodeName` pinned to a virtual node | The Pod bypasses the scheduler and reaches the provider; it succeeds or fails on the annotations alone |
| Missing required annotation | The provider's `CreatePod` fails; the Pod stays `Pending` and the error is on the Pod's events |
| `sandbox.cocoonstack.io/runtime` set to something other than `sandboxd` on a vk-sandbox node | `CreatePod` refuses it: that node serves one runtime |

The failure is visible on the Pod rather than on the API call that created the
`Sandbox`. Read `kubectl describe pod` when a sandbox stays `Pending`.

## Standard kubelet

A Pod with no virtual-node selector schedules onto an ordinary node and is a
plain container workload. This is upstream's own scope: agent-sandbox delegates
isolation to a sandbox runtime through `spec.runtimeClassName` (Kata Containers,
gVisor). No cocoon-specific behaviour is available on this path.
