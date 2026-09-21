# Runtime backends

## Decision

The rollout default is standard kubelet. vk-cocoon is an explicit per-Sandbox
opt-in until the target cluster is ready and the integration test suite has
passed.

vk-cocoon provides the intended Kubernetes-facing contract for Cocoon: Pods
schedule to virtual nodes and are materialized as MicroVMs, while Pod status,
exec, projected configuration, networking, recovery, and VM identity remain
visible through Kubernetes. It is retained behind explicit selection for the
upcoming cluster validation.

The current `containerd-shim-cocoon-v2` prototype is not yet a replacement for
that contract. It needs conformance work for complete multi-container Pod,
volume/mount, streaming I/O and signals, pause/resume persistence, and restart
recovery semantics before standard kubelet plus the Cocoon shim can replace
vk-cocoon as the Cocoon hot-MicroVM implementation.

## Selection rules

Runtime selection is deterministic, in this order:

1. `sandbox.cocoonstack.io/runtime: vk-cocoon|standard|sandboxd` on the Pod
   template.
2. Any explicit `spec.runtimeClassName` selects the standard kubelet path.
3. The operator's `--default-runtime` value, which defaults to `standard`.

An explicit `vk-cocoon` or `sandboxd` annotation conflicts with
`runtimeClassName` and is rejected, as is a pinned `spec.nodeName` — it would
bypass the node selector and misroute the Pod. A conflicting virtual-node
selector is also rejected. User-provided annotations and tolerations are
otherwise preserved.

## vk-cocoon contract

For a vk-cocoon Pod, the operator supplies these defaults:

| Key | Value or source |
|---|---|
| node selector `node.kubernetes.io/instance-type` | `virtual-node` |
| toleration `virtual-kubelet.io/provider` | `Exists`, `NoSchedule` |
| `sandbox.cocoonstack.io/runtime` | `vk-cocoon` |
| `cocoonset.cocoonstack.io/mode` | `run` |
| `cocoonset.cocoonstack.io/managed` | `true` |
| `cocoonset.cocoonstack.io/image` | first container image |
| `cocoonset.cocoonstack.io/os` | Pod OS, default `linux` |
| `vm.cocoonstack.io/name` | stable namespace/name-derived DNS label |

The image must exist in the Cocoon image library on eligible vk-cocoon nodes.

## sandboxd contract

For a sandboxd Pod, the operator supplies these defaults:

| Key | Value or source |
|---|---|
| node selector `sandbox.cocoonstack.io/runtime` | `sandboxd` |
| toleration `virtual-kubelet.io/provider` | `Exists`, `NoSchedule` (shared with vk-cocoon) |
| `sandbox.cocoonstack.io/runtime` annotation | `sandboxd` |
| `sandbox.cocoonstack.io/template` | first container image, if unset |
| `sandbox.cocoonstack.io/net` | the pod template's annotation, default `none`, if unset |
| `sandbox.cocoonstack.io/size` | `small`/`medium`/`large` from the first container's requests, if unset |

This routes the Pod to the vk-sandbox virtual node, which serves the claim
from a node-local sandboxd (`github.com/cocoonstack/sandbox`) in
sub-millisecond time. It is the successor to vk-cocoon for agent-sandbox
workloads: vk-cocoon no longer answers sandbox Pods when this mode is
selected.

## Standard kubelet contract

The standard path leaves the generated Pod runtime and scheduling fields
untouched. It can use the cluster default runtime or an explicit RuntimeClass
such as Kata Containers or gVisor. This path implements the upstream
agent-sandbox Kubernetes semantics, but Cocoon-specific hot-MicroVM behavior is
available only when the selected standard-kubelet runtime handler implements it.
