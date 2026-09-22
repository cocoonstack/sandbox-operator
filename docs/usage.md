# Using the API

Both paths serve `agents.x-k8s.io/v1beta1`, so any Kubernetes client works:
`kubectl`, client-go, controller-runtime, the dynamic client. The types come
from the `sigs.k8s.io/agent-sandbox` module; only the lifecycle payloads and
`NodeInventory` come from this one.

```go
import (
    sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
    cocoonv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
)

sandboxv1beta1.AddToScheme(scheme) // Sandbox
cocoonv1beta1.AddToScheme(scheme)  // NodeInventory
```

## Claiming through the aggregated apiserver

A `Create` against the L3 apiserver is a warm claim, not a scheduling request.
The server derives the pool key from the submitted object and hands back the
sandbox a node just delivered:

```yaml
apiVersion: agents.x-k8s.io/v1beta1
kind: Sandbox
metadata:
  name: demo
  namespace: default
  annotations:
    sandbox.cocoonstack.io/net: none          # pool network lane
    sandbox.cocoonstack.io/ttl-seconds: "600" # lease, when spec.shutdownTime is unset
spec:
  podTemplate:
    spec:
      containers:
        - name: agent
          image: ghcr.io/cocoonstack/sandbox/rt:24.04
          resources:
            requests: { cpu: "1", memory: 2Gi }
```

| Input | Becomes |
|---|---|
| first container's `image` | the pool's `template` |
| `sandbox.cocoonstack.io/net` on the Sandbox or its pod template | the pool's `net` (default `none`) |
| first container's CPU/memory request, else its limit | the pool's `size`: `>4 CPU` or `>8Gi` is `large`, `>1 CPU` or `>2Gi` is `medium`, otherwise `small` |
| `spec.shutdownTime`, else `sandbox.cocoonstack.io/ttl-seconds` | the claim's lease; neither means the node's default |

The response is synthesized, never stored. It carries
`metadata.creationTimestamp` (the claim time the node published),
`status.nodeName`, a `Ready` condition, and the annotations
`sandbox.cocoonstack.io/claim-id`, `/address`, `/token` and `/deadline` — the
granted expiry, which the node may clamp below what was asked for.

No warm microVM for the requested pool is a `503`, retryable as capacity
refills. The aggregated path never cold-starts one.

`Delete` releases the node-local claim and destroys the microVM. Server-side
dry-run is refused with `400` on create, delete and every lifecycle
subresource: the node APIs have no dry-run transaction.

### Reads are eventually consistent

`List` and `Get` are assembled from `NodeInventory`, which nodes republish on a
~30 s cadence, so a read immediately after a create legitimately returns
`NotFound`. Poll until visible — that is what
[`examples/lifecycle`](https://github.com/cocoonstack/sandbox-operator/blob/master/examples/lifecycle/example.go)
does:

```go
err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &sb)
if apierrors.IsNotFound(err) {
    // not published yet; retry
}
```

`watch` is served by re-deriving that view and diffing it, so it inherits the
same lag. Label selectors work against the axes the store stamps:
`sandbox.cocoonstack.io/node`, `/phase`, `/claim` and `/template`.

## Warm capacity

Warm capacity is a `SandboxWarmPool` plus the `SandboxTemplate` it names. On an
L3 cluster the in-process driver resolves that pair into a `(template, net,
size)` key and sets each node's warm target; nothing creates per-sandbox
objects:

```yaml
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxTemplate
metadata:
  name: rt
spec:
  podTemplate:
    metadata:
      annotations:
        sandbox.cocoonstack.io/net: none
    spec:
      containers:
        - name: agent
          image: ghcr.io/cocoonstack/sandbox/rt:24.04
---
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxWarmPool
metadata:
  name: rt
spec:
  replicas: 50
  sandboxTemplateRef:
    name: rt
```

The template's pod template must resolve to the same key a claim derives, or
claims never match the capacity provisioned for them and every create is a
`503`. `status.replicas` and `status.readyReplicas` report the warm microVMs
the fleet actually holds, sampled once per driver tick.

`kubectl patch sandboxwarmpool rt --type=merge -p '{"spec":{"replicas":200}}'`
is the whole scaling interface.

## The Pod path

Against upstream's controller, a `Sandbox` becomes a Pod and a `SandboxClaim`
adopts a pre-warmed one:

```yaml
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxClaim
metadata:
  name: demo
spec:
  warmPoolRef:
    name: rt
```

That path's semantics — claim binding, cold fallback, `spec.operatingMode`
suspend and resume — are upstream's; see
[agent-sandbox](https://agent-sandbox.sigs.k8s.io/docs/). What this repository
adds there is the [pod-template contract](runtime-backends.md) that puts the
Pod on a microVM node.

## Examples

[`examples/`](https://github.com/cocoonstack/sandbox-operator/tree/master/examples)
holds one runnable file per path: `l3/` (warm capacity plus a claim through the
aggregated apiserver), `sandboxd/` (the Pod path on a vk-sandbox node),
`vk-cocoon/`, `standard-kubelet/`, and `lifecycle/` (a Go walk-through of every
verb on both surfaces).

## Lifecycle verbs and the e2b surface

Pause, resume, fork and snapshot are subresources of the aggregated
`sandboxes`; see [lifecycle verbs](lifecycle.md). The optional
[e2b API](e2b-compat.md) maps the same operations onto an unmodified e2b SDK.
