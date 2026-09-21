# Using the Kubernetes API

The CRD operator uses standard Kubernetes clients. In the following fragment,
`c` is a configured controller-runtime client and `ctx` is the caller's context;
register `sandboxv1beta1.AddToScheme` on the client's scheme before use.

Typed (controller-runtime), or `unstructured` / dynamic client if you don't want
to vendor the types:

```go
import (
    corev1 "k8s.io/api/core/v1"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

    sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
)

sb := &sandboxv1beta1.Sandbox{
    ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
    Spec: sandboxv1beta1.SandboxSpec{
        SandboxBlueprint: sandboxv1beta1.SandboxBlueprint{
            PodTemplate: sandboxv1beta1.PodTemplate{
                // Requires a vk-cocoon virtual node.
                ObjectMeta: sandboxv1beta1.PodMetadata{
                    Annotations: map[string]string{"sandbox.cocoonstack.io/runtime": "vk-cocoon"},
                },
                Spec: corev1.PodSpec{
                    Containers: []corev1.Container{{Name: "agent", Image: "ghcr.io/cocoonstack/cocoon/ubuntu:24.04"}},
                },
            },
        },
    },
}
if err := c.Create(ctx, sb); err != nil {
    return err
}
```

Or plain YAML:

```yaml
apiVersion: agents.x-k8s.io/v1beta1
kind: Sandbox
metadata: { name: demo, namespace: default }
spec:
  podTemplate:
    metadata:
      annotations: { sandbox.cocoonstack.io/runtime: vk-cocoon }   # real microVM
    spec:
      containers:
        - { name: agent, image: ghcr.io/cocoonstack/cocoon/ubuntu:24.04 }
```

For low-latency acquisition, define a `SandboxTemplate` + `SandboxWarmPool` and
create `SandboxClaim`s — see [examples](https://github.com/cocoonstack/sandbox-operator/tree/master/examples).

## Selecting a warm pool

In v1alpha1, an omitted `spec.warmpool`, `"default"`, or `"none"` cold-starts
from the template. A named pool enables warm adoption; it does not search all
pools sharing that template. In v1beta1, use `spec.warmPoolRef.name` for the
same explicit selection. If a named CRD pool has no adoptable Sandbox, the
claim falls back to creating one from the template.

The L3 aggregated apiserver has a different capacity contract: it claims from
node-local pools and returns `503 ServiceUnavailable` if no warm capacity is
available. It does not cold-start a CRD Sandbox. See [scaling design](scaling-design.md).

## Lifecycle and other clients

CRD sandboxes suspend and resume through `spec.operatingMode`. The aggregated
apiserver additionally serves pause, resume, fork and snapshot subresources;
see [lifecycle verbs](lifecycle.md). Its optional [e2b API](e2b-compat.md) maps
SDK calls to the same node-local lifecycle.
