# sandbox-operator Helm chart

This chart installs the operator, all upstream agent-sandbox CRDs, RBAC, the
conversion webhook, and certificate management.

## Install

```bash
helm upgrade --install sandbox-operator ./helm \
  --namespace sandbox-system \
  --create-namespace \
  --set image.tag=<version>
```

Extensions are enabled and standard kubelet is selected by default. After the
vk-cocoon cluster is ready, it can be selected cluster-wide with:

```bash
helm upgrade --install sandbox-operator ./helm \
  --namespace sandbox-system \
  --create-namespace \
  --set image.tag=<version> \
  --set controller.defaultRuntime=vk-cocoon
```

To use an existing namespace, set both `namespace.create=false` and
`namespace.name=<namespace>`.

## Upgrade and uninstall

Helm does not upgrade or delete resources in a chart's `crds/` directory.
Apply changed CRDs before upgrading the controller:

```bash
kubectl apply -f helm/crds/
helm upgrade sandbox-operator ./helm \
  --namespace sandbox-system \
  --reuse-values \
  --set image.tag=<new-version>
```

Do not delete the CRDs while sandbox custom resources still exist.

## Important values

| Parameter | Description | Default |
|---|---|---|
| `image.repository` | Operator image repository | `ghcr.io/cocoonstack/sandbox-operator` |
| `image.tag` | Operator image tag; required | `""` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `replicaCount` | Operator replicas | `1` |
| `namespace.create` | Create the operator namespace | `true` |
| `namespace.name` | Operator namespace | `sandbox-system` |
| `podAnnotations` | Extra controller Pod annotations | `{}` |
| `podLabels` | Extra controller Pod labels | `{}` |
| `controller.leaderElect` | Enable leader election | `true` |
| `controller.extensions` | Enable Template, WarmPool, and Claim | `true` |
| `controller.defaultRuntime` | Default Pod backend | `standard` |
| `controller.clusterDomain` | Kubernetes cluster domain | unset (`cluster.local` flag default) |
| `controller.kubeApiQps` | API client QPS, `-1` means unlimited | unset |
| `controller.kubeApiBurst` | API client burst | unset |
| `controller.sandboxConcurrentWorkers` | Sandbox workers | unset (`16` flag default) |
| `controller.sandboxClaimConcurrentWorkers` | Claim workers | unset (`50` flag default) |
| `controller.sandboxWarmPoolConcurrentWorkers` | WarmPool workers | unset (`8` flag default) |
| `controller.sandboxTemplateConcurrentWorkers` | Template workers | unset (`1` flag default) |
| `controller.sandboxWarmPoolMaxBatchSize` | WarmPool batch size | unset (`300` flag default) |
| `controller.enableWarmPoolEviction` | Mark warm Pods safe to evict | unset (`true` flag default) |
| `controller.extraArgs` | Additional operator arguments | `[]` |
| `webhookServiceName` | Conversion-webhook Service | `sandbox-webhook-service` |
| `resources` | Operator requests and limits | `{}` |
| `nodeSelector`, `tolerations`, `affinity` | Operator scheduling | empty |
| `podSecurityContext` | Operator Pod security context | `null` |
| `containerSecurityContext` | Operator container security context | `null` |
