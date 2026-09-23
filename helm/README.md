# sandbox-operator Helm chart

The single source of deployment truth for this repo. It installs the L3
aggregated `sandbox-apiserver` (the `v1beta1.agents.x-k8s.io` APIService, its
Service, RBAC, PodDisruptionBudget and cert-manager serving-cert chain), the
`nodeinventories.sandbox.cocoonstack.io` CRD, and — with the e2b surface
enabled — the `sandbox-envd-proxy` data plane.

It does not install a Pod-path controller: that is upstream's own
`kubernetes-sigs/agent-sandbox` release. Do not install upstream's `Sandbox` CRD
for `agents.x-k8s.io/v1beta1` — the APIService in this chart shadows it.

## Install

`sandbox-system` is the conventional namespace; every namespaced object lands in
the release namespace.

```bash
helm upgrade --install sandbox-operator ./helm \
  --namespace sandbox-system \
  --create-namespace \
  --set apiserver.image.tag=<version> \
  --set envdProxy.image.tag=<version>
```

cert-manager must be installed first. Without it, set `certManager.enabled=false`
and supply both a serving-cert Secret named in `certManager.servingCertSecret`
and its CA in `certManager.caBundle`.

The e2b-compatible REST surface is off by default, and the proxy renders only
with it. Enabling it requires a domain and a Secret of API keys:

```bash
helm upgrade --install sandbox-operator ./helm \
  --namespace sandbox-system \
  --set apiserver.e2b.enabled=true \
  --set apiserver.e2b.domain=sandbox.example.com \
  --set apiserver.e2b.apiKeySecret.name=e2b-api-keys
```

Wildcard DNS for `*.{domain}` must resolve to the `sandbox-envd-proxy` Service:
the SDK addresses every sandbox as `{port}-{sandboxID}.{domain}`.

## Upgrade and uninstall

Helm does not upgrade or delete resources in a chart's `crds/` directory. Apply
a changed CRD before upgrading:

```bash
kubectl apply -f helm/crds/
helm upgrade sandbox-operator ./helm --namespace sandbox-system --reuse-values
```

An older `nodeinventories` CRD silently prunes entry fields it does not know
(`claimedAt` is one), so until the CRD is applied every node publishes like an
older node and the read view falls back accordingly.

`NodeInventory` moved from `extensions.agents.x-k8s.io` to
`sandbox.cocoonstack.io`. A fleet coming from the old group runs a vk-sandbox
that publishes into the new one on every node first, then deletes the old CRD;
its objects are not converted.

Do not delete the CRD while NodeInventory objects still exist.

## Values

| Parameter | Description | Default |
|---|---|---|
| `apiserver.image.repository` | Aggregated apiserver image | `ghcr.io/cocoonstack/sandbox-apiserver` |
| `apiserver.image.tag` | Image tag; pin a release | `latest` |
| `apiserver.image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `apiserver.replicaCount` | Apiserver replicas | `2` |
| `apiserver.securePort` | Port the aggregated API is served on | `6443` |
| `apiserver.warmPoolDriver` | Run the in-process SandboxWarmPool driver | `true` |
| `apiserver.sandboxdToken.secretName` | Secret holding the sandboxd api_token; empty sends node-local calls without one | `""` |
| `apiserver.sandboxdToken.key` | Key within that Secret | `token` |
| `apiserver.e2b.enabled` | Serve the e2b-compatible REST surface | `false` |
| `apiserver.e2b.domain` | Base domain sandbox hosts derive from; required once enabled | `""` |
| `apiserver.e2b.namespace` | Namespace e2b claims land in | `default` |
| `apiserver.e2b.envdVersion` | envd version reported to the SDK | `""` (binary default) |
| `apiserver.e2b.defaultTimeoutSeconds` | Lease granted to a create naming no timeout | `0` (binary default) |
| `apiserver.e2b.port` | Port the e2b surface listens on | `8080` |
| `apiserver.e2b.apiKeySecret.name` | Secret of accepted API keys; required once enabled | `""` |
| `apiserver.e2b.apiKeySecret.key` | Key within that Secret | `keys` |
| `apiserver.resources` | Apiserver requests and limits | 100m/128Mi, limit 512Mi |
| `envdProxy.image.repository` | Proxy image | `ghcr.io/cocoonstack/sandbox-envd-proxy` |
| `envdProxy.image.tag` | Image tag; pin a release | `latest` |
| `envdProxy.image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `envdProxy.replicaCount` | Proxy replicas | `2` |
| `envdProxy.domain` | Base domain; falls back to `apiserver.e2b.domain` | `""` |
| `envdProxy.namespace` | Namespace inventory lookups are narrowed to (not an access boundary); empty resolves across every namespace, which keyed namespaces need | `""` |
| `envdProxy.port` | Port the proxy listens on | `8443` |
| `envdProxy.tlsSecretName` | Wildcard cert for `*.{domain}`; empty serves h2c | `""` |
| `envdProxy.resources` | Proxy requests and limits | 100m/128Mi, limit 512Mi |
| `certManager.enabled` | Generate the serving-cert chain and inject the CA | `true` |
| `certManager.servingCertSecret` | Serving-cert Secret the apiserver mounts | `sandbox-apiserver-serving-certs` |
| `certManager.caBundle` | Base64 PEM CA for the APIService; required when cert-manager is disabled | `""` |
