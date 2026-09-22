# Configuration

This repository ships two binaries and one Helm chart. Flag text below is the
binaries' own `--help` output.

## Installation

### Pod path

Upstream's release installs the CRDs and the controller that serves Pods:

```bash
kubectl apply -f https://github.com/kubernetes-sigs/agent-sandbox/releases/download/v1.0.3/sandbox-with-extensions.yaml
```

That is the whole install for the Pod path; nothing from this repository is
needed. Routing a Sandbox Pod to a microVM node is the
[pod-template contract](runtime-backends.md).

### L3 path

Prerequisites: cert-manager, unless you set `certManager.enabled=false` and
supply the serving certificate yourself; and, for the e2b data plane, wildcard
DNS for `*.{domain}` resolving to the `sandbox-envd-proxy` Service.

The warm-pool driver reads two of upstream's CRDs. Apply those, then the chart:

```bash
VERSION=v1.0.3
for crd in extensions.agents.x-k8s.io_sandboxtemplates extensions.agents.x-k8s.io_sandboxwarmpools; do
  kubectl apply -f "https://raw.githubusercontent.com/kubernetes-sigs/agent-sandbox/${VERSION}/k8s/crds/${crd}.yaml"
done

helm upgrade --install sandbox-operator ./helm \
  --namespace sandbox-system --create-namespace \
  --set apiserver.image.tag="$VERSION_OF_THIS_REPO" \
  --set envdProxy.image.tag="$VERSION_OF_THIS_REPO"
```

Image tags default to `latest`, so the chart renders with no `--set` at all;
pin both to a release tag in production. The e2b surface is off by default and
needs three values together:

```bash
helm upgrade --install sandbox-operator ./helm \
  --namespace sandbox-system --create-namespace \
  --set apiserver.e2b.enabled=true \
  --set apiserver.e2b.domain=sandbox.example.com \
  --set apiserver.e2b.apiKeySecret.name=e2b-api-keys
```

Enabling it without the key secret fails at render
(`apiserver.e2b.apiKeySecret.name is required when apiserver.e2b.enabled is
true`) — the surface refuses to serve unauthenticated.

The chart's default render is 21 objects: the
`nodeinventories.sandbox.cocoonstack.io` CRD, a Deployment, Service and
ServiceAccount for each binary, the apiserver's PodDisruptionBudget, the
`APIService`, the RBAC (one ClusterRole per binary granting `get`/`list`/`watch`
on `nodeinventories`, the `system:auth-delegator` binding, the leader-election
Role, and the `sandbox-apiserver-auth-reader` RoleBinding that must live in
`kube-system` to read `extension-apiserver-authentication`), and the cert-manager
chain — a self-signed Issuer, a CA Certificate, a CA Issuer and the serving
Certificate. Every other object goes to `.Release.Namespace`; object names are
fixed (`sandbox-apiserver`, `sandbox-envd-proxy`), not release-prefixed, so the
release name is free.

It ships no upstream CRD.

Two rules follow from the design, and breaking either one breaks the cluster:

- **The `APIService` shadows the upstream `Sandbox` CRD.** Registering
  `agents.x-k8s.io/v1beta1` with an aggregated server sends every request for
  that group-version to `sandbox-apiserver`, which serves `sandboxes` and
  nothing else. This is deliberate — it is how `metrics.k8s.io` serves
  `PodMetrics`. The consequence is concrete: where both exist, aggregation wins
  and objects stored under the CRD are unreachable through that API. That is
  why the L3 install above applies only the `extensions.agents.x-k8s.io` CRDs,
  and why one cluster cannot serve both the Pod path and the L3 path for the
  same group-version.
- **Upstream's controller must not run against an L3 cluster.** Its
  `SandboxWarmPool` reconciler provisions warm capacity by creating `Sandbox`
  objects, and here the in-process warm-pool driver owns that capacity by
  setting node-local pool targets. Upstream's `--extensions` is a single
  boolean, so there is no way to keep its `SandboxClaim` controller while
  disabling its warm-pool one.

Images are published to GHCR on a release tag:
`ghcr.io/cocoonstack/sandbox-apiserver` and
`ghcr.io/cocoonstack/sandbox-envd-proxy`, multi-arch (amd64/arm64).

## sandbox-apiserver

The L3 aggregated apiserver. It reads `NodeInventory` through an informer and
needs `get`/`list`/`watch` on `nodeinventories.sandbox.cocoonstack.io`; the
chart's RBAC grants that. It takes its kube client from the in-cluster service
account, or from `KUBECONFIG` when run outside a cluster.

### Node-local claim path

| Flag | Default | Help |
|---|---|---|
| `--sandboxd-token` | — | Uniform fleet-wide sandboxd api_token presented on node-local claim/release. Prefer `--sandboxd-token-file` for a Secret mount. |
| `--sandboxd-token-file` | — | Path to a file (Secret mount) holding the sandboxd api_token; overrides `--sandboxd-token` when set. |

With both empty the write path stays disabled and fails closed: reads still
work, `Create`/`Delete` do not.

### Warm-pool driver

| Flag | Default | Help |
|---|---|---|
| `--enable-warm-pool-driver` | `true` | Run the in-process SandboxWarmPool → sandboxd pool reconcile loop (control-plane warm-capacity surface; pool-level, never per-sandbox). |
| `--warm-pool-sync-interval` | `0` (means 5s) | Resync cadence for the SandboxWarmPool driver, and with it the sampling period of the warm count in pool status. |

The driver watches `SandboxWarmPool` and `NodeInventory`, resolves each pool's
`SandboxTemplate` into a `(template, net, size)` key, spreads
`spec.replicas` evenly across the nodes that advertise a sandboxd address, and
`PUT`s each node its full pool set. It writes `status.replicas` and
`status.readyReplicas` back from the warm counts those calls report. It runs
under leader election (`cocoon-warmpool-driver`), so one replica drives the
pools. An empty sandboxd token leaves it fail-closed: it logs and sets no pools.

### e2b-compatible surface

| Flag | Default | Help |
|---|---|---|
| `--enable-e2b-api` | `false` | Serve the e2b-compatible REST surface, so an unmodified e2b SDK can claim from the same warm pools (point E2B_API_URL at it). |
| `--e2b-bind-address` | `:8080` | Address the e2b-compatible surface listens on. |
| `--e2b-namespace` | `default` | Namespace e2b claims are made in; e2b has no namespace concept, so every compat claim lands here. |
| `--e2b-domain` | — | Base domain the SDK derives the in-sandbox envd host from, as `{port}-{sandboxID}.{domain}`. Required with `--enable-e2b-api`: without it a created sandbox has no reachable data plane. |
| `--e2b-envd-version` | `0.4.0` when empty | envd version reported to the SDK. It must name the envd actually installed in the pool's image; the SDK version-compares it and kills the sandbox when it cannot parse one. |
| `--e2b-default-timeout` | `300` when `0` | Lease in seconds granted to a create that names no timeout, and the lease an SDK refresh renews for. |
| `--e2b-api-key-file` | — | Path to a file (Secret mount) of accepted e2b API keys, one per line, presented by the SDK as X-API-KEY. |
| `--e2b-allow-anonymous` | `false` | Serve the e2b surface with NO API key. Development only: it leaves the claim endpoint open to anyone who can reach the port. |

Startup fails when `--enable-e2b-api` is set with neither a key file nor
`--e2b-allow-anonymous`, and when `--e2b-domain` is empty. Details of the
surface are in [e2b compatibility](e2b-compat.md).

### Serving and delegated auth

The binary also takes the standard aggregated-apiserver option sets from
`k8s.io/apiserver` — secure serving, delegated authentication and
authorization, feature gates. `--help` prints all of them. The ones a
deployment sets are `--secure-port` (default `6443`), `--tls-cert-file` /
`--tls-private-key-file` (or `--cert-dir`, which self-signs when the pair is
absent), and `--authentication-kubeconfig` / `--authorization-kubeconfig` when
the server runs outside the cluster. There is deliberately no etcd option: this
server stores nothing.

## sandbox-envd-proxy

The e2b data plane. It resolves a sandbox from the same `NodeInventory`
informer and relays the request into the owning node's guest-port endpoint.

| Flag | Default | Help |
|---|---|---|
| `--bind-address` | `:8443` | Address the proxy listens on. |
| `--domain` | — | Base domain sandbox hosts are derived from, as `{port}-{sandboxID}.{domain}`. Must match the apiserver's `--e2b-domain`. |
| `--namespace` | `default` | Namespace sandbox lookups are scoped to; empty matches every namespace. |
| `--tls-cert-file` | — | Wildcard certificate for `*.{domain}`. Omit to serve cleartext h2c behind an edge that terminates TLS. |
| `--tls-private-key-file` | — | Private key for `--tls-cert-file`. |
| `--guest-http2` | `false` | Forward to the guest over cleartext HTTP/2. Off by default: envd 0.8.0 installs no h2c handler and refuses it. Clients still reach this proxy over HTTP/2. |

The two TLS flags must be set together. Routing, authorization and failure
mapping are in [envd-proxy](envd-proxy.md).

## Chart values

| Value | Default | Effect |
|---|---|---|
| `apiserver.image.repository` | `ghcr.io/cocoonstack/sandbox-apiserver` | Aggregated apiserver image |
| `apiserver.image.tag` | `latest` | Pin a release tag in production |
| `apiserver.image.pullPolicy` | `IfNotPresent` | |
| `apiserver.replicaCount` | `2` | Apiserver replicas |
| `apiserver.securePort` | `6443` | `--secure-port` |
| `apiserver.warmPoolDriver` | `true` | `--enable-warm-pool-driver` |
| `apiserver.sandboxdToken.secretName` | `""` | Secret holding the fleet sandboxd `api_token`, mounted for `--sandboxd-token-file`. Empty leaves the `Create`/`Delete` write path closed |
| `apiserver.sandboxdToken.key` | `token` | Key within that Secret |
| `apiserver.e2b.enabled` | `false` | `--enable-e2b-api` |
| `apiserver.e2b.domain` | `""` | `--e2b-domain`; required once enabled |
| `apiserver.e2b.namespace` | `default` | `--e2b-namespace` |
| `apiserver.e2b.envdVersion` | `""` | `--e2b-envd-version`; empty takes the binary's `0.4.0` |
| `apiserver.e2b.defaultTimeoutSeconds` | `0` | `--e2b-default-timeout`; `0` takes the binary's 300 |
| `apiserver.e2b.port` | `8080` | `--e2b-bind-address` |
| `apiserver.e2b.apiKeySecret.name` | `""` | Secret of accepted `X-API-KEY` values, mounted for `--e2b-api-key-file`; required once e2b is enabled |
| `apiserver.e2b.apiKeySecret.key` | `keys` | Key within that Secret |
| `apiserver.resources` | 100m / 128Mi requests, 512Mi limit | Apiserver resources |
| `envdProxy.image.repository` | `ghcr.io/cocoonstack/sandbox-envd-proxy` | Proxy image |
| `envdProxy.image.tag` | `latest` | Pin a release tag in production |
| `envdProxy.image.pullPolicy` | `IfNotPresent` | |
| `envdProxy.replicaCount` | `2` | Proxy replicas |
| `envdProxy.domain` | `""` | `--domain`; falls back to `apiserver.e2b.domain` |
| `envdProxy.namespace` | `default` | `--namespace` |
| `envdProxy.port` | `8443` | `--bind-address` |
| `envdProxy.tlsSecretName` | `""` | Wildcard certificate for `*.{domain}`; empty serves cleartext h2c |
| `envdProxy.resources` | 100m / 128Mi requests, 512Mi limit | Proxy resources |
| `certManager.enabled` | `true` | Render the Issuer/Certificate chain for the apiserver's serving cert |
| `certManager.servingCertSecret` | `sandbox-apiserver-serving-certs` | TLS Secret the apiserver serves from |
| `certManager.caBundle` | `""` | Base64 PEM; required when `certManager.enabled=false` |

Four behaviours are worth knowing before you set these:

- **One domain feeds both binaries.** `envdProxy.domain` defaults to
  `apiserver.e2b.domain`, because the apiserver and the proxy must spell
  `{port}-{sandboxID}.{domain}` identically. Set `envdProxy.domain` only to
  override. With neither set, `--domain` is omitted and the proxy exits at
  startup.
- **TLS on the proxy switches more than the flag.** An empty
  `envdProxy.tlsSecretName` also makes the probes HTTP and the Service port
  80/`http`; set, they become HTTPS and 443/`https`.
- **`apiserver.warmPoolDriver=false` narrows RBAC.** The chart grants the
  `extensions.agents.x-k8s.io` rules and the leader-election Role/RoleBinding
  only when the driver runs.
- **`certManager.enabled=false` makes you supply both halves.** You provide the
  pre-provisioned `certManager.servingCertSecret` *and* `certManager.caBundle`,
  which goes straight into `APIService.spec.caBundle` in place of the
  `cert-manager.io/inject-ca-from` annotation. Rendering without it fails with
  `certManager.caBundle is required when certManager.enabled is false`.

`helm show values ./helm` prints the chart's full value set.

## Observability

`sandbox-apiserver` serves the generic apiserver's own `/metrics`,
`/healthz`, `/readyz` and `/livez` on its secure port, and `--profiling` (on by
default) exposes `/debug/pprof`. The warm-pool driver's controller-runtime
metrics and health servers are disabled: the aggregated apiserver owns the
serving port. `sandbox-envd-proxy` serves an unauthenticated `GET /healthz` for
probes and no metrics endpoint.
