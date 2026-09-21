# Operator configuration

The operator is intentionally usable with its defaults: leader election and
all agent-sandbox extension controllers are enabled, and generated Pods use
standard kubelet scheduling.

## Runtime and API surface

- `--default-runtime` (`standard`): default backend for newly created Sandbox
  Pods. Valid values are `standard`, `vk-cocoon`, and `sandboxd`; `sandboxd`
  routes Sandbox Pods to the vk-sandbox hot-pool virtual node.
- `--extensions` (`true`): enable `SandboxTemplate`, `SandboxWarmPool`, and
  `SandboxClaim` controllers and webhooks.
- `--cluster-domain` (`cluster.local`): suffix used to construct Sandbox
  Service FQDNs.

An explicit Pod-template `runtimeClassName` selects standard kubelet, unless an
explicit `sandbox.cocoonstack.io/runtime=vk-cocoon` or `sandboxd` annotation is
also set — the two conflict, and the Pod is rejected. Otherwise an explicit
`sandbox.cocoonstack.io/runtime` annotation takes precedence over the default.

## Cache scoping

The operator's Pod, Service, and PersistentVolumeClaim informers are label
scoped to `agents.x-k8s.io/sandbox-name-hash`, so they watch only the children
the operator itself labels rather than every object in the cluster. A Pod,
Service, or PVC created outside the operator is therefore invisible to it: an
external warm pool that wants its objects adopted must set that label, and the
`agents.x-k8s.io/adoptable` label alone is not enough.

## Controller concurrency

The defaults are the configuration [PERFORMANCE.md](https://github.com/cocoonstack/sandbox-operator/blob/master/PERFORMANCE.md)
was measured with, so an out-of-box install reproduces the published numbers.

- `--sandbox-concurrent-workers` (16)
- `--sandbox-claim-concurrent-workers` (50)
- `--sandbox-warm-pool-concurrent-workers` (8)
- `--sandbox-template-concurrent-workers` (1)
- `--sandbox-warm-pool-max-batch-size` (300)
- `--enable-warm-pool-eviction` (`true`)
- `--sandbox-warm-pool-disable-cr-management` (`false`) — the warm-pool
  controller only reports pool status and creates no Sandbox CRs. Set it when
  the L3 aggregated apiserver owns warm capacity, so the two do not both
  provision
- `--kube-api-qps` (200) — a negative value disables client-side rate limiting
  entirely, which also makes `--kube-api-burst` meaningless
- `--kube-api-burst` (400)

## Webhook and leader election

- `--leader-elect` (`true`)
- `--leader-election-namespace` (auto-detected when empty)
- `--webhook-port` (9443)
- `--webhook-cert-dir` (`/tmp/k8s-webhook-server/serving-certs`)
- `--webhook-service-name` (`sandbox-webhook-service`)
- `--webhook-namespace` (`sandbox-system`)
- `--manage-webhook-certs` (`true`)

When `--manage-webhook-certs=true`, the operator creates serving certificates
and patches conversion-webhook CA bundles. A shared certificate within 30 days
of expiry is reissued at startup and the Secret updated; a replica that loses
that update adopts the winner's pair. Disable it only when the cluster manages
both externally.

## Observability

- `--metrics-bind-address` (`:8080`)
- `--health-probe-bind-address` (`:8081`)
- `--enable-tracing` (`false`)
- `--enable-pprof` (`false`)
- `--enable-pprof-debug` (`false`)
- `--pprof-block-profile-rate` (1000000) — goroutine block profiling rate,
  applied only with `--enable-pprof-debug`
- `--pprof-mutex-profile-fraction` (10) — mutex contention sampling, applied
  only with `--enable-pprof-debug`
- `--version` — print the build identity and exit

Use `--enable-pprof-debug` only in controlled environments because it exposes
process details and enables block/mutex sampling.

### Metrics

Beyond the controller-runtime defaults, `--metrics-bind-address` serves:

| Metric | Type | Notes |
|---|---|---|
| `agent_sandbox_claim_startup_latency_ms{launch_type,sandbox_template,warmpool_name}` | Histogram | SandboxClaim creation → Sandbox Ready, end to end. The number a caller feels |
| `agent_sandbox_claim_controller_startup_latency_ms{…}` | Histogram | Controller-first-observed → Ready. Same path minus webhook and apiserver time, so the gap between the two is admission overhead |
| `agent_sandbox_creation_latency_ms{namespace,launch_type,sandbox_template}` | Histogram | Sandbox creation → Pod Ready; for a warm launch this is controller sync overhead only, since the Pod is already up |
| `agent_sandbox_claim_creation_total{namespace,sandbox_template,launch_type,warmpool_name,pod_condition,created_by}` | Counter | Claims created; `created_by` separates go-client / python-client / controller / loadgen |
| `agent_sandbox_warm_created_total{namespace,warmpool_name}` | Counter | Sandboxes the warm-pool controller created — both initial fill and claim-consumed replacement. Event-level, so fill rate survives scrape gaps |
| `agent_sandboxes{namespace,ready_condition,expired,launch_type,sandbox_template,owned_by,created_by}` | Gauge | Point-in-time sandbox inventory, collected on scrape |
| `agent_sandbox_build_info` | Gauge | Constant 1 carrying version / commit / build-date labels |

`launch_type` is `warm` when a claim adopted a pre-booted sandbox and `cold`
when one was created for it (`unknown` only on a failure with no Sandbox) —
the ratio is the warm pool's hit rate, and the latency split between the two
is the reason the pool exists.

## Example patch

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sandbox-operator
  namespace: sandbox-system
spec:
  template:
    spec:
      containers:
        - name: sandbox-operator
          args:
            - --leader-elect=true
            - --extensions=true
            - --default-runtime=standard
            - --sandbox-concurrent-workers=32
            - --sandbox-claim-concurrent-workers=40
```

Helm exposes the common flags under `controller.*`; use
`controller.extraArgs` for other flags.
