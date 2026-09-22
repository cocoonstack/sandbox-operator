# Security model

## Trust boundaries

- The aggregated apiserver authenticates callers through standard Kubernetes
  authn/authz; access to `agents.x-k8s.io` resources is governed by RBAC. The
  e2b surface authenticates with the API keys it is given
  (`apiserver.e2b.apiKeySecret`).
- Node-local warm-pool claims are authorized by the sandboxd bearer token. A
  claimed sandbox is driven with the per-claim token handed back on create:
  the `sandbox.cocoonstack.io/token` annotation on the Kubernetes surface,
  `envdAccessToken` on the e2b surface. Both are secrets — leaking either
  grants control over the corresponding sandboxes, never over the host.
- `sandbox-envd-proxy` holds no fleet credential and mints nothing. It relays a
  request into a guest port only with the sandbox token the caller presents,
  and a caller never learns a node address.
- Sandboxes are hardware-isolated microVMs. A guest escape is a vulnerability
  in the hypervisor stack underneath, coordinated with the relevant upstream.

## Reporting a vulnerability

Do not open a public issue. Report privately through GitHub Security
Advisories — "Report a vulnerability" on the repository's Security tab. Fixes
land on `master` and the most recent tagged release.
