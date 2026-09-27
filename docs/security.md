# Security model

## Trust boundaries

- The aggregated apiserver authenticates callers through standard Kubernetes
  authn/authz; access to `agents.x-k8s.io` resources is governed by RBAC. The
  e2b surface authenticates with the API keys it is given
  (`apiserver.e2b.apiKeySecret`).
- Node-local warm-pool claims are authorized by the sandboxd bearer token. A
  claimed sandbox is driven with the per-claim token handed back on create:
  the `sandbox.cocoonstack.io/token` annotation on the Kubernetes surface.
  The e2b surface hands out `envdAccessToken` instead, an HMAC of that claim
  token under the envd secret (`apiserver.e2b.envdSecret`), which the sandbox's
  own `envd` enforces. Both are secrets: leaking one grants control over that
  sandbox, never over the host, and the e2b token reaches only its data plane.
- `sandbox-envd-proxy` holds the fleet sandboxd token and the envd secret. It
  reads a sandbox's claim token from the owning node, admits a request only
  when the presented token derives from it, and opens the relay with the claim
  token, which no client ever sees. A caller never learns a node address.
- Sandboxes are hardware-isolated microVMs. A guest escape is a vulnerability
  in the hypervisor stack underneath, coordinated with the relevant upstream.

## Reporting a vulnerability

Do not open a public issue. Report privately through GitHub Security
Advisories — "Report a vulnerability" on the repository's Security tab. Fixes
land on `master` and the most recent tagged release.
