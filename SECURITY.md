# Security Policy

## Reporting a vulnerability

Please do **not** open public issues for suspected vulnerabilities.

Report privately through GitHub Security Advisories — "Report a vulnerability"
on this repository's Security tab. If that is not possible, contact the
maintainers listed in [MAINTAINERS.md](MAINTAINERS.md) directly.

You will receive an acknowledgment within 3 business days. We aim to ship a
fix, coordinated with the reporter, within 90 days of triage; credit is given
unless you prefer otherwise.

## Supported versions

Security fixes land on `master` and the most recent tagged release.

## Trust boundaries

Useful context when assessing impact:

- The aggregated apiserver authenticates callers through standard Kubernetes
  authn/authz; access to `agents.x-k8s.io` resources is
  governed by RBAC.
- Node-local warm-pool claims are authorized by the sandboxd bearer token;
  per-sandbox exec is authorized by a per-claim token surfaced as the
  `sandbox.cocoonstack.io/token` annotation. Both are secrets — leaking either
  grants control over the corresponding sandbox(es), though never over the
  host.
- Sandboxes themselves are hardware-isolated microVMs; a guest escape is a
  vulnerability in the underlying hypervisor stack, but we still want to hear
  about it and will coordinate with the relevant upstream.
