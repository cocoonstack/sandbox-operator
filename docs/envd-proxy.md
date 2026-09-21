# envd-proxy

`envd-proxy` is the **data plane** half of the [e2b-compatible
API](e2b-compat.md). The apiserver's compat surface answers
`Sandbox.create()`; everything the SDK does afterwards — `files`, `commands`,
`pty` — goes to the in-sandbox `envd` daemon instead, over a completely
different address. This is the proxy that carries it there.

It translates nothing. The SDK speaks `envd`'s own contract end to end; the
proxy only decides which sandbox a request belongs to, authorizes it with that
sandbox's token, and hands the bytes to the owning node.

## The path

```
           unmodified e2b SDK
                   │
    ┌──────────────┼───────────────────────────┐
    │ control REST │ data plane HTTPS          │
    │ E2B_API_URL  │ {port}-{sandboxID}.{domain}
    ▼              ▼
sandbox-apiserver  envd-proxy          ← own Deployment, never a compute node
--enable-e2b-api        │ id → owning node (NodeInventory)
    │                   ▼
    │              sandboxd  GET /v1/sandboxes/{id}/ports/{port}
    │                   │ vsock → silkd port_forward
    │                   ▼
    └─── claim ──── guest 127.0.0.1:{port}   (envd, loopback only)
```

A sandbox on the hardened `none` lane has **no NIC**, so the node's guest-port
relay is the only path in. The guest's `envd` binds loopback and is never
exposed; a client only ever reaches one public name and never learns a node
address.

## Run it

```bash
envd-proxy \
  --bind-address=:8443 \
  --domain=sandbox.example.com \
  --namespace=sandboxes \
  --tls-cert-file=/etc/tls/tls.crt \
  --tls-private-key-file=/etc/tls/tls.key
```

| Flag | Default | Meaning |
|---|---|---|
| `--bind-address` | `:8443` | Public listener. |
| `--domain` | — | **Required.** Base domain sandbox hosts are derived from. Must match the apiserver's `--e2b-domain`. |
| `--namespace` | `default` | Namespace sandbox lookups are scoped to; empty matches every namespace. |
| `--tls-cert-file` | — | Wildcard certificate for `*.{domain}`. Omit to serve cleartext behind an edge that terminates TLS. |
| `--tls-private-key-file` | — | Key for the above; the two must be set together. |
| `--guest-http2` | `false` | Forward to the guest over cleartext HTTP/2. `envd` 0.8.0 does not serve it. |

The deployment needs wildcard DNS for `*.{domain}` pointing at the proxy, and a
certificate covering it. `GET /healthz` is unauthenticated, for probes.

It reads `NodeInventory` through an informer, so a data-plane request never
becomes a LIST against the kube-apiserver; RBAC needs `get`/`list`/`watch` on
`nodeinventories.extensions.agents.x-k8s.io`.

## Routing

Two request shapes, both of which the SDK sends:

1. **Derived host** — `{port}-{sandboxID}.{domain}`, e.g.
   `49983-sb-0123abcd.sandbox.example.com`. A sandbox id carries hyphens of its
   own, so only the *first* hyphen separates the port.
2. **Headers** — `E2b-Sandbox-Id` + `E2b-Sandbox-Port`, used when every sandbox
   shares one host. Headers win when both are present.

Ids are accepted in either spelling, `sb_0123abcd` or the DNS-safe
`sb-0123abcd` the compat API publishes.

`49983` is `envd`'s port, but nothing here is specific to it: this is a general
guest-port gateway, so a server a user started on `3000` inside the sandbox is
reached the same way.

## Authorization

The client sends `X-Access-Token`, the per-sandbox token minted at claim time
and returned by `POST /sandboxes` — the e2b SDK does this on its own. The proxy
presents it to the owning node as `Authorization: Bearer`, and **sandboxd** is
what verifies it, against the same claim the silkd relay checks.

The proxy holds no per-sandbox secret and mints nothing. Node inventory
deliberately carries none either, which is why a reconnecting client must keep
the token from its create response (see
[e2b-compat](e2b-compat.md#limits-worth-knowing)).

Host-side credentials are **stripped** before the request crosses into the
guest: `X-Access-Token` authorizes the proxy→node hop only, and a guest that
learned it could drive its own sandbox's control plane. `X-API-KEY` never
belongs to `envd` either.

`envd`'s own internal endpoints — the ones its spec marks `x-internal`:
`/init`, `/freeze`, `/unfreeze`, `/fsfreeze`, `/fsthaw`, `/collapse` — are refused
at the edge with `404`. They reconfigure or freeze the
guest and are not part of the SDK's data plane; pausing is the control plane's
job, through `POST /sandboxes/{id}/pause`.

## Protocols

The relay is a **raw byte** passthrough, so HTTP/1.1 and HTTP/2 both survive it
untouched. The two legs are chosen separately:

- **Client → proxy** offers HTTP/1.1 and HTTP/2, the latter both over TLS and in
  the clear, so ConnectRPC keeps working behind an edge that already terminated
  TLS. That is `Protocols()`, which the binary hands to its `http.Server`.
- **Proxy → guest** is HTTP/1.1. `envd` 0.8.0 installs no h2c handler — measured,
  not assumed: `envdsmoke`'s "envd serves http/1.1 only" step fails the day that
  changes. Forwarding HTTP/2 upstream would break every request, so `--guest-http2`
  is opt-in, for a guest daemon on another port that does serve h2c.

Connections are not pooled across requests: one would outlive the sandboxd relay
carrying it, and an open relay holds the sandbox's idle clock, which would keep
an unused sandbox from ever hibernating.

## Proving it

`pkg/envdproxy`'s tests cover the routing, stripping and error mapping against a
fake node. The hardware half is `test/envdproxysmoke` (build tag
`envdproxysmoke`), which serves the real proxy in-process and reaches an HTTP
listener inside a live microVM:

```bash
# on the node: claim a sandbox and start a listener in it
portsmoke -addr 127.0.0.1:7990 -token <node-token> -template <ref> \
  -listener ./guestserver -hold 300s          # prints: SANDBOX <id> <token> <owner> <port>

go run -tags envdproxysmoke ./test/envdproxysmoke \
  -node <owner> -sandbox <id> -token <token> -port 49983
```

`-guest envd` swaps the assertions for the real daemon (health, a ConnectRPC
unary) when the sandbox came from the `e2b-rt` flavor; `envdsmoke -hold` in the
sandbox repo prepares that one. Default `-guest echo` expects `guestserver`,
which reports back what the guest received and is what proves the credential
stripping. Both harnesses, and `scripts/port-e2e.sh` for the node half alone,
live in the sandbox repo under `e2e/cmd/`.

## Failures

| Status | Meaning |
|---|---|
| `400` | The host and headers name no sandbox, or the port is outside 1-65535. |
| `401` | No `X-Access-Token`, or the node rejected the one presented. |
| `404` | An `envd` internal path. |
| `502` | Sandbox unknown, paused past recovery, or its node unreachable. |

`502` is deliberately the answer for an unknown id as well as an unreachable
node: a caller must not be able to probe which sandbox ids exist, which node
holds one, or whether the fleet is healthy. No reply names a node.
