# e2b-compatible API

The aggregated apiserver can serve an [e2b](https://e2b.dev)-compatible REST
surface, so an **unmodified e2b SDK** (JS or Python) claims from the same warm
microVM pools this operator already manages. Point `E2B_API_URL` at it and
`Sandbox.create()` works.

It is a translation layer, not a second control plane. Every request lands on
the same `SandboxStore` the Kubernetes API uses, so an e2b create *is* the
node-local claim a `kubectl create sandbox` performs — and the sandbox it
returns shows up in `kubectl get sandboxes`. Nothing extra is stored: sandbox
identity is a DNS-safe rendering of the sandboxd claim id the owning node
already assigns; requests accept either spelling.

## Enable it

```bash
sandbox-apiserver \
  --enable-e2b-api \
  --e2b-bind-address=:8080 \
  --e2b-namespace=sandboxes \
  --e2b-api-key-file=/etc/e2b/keys \
  --e2b-domain=sandbox.example.com
```

| Flag | Default | Meaning |
|---|---|---|
| `--enable-e2b-api` | `false` | Serve the surface at all. |
| `--e2b-bind-address` | `:8080` | Its own listener; the aggregated API is untouched. |
| `--e2b-namespace` | `default` | Namespace claims land in — e2b has no namespace concept. |
| `--e2b-api-key-file` | — | File of accepted `X-API-KEY` values, one per line (`#` comments ignored). |
| `--e2b-domain` | — | Base domain reported to the SDK for reaching in-sandbox `envd`. |
| `--e2b-allow-anonymous` | `false` | Serve with **no** API key. Development only. |

Startup **fails** if neither `--e2b-api-key-file` nor `--e2b-allow-anonymous` is
set, so a misconfiguration cannot silently expose an open claim endpoint.

## Use it

```bash
export E2B_API_URL=http://your-apiserver:8080
export E2B_API_KEY=e2b_yourkey
```

```js
import { Sandbox } from '@e2b/code-interpreter'

// templateID is the pool template — the container image the warm pool is built
// from, the same axis SandboxWarmPool keys on.
const sandbox = await Sandbox.create('registry.example.com/rt:24.04')
```

## Endpoint mapping

| e2b endpoint | Maps to | Notes |
|---|---|---|
| `POST /sandboxes` | `store.Claim` | `templateID` → pool template; `timeout` → the claim's TTL (15s when omitted); `allow_internet_access` → `egress` lane, else the hardened `none` lane. `201` on success, `503` when the pool is drained (retryable). |
| `GET /sandboxes`, `GET /v2/sandboxes` | `store.List` | Live sandboxes in the compat namespace. |
| `GET /sandboxes/{id}` | `store.GetByClaimID` | Resolves the owning node and materializes only that entry; `404` when no live sandbox carries the id. |
| `DELETE /sandboxes/{id}` | `store.Release` | Releases the node-local claim id, never by Kubernetes name. `204`; `404` when no live sandbox carries the id or the owning node already reaped it. |
| `POST /sandboxes/{id}/timeout` | existence check | TTL is fixed by the node at claim time; the call is verified and acknowledged, not silently faked. |
| `POST /sandboxes/{id}/refreshes` | existence check | Verifies that the sandbox is still live; it does not extend or refresh the node-owned deadline. |
| `POST /sandboxes/{id}/pause` | `store.Pause` | Hibernates the owning node's claim. Omitted or `memory: true` snapshots memory; `memory: false` asks for an unsupported filesystem-only pause and returns `400`. Returns `409` when already paused. |
| `POST /sandboxes/{id}/connect` | `store.Resume` when paused | The SDK's resume operation. Returns `200` when already running or `201` after restoring a paused sandbox. Its `timeout` field does not change the node-owned lease. |
| `POST /sandboxes/{id}/fork` | `store.Fork` | Creates `count` children (`1` by default), each with its own id and requested claim-time TTL. A paused source returns `409`; resume it first. |
| `POST /sandboxes/{id}/snapshots` | `store.Snapshot` | Captures a checkpoint while the source keeps running; returns its `snapshotID`. |
| `GET /snapshots` | `store.Snapshots` across nodes | Lists fleet checkpoints. One unreachable node is skipped rather than blanking the whole result. |
| `DELETE /templates/{snapshotID}` | `store.DeleteSnapshot` across nodes | e2b addresses snapshot deletion through the templates path; deletion is idempotent and best-effort across nodes. |
| `GET /templates`, `GET /v2/templates` | advertised warm-pool keys | Lists the distinct templates the fleet can currently claim; these are pool-derived entries, not e2b-hosted template builds. |
| `GET /sandboxes/{id}/metrics` | `store.Stats` | Returns the complete e2b metric schema; see the zero-valued fields below. |
| `GET /health` | — | Unauthenticated, for probes. |

## Limits worth knowing

- **Reaching `envd` (the in-sandbox data plane).** The SDK derives the sandbox
  host as `{port}-{sandboxID}.{domain}`. The compatibility API renders the
  sandboxd claim id as a DNS-safe public id, but the deployment still needs
  wildcard DNS/TLS and a proxy that routes the derived host or the
  `E2b-Sandbox-Id` / `E2b-Sandbox-Port` headers the SDK sends. Otherwise set
  `E2B_SANDBOX_URL` explicitly.
- **`envdVersion`** is reported as `0.4.0` by default. The SDK version-compares
  it and *kills the sandbox* if it cannot parse it, so it is always sent.
- **Metrics are schema-complete, not measurement-complete.** `cpuCount`,
  `memUsed`, and `memTotal` come from the owning node when available;
  `cpuUsedPct`, `memCache`, `diskUsed`, and `diskTotal` are reported as zero.
- **List/detail schema fields are compatibility values.** A synthesized Sandbox
  carries no creation time, so `startedAt` is the time of the read; `endAt` is
  the node-granted deadline when the owning node published one, and
  `startedAt + 15s` otherwise. `cpuCount`, `memoryMB`, and `diskSizeMB` are
  reported as zero on these responses.
- **`envdAccessToken` is returned only at claim time.** `POST /sandboxes` and
  `POST /sandboxes/{id}/fork` carry the token the node just issued. The read
  paths (`GET /sandboxes`, `GET /sandboxes/{id}`, `POST /sandboxes/{id}/connect`)
  report it empty: node inventory deliberately carries no per-sandbox secret,
  so a reconnecting client must keep the token from its create response.
- **`templateID` on read paths comes from node inventory.** The owning node
  publishes the pool template with each entry; a node that does not yet publish
  it makes `GET /sandboxes` and `GET /sandboxes/{id}` report an empty
  `templateID`. `POST /sandboxes` always echoes the requested one.
- **Size class** is pinned (`small`) — e2b's `NewSandbox` carries no size
  selector.
- `metadata`, `envVars`, `autoPause`, and `secure` are accepted so SDK calls do
  not fail, but are discarded: the node-local claim path takes none of them and
  the compatibility layer stores no per-sandbox copy.
- **Not implemented:** team/node administration and e2b-hosted template
  build/management endpoints. Template listing is the pool-derived surface
  above; snapshots use the implemented checkpoint lifecycle.
