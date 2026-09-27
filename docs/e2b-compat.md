# e2b-compatible API

The aggregated apiserver can serve an [e2b](https://e2b.dev)-compatible REST
surface, so an **unmodified e2b SDK** (JS or Python) claims from the same warm
microVM pools the warm-pool driver already fills. Point `E2B_API_URL` at it and
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
| `--e2b-namespace` | `default` | Namespace a key that names none claims in, and where anonymous claims land — e2b has no namespace concept. |
| `--e2b-api-key-file` | — | File of accepted `X-API-KEY` values, one per line as `key` or `key namespace` (`#` comments ignored). A key's sandboxes and snapshots live in its namespace, `--e2b-namespace` when none is given, and a key sees nothing outside it. |
| `--e2b-domain` | — | **Required.** Base domain the SDK derives the in-sandbox `envd` host from. |
| `--e2b-envd-version` | `0.4.0` | `envd` version reported to the SDK. Set it to the one actually in the image. |
| `--e2b-default-timeout` | `300` | Lease in seconds for a create that names no timeout, and what a refresh renews for. |
| `--e2b-allow-anonymous` | `false` | Serve with **no** API key. Development only. |
| `--e2b-template-aliases` | — | File of template aliases, one per line as `alias pool-image` (`#` comments ignored). A create naming the alias claims from that image's pool. The SDKs create `base` when no template is named, so the file needs a `base` line for `Sandbox.create()` to work. The file is read at startup, so a change needs a restart. Startup fails on a malformed line or an alias named twice. |

Startup **fails** if neither `--e2b-api-key-file` nor `--e2b-allow-anonymous` is
set, so a misconfiguration cannot silently expose an open claim endpoint. It
also fails without `--e2b-domain`: the SDK derives the sandbox host from it, so
a deployment without one hands out sandboxes whose data plane no client can
address.

## Mesh mode (no Kubernetes)

`sandbox-e2b` serves the same surface on a sandboxd mesh (memberlist, see the
sandbox repo's `docs/cluster.md`) with no Kubernetes: no kubeconfig, no
`NodeInventory`, no APIService. It dials the seeds, learns the rest of the mesh
from each node's `GET /v1/info` peers, and reads every node's `GET
/v1/sandboxes` on a tick. Claims, reads and watches are served from that
in-memory snapshot, as the aggregated apiserver serves them from its informer
cache.

```bash
sandbox-e2b \
  --sandboxd-seeds=172.20.0.5:7777,172.20.0.6:7777 \
  --sandboxd-token-file=/etc/sandboxd/token \
  --e2b-bind-address=:8080 --e2b-domain=sandbox.example.com \
  --e2b-api-key-file=/etc/e2b/keys --e2b-template-aliases=/etc/e2b/aliases \
  --envd-proxy-bind-address=:8443
```

| Flag | Default | Meaning |
|---|---|---|
| `--sandboxd-seeds` | — | **Required.** Comma-separated sandboxd addresses dialed at start, each naming one node. A node is keyed by its own `advertise_addr` from `GET /v1/info`, so a seed written as a hostname and gossiped as an address is one node. |
| `--sandboxd-token`, `--sandboxd-token-file` | — | The fleet root `api_token`: `GET /v1/info` and the full `GET /v1/sandboxes` need root. The file wins when both are set. |
| `--inventory-poll` | `10s` | The tick between polls. A tick costs 2 requests per node, all nodes at once and bounded by the tick; List and Watch see a change within one tick. |
| `--e2b-*` | | The flags above. The surface is always on, so there is no `--enable-e2b-api`. |
| `--envd-proxy-bind-address` | — | When set, the [envd-proxy](envd-proxy.md) data plane listens here in the same process and shares the one poller, store and index. `--envd-proxy-tls-cert-file`, `--envd-proxy-tls-private-key-file` and `--envd-proxy-guest-http2` are its TLS and guest flags, and its domain is `--e2b-domain`. |

- **The node key is the node's `advertise_addr` as `GET /v1/info` reports
  it**, which is its `client_advertise` when that is set. The process must
  reach every node at that address with the root token.
- **sandboxd v0.1.15 or later** — the first release whose `GET /v1/info`
  carries `advertise_addr`. A seed without it fails startup (`advertises no
  host`); a discovered peer without it is skipped.
- **Startup fails loud** when no seed answers or a seed refuses the token
  (`GET /v1/info needs the fleet root api_token`).
- **A silent node** keeps its last snapshot for 3 ticks and then leaves the
  listing; its sandboxes die with it, as on the Kubernetes path.
- **Warm pools** are set through the sandbox SDK's `SetPoolsCluster` or
  sandboxd's own config; mesh mode has no warm-pool driver.
- **Claims follow sandboxd's redirects**: a warm miss at one node lands on the
  warm peer it names, one extra call.

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
| `POST /sandboxes`, `POST /v2/sandboxes` | `store.Claim` | `templateID` → pool template, through `--e2b-template-aliases` when it names an alias; `timeout` → the claim's TTL (`--e2b-default-timeout` when omitted); `allow_internet_access: true` → `egress` lane, anything else the hardened `none` lane. `201` on success, `400` for an option this backend cannot honor (see below), `503` when the pool is drained (retryable). SDK 2.51 creates through `/v2`. |
| `GET /sandboxes`, `GET /v2/sandboxes` | `store.List` | Live sandboxes in the key's namespace. The `state` (`running`, `paused`), `template` and `startedAfter` (at the second precision `startedAt` carries) filters are honored; `metadata` is refused with `400`, since metadata is not stored. `/v2` pages as the spec says: `limit` (1 to 100, default 100), `order` by start time (`desc`, newest first, by default, or `asc`) and `nextToken`, the opaque cursor the previous page returned in `X-Next-Token`. The cursor names the last sandbox served, so a sandbox created or released between pages neither repeats nor skips the rest. An out-of-range `limit`, an unknown `order` or a malformed `nextToken` is `400`. The legacy `GET /sandboxes` takes no page parameters and returns every match. |
| `GET /sandboxes/{id}` | `store.GetByClaimID` | Resolves the owning node and materializes only that entry; `404` when no live sandbox carries the id. |
| `DELETE /sandboxes/{id}` | `store.Release` | Releases the node-local claim id, never by Kubernetes name. `204`, also when the owning node already reaped it: release is idempotent. `404` when the read view no longer lists the id. |
| `POST /sandboxes/{id}/timeout` | `store.Renew` | Moves the owning node's lease to `timeout` seconds from now. `204` once the node has renewed; a node's refusal keeps its 4xx status (`409` for an archived sandbox), and any other failure is `500`. |
| `POST /sandboxes/{id}/refreshes` | `store.Renew` | The SDK keepalive. Renews for the body's `duration` when it carries one, otherwise `--e2b-default-timeout`. |
| `POST /sandboxes/{id}/pause` | `store.Pause` | Hibernates the owning node's claim. Omitted or `memory: true` snapshots memory; `memory: false` asks for an unsupported filesystem-only pause and returns `400`. Returns `409` when already paused. |
| `POST /sandboxes/{id}/connect`, `POST /v2/sandboxes/{id}/connect` | `store.Resume` when paused | The SDK's resume operation. Returns `200` when already running or `201` after restoring a paused or archived sandbox, carrying the sandbox's `envdAccessToken` read from the owning node (a sandboxd with sandbox#229, whose root by-id read carries the token). `timeout` (default `--e2b-default-timeout`) extends the lease to that many seconds from now when the node's deadline is nearer and never shortens it; `memory: false` (the SDK's `onResume: 'reboot'`) is refused with `400`, since a paused sandbox here resumes from its memory snapshot. |
| `POST /sandboxes/{id}/resume` | `store.Resume` | The legacy resume, deprecated in the e2b spec. It restores a paused or archived sandbox exactly as connect does and answers `201` with its `envdAccessToken`. `timeout` follows connect's rule: it only extends the lease, and an omitted value means `--e2b-default-timeout`, not the spec's 15. A running sandbox answers `409`, the spec's refusal. `autoPause: true` is `400` as on create, and `memory: false` is `400` as on connect. |
| `POST /sandboxes/{id}/fork` | `store.Fork` | Creates `count` children (`1` by default), each with its own id and requested claim-time TTL. A paused source returns `409`; resume it first. |
| `POST /sandboxes/{id}/snapshots` | `store.Snapshot` | Captures a checkpoint while the source keeps running; `201` with its `snapshotID`. The name plus its namespace stamp must fit the node's 63-character name budget; longer is `400`. |
| `GET /snapshots` | `store.Snapshots` across nodes | Lists the key's checkpoints: create stamps the namespace on the checkpoint name (`<namespace>/<name>`), listing keeps only that prefix and strips it; the `sandboxID` and `name` filters are honored, `limit` and `nextToken` are ignored. One unreachable node is skipped rather than blanking the whole result. |
| `DELETE /templates/{snapshotID}` | `store.DeleteSnapshot` on the holding node | e2b addresses snapshot deletion through the templates path. The id is looked up among the key's checkpoints first, so `404` for one in another namespace, then deleted on the node that holds it; `500` when the id is not listed and a node did not answer, so an outage never reads as already gone. |
| `GET /templates`, `GET /v2/templates` | advertised warm-pool keys | Lists the distinct templates the fleet can currently claim; these are pool-derived entries, not e2b-hosted template builds. Each entry's `aliases` lists the aliases that name its image. |
| `GET /templates/aliases/{alias}` | the alias table | `200 {"templateID": "<pool image>", "public": true}` for an alias the table names, else `404`. The SDKs call this to check whether a template alias exists. |
| `GET /sandboxes/{id}/metrics` | `store.Stats` | Returns the complete e2b metric schema; see the zero-valued fields below. |
| `GET /sandboxes/{id}/logs`, `GET /v2/sandboxes/{id}/logs` | `store.GetByClaimID` | This backend keeps no sandbox logs. A sandbox the key can see answers `200` with an empty page: `{"logs":[],"logEntries":[]}` on the legacy route, `{"logs":[]}` on `/v2`. `e2b sandbox logs` then prints no log lines and exits `0`. With `-f` it polls until the sandbox is released, then exits `1` on the `404`; a paused sandbox makes it report "not found". An unknown id is `404`. |
| `GET /health` | — | Unauthenticated, for probes. |

## Limits worth knowing

- **Reaching `envd` (the in-sandbox data plane).** The SDK derives the sandbox
  host as `{port}-{sandboxID}.{domain}`. The compatibility API renders the
  sandboxd claim id as a DNS-safe public id, but the deployment still needs
  wildcard DNS/TLS and a proxy that routes the derived host or the
  `E2b-Sandbox-Id` / `E2b-Sandbox-Port` headers the SDK sends.
  `sandbox-envd-proxy` is that proxy; see [envd-proxy](envd-proxy.md). Control
  plane without it means
  `Sandbox.create()` works and `files`/`commands`/`pty` do not. The pool must
  also run an image that carries `envd` — the sandbox repo's `e2b-rt` flavor —
  or there is nothing on the other end of the proxy.
- **`GET /sandboxes` lags a create by up to one inventory publish.** The list
  is assembled from `NodeInventory`, which each node republishes on a cadence
  (30 s by default). `GET /sandboxes/{id}`, the lifecycle verbs and
  `sandbox-envd-proxy` do not wait for it: a sandbox the inventory does not list
  yet is looked up on the nodes themselves, so `Sandbox.create()` followed at
  once by `files`/`commands` works. The proxy's node lookups share one budget
  per replica (200 new sandboxes/s, see [envd-proxy](envd-proxy.md)); past it a
  sandbox its node has not published answers `502` until the node publishes.
- **`envdVersion`** is reported as `0.4.0` unless `--e2b-envd-version` says
  otherwise. The SDK version-compares it and *kills the sandbox* if it cannot
  parse it, so it is always sent. Set it to the version actually installed in
  the pool's image (`e2b-rt` records its own in `/etc/envd-version`); the
  default is a floor, not a measurement.
- **Metrics are schema-complete, not measurement-complete.** `cpuCount`,
  `memUsed`, and `memTotal` come from the owning node when available;
  `cpuUsedPct`, `memCache`, `diskUsed`, and `diskTotal` are reported as zero.
- **List/detail schema fields are compatibility values.** `startedAt` is the
  claim time the owning node publishes; a node that does not publish it makes
  `startedAt` the time of the read. `endAt` is the node-granted deadline when
  the owning node published one, and `startedAt + --e2b-default-timeout`
  otherwise. `cpuCount`, `memoryMB`, and `diskSizeMB` are reported as zero on
  these responses.
- **`envdAccessToken` is minted once, at claim time, by the owning node.**
  `POST /sandboxes` and `POST /sandboxes/{id}/fork` carry the token the node
  just issued, and `POST /sandboxes/{id}/connect` reads it back from the owning
  node (one node round trip; sandbox#229 or later), so a process that never
  saw the sandbox before can connect and use the data plane. Node inventory
  deliberately carries no per-sandbox secret, so `GET /sandboxes` and
  `GET /sandboxes/{id}` report it empty.
- **`templateID` on read paths comes from node inventory.** The owning node
  publishes the pool template with each entry; a node that does not yet publish
  it makes `GET /sandboxes` and `GET /sandboxes/{id}` report an empty
  `templateID`. `templateID` is always the pool image, on every reply
  including create, so a stored value creates the same sandbox again. `alias`
  carries the image's first alias in sort order, and the list's `template`
  filter accepts either spelling.
- **Size class** is pinned (`small`) — e2b's `NewSandbox` carries no size
  selector.
- **Options this backend cannot honor are refused, not dropped.** `secure:
  false` (every sandbox here is reachable only with its own access token), a
  non-empty `envVars`, `autoPause: true`, and the `/v2` fields `network` (any
  rule), `volumeMounts`, `autoPauseMemory`, `autoResume: {enabled: true}`,
  `mcp` and `iam` each return `400`. Honoring them silently would hand back a
  different sandbox than the caller asked for. `metadata` is still accepted and
  discarded — it changes no behavior, and the node-local claim path takes no
  per-sandbox copy.
- **No internet unless asked.** A create without `allow_internet_access: true`
  lands on the `none` lane, whatever the SDK's own default: JS SDKs 2.3 to 2.50
  send `true` unless told otherwise, 2.51 sends nothing. A fleet that serves
  those clients needs an `egress` pool for the same template.
- **One key, one namespace.** A key sees and drives only the sandboxes and
  checkpoints of its namespace. The Kubernetes `snapshot` subresource stamps
  the sandbox's namespace on a checkpoint the same way, so one made through
  kubectl is listed and deleted by that namespace's key; checkpoints created
  before this scoping carry no namespace and are not listed.
  Fork children are recorded in the key's namespace under their own claim id
  (a sandboxd with cocoonstack/sandbox#230); an older node records them
  without a namespace, so they surface in the `default` namespace.
- **Checked against the real SDKs.** JS 2.50.0, JS 2.51.0 and Python 2.51.0
  ran create, exec, files, list, pause, connect from a fresh process, exec
  after the resume, the second key's refusals, the default lane and a refused
  network rule against a two-node fleet on 2026-09-23. JS 2.50's default
  create asks for internet and gets `503` on a fleet without an egress pool;
  2.51's lands on `none`.
- **Not implemented:** team/node administration and e2b-hosted template
  build/management endpoints. Template listing is the pool-derived surface
  above; snapshots use the implemented checkpoint lifecycle.
