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
  --e2b-envd-secret-file=/etc/e2b/envd-secret \
  --e2b-domain=sandbox.example.com
```

| Flag | Default | Meaning |
|---|---|---|
| `--enable-e2b-api` | `false` | Serve the surface at all. |
| `--e2b-bind-address` | `:8080` | Its own listener; the aggregated API is untouched. |
| `--e2b-namespace` | `default` | Namespace a key that names none claims in, and where anonymous claims land — e2b has no namespace concept. |
| `--e2b-api-key-file` | — | File of accepted `X-API-KEY` values, one per line as `key` or `key namespace` (`#` comments ignored). A key's sandboxes and snapshots live in its namespace, `--e2b-namespace` when none is given, and a key sees nothing outside it. |
| `--e2b-domain` | — | **Required.** Base domain the SDK derives the in-sandbox `envd` host from. |
| `--e2b-envd-version` | `0.8.0` | `envd` version reported to the SDK. Set it to the one actually in the image. |
| `--e2b-envd-secret-file` | — | **Required.** File holding the key every sandbox's `envd` access token derives from; the envd-proxy reads the same file. |
| `--e2b-default-timeout` | `300` | Lease in seconds for a create that names no timeout, and what a refresh renews for. |
| `--e2b-allow-anonymous` | `false` | Serve with **no** API key. Development only. |
| `--e2b-template-alias-file` | — | File of template aliases, one per line as `alias pool-image [size]` (`#` comments ignored). A create naming the alias claims from that image's pool at `size`: `small` (the default), `medium` or `large`. The SDKs create `base` when no template is named, and the code-interpreter SDKs create `code-interpreter-v1`, so the file needs those lines for `Sandbox.create()` to work (see [Pools for the aliases](#pools-for-the-aliases)). The file is read at startup, so a change needs a restart. Startup fails on a malformed line, an unknown size or an alias named twice. |
| `--e2b-builds` | `false` | Serve the [template build API](#template-builds). |
| `--e2b-build-parallel` | `2` | Builds that run at once; a start beyond them answers `429` and the build stays waiting. |
| `--e2b-build-timeout` | `30m` | Bound on one build, claim through publish, and the lease of its sandbox. |
| `--e2b-build-log-lines` | `10000` | Log lines kept per build; later lines are dropped. |
| `--e2b-build-dir` | — | Directory that keeps the archives `COPY` steps upload, through this surface's own signed `PUT` (see [Uploads](#uploads)). Exclusive with the S3 bucket. |
| `--e2b-build-upload-max` | `1073741824` | Largest archive in bytes a signed `PUT` to `--e2b-build-dir` takes; a larger one answers `413`. |
| `--e2b-build-store-s3-bucket` | — | S3 bucket that keeps the archives instead, through presigned `PUT`s; credentials come from the default AWS chain. |
| `--e2b-build-store-s3-prefix` | — | Key prefix in that bucket; archives live under `<prefix>/e2b-files/<namespace>/<template>/<hash>.tar`. |
| `--e2b-build-store-s3-endpoint` | — | Endpoint of an S3-compatible store instead of AWS. |
| `--e2b-build-store-s3-region` | — | Region of the bucket. |
| `--e2b-build-store-s3-force-path-style` | `false` | Address the bucket in the path, as most S3-compatible stores need. |

Startup **fails** if neither `--e2b-api-key-file` nor `--e2b-allow-anonymous` is
set, so a misconfiguration cannot silently expose an open claim endpoint. It
also fails without `--e2b-domain`: the SDK derives the sandbox host from it, so
a deployment without one hands out sandboxes whose data plane no client can
address. It fails without `--e2b-envd-secret-file` too: a sandbox's access
token derives from it.

## Mesh mode (no Kubernetes)

`sandbox-e2b` serves the same surface on a sandboxd mesh (memberlist, see the
sandbox repo's `docs/cluster.md`) with no Kubernetes: no kubeconfig, no
`NodeInventory`, no APIService. It dials the seeds, learns the rest of the mesh
from each node's `GET /v1/info` peers, and reads every node's `GET
/v1/sandboxes` on a tick. Reads and watches are served from that in-memory
snapshot and claims are routed from it, as the aggregated apiserver does from
its informer cache.

```bash
sandbox-e2b \
  --sandboxd-seeds=172.20.0.5:7777,172.20.0.6:7777 \
  --sandboxd-token-file=/etc/sandboxd/token \
  --e2b-bind-address=:8080 --e2b-domain=sandbox.example.com \
  --e2b-api-key-file=/etc/e2b/keys --e2b-template-alias-file=/etc/e2b/aliases \
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
- **The first sandboxd release carrying `advertise_addr` in `GET /v1/info`**
  (sandbox main `acfca8b` today). A seed without it fails startup (`reports no
  advertise_addr`); a discovered peer without it is skipped.
- **Startup fails loud** when no seed answers or a seed refuses the token
  (`GET /v1/info needs the fleet root api_token`).
- **A silent node** keeps its last snapshot for 3 ticks and then leaves the
  listing; a peer the mesh no longer names is dropped after those same ticks,
  so a graceful restart inside that window is invisible. A dead node's
  sandboxes die with it, as on the Kubernetes path.
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

## Pools for the aliases

Each key an alias names needs warm capacity: a `SandboxWarmPool` whose
template maps to that image and size on Kubernetes, a sandboxd pool on a
mesh. The e2b flavors' own defaults are:

```text
base                 ghcr.io/cocoonstack/sandbox/e2b-rt:24.04
code-interpreter-v1  ghcr.io/cocoonstack/sandbox/e2b-ci:24.04  medium
```

A clone must not be handed out before `envd` (and on `e2b-ci` the interpreter)
answers, so every node's config carries the pool's `warmup` for the key, as the
sandbox repo's `docs/e2b.md` gives it. `warmup` is config-owned: `PUT
/v1/pools` cannot set it, and the node keeps it for the key under whatever warm
target the warm-pool driver applies:

```json
{"template": "ghcr.io/cocoonstack/sandbox/e2b-ci:24.04",
 "net": "none", "size": "medium", "warm": 2,
 "warmup": ["sh", "-c",
   "for i in $(seq 1 1200); do curl -sf -m 1 -o /dev/null http://127.0.0.1:49983/health && curl -sf -m 1 -o /dev/null http://127.0.0.1:49999/health && exit 0; sleep 0.05; done; exit 1"]}
```

Without it a create can land on a clone whose interpreter is still starting,
and its first `runCode` fails with `502`.

## Endpoint mapping

| e2b endpoint | Maps to | Notes |
|---|---|---|
| `POST /sandboxes`, `POST /v2/sandboxes` | `store.Claim` | `templateID` → pool template, through `--e2b-template-alias-file` when it names an alias; `timeout` → the claim's TTL (`--e2b-default-timeout` when omitted); `allow_internet_access: true` → `egress` lane, anything else the hardened `none` lane; `metadata` → the claim's metadata; `autoPause: true` → the claim is paused (hibernated and archived) at lease end instead of destroyed, and connect resumes it. Both ride on the same claim call and need sandboxd built from 3fd7af2 or later. `envVars` becomes `envd`'s default environment through the `/init` that follows the claim (see the access token below); a failed `/init` releases the claim and answers `500`. `201` on success, `400` for an option this backend cannot honor (see below), `503` when the pool is drained (retryable), `404` when no warm capacity answers and the `templateID` names nothing known — no alias, no [built template](#built-templates) of the namespace, no advertised pool image. The check runs only on that failure branch. SDK 2.51 creates through `/v2`. |
| `GET /sandboxes`, `GET /v2/sandboxes` | `store.List` | Live sandboxes in the key's namespace. The `state` (`running`, `paused`), `template` and `startedAfter` (at the second precision `startedAt` carries) filters are honored, and so is `metadata`: `key=value` pairs joined by `&`, each key and value URL-encoded as the JS and Python SDKs send `query.metadata`; every pair must match; a pair without `=`, an empty key or a repeated key is `400`. Each item carries its `metadata`, `cpuCount` and `memoryMB`. `/v2` pages as the spec says: `limit` (1 to 100, default 100), `order` by start time (`desc`, newest first, by default, or `asc`) and `nextToken`, the opaque cursor the previous page returned in `X-Next-Token`. The cursor names the last sandbox served, so a sandbox created or released between pages neither repeats nor skips the rest. An out-of-range `limit`, an unknown `order` or a malformed `nextToken` is `400`. The legacy `GET /sandboxes` takes no page parameters and returns every match. |
| `GET /sandboxes/{id}` | `store.GetByClaimID`, then `store.Read` | Resolves the owning node and materializes only that entry; `state` and `endAt` come from the node's own record, so they reflect a pause, resume or renew at once; `404` when no live sandbox carries the id. |
| `DELETE /sandboxes/{id}` | `store.Release` | Releases the node-local claim id, never by Kubernetes name. `204`, also when the owning node already reaped it: release is idempotent. `404` when the read view no longer lists the id. |
| `POST /sandboxes/{id}/timeout` | `store.Renew` | Moves the owning node's lease to `timeout` seconds from now. `204` once the node has renewed; a node's refusal keeps its 4xx status (`409` for an archived sandbox), and any other failure is `500`. |
| `POST /sandboxes/{id}/refreshes` | `store.Renew` | The SDK keepalive. Renews for the body's `duration` when it carries one, otherwise `--e2b-default-timeout`. |
| `POST /sandboxes/{id}/pause` | `store.Pause` | Hibernates the owning node's claim. Omitted or `memory: true` snapshots memory; `memory: false` asks for an unsupported filesystem-only pause and returns `400`. Returns `409` when already paused. |
| `POST /sandboxes/{id}/connect`, `POST /v2/sandboxes/{id}/connect` | `store.Resume` when paused | The SDK's resume operation. Returns `200` when already running or `201` after restoring a paused or archived sandbox, carrying the sandbox's `envdAccessToken` read from the owning node (a sandboxd with sandbox#229, whose root by-id read carries the token). `timeout` (default `--e2b-default-timeout`) extends the lease to that many seconds from now when the node's deadline is nearer and never shortens it; `memory: false` (the SDK's `onResume: 'reboot'`) is refused with `400`, since a paused sandbox here resumes from its memory snapshot. |
| `POST /sandboxes/{id}/resume` | `store.Resume` | The legacy resume, deprecated in the e2b spec. It restores a paused or archived sandbox exactly as connect does and answers `201` with its `envdAccessToken`. `timeout` follows connect's rule: it only extends the lease, and an omitted value means `--e2b-default-timeout`, not the spec's 15. A running sandbox answers `409`, the spec's refusal. `autoPause` sets the claim's lease-end action (`true` pauses, `false` destroys) and then renews the lease to the larger of `timeout` and what is left, so it never shortens. `memory: false` is `400` as on connect. |
| `POST /sandboxes/{id}/fork` | `store.Fork` | Creates `count` children (`1` by default), each with its own id, requested claim-time TTL and access token. A paused source returns `409`; resume it first. |
| `POST /sandboxes/{id}/snapshots` | `store.Snapshot` | Captures a checkpoint while the source keeps running; `201` with its `snapshotID`. The name plus its namespace stamp must fit the node's 63-character name budget; longer is `400`. |
| `GET /snapshots` | `store.Snapshots` across nodes | Lists the key's checkpoints: create stamps the namespace on the checkpoint name (`<namespace>/<name>`), listing keeps only that prefix and strips it; the `sandboxID` and `name` filters are honored, `limit` and `nextToken` are ignored. One unreachable node is skipped rather than blanking the whole result. |
| `DELETE /templates/{templateID}` | `store.DeleteTemplate` on every holder, else `store.DeleteSnapshot` on the holding node | A [built template](#built-templates) of the namespace is deleted on every node whose inventory lists it, alone (`no_redirect`), and its tags, being its labels, go with it; a node that no longer holds it counts as done, and one that fails answers `500`, which a retry converges. Otherwise e2b addresses snapshot deletion through the templates path: the id is looked up among the key's checkpoints first, so `404` for one in another namespace, then deleted on the node that holds it; `500` when the id is not listed and a node did not answer, so an outage never reads as already gone. |
| `GET /templates`, `GET /v2/templates` | advertised warm-pool keys and the namespace's built templates | Lists the distinct pool images the fleet can currently claim, each entry's `aliases` the aliases that name its image, then one entry per built template: `templateID` its name, `buildID` a UUIDv5 of its content digest (the same on every replica and across restarts), `names` the name plus `name:tag` per tag, `cpuCount`/`memoryMB` its size tier. `buildStatus` is `ready` on every entry. Entries are read from node inventory, so a promote, delete or tag shows up within one publish tick. |
| `GET /templates/{templateID}` | the namespace's built templates | One `TemplateBuild` (`ready`) per content digest the holders report, its `buildID` that digest's UUIDv5. `404` for anything else, a pool image included (its message says so), `400` for a name over the budget. |
| `PATCH /templates/{templateID}` | nothing | `200` for a built template, `public` accepted and ignored: every template is visible to every key of the namespace. `404` otherwise. |
| `POST /templates/tags`, `DELETE /templates/tags`, `GET /templates/{templateID}/tags` | the template's sandboxd labels (`PUT /v1/templates/labels`) on every holder | A tag is a label on the template's record, its name the key and the content digest it points at the value, so every replica and the mesh front agree and a restart keeps it. `target` is `name` or `name:tag`; a tag points at the target's build (`default` or none means the current one). A write reads the current labels from a holder itself, merges, and writes the whole map to every holder; two writes racing from two replicas keep the later one. A tag whose digest no holder reports any more is dropped, and a rebuild (re-promote) clears the template's tags. |
| `POST /v3/templates`, `POST /v2/templates/{templateID}/builds/{buildID}`, `GET /templates/{templateID}/builds/{buildID}/status` | the [build executor](#template-builds) | Served with `--e2b-builds`. |
| `GET /templates/aliases/{alias}` | the alias table, then the namespace's built templates, then the fleet's pools | `200 {"templateID": "…", "public": true}` for an alias the table names (its pool image), a built template (its name) or an image a pool advertises, else `404`. The alias table wins a name clash. The SDKs call this to check whether a template exists. |
| `GET /sandboxes/{id}/metrics` | `store.Read`, then envd `GET /metrics` | One live sample read from envd's `/metrics` inside the guest, with the sandbox's access token, through the owning node's passive guest-port relay; the node's record comes first, so a paused sandbox is answered without a dial (sandboxd built from 288103d or later): `cpuCount`, `cpuUsedPct`, `memUsed`, `memTotal`, `memCache`, `diskUsed` and `diskTotal` as the guest reports them. A paused or archived sandbox, or one mid pause, answers `[]` and is not woken. `start` and `end` are ignored: there is no history. |
| `GET /sandboxes/metrics` | the same, per id | `sandbox_ids` is 1 to 100 distinct comma-separated ids (else `400`). Answers `{"sandboxes": {id: sample}}` for the running ones among them; a paused sandbox, an id the key cannot see, or one whose read fails is left out. |
| `GET /sandboxes/{id}/logs`, `GET /v2/sandboxes/{id}/logs` | `store.GetByClaimID` | This backend keeps no sandbox logs. A sandbox the key can see answers `200` with an empty page: `{"logs":[],"logEntries":[]}` on the legacy route, `{"logs":[]}` on `/v2`. `e2b sandbox logs` then prints no log lines and exits `0`. With `-f` it polls until the sandbox is released, then exits `1` on the `404`; a paused sandbox makes it report "not found". An unknown id is `404`. |
| `GET /health` | — | Unauthenticated, for probes. |

## Built templates

A built template is a sandboxd promoted template named
`e2b/<namespace>/<name>` and promoted with the fleet token (no tenant): the SDK
sees the bare `<name>`, and a key sees only its namespace's. The fleet's
promoted templates are the registry: each node's `GET /v1/info` lists what it
holds, and vk-sandbox or the mesh source copies it into the node's inventory
next to its pools, so there is no separate record to keep in sync; it needs
sandboxd built from 7e9f9a8 or later, which lists templates with their labels and
takes `PUT /v1/templates/labels`. `<namespace>`,
the slash and `<name>` together fit sandboxd's 63-character name budget after the
`e2b/` prefix; a longer name is refused with `400` naming the room left.

A create naming a built template claims it: when no warm pool serves the name,
the create looks the name up among the namespace's built templates and claims
that template's key. No pool serves a promoted key, so the store sends the claim
to a node whose inventory advertises the key, with `require_promoted`: the node
clones from the template, a node that no longer holds it answers 404 and the
store tries the next advertiser, and a node that lost it to a peer redirects
there. Every advertiser exhausted answers `503` as a drained pool does. A clone
from a template is a cold clone — the export is fetched and the VM restored —
where a pool claim takes a warm VM. The lookup runs only after the pool claim
finds nothing, so a create naming a pool image pays nothing for it. A built
template runs on the `none` lane and reaches the network as its source pool
does (see [Network for builds](#network-for-builds)). `allow_internet_access:
false` claims any create, a built template's or a pool image's, with no egress
at all. The
inventory lags a template by one publish tick (vk-sandbox's
`--publish-interval`, the mesh source's `--inventory-poll`), so a create right
after a build can answer `404` until the next publish. Needs sandboxd built
from 559f821 or later, which honors `require_promoted` on a plain claim.

## Template builds

With `--e2b-builds`, `Template.build()` on the unmodified SDKs builds a template
from an image. `POST /v3/templates` opens a waiting build of the name (a `:tag`
on the name joins `tags`); `cpuCount` and `memoryMB` pick the size class by the
same thresholds a `SandboxTemplate`'s resources follow, and without them the
image's alias size (else `small`) holds. `POST
/v2/templates/{templateID}/builds/{buildID}` resolves `fromImage` like a create's
`templateID` — the alias table, else the image itself — so the SDK's default base
needs an alias line such as `e2bdev/base ghcr.io/cocoonstack/sandbox/e2b-rt:24.04`.
The build then claims a sandbox of that pool on the `none` lane, runs its steps
and start command in it, promotes it as `e2b/<namespace>/<name>`, deletes the
name on every other node that held an earlier build, sets the requested tags,
and releases the claim; a rebuild replaces the previous build. `fromTemplate`
and `fromImageRegistry` answer `400`, as does a malformed step;
`force` is accepted and ignored (there is no layer cache); `waiting` stays
reported while every build slot runs, and the start answers `429`.

Steps run through envd's process API, the one `commands.run` uses, as upstream
e2b runs them. A `RUN` runs `bash -l -c` as the current user (or the step's own)
in the current workdir with the environment so far, its output in the build log
and a non-zero exit failing the build. `ENV` evaluates each value in the guest's
shell, so `$PATH` resolves, and adds it for later steps; `WORKDIR` creates the
directory as root, owned by the current user, and `USER` creates a missing user;
the user starts as `user` and the workdir as that user's home. After the last
step the build hands envd the final user, workdir and environment as its
defaults, starts `startCmd` in the background, and runs `readyCmd` every second
until it exits 0 (the build timeout bounds it). The promote captures the running
sandbox, so every clone resumes with the start command already running and
envd holding those defaults: a create from a built template sends envd only its
access token, and the request's `envVars` on top of the template's environment,
which it reads from envd first because envd replaces the map it is given. A
create without `envVars` makes no extra call, and a create from a pool image is
unchanged.

### Network for builds

A build sandbox and every sandbox made from its template are on the `none`
lane, so they reach the network only through sandboxd's guarded egress proxy,
under the egress policy of the pool the build claimed from (sandboxd
cocoonstack/sandbox@dcffdec or later: a promoted template's clone takes its
source pool's policy, and the claim reports `net_route`). When a claim reports
`relay`, this surface points envd's processes at the proxy — envd builds a
child's environment only from its own defaults and the request, never from its
unit — by setting `http_proxy`, `https_proxy` (`http://127.0.0.1:3128`) and
`no_proxy` (`localhost,127.0.0.1,::1,169.254.169.254`), as silkd's unit does:
a build starts its steps with them, so `RUN pip install` goes through the proxy
and the template keeps them as defaults (an `ENV` step can override them), and a
create from a pool image hands them to envd under the request's own
`envVars`. A `direct` or `none` route sets nothing. So the pools of the build
images need an egress policy that allow-lists what builds download, for
example the package indexes:

```jsonc
{
  "pools": [
    { "template": "ghcr.io/cocoonstack/sandbox/e2b-rt:24.04", "net": "none", "size": "small", "warm": 2,
      "egress": { "allow": [
        { "host": "pypi.org" }, { "host": "files.pythonhosted.org" },
        { "host": "registry.npmjs.org" },
        { "host": "archive.ubuntu.com" }, { "host": "security.ubuntu.com" },
        { "host": "deb.nodesource.com" },
        { "host": "github.com" }, { "host": "objects.githubusercontent.com" }
      ] } },
    { "template": "ghcr.io/cocoonstack/sandbox/e2b-ci:24.04", "net": "none", "size": "medium", "warm": 1,
      "egress": { "allow": [ { "host": "pypi.org" }, { "host": "files.pythonhosted.org" } ] } }
  ]
}
```

A template keeps the variables its build had, so one built before its pool
gained a policy is rebuilt to reach the network. A step that uses `sudo` drops
them (sudo resets the environment), so package installs run as `root`, as the
SDK's `aptInstall` does. The policy is the whole grant: a template built from a
pool without one has no egress, and neither have its clones, even a tenant's
with its own policy
(sandboxd's [egress rules](https://github.com/cocoonstack/sandbox/blob/main/docs/egress.md)).

### Uploads

A `COPY` step reads an archive the SDK uploads before the build starts. The SDK
asks `GET /templates/{templateID}/files/{hash}` whether the archive of that
step's sources is stored; `hash` is the SDK's own digest of the step and its
files, so a rebuild with the same sources uploads nothing. When the archive is
missing the answer carries a URL the SDK `PUT`s the archive to. With
`--e2b-build-dir` that URL is this surface's own `PUT` on the same path, signed
with an HMAC of the namespace, template, hash and an expiry one hour out, keyed
by the envd secret: the `PUT` carries no API key, so the signature is its only
credential, and a wrong or expired one answers `401`. The body is bounded by
`--e2b-build-upload-max` and lands under a temporary name that is renamed into
place once complete. With `--e2b-build-store-s3-bucket` the URL is an S3
presigned `PUT`, which `--e2b-build-upload-max` does not bound, and presence is a
`HEAD` of the object. The build writes the
archive into the sandbox through envd as `/tmp/<hash>.tar`, unpacks it as root
and moves the step's source to its destination with Docker `COPY` semantics, as
upstream e2b's copy script does: a destination ending in `/` is a directory, a
relative one is under the current workdir, and the entries are owned by the
step's user (else the current one) with the step's mode when it names one.
Neither backend can check the archive against `hash`, which covers the files'
modes and contents rather than the archive's bytes; an archive that does not
unpack or lacks the step's source fails the step. An archive stays after its
template is deleted: the operator removes no upload. Without either flag the files
endpoint is not served and a build with a `COPY` step answers `400`. A deployment with more than one
replica needs S3 or one volume every replica mounts: the chart mounts
`apiserver.e2b.builds.uploads.persistentVolumeClaim` there when it is set.

`GET /templates/{templateID}/builds/{buildID}/status` pages `logEntries` by
`logsOffset` and `limit` and filters them by `level`; a failed build reports
`reason.step` `base` for the claim, the 1-based step number for a step (the
index the SDK maps onto its stack traces), or `finalize` for the start and
ready commands, the promote and the publish. The `buildID` a build answers with
is a random UUID that names this build attempt; once it is ready, the template
list reports the UUIDv5 of the published content digest, so the two differ.

Builds live in the process that took `POST /v3/templates`, one goroutine each: a
status poll that reaches another replica answers `404 build not found on this
replica`. While builds are on, the chart sets `sessionAffinity: ClientIP` on the
e2b Service, `sandbox-apiserver-e2b`, so a client reaching it directly stays on
one replica; that keys on the source address the Service sees, so an ingress or
load balancer in front that routes to endpoints itself, or rewrites the source,
needs its own stickiness (or run one replica). A finished build is kept for an hour. A restart
loses a build in flight; the SDK's next poll throws, and a rebuild converges
because a promote replaces.

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
- **`GET /sandboxes` lags a create, a pause and a resume by up to one inventory
  publish.** The list is assembled from `NodeInventory`, which each node
  republishes on a cadence (30 s by default), so a listed `state` can trail a
  pause; `GET /sandboxes/{id}` reads the state live. `GET /sandboxes/{id}`, the lifecycle verbs and
  `sandbox-envd-proxy` do not wait for it: a sandbox the inventory does not list
  yet is looked up on the nodes themselves, so `Sandbox.create()` followed at
  once by `files`/`commands` works. The proxy's node lookups share one budget
  per replica (200 new sandboxes/s, see [envd-proxy](envd-proxy.md)); past it a
  sandbox its node has not published answers `502` until the node publishes.
- **A node that stops publishing leaves after `--inventory-stale-after`**
  (90 s by default). Until then a dead node's sandboxes stay listed and a
  claim can still sample it; past it they leave `GET /sandboxes`, and
  `GET`, `DELETE` and the verbs on them answer `404`. The window is measured
  against the fleet's newest publish, so a control-plane outage never drops
  a node, and a vk-sandbox restart inside the window is invisible. A
  vk-sandbox that predates `publishedAt` never goes stale.
- **`envdVersion`** is reported as `0.8.0`, the version the e2b flavors ship,
  unless `--e2b-envd-version` says otherwise. The SDK version-compares it and
  *kills the sandbox* if it cannot parse it, so it is always sent. Set it to
  the version actually installed in the pool's image (`e2b-rt` records its own
  in `/etc/envd-version`).
- **Metrics are the guest's own view.** envd measures inside the VM, so
  `memTotal` is what the guest kernel sees, a little under the size tier's
  memory. Each read opens one relay through the owning node and closes it,
  and the node counts it as no activity, so polling neither holds a
  connection nor keeps a sandbox from idling.
- **List/detail schema fields are compatibility values.** `startedAt` is the
  claim time the owning node publishes; a node that does not publish it makes
  `startedAt` the time of the read. `endAt` is the node-granted deadline when
  the owning node published one, and `startedAt + --e2b-default-timeout`
  otherwise. `cpuCount` and `memoryMB` are the size tier the owning node
  reports with each entry; `diskSizeMB` is reported as zero.
- **`envdAccessToken` is `envd`'s own credential, derived from the claim.** It
  is `hex(HMAC-SHA256(secret, claim token))`, with the secret from
  `--e2b-envd-secret-file`, so the node's claim token never reaches a client.
  Create sends `envd` a first-time `POST /init` with it (plus `envVars`,
  default user `user`, workdir `/home/user` and the host clock) through the
  owning node's passive relay before answering `201`: one guest round trip.
  A forked child starts with its parent's `envd`, so fork first sets each
  child's instance-metadata document to `{"accessTokenHash": sha512(child
  token)}` and then sends its `/init`, which `envd` accepts only against that
  hash. `POST /sandboxes/{id}/connect` reads the claim token from the owning
  node and derives the same value, so a process that never saw the sandbox
  before can connect. Node inventory carries no per-sandbox secret, so
  `GET /sandboxes` and `GET /sandboxes/{id}` report it empty; reconnect by id
  therefore needs JS SDK 2.6 or later.
- **The guest's `E2B_SANDBOX_ID` and `E2B_TEMPLATE_ID` are empty.** `envd`
  reads them from the instance-metadata document, which create never writes
  and fork writes with the token hash only.
- **`templateID` on read paths comes from node inventory.** The owning node
  publishes the pool template with each entry; a node that does not yet publish
  it makes `GET /sandboxes` and `GET /sandboxes/{id}` report an empty
  `templateID`. `templateID` is always the pool image, on every reply
  including create, so a stored value creates the same sandbox again. `alias`
  carries the image's first alias in sort order, and the list's `template`
  filter accepts either spelling.
- **Size class** comes from the alias. e2b's `NewSandbox` carries no size
  selector, so a create naming an image directly claims `small`.
- **Options this backend cannot honor are refused, not dropped.** `secure:
  false` (every sandbox here is reachable only with its own access token),
  `autoPause: true` with `allow_internet_access: true`
  (the internet lane cannot pause, so a timeout would destroy what autoPause
  asks to keep), and the `/v2` fields `network` (any rule), `volumeMounts`,
  `autoPauseMemory`, `autoResume: {enabled: true}`, `mcp` and `iam` each return
  `400`. Honoring them silently would hand back a different sandbox than the
  caller asked for. `metadata` rides on the claim; the owning node caps it at
  16 pairs and 4 KiB and answers more with `400`, which create passes through.
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
