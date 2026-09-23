---
title: Lifecycle verbs
---

# Lifecycle verbs

The L3 aggregated apiserver serves four action verbs beyond create/delete as
**subresources** of `sandboxes.agents.x-k8s.io`. They are installed alongside
the `sandboxes` storage, so the standard Sandbox schema is untouched and an
unmodified upstream client keeps working. Upstream's own controller has no
equivalent; on the Pod path, suspend and resume are `spec.operatingMode`.

L3 create, delete and these action subresources reject server-side dry-run
(`dryRun=All`) with `400 BadRequest` before accessing the store or issuing a
node-local RPC. The node APIs do not provide a dry-run transaction.

| subresource | body | effect |
|---|---|---|
| `sandboxes/pause` | `SandboxPauseOptions` | snapshots guest memory and stops the VM |
| `sandboxes/resume` | `SandboxResumeOptions` | restores it via cocoon's mmap fast path |
| `sandboxes/fork` | `SandboxForkOptions{count,ttlSeconds}` | branches N children; the source keeps running |
| `sandboxes/snapshot` | `SandboxSnapshotOptions{name}` | captures a checkpoint later sandboxes branch from |

The bodies are this repository's types, registered in upstream's
`agents.x-k8s.io/v1beta1` because that is the group-version the subresources
are served under:

```bash
kubectl create --raw \
  /apis/agents.x-k8s.io/v1beta1/namespaces/default/sandboxes/my-sandbox/snapshot \
  -f - <<<'{"apiVersion":"agents.x-k8s.io/v1beta1","kind":"SandboxSnapshotOptions","name":"before-migration"}'
```

`fork` replies with `SandboxForkResult`: one `{sandboxID, nodeName, address}`
per child, in request order. A fork is node-local, so every child lands on the
source's node. `count` defaults to 1 and is bounded by the node's configured
fork limit; `ttlSeconds` is each child's own lease — children never inherit the
parent's. `snapshot` replies with `SandboxSnapshotResult{snapshotID, name,
nodeName, creationTimestamp}`.

A checkpoint's name is stamped with the sandbox's namespace (`<namespace>/<name>`)
so the e2b surface can scope it to a key; the stamped name must fit sandboxd's
63-character checkpoint name, or the call fails with `400`.

A verb against a sandbox the read view cannot resolve is a `404`; a sandbox
whose inventory entry names no owning node or carries no claim id is a `500`,
because acting by Kubernetes name would target the wrong microVM.

**These verbs are not uniformly fast.** `resume` takes cocoon's mmap restore
path and a fork's children clone node-locally, but `pause` and `snapshot` write
the guest's memory out, so they cost time proportional to its size. Where a
checkpoint lives, and what happens when its node cannot serve a branch, is in
[snapshot-placement.md](snapshot-placement.md).

## Runnable walk-through

[`examples/lifecycle/example.go`](https://github.com/cocoonstack/sandbox-operator/blob/master/examples/lifecycle/example.go)
exercises **every** operation on **both** surfaces against a live cluster —
create, get, list, snapshot, fork, pause, resume, delete over the Kubernetes
API, then the same lifecycle plus templates, metrics, snapshot listing, timeout
and keepalive over the e2b REST API:

```bash
go run ./examples/lifecycle \
  -kubeconfig ~/.kube/config -namespace default \
  -e2b-url http://localhost:8080 -e2b-key "$E2B_API_KEY"
```

It discovers a template from the fleet's advertised warm pools, so no image
argument is needed. Omit `-e2b-url` to run only the Kubernetes half. Each step
prints what it did, so the output doubles as acceptance evidence:

```
=== Kubernetes API ===
  create     Sandbox default/example-219526000
  get        node=node-a-sandboxd claimID=sb_77ace349e7cc1db6
  snapshot   snapshotID=ck_92799687f14e8fea on node=node-a-sandboxd
  fork       child[0] sandboxID=sb_06d1b2438dcc1d1b node=node-a-sandboxd
  pause      took 315ms (proportional to guest memory)
  resume     took 109ms (mmap restore fast path)

=== e2b-compatible REST API ===
  create     sandboxID=sb-175124667cfa1281 envdVersion=0.4.0
  metrics    cpuCount=1 memTotal=5.36870912e+08
  pause      409 on repeat — the already-paused contract holds
  connect    201 — restored via the mmap fast path
```

## Lifetime

Nothing is stored for a claimed sandbox, so the lease is fixed by the node at
claim time and the submitted object is the only place `Create` can hear it:

- `spec.shutdownTime` wins, rounded up to whole seconds; the
  `sandbox.cocoonstack.io/ttl-seconds` annotation covers clients that cannot
  set the field; neither (or `0`) asks for the node's default lease.
- A `shutdownTime` already in the past or a malformed/negative annotation is
  a `400` before any warm microVM is spent.
- The node clamps the ask to its own default and maximum, so the response
  carries the **granted** expiry as the `sandbox.cocoonstack.io/deadline`
  annotation (RFC3339) — the submitted spec is echoed untouched. `Get`/`List`
  stamp the same annotation once the owning node publishes the deadline in
  its `NodeInventory`.

## Behaviors callers must handle

- **Lists are eventually consistent.** `Create` returns as soon as the
  node-local claim completes. `List` and `Watch` are served from
  `NodeInventory`, which nodes republish on a ~30s cadence, so a list right
  after a create may not show it yet. `Get` and the lifecycle verbs, by name
  or by claim id (the e2b surface), ask the nodes and answer at once on any
  apiserver replica. A deleted sandbox stays readable until its node
  publishes, and a fork child is readable by name only after that publish.
- **`Create` is a claim, not an upsert.** No Sandbox object is stored, so
  `metadata.name` is never checked against the fleet: a repeated `Create` under
  one name claims a second microVM, and the by-name verbs then reach whichever
  of the two a node answers for first. A caller that may repeat a `Create` uses
  `metadata.generateName`; the `claim-id` annotation on each response names the
  microVM that call delivered.
- **The published sandbox id is DNS-label safe.** The e2b SDK builds the
  in-sandbox host as `{port}-{sandboxID}.{domain}`, so the node's raw claim id
  (`sb_...`) is rendered as `sb-...` on the e2b surface.
