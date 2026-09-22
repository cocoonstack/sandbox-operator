# Snapshot placement: where a checkpoint lives, and what happens when its node cannot serve it

A snapshot in this system is a **checkpoint**: an immutable capture of a
sandbox's state that new sandboxes branch from. Taking one is
`POST sandboxes/{name}/snapshot` (or e2b's `POST /sandboxes/{id}/snapshots`);
branching one produces a fresh sandbox with its own id and lease.

This document is about the part that is not obvious: a checkpoint is **created
on one node's local disk**, and the node that eventually has to serve a branch
of it may not be that node.

## The constraint everything else follows from

Cloning a sandbox is fast — 28–75 ms — because nothing is copied:

| step | mechanism |
|---|---|
| guest memory | `os.Link` hardlink of the snapshot's `memory-range-*` files |
| writable disks | reflink (`FICLONE`), concurrent, `NoSync` |
| restore | mmap copy-on-write — the guest maps the file, pages privatize on write |

All three require the checkpoint and the new VM's run directory to be **on the
same local filesystem**, and that filesystem to support reflink (XFS/btrfs).
Hardlinks do not cross filesystems. `FICLONE` does not work on NFS, Lustre, or
any FUSE object-storage mount. mmap over a network filesystem turns a page
fault into a network round trip.

So the fast path is not an optimization layered on top of the storage choice —
**it is the storage choice**. Any design that puts checkpoints on shared network
storage has already given up the number that makes the system worth using.

That is the whole reason the design below prefers *moving the placement to the
data* over *moving the data to the placement*.

## Three tiers

### L1 — local hit (the normal case)

A checkpoint stays on the node that took it. A branch of it issued to that node
reads it from the local store and clones on the local reflink path.

Nothing in this tier is new; it is what a single-node deployment already does.
It is listed because it is the case that must stay fast, and the two tiers below
exist only to avoid degrading it.

### L2 — probe + redirect (the cross-node case)

A node asked to branch a checkpoint it does not hold `HEAD`-probes its mesh
peers in parallel (HMAC-signed when the mesh carries a `cluster_key`) and
answers with a **redirect** to the first owners that respond, capped at three
addresses — the same `200` + `redirect: [addrs]` contract a warm-miss claim
already uses, and a client that chases redirects retries there with
`no_redirect: true`. This repository's sandboxd client deliberately does not
chase them: a redirect-only answer is a capacity miss, which the L3 store turns
into a retryable `503` and the L2 gateway into its fallback signal.

The record does not move. The clone still happens on a node whose disk already
holds the data, on its local fast path. Cross-node correctness is bought with
one probe fan-out, one extra client round trip, and **zero** bytes transferred.

This is the tier that makes "snapshot on node A, branch from anywhere" work.

### L3 — peer heal (the last resort)

Redirect cannot answer when no owner can serve: the owning node is gone,
draining, or out of capacity. The choice then is between paying one transfer
and failing the request.

With `checkpoint_peer_heal` enabled, the node pulls the record from an owner
over sandboxd's own peer transport, publishes it into its local store through
the same atomic staging path a locally-created checkpoint takes, and then
serves the branch locally.

The cost is paid **once**: afterwards this node is itself an owner, answers probes for the
record, and serves later branches of it at L1 speed.

It is **off by default**. Trading a transfer for availability is an operator's
decision, not a default.

#### Why the transport is sandboxd's own

The peer transport is deliberately **not** cocoon's image/snapshot mover.
cocoon's transfer is bound to its VM store layout and its own addressing;
reusing it would tie a checkpoint's mobility to the engine's release cycle and
to a layout that has nothing to do with the record format being moved.

sandboxd's transport moves exactly one thing — a store record: an `export/`
directory plus its `meta.json` — between two sandboxd nodes that already
authenticate to each other with the fleet `api_token`, over the control-plane
port they already share. It is a tar stream (`GET /v1/checkpoints/{id}/blob`),
so a pull is one round trip and never materializes the record twice. Entry
names are validated against path traversal and the stream is size-capped: a
peer is authenticated, but an authenticated peer is still not allowed to write
outside the destination this node chose.

## Why not JuiceFS (or any shared filesystem)

One reason is decisive, and a second makes the alternative pointless:

1. **It destroys the fast path.** JuiceFS does not support reflink. Putting
   checkpoints on it turns every clone from "hardlink + reflink, 28 ms, zero
   bytes moved" into a full read over the network. The system's headline number
   is the thing being traded away.

2. **There is a simpler way to get the same reach.** JuiceFS is a metadata engine plus
   an object store. With no S3 available, running JuiceFS means first running
   MinIO/SeaweedFS — at which point object storage exists, and the
   already-implemented `s3` store backend is strictly simpler than adding a
   POSIX layer on top of it.

sandboxd does accept a shared mount for `checkpoint_dir` (JuiceFS, NFS; see the
sandbox deploy docs): every node sharing the mount resolves every checkpoint
directly and none of the tiers above applies. This design keeps checkpoints
node-local anyway, for reason 1. If a shared backend is ever required, prefer
the `s3` backend (which keeps a local cache generation and therefore preserves
the local-clone step) over a network POSIX mount.

## Durability: what this design does *not* give you

A checkpoint lives on one node. **If that node's disk is lost, the checkpoint is
lost.** L3 heals a node that cannot *reach* a record; it does not replicate one.

This is a deliberate, scoped decision, not an oversight — checkpoints here are
branch points for agent workloads, not backups. Making them survive node loss
needs asynchronous replication to N peers and a placement policy that tracks
replica sets; that work is recorded in [the roadmap](roadmap.md) and is
explicitly not built.

Operationally: **treat a checkpoint as durable only while its node is.** If a
checkpoint matters beyond that, promote it to a template
(`POST /v1/sandboxes/{id}/promote`) and distribute it as one, or export it out
of the fleet.

## Configuration

| setting | default | effect |
|---|---|---|
| `checkpoint_dir` | `<data_dir>/checkpoints` | where records live. Keep it on the same local reflink filesystem as the VM run directory. |
| `checkpoint_store.kind` | `dir` | `s3` switches to object storage with a local cache generation. |
| `checkpoint_ttl_hours` | `0` (keep forever) | ages out checkpoints older than this; the sweep runs hourly and at startup. Must be nonzero and fleet-wide consistent when `checkpoint_peer_heal` is on. |
| `checkpoint_peer_heal` | `false` | enables L3. Requires a nonempty `api_token`, `mesh.cluster_key` and a nonzero `checkpoint_ttl_hours`, all enforced at config load: a partial setup fails to start rather than silently disabling heal. A shared `checkpoint_store` (kind `s3`) ignores it: every node already resolves every checkpoint. |

L1 and L2 need no configuration: probe and redirect are active whenever a mesh
is configured.
