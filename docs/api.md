# API Reference

## Packages
- [sandbox.cocoonstack.io/v1beta1](#sandboxcocoonstackiov1beta1)


## sandbox.cocoonstack.io/v1beta1

Package v1beta1 contains the API Schema definitions this operator owns on top
of upstream agent-sandbox: the NodeInventory CRD and the sandboxes action
subresource payloads.

### Resource Types
- [NodeInventory](#nodeinventory)
- [NodeInventoryList](#nodeinventorylist)
- [SandboxForkOptions](#sandboxforkoptions)
- [SandboxForkResult](#sandboxforkresult)
- [SandboxPauseOptions](#sandboxpauseoptions)
- [SandboxResumeOptions](#sandboxresumeoptions)
- [SandboxSnapshotOptions](#sandboxsnapshotoptions)
- [SandboxSnapshotResult](#sandboxsnapshotresult)



#### ForkedSandbox



ForkedSandbox identifies one child of a fork.



_Appears in:_
- [SandboxForkResult](#sandboxforkresult)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `sandboxID` _string_ | sandboxID is the child's node-local claim id. |  |  |
| `nodeName` _string_ | nodeName is the node that owns the child. A fork is node-local, so every<br />child lands on the source's node. |  |  |
| `address` _string_ | address is the child's connection address, when the node published one. |  |  |


#### InventoryEntry



InventoryEntry is one live sandbox as summarized by its owning node.



_Appears in:_
- [NodeInventory](#nodeinventory)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `name` _string_ | name is the sandbox "<namespace>/<name>"; an unqualified name means the<br />default namespace. |  |  |
| `id` _string_ | id is the owning node's sandboxd claim id ("sb_..."), the handle its<br />sandbox-release verb needs. The aggregated apiserver surfaces it on the<br />synthesized Sandbox so Delete can release exactly this node-local microVM<br />(releasing by k8s name would target the wrong claim). Empty until the<br />node publishes it. |  |  |
| `phase` _string_ | phase is the node-reported sandbox phase (e.g. Running). |  |  |
| `template` _string_ | template is the pool template (base image) the sandbox was claimed from.<br />It is the only place the aggregated read path can recover it: no<br />per-sandbox object holds the pod spec. |  |  |
| `claimRef` _string_ | claimRef is the "<namespace>/<name>" of the SandboxClaim the sandbox is<br />bound to, if any. |  |  |
| `addr` _string_ | addr is the sandbox "host:port" address, if published. |  |  |
| `deadline` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#time-v1-meta)_ | deadline is the node-granted lease expiry, if published. |  |  |
| `claimedAt` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#time-v1-meta)_ | claimedAt is when the node first granted the claim, if published; a renew<br />or a wake moves deadline, never this. |  |  |


#### NodeInventory



NodeInventory is the single O(nodes) etcd object per node: the durable summary
of that node's live sandboxes, server-side-applied on a slow cadence and
scatter-gathered by the aggregated sandbox-apiserver. It is deliberately
spec-less (pure reported summary, no desired state) and cluster-scoped with
metadata.name equal to the node name. It lives in this CRD extensions group —
NOT in the aggregated agents.x-k8s.io group, whose entire v1beta1 the
APIService hands to the aggregated server (which serves only `sandboxes`).



_Appears in:_
- [NodeInventoryList](#nodeinventorylist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `sandbox.cocoonstack.io/v1beta1` | | |
| `kind` _string_ | `NodeInventory` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `node` _string_ | node is the owning node name; it matches metadata.name. |  |  |
| `entries` _[InventoryEntry](#inventoryentry) array_ | entries summarizes the node's live sandboxes. |  |  |
| `address` _string_ | address is the node's sandboxd advertise address ("host:port"); the<br />aggregated apiserver routes a claim to this node's sandboxd through it. |  |  |
| `pools` _[PoolCapacity](#poolcapacity) array_ | pools is the node's per-pool warm capacity, used to pick a node that<br />already holds a warm microVM for a requested (template, net, size). |  |  |


#### NodeInventoryList



NodeInventoryList contains a list of NodeInventory.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `sandbox.cocoonstack.io/v1beta1` | | |
| `kind` _string_ | `NodeInventoryList` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[NodeInventory](#nodeinventory) array_ |  |  |  |


#### PoolCapacity



PoolCapacity is one sandboxd warm pool's capacity as reported by its owning
node's GET /v1/info: the pool key plus its warm/target counts. The aggregated
apiserver reads it to pick a node that already holds a warm microVM for a
requested (template, net, size).



_Appears in:_
- [NodeInventory](#nodeinventory)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `template` _string_ | template is the pool's base image (the sandbox template). |  |  |
| `net` _string_ | net is the pool's network shape (e.g. "none", "egress"). |  |  |
| `size` _string_ | size is the pool's VM size class (e.g. "small"). |  |  |
| `warm` _integer_ | warm is the number of ready-to-claim warm microVMs currently in the pool. |  |  |
| `target` _integer_ | target is the pool's desired warm depth. |  |  |


#### SandboxForkOptions



SandboxForkOptions is the body of POST sandboxes/{name}/fork. The source is
checkpointed in place and keeps running; each child is a brand-new sandbox
with its own id and lease, not a replica of the source's identity.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `sandbox.cocoonstack.io/v1beta1` | | |
| `kind` _string_ | `SandboxForkOptions` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `count` _integer_ | count is how many children to branch. Defaults to 1, and is bounded by<br />the owning node's configured fork limit. |  |  |
| `ttlSeconds` _integer_ | ttlSeconds is each child's lease. Children never inherit the parent's<br />remaining lease — a lease is a per-sandbox resource bound. Zero takes the<br />node's default. |  |  |


#### SandboxForkResult



SandboxForkResult is the reply to a fork: one entry per child, in request
order.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `sandbox.cocoonstack.io/v1beta1` | | |
| `kind` _string_ | `SandboxForkResult` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `children` _[ForkedSandbox](#forkedsandbox) array_ | children are the branched sandboxes. |  |  |


#### SandboxPauseOptions



SandboxPauseOptions is the body of POST sandboxes/{name}/pause. Pausing
snapshots the guest's memory and stops its VM, so it costs time proportional
to that memory — unlike resume, which takes the mmap restore fast path.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `sandbox.cocoonstack.io/v1beta1` | | |
| `kind` _string_ | `SandboxPauseOptions` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |


#### SandboxResumeOptions



SandboxResumeOptions is the body of POST sandboxes/{name}/resume. Resuming a
paused sandbox restores it through cocoon's mmap fast path and is idempotent
on one that is already running.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `sandbox.cocoonstack.io/v1beta1` | | |
| `kind` _string_ | `SandboxResumeOptions` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |


#### SandboxSnapshotOptions



SandboxSnapshotOptions is the body of POST sandboxes/{name}/snapshot. The
source keeps running; the checkpoint is an immutable state later sandboxes
can branch from.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `sandbox.cocoonstack.io/v1beta1` | | |
| `kind` _string_ | `SandboxSnapshotOptions` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `name` _string_ | name labels the checkpoint. Optional; the node assigns an id regardless. |  |  |


#### SandboxSnapshotResult



SandboxSnapshotResult is the reply to a snapshot.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `sandbox.cocoonstack.io/v1beta1` | | |
| `kind` _string_ | `SandboxSnapshotResult` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `snapshotID` _string_ | snapshotID is the checkpoint's node-local id. |  |  |
| `name` _string_ | name echoes the requested label, when one was given. |  |  |
| `nodeName` _string_ | nodeName is the node holding the checkpoint. Checkpoints are node-local,<br />so branching from or deleting one requires knowing its node. |  |  |
| `creationTimestamp` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#time-v1-meta)_ | creationTimestamp is when the node captured the checkpoint. |  |  |


