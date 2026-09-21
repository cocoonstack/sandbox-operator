# API Reference

## Packages
- [agents.x-k8s.io/v1alpha1](#agentsx-k8siov1alpha1)
- [agents.x-k8s.io/v1beta1](#agentsx-k8siov1beta1)
- [extensions.agents.x-k8s.io/v1alpha1](#extensionsagentsx-k8siov1alpha1)
- [extensions.agents.x-k8s.io/v1beta1](#extensionsagentsx-k8siov1beta1)


## agents.x-k8s.io/v1alpha1

Package v1alpha1 contains API Schema definitions for the agents v1alpha1 API group.

### Resource Types
- [Sandbox](#sandbox)
- [SandboxList](#sandboxlist)





#### EmbeddedObjectMetadata







_Appears in:_
- [PersistentVolumeClaimTemplate](#persistentvolumeclaimtemplate)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `name` _string_ | name must be unique within a namespace. Is required when creating resources, although<br />some resources may allow a client to request the generation of an appropriate name<br />automatically. Name is primarily intended for creation idempotence and configuration<br />definition.<br />Cannot be updated.<br />More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names#names |  |  |
| `labels` _object (keys:string, values:string)_ | labels defines the map of string keys and values that can be used to organize and categorize<br />(scope and select) objects. May match selectors of replication controllers<br />and services.<br />More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels |  |  |
| `annotations` _object (keys:string, values:string)_ | annotations is an unstructured key value map stored with a resource that may be<br />set by external tools to store and retrieve arbitrary metadata. They are not<br />queryable and should be preserved when modifying objects.<br />More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations |  |  |


#### Lifecycle



Lifecycle defines the lifecycle management for the Sandbox.



_Appears in:_
- [SandboxSpec](#sandboxspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `shutdownTime` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#time-v1-meta)_ | shutdownTime is the absolute time when the sandbox expires. |  | Format: date-time <br /> |
| `shutdownPolicy` _[ShutdownPolicy](#shutdownpolicy)_ | shutdownPolicy determines if the Sandbox resource itself should be deleted when it expires.<br />Underlying resources(Pods, Services) are always deleted on expiry. | Retain | Enum: [Delete Retain] <br /> |


#### PersistentVolumeClaimTemplate







_Appears in:_
- [SandboxSpec](#sandboxspec)
- [SandboxTemplateSpec](#sandboxtemplatespec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `metadata` _[EmbeddedObjectMetadata](#embeddedobjectmetadata)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[PersistentVolumeClaimSpec](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#persistentvolumeclaimspec-v1-core)_ | spec is the PVC's spec |  |  |


#### PodMetadata







_Appears in:_
- [PodTemplate](#podtemplate)
- [SandboxClaimSpec](#sandboxclaimspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `labels` _object (keys:string, values:string)_ | labels defines the map of string keys and values that can be used to organize and categorize<br />(scope and select) objects. May match selectors of replication controllers<br />and services.<br />More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels |  |  |
| `annotations` _object (keys:string, values:string)_ | annotations is an unstructured key value map stored with a resource that may be<br />set by external tools to store and retrieve arbitrary metadata. They are not<br />queryable and should be preserved when modifying objects.<br />More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations |  |  |


#### PodTemplate







_Appears in:_
- [SandboxSpec](#sandboxspec)
- [SandboxTemplateSpec](#sandboxtemplatespec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `spec` _[PodSpec](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#podspec-v1-core)_ | spec is the Pod's spec |  |  |
| `metadata` _[PodMetadata](#podmetadata)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |


#### Sandbox



Sandbox is the Schema for the sandboxes API.



_Appears in:_
- [SandboxList](#sandboxlist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `agents.x-k8s.io/v1alpha1` | | |
| `kind` _string_ | `Sandbox` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[SandboxSpec](#sandboxspec)_ | spec defines the desired state of Sandbox |  |  |
| `status` _[SandboxStatus](#sandboxstatus)_ | status defines the observed state of Sandbox |  |  |


#### SandboxList



SandboxList contains a list of Sandbox.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `agents.x-k8s.io/v1alpha1` | | |
| `kind` _string_ | `SandboxList` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[Sandbox](#sandbox) array_ |  |  |  |


#### SandboxSpec



SandboxSpec defines the desired state of Sandbox.



_Appears in:_
- [Sandbox](#sandbox)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `podTemplate` _[PodTemplate](#podtemplate)_ | podTemplate describes the pod spec that will be used to create an agent sandbox. |  |  |
| `volumeClaimTemplates` _[PersistentVolumeClaimTemplate](#persistentvolumeclaimtemplate) array_ | volumeClaimTemplates is a list of claims that the sandbox pod is allowed to reference.<br />Every claim in this list must have at least one matching access mode with a provisioner volume. |  |  |
| `shutdownTime` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#time-v1-meta)_ | shutdownTime is the absolute time when the sandbox expires. |  | Format: date-time <br /> |
| `shutdownPolicy` _[ShutdownPolicy](#shutdownpolicy)_ | shutdownPolicy determines if the Sandbox resource itself should be deleted when it expires.<br />Underlying resources(Pods, Services) are always deleted on expiry. | Retain | Enum: [Delete Retain] <br /> |
| `replicas` _integer_ | replicas is the number of desired replicas.<br />The only allowed values are 0 and 1.<br />Defaults to 1. | 1 | Maximum: 1 <br />Minimum: 0 <br /> |
| `service` _boolean_ | service controls whether the controller should automatically create a<br />headless Service for this Sandbox.<br />When unset, the controller preserves existing Services for backward<br />compatibility but does not create new ones. Set to true to enable or false<br />to explicitly disable and remove the Service. |  |  |


#### SandboxStatus



SandboxStatus defines the observed state of Sandbox.



_Appears in:_
- [Sandbox](#sandbox)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `serviceFQDN` _string_ | serviceFQDN that is valid for default cluster settings<br />The domain defaults to cluster.local but is configurable via the controller's --cluster-domain flag. |  |  |
| `service` _string_ | Service is the headless Service name fronting the Sandbox pod, set once the controller creates it and cleared when it is removed. |  |  |
| `conditions` _[Condition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#condition-v1-meta) array_ | conditions defines the status conditions array |  |  |
| `replicas` _integer_ | replicas is the number of actual replicas. |  | Minimum: 0 <br /> |
| `selector` _string_ | selector is the label selector for pods. |  |  |
| `podIPs` _string array_ | podIPs are the IP addresses of the underlying pod.<br />A pod may have multiple IPs in dual-stack clusters. |  |  |


#### ShutdownPolicy

_Underlying type:_ _string_

ShutdownPolicy describes the policy for deleting the Sandbox when it expires.

_Validation:_
- Enum: [Delete Retain]

_Appears in:_
- [Lifecycle](#lifecycle)
- [SandboxSpec](#sandboxspec)

| Field | Description |
| --- | --- |
| `Delete` | ShutdownPolicyDelete deletes the Sandbox when expired.<br /> |
| `Retain` | ShutdownPolicyRetain keeps the Sandbox when expired (Status will show Expired).<br /> |



## agents.x-k8s.io/v1beta1

Package v1beta1 contains API Schema definitions for the agents v1beta1 API group.

### Resource Types
- [Sandbox](#sandbox)
- [SandboxForkOptions](#sandboxforkoptions)
- [SandboxForkResult](#sandboxforkresult)
- [SandboxList](#sandboxlist)
- [SandboxPauseOptions](#sandboxpauseoptions)
- [SandboxResumeOptions](#sandboxresumeoptions)
- [SandboxSnapshotOptions](#sandboxsnapshotoptions)
- [SandboxSnapshotResult](#sandboxsnapshotresult)





#### EmbeddedObjectMetadata







_Appears in:_
- [PersistentVolumeClaimTemplate](#persistentvolumeclaimtemplate)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `name` _string_ | name must be unique within a namespace. Is required when creating resources, although<br />some resources may allow a client to request the generation of an appropriate name<br />automatically. Name is primarily intended for creation idempotence and configuration<br />definition.<br />Cannot be updated.<br />More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names#names |  |  |
| `labels` _object (keys:string, values:string)_ | labels defines the map of string keys and values that can be used to organize and categorize<br />(scope and select) objects. May match selectors of replication controllers<br />and services.<br />More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels |  |  |
| `annotations` _object (keys:string, values:string)_ | annotations is an unstructured key value map stored with a resource that may be<br />set by external tools to store and retrieve arbitrary metadata. They are not<br />queryable and should be preserved when modifying objects.<br />More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations |  |  |


#### ForkedSandbox



ForkedSandbox identifies one child of a fork.



_Appears in:_
- [SandboxForkResult](#sandboxforkresult)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `sandboxID` _string_ | sandboxID is the child's node-local claim id. |  |  |
| `nodeName` _string_ | nodeName is the node that owns the child. A fork is node-local, so every<br />child lands on the source's node. |  |  |
| `address` _string_ | address is the child's connection address, when the node published one. |  |  |


#### Lifecycle



Lifecycle defines the lifecycle management for the Sandbox.



_Appears in:_
- [SandboxSpec](#sandboxspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `shutdownTime` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#time-v1-meta)_ | shutdownTime is the absolute time when the sandbox expires. |  | Format: date-time <br /> |
| `shutdownPolicy` _[ShutdownPolicy](#shutdownpolicy)_ | shutdownPolicy determines if the Sandbox resource itself should be deleted when it expires.<br />Underlying resources(Pods, Services) are always deleted on expiry. | Retain | Enum: [Delete Retain] <br /> |


#### PersistentVolumeClaimTemplate







_Appears in:_
- [SandboxBlueprint](#sandboxblueprint)
- [SandboxClaimSpec](#sandboxclaimspec)
- [SandboxSpec](#sandboxspec)
- [SandboxTemplateSpec](#sandboxtemplatespec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `metadata` _[EmbeddedObjectMetadata](#embeddedobjectmetadata)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[PersistentVolumeClaimSpec](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#persistentvolumeclaimspec-v1-core)_ | spec is the PVC's spec |  |  |


#### PodMetadata







_Appears in:_
- [PodTemplate](#podtemplate)
- [SandboxClaimSpec](#sandboxclaimspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `labels` _object (keys:string, values:string)_ | labels defines the map of string keys and values that can be used to organize and categorize<br />(scope and select) objects. May match selectors of replication controllers<br />and services.<br />More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels |  |  |
| `annotations` _object (keys:string, values:string)_ | annotations is an unstructured key value map stored with a resource that may be<br />set by external tools to store and retrieve arbitrary metadata. They are not<br />queryable and should be preserved when modifying objects.<br />More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations |  |  |


#### PodTemplate







_Appears in:_
- [SandboxBlueprint](#sandboxblueprint)
- [SandboxSpec](#sandboxspec)
- [SandboxTemplateSpec](#sandboxtemplatespec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `spec` _[PodSpec](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#podspec-v1-core)_ | spec is the Pod's spec |  |  |
| `metadata` _[PodMetadata](#podmetadata)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |


#### Sandbox



Sandbox is the Schema for the sandboxes API.



_Appears in:_
- [SandboxList](#sandboxlist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `agents.x-k8s.io/v1beta1` | | |
| `kind` _string_ | `Sandbox` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[SandboxSpec](#sandboxspec)_ | spec defines the desired state of Sandbox |  |  |
| `status` _[SandboxStatus](#sandboxstatus)_ | status defines the observed state of Sandbox |  |  |


#### SandboxBlueprint



SandboxBlueprint defines the configuration shared between Sandbox and SandboxTemplate.
It deliberately excludes runtime-only fields (operatingMode, lifecycle).



_Appears in:_
- [SandboxSpec](#sandboxspec)
- [SandboxTemplateSpec](#sandboxtemplatespec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `podTemplate` _[PodTemplate](#podtemplate)_ | podTemplate describes the pod that will be created in the sandbox.<br />Note: When provisioned via a SandboxTemplate (such as by a SandboxClaim or SandboxWarmPool),<br />if AutomountServiceAccountToken is not specified in the PodSpec, the controller defaults it<br />to false to ensure a secure-by-default environment. |  |  |
| `volumeClaimTemplates` _[PersistentVolumeClaimTemplate](#persistentvolumeclaimtemplate) array_ | volumeClaimTemplates is a list of claims that the sandbox pod is allowed to reference.<br />When creating a sandbox, PVCs will be created from these templates.<br />Every claim in this list must have at least one matching access mode with a provisioner volume.<br />NOTE: This list is atomic. Updates to this field will replace the entire list rather than merging with existing entries. |  |  |
| `service` _boolean_ | service controls whether the controller should automatically create a<br />headless Service for the Sandbox workload.<br />When unset, the controller preserves existing Services for backward<br />compatibility but does not create new ones. Set to true to enable or false<br />to explicitly disable and remove the Service. |  |  |


#### SandboxForkOptions



SandboxForkOptions is the body of POST sandboxes/{name}/fork. The source is
checkpointed in place and keeps running; each child is a brand-new sandbox
with its own id and lease, not a replica of the source's identity.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `agents.x-k8s.io/v1beta1` | | |
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
| `apiVersion` _string_ | `agents.x-k8s.io/v1beta1` | | |
| `kind` _string_ | `SandboxForkResult` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `children` _[ForkedSandbox](#forkedsandbox) array_ | children are the branched sandboxes. |  |  |


#### SandboxList



SandboxList contains a list of Sandbox.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `agents.x-k8s.io/v1beta1` | | |
| `kind` _string_ | `SandboxList` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[Sandbox](#sandbox) array_ |  |  |  |


#### SandboxOperatingMode

_Underlying type:_ _string_

SandboxOperatingMode defines the desired operational state of the Sandbox.



_Appears in:_
- [SandboxSpec](#sandboxspec)

| Field | Description |
| --- | --- |
| `Running` | SandboxOperatingModeRunning indicates the sandbox should be actively running.<br /> |
| `Suspended` | SandboxOperatingModeSuspended indicates the sandbox should be suspended.<br /> |


#### SandboxPauseOptions



SandboxPauseOptions is the body of POST sandboxes/{name}/pause. Pausing
snapshots the guest's memory and stops its VM, so it costs time proportional
to that memory — unlike resume, which takes the mmap restore fast path.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `agents.x-k8s.io/v1beta1` | | |
| `kind` _string_ | `SandboxPauseOptions` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |


#### SandboxResumeOptions



SandboxResumeOptions is the body of POST sandboxes/{name}/resume. Resuming a
paused sandbox restores it through cocoon's mmap fast path and is idempotent
on one that is already running.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `agents.x-k8s.io/v1beta1` | | |
| `kind` _string_ | `SandboxResumeOptions` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |


#### SandboxSnapshotOptions



SandboxSnapshotOptions is the body of POST sandboxes/{name}/snapshot. The
source keeps running; the checkpoint is an immutable state later sandboxes
can branch from.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `agents.x-k8s.io/v1beta1` | | |
| `kind` _string_ | `SandboxSnapshotOptions` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `name` _string_ | name labels the checkpoint. Optional; the node assigns an id regardless. |  |  |


#### SandboxSnapshotResult



SandboxSnapshotResult is the reply to a snapshot.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `agents.x-k8s.io/v1beta1` | | |
| `kind` _string_ | `SandboxSnapshotResult` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `snapshotID` _string_ | snapshotID is the checkpoint's node-local id. |  |  |
| `name` _string_ | name echoes the requested label, when one was given. |  |  |
| `nodeName` _string_ | nodeName is the node holding the checkpoint. Checkpoints are node-local,<br />so branching from or deleting one requires knowing its node. |  |  |
| `creationTimestamp` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#time-v1-meta)_ | creationTimestamp is when the node captured the checkpoint. |  |  |


#### SandboxSpec



SandboxSpec defines the desired state of Sandbox.



_Appears in:_
- [Sandbox](#sandbox)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `podTemplate` _[PodTemplate](#podtemplate)_ | podTemplate describes the pod that will be created in the sandbox.<br />Note: When provisioned via a SandboxTemplate (such as by a SandboxClaim or SandboxWarmPool),<br />if AutomountServiceAccountToken is not specified in the PodSpec, the controller defaults it<br />to false to ensure a secure-by-default environment. |  |  |
| `volumeClaimTemplates` _[PersistentVolumeClaimTemplate](#persistentvolumeclaimtemplate) array_ | volumeClaimTemplates is a list of claims that the sandbox pod is allowed to reference.<br />When creating a sandbox, PVCs will be created from these templates.<br />Every claim in this list must have at least one matching access mode with a provisioner volume.<br />NOTE: This list is atomic. Updates to this field will replace the entire list rather than merging with existing entries. |  |  |
| `service` _boolean_ | service controls whether the controller should automatically create a<br />headless Service for the Sandbox workload.<br />When unset, the controller preserves existing Services for backward<br />compatibility but does not create new ones. Set to true to enable or false<br />to explicitly disable and remove the Service. |  |  |
| `shutdownTime` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#time-v1-meta)_ | shutdownTime is the absolute time when the sandbox expires. |  | Format: date-time <br /> |
| `shutdownPolicy` _[ShutdownPolicy](#shutdownpolicy)_ | shutdownPolicy determines if the Sandbox resource itself should be deleted when it expires.<br />Underlying resources(Pods, Services) are always deleted on expiry. | Retain | Enum: [Delete Retain] <br /> |
| `operatingMode` _[SandboxOperatingMode](#sandboxoperatingmode)_ | operatingMode specifies the desired operational state of the Sandbox.<br />Defaults to Running if not specified. | Running | Enum: [Running Suspended] <br /> |


#### SandboxStatus



SandboxStatus defines the observed state of Sandbox.



_Appears in:_
- [Sandbox](#sandbox)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `serviceFQDN` _string_ | serviceFQDN that is valid for default cluster settings<br />The domain defaults to cluster.local but is configurable via the controller's --cluster-domain flag. |  |  |
| `service` _string_ | Service is the headless Service name fronting the Sandbox pod, set once the controller creates it and cleared when it is removed. |  |  |
| `conditions` _[Condition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#condition-v1-meta) array_ | conditions defines the status conditions array |  |  |
| `selector` _string_ | selector is the label selector for pods. |  |  |
| `podIPs` _string array_ | podIPs are the IP addresses of the underlying pod.<br />A pod may have multiple IPs in dual-stack clusters. |  |  |
| `nodeName` _string_ | nodeName is the name of the node where the underlying pod is scheduled. |  |  |


#### ShutdownPolicy

_Underlying type:_ _string_

ShutdownPolicy describes the policy for deleting the Sandbox when it expires.

_Validation:_
- Enum: [Delete Retain]

_Appears in:_
- [Lifecycle](#lifecycle)
- [SandboxSpec](#sandboxspec)

| Field | Description |
| --- | --- |
| `Delete` | ShutdownPolicyDelete deletes the Sandbox when expired.<br /> |
| `Retain` | ShutdownPolicyRetain keeps the Sandbox when expired (Status will show Expired).<br /> |



## extensions.agents.x-k8s.io/v1alpha1

Package v1alpha1 contains API Schema definitions for the extensions.agents v1alpha1 API group.

### Resource Types
- [SandboxClaim](#sandboxclaim)
- [SandboxClaimList](#sandboxclaimlist)
- [SandboxTemplate](#sandboxtemplate)
- [SandboxTemplateList](#sandboxtemplatelist)
- [SandboxWarmPool](#sandboxwarmpool)
- [SandboxWarmPoolList](#sandboxwarmpoollist)



#### EnvVar



EnvVar represents a custom environment variable key-value pair.



_Appears in:_
- [SandboxClaimSpec](#sandboxclaimspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `name` _string_ | name of the environment variable. |  |  |
| `value` _string_ | value of the environment variable. |  |  |
| `containerName` _string_ | containerName specifies the target container for the environment variable.<br />If not specified, it defaults to the first container defined in the template. |  |  |


#### EnvVarsInjectionPolicy

_Underlying type:_ _string_

EnvVarsInjectionPolicy defines whether a SandboxClaim is allowed to inject or override environment variables.



_Appears in:_
- [SandboxTemplateSpec](#sandboxtemplatespec)

| Field | Description |
| --- | --- |
| `Allowed` | EnvVarsInjectionPolicyAllowed allows a SandboxClaim to inject new environment variables, but not override existing ones.<br /> |
| `Overrides` | EnvVarsInjectionPolicyOverrides allows a SandboxClaim to inject new and override existing environment variables.<br /> |
| `Disallowed` | EnvVarsInjectionPolicyDisallowed prevents a SandboxClaim from injecting any environment variables.<br /> |


#### Lifecycle



Lifecycle defines the lifecycle management for the SandboxClaim.



_Appears in:_
- [SandboxClaimSpec](#sandboxclaimspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `shutdownTime` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#time-v1-meta)_ | shutdownTime is the absolute time when the SandboxClaim expires.<br />This time governs the lifecycle of the claim. It is not propagated to the<br />underlying Sandbox. Instead, the SandboxClaim controller enforces this<br />expiration by deleting the Sandbox resources when the time is reached.<br />If this field is omitted or set to nil, the SandboxClaim itself won't expire.<br />This implies unsetting a Sandbox's ShutdownTime via SandboxClaim isn't supported. |  | Format: date-time <br /> |
| `ttlSecondsAfterFinished` _integer_ | ttlSecondsAfterFinished limits how long a finished claim is retained.<br />The timer starts from the mirrored Finished condition's LastTransitionTime. |  | Minimum: 0 <br /> |
| `shutdownPolicy` _[ShutdownPolicy](#shutdownpolicy)_ | shutdownPolicy determines the behavior when the SandboxClaim expires. | Retain | Enum: [Delete DeleteForeground Retain] <br /> |


#### NetworkPolicyManagement

_Underlying type:_ _string_

NetworkPolicyManagement defines whether the controller automatically generates
and manages a shared NetworkPolicy for this template.



_Appears in:_
- [SandboxTemplateSpec](#sandboxtemplatespec)

| Field | Description |
| --- | --- |
| `Managed` | NetworkPolicyManagementManaged means the controller will ensure a shared NetworkPolicy exists.<br />This shared NetworkPolicy will be a user provide one or a default controller created policy.<br />This is the default behavior if the field is omitted.<br /> |
| `Unmanaged` | NetworkPolicyManagementUnmanaged means the controller will skip NetworkPolicy<br />creation entirely, allowing external systems (like Cilium) to manage networking.<br /> |


#### NetworkPolicySpec



NetworkPolicySpec defines the desired state of the NetworkPolicy.



_Appears in:_
- [SandboxTemplateSpec](#sandboxtemplatespec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `ingress` _[NetworkPolicyIngressRule](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#networkpolicyingressrule-v1-networking) array_ | ingress is a list of ingress rules to be applied to the sandbox.<br />Traffic is allowed to the sandbox if it matches at least one rule.<br />If this list is empty, all ingress traffic is blocked (Default Deny). |  |  |
| `egress` _[NetworkPolicyEgressRule](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#networkpolicyegressrule-v1-networking) array_ | egress is a list of egress rules to be applied to the sandbox.<br />Traffic is allowed out of the sandbox if it matches at least one rule.<br />If this list is empty, all egress traffic is blocked (Default Deny). |  |  |


#### SandboxClaim



SandboxClaim is the Schema for the sandbox Claim API.



_Appears in:_
- [SandboxClaimList](#sandboxclaimlist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `extensions.agents.x-k8s.io/v1alpha1` | | |
| `kind` _string_ | `SandboxClaim` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[SandboxClaimSpec](#sandboxclaimspec)_ | spec defines the desired state of Sandbox |  |  |
| `status` _[SandboxClaimStatus](#sandboxclaimstatus)_ | status defines the observed state of Sandbox |  |  |


#### SandboxClaimList



SandboxClaimList contains a list of SandboxClaim.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `extensions.agents.x-k8s.io/v1alpha1` | | |
| `kind` _string_ | `SandboxClaimList` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[SandboxClaim](#sandboxclaim) array_ |  |  |  |


#### SandboxClaimSpec



SandboxClaimSpec defines the desired state of Sandbox.



_Appears in:_
- [SandboxClaim](#sandboxclaim)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `sandboxTemplateRef` _[SandboxTemplateRef](#sandboxtemplateref)_ | sandboxTemplateRef defines the name of the SandboxTemplate to be used for creating a Sandbox. |  |  |
| `lifecycle` _[Lifecycle](#lifecycle)_ | lifecycle defines when and how the SandboxClaim should be shut down. |  |  |
| `warmpool` _[WarmPoolPolicy](#warmpoolpolicy)_ | warmpool specifies the warm pool policy for sandbox adoption.<br />- "none": Do not use any warm pool, always create fresh sandboxes<br />- "default": Cold-start from the template; warm adoption needs a named pool (default)<br />- A warm pool name: Select only from the specified warm pool (e.g., "fast-pool", "secure-pool") | default |  |
| `additionalPodMetadata` _[PodMetadata](#podmetadata)_ | additionalPodMetadata defines the labels and annotations to be propagated to the Sandbox Pod.<br />Label values are limited to 63 characters and must match Kubernetes label value patterns. |  |  |
| `env` _[EnvVar](#envvar) array_ | env is a list of environment variables to inject into the sandbox |  |  |


#### SandboxClaimStatus



SandboxClaimStatus defines the observed state of Sandbox.



_Appears in:_
- [SandboxClaim](#sandboxclaim)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `conditions` _[Condition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#condition-v1-meta) array_ | conditions represent the latest available observations of a Sandbox's current state. |  |  |
| `sandbox` _[SandboxStatus](#sandboxstatus)_ | sandbox defines the state of Sandbox |  |  |


#### SandboxStatus







_Appears in:_
- [SandboxClaimStatus](#sandboxclaimstatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `name` _string_ | name is the name of the Sandbox created from this claim |  |  |
| `podIPs` _string array_ | podIPs are the IP addresses of the underlying pod.<br />A pod may have multiple IPs in dual-stack clusters. |  |  |


#### SandboxTemplate



SandboxTemplate is the Schema for the sandbox template API.



_Appears in:_
- [SandboxTemplateList](#sandboxtemplatelist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `extensions.agents.x-k8s.io/v1alpha1` | | |
| `kind` _string_ | `SandboxTemplate` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[SandboxTemplateSpec](#sandboxtemplatespec)_ | spec defines the desired state of Sandbox |  |  |


#### SandboxTemplateList



SandboxTemplateList contains a list of Sandbox.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `extensions.agents.x-k8s.io/v1alpha1` | | |
| `kind` _string_ | `SandboxTemplateList` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[SandboxTemplate](#sandboxtemplate) array_ |  |  |  |


#### SandboxTemplateRef



SandboxTemplateRef references a SandboxTemplate.



_Appears in:_
- [SandboxClaimSpec](#sandboxclaimspec)
- [SandboxWarmPoolSpec](#sandboxwarmpoolspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `name` _string_ | name of the SandboxTemplate |  |  |


#### SandboxTemplateSpec



SandboxTemplateSpec defines the desired state of Sandbox.



_Appears in:_
- [SandboxTemplate](#sandboxtemplate)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `podTemplate` _[PodTemplate](#podtemplate)_ | podTemplate defines the object template that describes the pod spec that will be used to create<br />an agent sandbox.<br />If AutomountServiceAccountToken is not specified in the PodSpec, it defaults to false<br />to ensure a secure-by-default environment. |  |  |
| `volumeClaimTemplates` _[PersistentVolumeClaimTemplate](#persistentvolumeclaimtemplate) array_ | volumeClaimTemplates is a list of claims that pods created from this template<br />are allowed to reference. When a SandboxClaim or SandboxWarmPool creates a sandbox<br />from this template, PVCs will be created from these templates.<br />Every claim in this list must have at least one matching access mode with a provisioner volume.<br />NOTE: This list is atomic. Updates to this field will replace the entire list rather than merging with existing entries. |  |  |
| `networkPolicy` _[NetworkPolicySpec](#networkpolicyspec)_ | networkPolicy defines the network policy to be applied to the sandboxes<br />created from this template. A single shared NetworkPolicy is created per Template.<br />Behavior is dictated by the NetworkPolicyManagement field:<br />- If Management is "Unmanaged": This field is completely ignored.<br />- If Management is "Managed" (default) and this field is omitted (nil): The controller<br />  automatically applies a strict Secure Default policy:<br />    * Ingress: Allow traffic only from the Sandbox Router.<br />    * Egress: Allow Public Internet only. Blocks internal IPs (RFC1918), Metadata Server, etc.<br />- If Management is "Managed" and this field is provided: The controller applies your custom rules.<br />Update Behavior:<br />Because the NetworkPolicy is shared at the template level, any updates to these rules<br />will be applied to the single shared policy object. The underlying Kubernetes CNI will then<br />dynamically enforce the updated rules across all existing and future sandboxes<br />referencing this template.<br />NOTE: This is a restricted subset of the standard Kubernetes NetworkPolicySpec.<br />Fields like 'PodSelector' and 'PolicyTypes' are intentionally excluded because<br />they are managed by the controller to ensure strict isolation and default-deny posture.<br />WARNING: This policy enforces a strict "Default Deny" ingress posture.<br />If your Pod uses sidecars (e.g., Istio proxy, monitoring agents) that listen<br />on their own ports, the NetworkPolicy will BLOCK traffic to them by default.<br />You MUST explicitly allow traffic to these sidecar ports using 'Ingress',<br />otherwise the sidecars may fail health checks. |  |  |
| `networkPolicyManagement` _[NetworkPolicyManagement](#networkpolicymanagement)_ | networkPolicyManagement defines whether the controller manages the NetworkPolicy.<br />Valid values are "Managed" (default) or "Unmanaged". | Managed | Enum: [Managed Unmanaged] <br /> |
| `envVarsInjectionPolicy` _[EnvVarsInjectionPolicy](#envvarsinjectionpolicy)_ | envVarsInjectionPolicy allows a SandboxClaim to inject or override environment variables defined in the template.<br />If set to Disallowed, the SandboxClaim will be rejected if it specifies any environment variables. | Disallowed | Enum: [Allowed Overrides Disallowed] <br /> |
| `service` _boolean_ | service controls whether the controller should automatically create a<br />headless Service for Sandboxes created from this template.<br />When unset, the controller preserves existing Services for backward<br />compatibility but does not create new ones. Set to true to enable or false<br />to explicitly disable and remove the Service. |  |  |


#### SandboxWarmPool



SandboxWarmPool is the Schema for the sandboxwarmpools API.



_Appears in:_
- [SandboxWarmPoolList](#sandboxwarmpoollist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `extensions.agents.x-k8s.io/v1alpha1` | | |
| `kind` _string_ | `SandboxWarmPool` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[SandboxWarmPoolSpec](#sandboxwarmpoolspec)_ | spec defines the desired state of SandboxWarmPool |  |  |
| `status` _[SandboxWarmPoolStatus](#sandboxwarmpoolstatus)_ | status defines the observed state of SandboxWarmPool |  |  |


#### SandboxWarmPoolList



SandboxWarmPoolList contains a list of SandboxWarmPool.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `extensions.agents.x-k8s.io/v1alpha1` | | |
| `kind` _string_ | `SandboxWarmPoolList` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[SandboxWarmPool](#sandboxwarmpool) array_ |  |  |  |


#### SandboxWarmPoolSpec



SandboxWarmPoolSpec defines the desired state of SandboxWarmPool.



_Appears in:_
- [SandboxWarmPool](#sandboxwarmpool)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `replicas` _integer_ | replicas is the desired number of sandboxes in the pool.<br />This field is controlled by an HPA if specified. |  | Minimum: 0 <br /> |
| `sandboxTemplateRef` _[SandboxTemplateRef](#sandboxtemplateref)_ | sandboxTemplateRef - name of the SandboxTemplate to be used for creating a Sandbox<br />Warning: Any change to the json tag "sandboxTemplateRef" must be synchronized with the TemplateRefField constant. |  |  |
| `updateStrategy` _[SandboxWarmPoolUpdateStrategy](#sandboxwarmpoolupdatestrategy)_ | updateStrategy - strategy for updating the SandboxWarmPool pods based on sandboxTemplateRef name change or underlying template changes |  |  |


#### SandboxWarmPoolStatus



SandboxWarmPoolStatus defines the observed state of SandboxWarmPool.



_Appears in:_
- [SandboxWarmPool](#sandboxwarmpool)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `replicas` _integer_ | replicas is the total number of sandboxes in the pool. |  |  |
| `readyReplicas` _integer_ | readyReplicas is the total number of sandboxes in the pool that are in a ready state. |  |  |
| `selector` _string_ | selector is the label selector used to find the pods in the pool. |  |  |


#### SandboxWarmPoolUpdateStrategy



SandboxWarmPoolUpdateStrategy defines the update strategy for the SandboxWarmPool.



_Appears in:_
- [SandboxWarmPoolSpec](#sandboxwarmpoolspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `type` _[SandboxWarmPoolUpdateStrategyType](#sandboxwarmpoolupdatestrategytype)_ | type indicates the type of the SandboxWarmPoolUpdateStrategy.<br />Default is OnReplenish. | OnReplenish | Enum: [Recreate OnReplenish] <br /> |


#### SandboxWarmPoolUpdateStrategyType

_Underlying type:_ _string_

SandboxWarmPoolUpdateStrategyType is a string enumeration type that enumerates
all possible update strategies for the SandboxWarmPool controller.

_Validation:_
- Enum: [Recreate OnReplenish]

_Appears in:_
- [SandboxWarmPoolUpdateStrategy](#sandboxwarmpoolupdatestrategy)

| Field | Description |
| --- | --- |
| `Recreate` | RecreateSandboxWarmPoolUpdateStrategyType indicates that stale pods are deleted immediately to ensure the pool only contains fresh pods.<br />Note: This applies to PodTemplate spec changes only. Changes to annotations or labels in the template do not trigger recreate.<br /> |
| `OnReplenish` | OnReplenishSandboxWarmPoolUpdateStrategyType indicates that stale pods are only replaced when they are manually deleted or when these stale pods are adopted by sandboxclaims and hence replaced by fresh pods.<br /> |


#### ShutdownPolicy

_Underlying type:_ _string_

ShutdownPolicy describes the policy for shutting down the underlying Sandbox when the SandboxClaim expires.

_Validation:_
- Enum: [Delete DeleteForeground Retain]

_Appears in:_
- [Lifecycle](#lifecycle)

| Field | Description |
| --- | --- |
| `Delete` | ShutdownPolicyDelete deletes the SandboxClaim (and cascadingly the Sandbox) when expired.<br /> |
| `DeleteForeground` | ShutdownPolicyDeleteForeground deletes the SandboxClaim when expired using foreground<br />cascade deletion. The claim remains in the API (with a deletionTimestamp) until its<br />underlying Sandbox and Pod are fully terminated. This allows external systems to observe<br />shutdown progress by checking whether the claim still exists.<br /> |
| `Retain` | ShutdownPolicyRetain keeps the SandboxClaim when expired (Status will show Expired).<br />The underlying SandboxClaim resources (Sandbox, Pod, Service) are deleted to save resources,<br />but the SandboxClaim object itself remains.<br /> |


#### WarmPoolPolicy

_Underlying type:_ _string_

WarmPoolPolicy describes the policy for using warm pools.
It can be one of the following:
  - "none": Do not use any warm pool, always create fresh sandboxes
  - "default": Cold-start from the template; warm adoption requires a named pool (default)
  - A warm pool name: Select only from the specified warm pool (e.g., "fast-pool", "secure-pool")



_Appears in:_
- [SandboxClaimSpec](#sandboxclaimspec)

| Field | Description |
| --- | --- |
| `none` | WarmPoolPolicyNone indicates that no warm pool should be used.<br />A fresh sandbox will always be created.<br /> |
| `default` | WarmPoolPolicyDefault is the default when warmpool is unset: the claim<br />cold-starts from its template; warm adoption needs a named pool.<br /> |



## extensions.agents.x-k8s.io/v1beta1

Package v1beta1 contains API Schema definitions for the extensions.agents v1beta1 API group.

### Resource Types
- [NodeInventory](#nodeinventory)
- [NodeInventoryList](#nodeinventorylist)
- [SandboxClaim](#sandboxclaim)
- [SandboxClaimList](#sandboxclaimlist)
- [SandboxTemplate](#sandboxtemplate)
- [SandboxTemplateList](#sandboxtemplatelist)
- [SandboxWarmPool](#sandboxwarmpool)
- [SandboxWarmPoolList](#sandboxwarmpoollist)



#### EnvVar



EnvVar represents a custom environment variable key-value pair.



_Appears in:_
- [SandboxClaimSpec](#sandboxclaimspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `name` _string_ | name of the environment variable. |  |  |
| `value` _string_ | value of the environment variable. |  |  |
| `containerName` _string_ | containerName specifies the target container for the environment variable.<br />If not specified, it defaults to the first container defined in the template. |  |  |


#### EnvVarsInjectionPolicy

_Underlying type:_ _string_

EnvVarsInjectionPolicy defines whether a SandboxClaim is allowed to inject or override environment variables.



_Appears in:_
- [SandboxTemplateSpec](#sandboxtemplatespec)

| Field | Description |
| --- | --- |
| `Allowed` | EnvVarsInjectionPolicyAllowed allows a SandboxClaim to inject new environment variables, but not override existing ones.<br /> |
| `Overrides` | EnvVarsInjectionPolicyOverrides allows a SandboxClaim to inject new and override existing environment variables.<br /> |
| `Disallowed` | EnvVarsInjectionPolicyDisallowed prevents a SandboxClaim from injecting any environment variables.<br /> |


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


#### Lifecycle



Lifecycle defines the lifecycle management for the SandboxClaim.



_Appears in:_
- [SandboxClaimSpec](#sandboxclaimspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `shutdownTime` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#time-v1-meta)_ | shutdownTime is the absolute time when the SandboxClaim expires.<br />This time governs the lifecycle of the claim. It is not propagated to the<br />underlying Sandbox. Instead, the SandboxClaim controller enforces this<br />expiration by deleting the Sandbox resources when the time is reached.<br />If this field is omitted or set to nil, the SandboxClaim itself won't expire.<br />This implies unsetting a Sandbox's ShutdownTime via SandboxClaim isn't supported. |  | Format: date-time <br /> |
| `ttlSecondsAfterFinished` _integer_ | ttlSecondsAfterFinished limits how long a finished claim is retained.<br />The timer starts from the mirrored Finished condition's LastTransitionTime. |  | Minimum: 0 <br /> |
| `shutdownPolicy` _[ShutdownPolicy](#shutdownpolicy)_ | shutdownPolicy determines the behavior when the SandboxClaim expires. | Retain | Enum: [Delete DeleteForeground Retain] <br /> |


#### NetworkPolicyManagement

_Underlying type:_ _string_

NetworkPolicyManagement defines whether the controller automatically generates
and manages a shared NetworkPolicy for this template.



_Appears in:_
- [SandboxTemplateSpec](#sandboxtemplatespec)

| Field | Description |
| --- | --- |
| `Managed` | NetworkPolicyManagementManaged means the controller will ensure a shared NetworkPolicy exists.<br />This shared NetworkPolicy will be a user provide one or a default controller created policy.<br />This is the default behavior if the field is omitted.<br /> |
| `Unmanaged` | NetworkPolicyManagementUnmanaged means the controller will skip NetworkPolicy<br />creation entirely, allowing external systems (like Cilium) to manage networking.<br /> |


#### NetworkPolicySpec



NetworkPolicySpec defines the desired state of the NetworkPolicy.



_Appears in:_
- [SandboxTemplateSpec](#sandboxtemplatespec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `ingress` _[NetworkPolicyIngressRule](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#networkpolicyingressrule-v1-networking) array_ | ingress is a list of ingress rules to be applied to the sandbox.<br />Traffic is allowed to the sandbox if it matches at least one rule.<br />If this list is empty, all ingress traffic is blocked (Default Deny). |  |  |
| `egress` _[NetworkPolicyEgressRule](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#networkpolicyegressrule-v1-networking) array_ | egress is a list of egress rules to be applied to the sandbox.<br />Traffic is allowed out of the sandbox if it matches at least one rule.<br />If this list is empty, all egress traffic is blocked (Default Deny). |  |  |


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
| `apiVersion` _string_ | `extensions.agents.x-k8s.io/v1beta1` | | |
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
| `apiVersion` _string_ | `extensions.agents.x-k8s.io/v1beta1` | | |
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


#### SandboxClaim



SandboxClaim is the Schema for the sandbox Claim API.



_Appears in:_
- [SandboxClaimList](#sandboxclaimlist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `extensions.agents.x-k8s.io/v1beta1` | | |
| `kind` _string_ | `SandboxClaim` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[SandboxClaimSpec](#sandboxclaimspec)_ | spec defines the desired state of Sandbox |  |  |
| `status` _[SandboxClaimStatus](#sandboxclaimstatus)_ | status defines the observed state of Sandbox |  |  |


#### SandboxClaimList



SandboxList contains a list of Sandbox.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `extensions.agents.x-k8s.io/v1beta1` | | |
| `kind` _string_ | `SandboxClaimList` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[SandboxClaim](#sandboxclaim) array_ |  |  |  |


#### SandboxClaimSpec



SandboxClaimSpec defines the desired state of Sandbox.



_Appears in:_
- [SandboxClaim](#sandboxclaim)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `warmPoolRef` _[SandboxWarmPoolRef](#sandboxwarmpoolref)_ | warmPoolRef targets the specific pre-warmed infrastructure pool to check out from. |  |  |
| `lifecycle` _[Lifecycle](#lifecycle)_ | lifecycle defines when and how the SandboxClaim should be shut down. |  |  |
| `additionalPodMetadata` _[PodMetadata](#podmetadata)_ | additionalPodMetadata defines the labels and annotations to be propagated to the Sandbox Pod.<br />Label values are limited to 63 characters and must match Kubernetes label value patterns. |  |  |
| `env` _[EnvVar](#envvar) array_ | env is a list of environment variables to inject into the sandbox.<br />Please note adding this field means the Sandbox will always be cold-started from the<br />template of the warmpool. |  |  |
| `volumeClaimTemplates` _[PersistentVolumeClaimTemplate](#persistentvolumeclaimtemplate) array_ | volumeClaimTemplates is a list of persistent volume claims to be created for the sandbox.<br />Specifying this field forces a cold start because warm pool pods will not have these volumes. |  |  |


#### SandboxClaimStatus



SandboxClaimStatus defines the observed state of Sandbox.



_Appears in:_
- [SandboxClaim](#sandboxclaim)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `conditions` _[Condition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#condition-v1-meta) array_ | conditions represent the latest available observations of a Sandbox's current state. |  |  |
| `sandbox` _[SandboxStatus](#sandboxstatus)_ | sandbox defines the state of Sandbox |  |  |


#### SandboxStatus







_Appears in:_
- [SandboxClaimStatus](#sandboxclaimstatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `name` _string_ | name is the name of the Sandbox created from this claim |  |  |
| `podIPs` _string array_ | podIPs are the IP addresses of the underlying pod.<br />A pod may have multiple IPs in dual-stack clusters. |  |  |


#### SandboxTemplate



SandboxTemplate is the Schema for the sandbox template API.



_Appears in:_
- [SandboxTemplateList](#sandboxtemplatelist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `extensions.agents.x-k8s.io/v1beta1` | | |
| `kind` _string_ | `SandboxTemplate` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[SandboxTemplateSpec](#sandboxtemplatespec)_ | spec defines the desired state of Sandbox |  |  |


#### SandboxTemplateList



SandboxTemplateList contains a list of Sandbox.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `extensions.agents.x-k8s.io/v1beta1` | | |
| `kind` _string_ | `SandboxTemplateList` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[SandboxTemplate](#sandboxtemplate) array_ |  |  |  |


#### SandboxTemplateRef



SandboxTemplateRef references a SandboxTemplate.



_Appears in:_
- [SandboxWarmPoolSpec](#sandboxwarmpoolspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `name` _string_ | name of the SandboxTemplate |  |  |


#### SandboxTemplateSpec



SandboxTemplateSpec defines the desired state of Sandbox.



_Appears in:_
- [SandboxTemplate](#sandboxtemplate)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `podTemplate` _[PodTemplate](#podtemplate)_ | podTemplate describes the pod that will be created in the sandbox.<br />Note: When provisioned via a SandboxTemplate (such as by a SandboxClaim or SandboxWarmPool),<br />if AutomountServiceAccountToken is not specified in the PodSpec, the controller defaults it<br />to false to ensure a secure-by-default environment. |  |  |
| `volumeClaimTemplates` _[PersistentVolumeClaimTemplate](#persistentvolumeclaimtemplate) array_ | volumeClaimTemplates is a list of claims that the sandbox pod is allowed to reference.<br />When creating a sandbox, PVCs will be created from these templates.<br />Every claim in this list must have at least one matching access mode with a provisioner volume.<br />NOTE: This list is atomic. Updates to this field will replace the entire list rather than merging with existing entries. |  |  |
| `service` _boolean_ | service controls whether the controller should automatically create a<br />headless Service for the Sandbox workload.<br />When unset, the controller preserves existing Services for backward<br />compatibility but does not create new ones. Set to true to enable or false<br />to explicitly disable and remove the Service. |  |  |
| `networkPolicy` _[NetworkPolicySpec](#networkpolicyspec)_ | networkPolicy defines the network policy to be applied to the sandboxes<br />created from this template. A single shared NetworkPolicy is created per Template.<br />Behavior is dictated by the NetworkPolicyManagement field:<br />- If Management is "Unmanaged": This field is completely ignored.<br />- If Management is "Managed" (default) and this field is omitted (nil): The controller<br />  automatically applies a strict Secure Default policy:<br />    * Ingress: Allow traffic only from the Sandbox Router.<br />    * Egress: Allow Public Internet only. Blocks internal IPs (RFC1918), Metadata Server, etc.<br />- If Management is "Managed" and this field is provided: The controller applies your custom rules.<br />Update Behavior:<br />Because the NetworkPolicy is shared at the template level, any updates to these rules<br />will be applied to the single shared policy object. The underlying Kubernetes CNI will then<br />dynamically enforce the updated rules across all existing and future sandboxes<br />referencing this template.<br />NOTE: This is a restricted subset of the standard Kubernetes NetworkPolicySpec.<br />Fields like 'PodSelector' and 'PolicyTypes' are intentionally excluded because<br />they are managed by the controller to ensure strict isolation and default-deny posture.<br />WARNING: This policy enforces a strict "Default Deny" ingress posture.<br />If your Pod uses sidecars (e.g., Istio proxy, monitoring agents) that listen<br />on their own ports, the NetworkPolicy will BLOCK traffic to them by default.<br />You MUST explicitly allow traffic to these sidecar ports using 'Ingress',<br />otherwise the sidecars may fail health checks. |  |  |
| `networkPolicyManagement` _[NetworkPolicyManagement](#networkpolicymanagement)_ | networkPolicyManagement defines whether the controller manages the NetworkPolicy.<br />Valid values are "Managed" (default) or "Unmanaged". | Managed | Enum: [Managed Unmanaged] <br /> |
| `envVarsInjectionPolicy` _[EnvVarsInjectionPolicy](#envvarsinjectionpolicy)_ | envVarsInjectionPolicy allows a SandboxClaim to inject or override environment variables defined in the template.<br />If set to Disallowed, the SandboxClaim will be rejected if it specifies any environment variables. | Disallowed | Enum: [Allowed Overrides Disallowed] <br /> |
| `volumeClaimTemplatesPolicy` _[VolumeClaimTemplatesPolicy](#volumeclaimtemplatespolicy)_ | volumeClaimTemplatesPolicy allows a SandboxClaim to inject or override volume claim templates defined in the template.<br />If set to Disallowed, the SandboxClaim will be rejected if it specifies any volume claim templates. | Disallowed | Enum: [Disallowed Allowed Overrides] <br /> |


#### SandboxWarmPool



SandboxWarmPool is the Schema for the sandboxwarmpools API.



_Appears in:_
- [SandboxWarmPoolList](#sandboxwarmpoollist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `extensions.agents.x-k8s.io/v1beta1` | | |
| `kind` _string_ | `SandboxWarmPool` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[SandboxWarmPoolSpec](#sandboxwarmpoolspec)_ | spec defines the desired state of SandboxWarmPool |  |  |
| `status` _[SandboxWarmPoolStatus](#sandboxwarmpoolstatus)_ | status defines the observed state of SandboxWarmPool |  |  |


#### SandboxWarmPoolList



SandboxWarmPoolList contains a list of SandboxWarmPool.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `extensions.agents.x-k8s.io/v1beta1` | | |
| `kind` _string_ | `SandboxWarmPoolList` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[SandboxWarmPool](#sandboxwarmpool) array_ |  |  |  |


#### SandboxWarmPoolRef



SandboxWarmPoolRef references a SandboxWarmPool.



_Appears in:_
- [SandboxClaimSpec](#sandboxclaimspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `name` _string_ | name of the SandboxWarmPool |  |  |


#### SandboxWarmPoolSpec



SandboxWarmPoolSpec defines the desired state of SandboxWarmPool.



_Appears in:_
- [SandboxWarmPool](#sandboxwarmpool)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `replicas` _integer_ | replicas is the desired number of sandboxes in the pool.<br />This field is controlled by an HPA if specified. | 1 | Minimum: 0 <br /> |
| `sandboxTemplateRef` _[SandboxTemplateRef](#sandboxtemplateref)_ | sandboxTemplateRef - name of the SandboxTemplate to be used for creating a Sandbox<br />Warning: Any change to the json tag "sandboxTemplateRef" must be synchronized with the TemplateRefField constant. |  |  |
| `updateStrategy` _[SandboxWarmPoolUpdateStrategy](#sandboxwarmpoolupdatestrategy)_ | updateStrategy - strategy for updating the SandboxWarmPool pods based on sandboxTemplateRef name change or underlying template changes |  |  |


#### SandboxWarmPoolStatus



SandboxWarmPoolStatus defines the observed state of SandboxWarmPool.



_Appears in:_
- [SandboxWarmPool](#sandboxwarmpool)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `replicas` _integer_ | replicas is the total number of sandboxes in the pool. |  |  |
| `readyReplicas` _integer_ | readyReplicas is the total number of sandboxes in the pool that are in a ready state. |  |  |
| `selector` _string_ | selector is the label selector used to find the pods in the pool. |  |  |


#### SandboxWarmPoolUpdateStrategy



SandboxWarmPoolUpdateStrategy defines the update strategy for the SandboxWarmPool.



_Appears in:_
- [SandboxWarmPoolSpec](#sandboxwarmpoolspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `type` _[SandboxWarmPoolUpdateStrategyType](#sandboxwarmpoolupdatestrategytype)_ | type indicates the type of the SandboxWarmPoolUpdateStrategy.<br />Default is OnReplenish. | OnReplenish | Enum: [Recreate OnReplenish] <br /> |


#### SandboxWarmPoolUpdateStrategyType

_Underlying type:_ _string_

SandboxWarmPoolUpdateStrategyType is a string enumeration type that enumerates
all possible update strategies for the SandboxWarmPool controller.

_Validation:_
- Enum: [Recreate OnReplenish]

_Appears in:_
- [SandboxWarmPoolUpdateStrategy](#sandboxwarmpoolupdatestrategy)

| Field | Description |
| --- | --- |
| `Recreate` | RecreateSandboxWarmPoolUpdateStrategyType indicates that stale sandboxes are deleted immediately to ensure the pool only contains fresh sandboxes.<br />Note: This applies to changes in the template's SandboxBlueprint only. Changes to annotations, labels, or template-level policies do not trigger recreate.<br /> |
| `OnReplenish` | OnReplenishSandboxWarmPoolUpdateStrategyType indicates that stale sandboxes are only replaced when they are manually deleted or when these stale sandboxes are adopted by sandboxclaims and hence replaced by fresh sandboxes.<br /> |


#### ShutdownPolicy

_Underlying type:_ _string_

ShutdownPolicy describes the policy for shutting down the underlying Sandbox when the SandboxClaim expires.

_Validation:_
- Enum: [Delete DeleteForeground Retain]

_Appears in:_
- [Lifecycle](#lifecycle)

| Field | Description |
| --- | --- |
| `Delete` | ShutdownPolicyDelete deletes the SandboxClaim (and cascadingly the Sandbox) when expired.<br /> |
| `DeleteForeground` | ShutdownPolicyDeleteForeground deletes the SandboxClaim when expired using foreground<br />cascade deletion. The claim remains in the API (with a deletionTimestamp) until its<br />underlying Sandbox and Pod are fully terminated. This allows external systems to observe<br />shutdown progress by checking whether the claim still exists.<br /> |
| `Retain` | ShutdownPolicyRetain keeps the SandboxClaim when expired (Status will show Expired).<br />The underlying SandboxClaim resources (Sandbox, Pod, Service) are deleted to save resources,<br />but the SandboxClaim object itself remains.<br /> |


#### VolumeClaimTemplatesPolicy

_Underlying type:_ _string_

VolumeClaimTemplatesPolicy defines whether a SandboxClaim is allowed to inject or override volume claim templates.



_Appears in:_
- [SandboxTemplateSpec](#sandboxtemplatespec)

| Field | Description |
| --- | --- |
| `Disallowed` | VolumeClaimTemplatesPolicyDisallowed prevents a SandboxClaim from specifying any volume claim templates.<br /> |
| `Allowed` | VolumeClaimTemplatesPolicyAllowed allows a SandboxClaim to inject new volume claim templates, but not override existing ones.<br /> |
| `Overrides` | VolumeClaimTemplatesPolicyOverrides allows a SandboxClaim to inject new and override existing volume claim templates.<br /> |


