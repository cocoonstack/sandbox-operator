package scale

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"

	cocoonv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
)

// ListOptions is the subset of client list and watch parameters the aggregated store honors.
type ListOptions struct {
	Namespace     string
	LabelSelector string
	FieldSelector string
	WatchList     bool
}

// PoolKey identifies a warm pool by the claim axes Create derives from a Sandbox.
type PoolKey struct {
	Template string
	Net      string
	Size     string
}

// PoolCapacity is one node's warm capacity for a single pool.
type PoolCapacity = cocoonv1beta1.PoolCapacity

// Assignment is a successful claim: the sandbox, the node serving it and its address.
type Assignment struct {
	SandboxName string
	Node        string
	Address     string
	// Token is the sandbox's ownership credential from sandboxd, which the L3 Create path returns as an annotation.
	Token string
	// Deadline is the node-granted lease expiry, which overrides the requested TTL, or zero when unreported.
	Deadline time.Time
}

// SandboxStore is the L3 storage contract behind sandboxes.agents.x-k8s.io, with no per-sandbox etcd object.
type SandboxStore interface {
	// List assembles a SandboxList from the node inventories. A list pinned to one name in a namespace resolves like Get.
	List(ctx context.Context, opts ListOptions) (*sandboxv1beta1.SandboxList, error)
	// Get resolves one sandbox from the node inventories, or from its node before the node republishes.
	Get(ctx context.Context, namespace, name string) (*sandboxv1beta1.Sandbox, error)
	// Watch emits sandbox events as the node inventories change. A watch pinned to one name in a namespace starts from Get.
	Watch(ctx context.Context, opts ListOptions) (watch.Interface, error)
	// Claim delivers a warm microVM for namespace/name from a node with warm capacity for pool, and writes no etcd object.
	// With no warm node it returns an error that IsNoWarmCapacity reports, and a ttlSeconds of 0 asks for the node default.
	Claim(ctx context.Context, namespace, name string, pool PoolKey, ttlSeconds int) (Assignment, error)
	// Release destroys the claimed microVM on its owning node, for an owner-authorized delete only.
	Release(ctx context.Context, node, id string) error

	SandboxLifecycle
}

// SandboxLifecycle is the verb set of a claimed sandbox, each verb routed to its owning node.
// Pause and Snapshot write guest memory out, so their cost grows with its size.
type SandboxLifecycle interface {
	// Pause snapshots and stops the sandbox, and is idempotent on a paused one.
	Pause(ctx context.Context, node, id string) error
	// Resume restores a paused sandbox, and is idempotent on a running one.
	Resume(ctx context.Context, node, id string) error
	// Fork branches the sandbox into count claims named by their ids in namespace, and the parent keeps running.
	Fork(ctx context.Context, namespace, node, id string, count int, ttlSeconds int) ([]Assignment, error)
	// Snapshot captures the sandbox as a named checkpoint later claims can branch from, and the source keeps running.
	Snapshot(ctx context.Context, node, id, name string) (Snapshot, error)
	// Snapshots lists the checkpoints on a node, newest first.
	Snapshots(ctx context.Context, node string) ([]Snapshot, error)
	// DeleteSnapshot removes a checkpoint. A missing checkpoint is success.
	DeleteSnapshot(ctx context.Context, node, snapshotID string) error
	// Stats reports one sandbox's resource usage.
	Stats(ctx context.Context, node, id string) (SandboxStats, error)
	// Read reports the sandbox as its owning node holds it: token, paused state and lease deadline.
	Read(ctx context.Context, node, id string) (SandboxRecord, error)
	// Renew resets the lease to ttlSeconds from now, 0 for the node default, and returns the granted deadline.
	Renew(ctx context.Context, node, id string, ttlSeconds int) (time.Time, error)
}

// ClaimIDResolver resolves one sandbox by its node-local claim id without materializing the fleet.
// match decides which node-local id matches, and an empty namespace matches every namespace.
type ClaimIDResolver interface {
	GetByClaimID(ctx context.Context, namespace, id string, match func(claimID string) bool) (*sandboxv1beta1.Sandbox, error)
}

// Snapshot is a captured sandbox state that new sandboxes can branch from.
type Snapshot struct {
	ID        string
	Name      string
	SandboxID string
	Pool      PoolKey
	CreatedAt time.Time
	// Node is the node that holds the checkpoint, needed to branch from or delete it.
	Node string
}

// SandboxRecord is one live claim as its owning node holds it.
type SandboxRecord struct {
	Token    string
	Paused   bool
	Deadline time.Time
}

// SandboxStats is one sandbox's resource usage, where MemUsedBytes is valid only when MemUsedMeasured is true.
type SandboxStats struct {
	CPUCount        int
	MemTotalBytes   int64
	MemUsedBytes    int64
	MemUsedMeasured bool
	MeasuredAt      time.Time
}

// InventoryEntry is one live sandbox as summarized by its owning node.
type InventoryEntry = cocoonv1beta1.InventoryEntry

var _ runtime.Object = (*NodeInventory)(nil)

// NodeInventory is the single etcd object per node that summarizes its live sandboxes.
type NodeInventory = cocoonv1beta1.NodeInventory
