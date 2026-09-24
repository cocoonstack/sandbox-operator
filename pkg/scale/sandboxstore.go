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

// PoolKey identifies a warm pool by the claim axes the aggregated Create path
// derives from a Sandbox: the template (blueprint image), the network mode, and
// the size class. A node advertises matching warm capacity as a NodeInventory
// PoolCapacity; Create samples two such nodes and claims from the warmer.
type PoolKey struct {
	Template string
	Net      string
	Size     string
}

// PoolCapacity is one node's warm capacity for a single pool. It aliases the
// canonical API type so the scale contracts stay self-contained.
type PoolCapacity = cocoonv1beta1.PoolCapacity

// Assignment is the result of a successful claim: the warm sandbox whose
// ownership was transferred, the node serving it, and its connection address.
type Assignment struct {
	SandboxName string
	Node        string
	Address     string
	// Token is the per-sandbox ownership credential returned by sandboxd on the
	// claim. It authenticates agent/exec against the delivered VM; the L3 Create
	// path surfaces it as an annotation so a caller can exec into what it claimed.
	Token string
	// Deadline is the node-granted lease expiry — authoritative over the requested
	// TTL (node default when unasked, clamped to the node maximum); zero if unreported.
	Deadline time.Time
}

// SandboxStore is the L3 storage contract behind an aggregated apiserver serving
// sandboxes.agents.x-k8s.io. It holds NO per-sandbox etcd objects: List/Get/Watch
// scatter-gather live node inventories and Create/Delete are synchronous node-local
// claim/release, so etcd stores only intent (warm-pool desired replicas plus one
// O(nodes) NodeInventory per node). This is the metrics.k8s.io pattern applied to
// sandboxes, so object count drops from O(sandboxes) to O(pools+nodes) while
// kubectl/RBAC/watch keep working.
type SandboxStore interface {
	// List assembles a SandboxList by fanning out to node inventories.
	List(ctx context.Context, opts ListOptions) (*sandboxv1beta1.SandboxList, error)
	// Get resolves one sandbox from the cache-fed node inventories, or from its
	// node when the node has not republished.
	Get(ctx context.Context, namespace, name string) (*sandboxv1beta1.Sandbox, error)
	// Watch emits sandbox events as the node inventories change.
	Watch(ctx context.Context, opts ListOptions) (watch.Interface, error)
	// Claim delivers a warm microVM for namespace/name from a node advertising warm
	// capacity for pool, returning the node-local assignment (claim id, node,
	// connection address). ttlSeconds sets the claim's lease; 0 means the node
	// default. It writes NO per-sandbox etcd object: the claim is a synchronous
	// node-local ownership transfer via the owning node's sandboxd.
	// When no node has a warm microVM for the pool it returns an error for which
	// IsNoWarmCapacity is true, so the caller can surface a retryable 503.
	Claim(ctx context.Context, namespace, name string, pool PoolKey, ttlSeconds int) (Assignment, error)
	// Release destroys the claimed microVM on its owning node. It is
	// owner-authorized teardown only (the Sandbox resource itself being deleted);
	// it never destroys a VM on pod state alone. The node's sandboxd address is
	// resolved from its NodeInventory.
	Release(ctx context.Context, node, id string) error

	// SandboxLifecycle is the post-claim verb set. Every verb routes to the
	// owning node and stores nothing: the control plane stays O(pools+nodes)
	// however many sandboxes are paused, forked, or checkpointed.
	SandboxLifecycle
}

// SandboxLifecycle is the verb set a claimed sandbox supports after delivery.
// It is separate from SandboxStore's placement verbs because these all address
// an existing sandbox on a known node — there is no pool selection involved,
// only routing to the owner.
//
// Latency is not uniform across these verbs and callers should not assume it
// is: Resume takes cocoon's mmap restore fast path (~55 ms) and Fork's children
// clone at 28–75 ms each, but Pause and Snapshot write the guest's memory out
// and therefore cost time proportional to its size.
type SandboxLifecycle interface {
	// Pause hibernates the sandbox: its state is snapshotted and the VM stops,
	// freeing the node's memory. Idempotent on an already-paused sandbox.
	Pause(ctx context.Context, node, id string) error
	// Resume restores a paused sandbox and leaves it running. Idempotent on a
	// running one.
	Resume(ctx context.Context, node, id string) error
	// Fork branches the sandbox into count children, each a fresh claim with
	// its own id and lease, recorded in namespace under that id so they are
	// addressable by name. The parent is checkpointed in place and keeps running.
	Fork(ctx context.Context, namespace, node, id string, count int, ttlSeconds int) ([]Assignment, error)
	// Snapshot captures the sandbox's state as a named checkpoint that later
	// claims can branch from. The source keeps running.
	Snapshot(ctx context.Context, node, id, name string) (Snapshot, error)
	// Snapshots lists the checkpoints on a node, newest first.
	Snapshots(ctx context.Context, node string) ([]Snapshot, error)
	// DeleteSnapshot removes a checkpoint. A missing checkpoint is success.
	DeleteSnapshot(ctx context.Context, node, snapshotID string) error
	// Stats reports one sandbox's resource usage.
	Stats(ctx context.Context, node, id string) (SandboxStats, error)
	// Read reports the sandbox as its owning node holds it: token, paused state and lease deadline.
	Read(ctx context.Context, node, id string) (SandboxRecord, error)
	// Renew resets the sandbox's lease to ttlSeconds from now and reports the
	// deadline the node granted; 0 asks for the node default.
	Renew(ctx context.Context, node, id string, ttlSeconds int) (time.Time, error)
}

// ClaimIDResolver is the store fast path that resolves one sandbox by its
// node-local claim id without materializing the fleet. id is the caller's
// spelling of the id and keys the owning-node index; match decides which
// node-local id it accepts. An empty namespace matches every namespace.
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
	// Node is the node holding the checkpoint. Checkpoints are node-local, so
	// a caller needs it to branch from or delete this snapshot later.
	Node string
}

// SandboxRecord is one live claim as its owning node holds it.
type SandboxRecord struct {
	Token    string
	Paused   bool
	Deadline time.Time
}

// SandboxStats is one sandbox's resource usage. CPUCount and MemTotalBytes are
// the tier the VM was booted with and are authoritative; MemUsedBytes is only
// meaningful when MemUsedMeasured is true (a paused sandbox has no process to
// measure), so callers must not read zero as "idle".
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

// NodeInventory is the single O(nodes) etcd object per node: the durable summary
// of that node's live sandboxes, server-side-applied on a slow cadence. The
// per-sandbox truth lives in the node (the L0 node-scoped cache), not etcd; a
// lost NodeInventory is rebuilt from the node's own live state on next publish.
// The canonical type (and its CRD) lives in the sandbox.cocoonstack.io group;
// these aliases keep the scale contracts self-contained for callers.
type NodeInventory = cocoonv1beta1.NodeInventory
