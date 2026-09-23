// Package scale holds the L2 and L3 contracts of docs/scaling-design.md. L0
// (API hygiene) belongs to the node providers and L1 (claim ownership
// transfer) to upstream's controller.
//
//   - ClaimGateway (L2): the node-local claim fast path over sandboxd. A claim
//     is served by the node that already holds a warm microVM; the SandboxClaim
//     object is reconciled to Bound asynchronously afterward (kubelet static-Pod
//     semantics: the node acts first, the apiserver records after).
//
//   - SandboxStore + NodeInventory (L3): the aggregated-apiserver storage
//     contract. sandboxes.agents.x-k8s.io is served by scatter-gathering live
//     node inventories; etcd stores only intent (warm-pool desired replicas plus
//     one O(nodes) NodeInventory object per node), the metrics.k8s.io pattern.
//
// Both are implemented here: the sandboxd-backed ClaimGateway and its orphan
// reconciler (claimgateway_impl.go), and the scatter-gather store with its
// cache-fed inventory source (sandboxstore_impl.go), served by
// cmd/sandbox-apiserver via pkg/scale/apiserver. The NodeInventory publisher
// is vk-sandbox's, built on InventoryApplier, NodeLiveSource and EntryFromSummary.
package scale
