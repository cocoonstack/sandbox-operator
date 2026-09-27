// Package scale holds the L3 contract of docs/scaling-design.md: the
// aggregated-apiserver storage that serves sandboxes.agents.x-k8s.io by
// scatter-gathering live node inventories, so etcd stores only intent
// (warm-pool desired replicas plus one O(nodes) NodeInventory object per
// node), the metrics.k8s.io pattern. L0 (API hygiene) belongs to the node
// providers, L1 (claim ownership transfer) to upstream's controller, and L2
// (a node-local claim gateway) is designed there and not built.
//
// The scatter-gather store lives in sandboxstore_impl.go; its inventory
// sources are pkg/scale/kubeinventory (NodeInventory objects) and
// pkg/scale/meshinventory (a sandboxd mesh). cmd/sandbox-apiserver serves the
// store via pkg/scale/apiserver. The NodeInventory publisher is vk-sandbox's,
// built on InventoryApplier, NodeLiveSource and EntryFromSummary.
package scale
