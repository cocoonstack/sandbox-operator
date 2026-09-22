package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// PoolCapacity is one sandboxd warm pool's capacity as reported by its owning
// node's GET /v1/info: the pool key plus its warm/target counts. The aggregated
// apiserver reads it to pick a node that already holds a warm microVM for a
// requested (template, net, size).
type PoolCapacity struct {
	// template is the pool's base image (the sandbox template).
	Template string `json:"template"`
	// net is the pool's network shape (e.g. "none", "egress").
	// +optional
	Net string `json:"net,omitempty"`
	// size is the pool's VM size class (e.g. "small").
	// +optional
	Size string `json:"size,omitempty"`
	// warm is the number of ready-to-claim warm microVMs currently in the pool.
	Warm int `json:"warm"`
	// target is the pool's desired warm depth.
	Target int `json:"target"`
}

// InventoryEntry is one live sandbox as summarized by its owning node.
type InventoryEntry struct {
	// name is the sandbox "<namespace>/<name>"; an unqualified name means the
	// default namespace.
	Name string `json:"name"`
	// id is the owning node's sandboxd claim id ("sb_..."), the handle its
	// sandbox-release verb needs. The aggregated apiserver surfaces it on the
	// synthesized Sandbox so Delete can release exactly this node-local microVM
	// (releasing by k8s name would target the wrong claim). Empty until the
	// node publishes it.
	// +optional
	ID string `json:"id,omitempty"`
	// phase is the node-reported sandbox phase (e.g. Running).
	Phase string `json:"phase"`
	// template is the pool template (base image) the sandbox was claimed from.
	// It is the only place the aggregated read path can recover it: no
	// per-sandbox object holds the pod spec.
	// +optional
	Template string `json:"template,omitempty"`
	// claimRef is the "<namespace>/<name>" of the SandboxClaim the sandbox is
	// bound to, if any.
	// +optional
	ClaimRef string `json:"claimRef,omitempty"`
	// addr is the sandbox "host:port" address, if published.
	// +optional
	Address string `json:"addr,omitempty"`
	// deadline is the node-granted lease expiry, if published.
	// +optional
	Deadline *metav1.Time `json:"deadline,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="Node",type="string",JSONPath=".node"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// NodeInventory is the single O(nodes) etcd object per node: the durable summary
// of that node's live sandboxes, server-side-applied on a slow cadence and
// scatter-gathered by the aggregated sandbox-apiserver. It is deliberately
// spec-less (pure reported summary, no desired state) and cluster-scoped with
// metadata.name equal to the node name. It lives in this CRD extensions group —
// NOT in the aggregated agents.x-k8s.io group, whose entire v1beta1 the
// APIService hands to the aggregated server (which serves only `sandboxes`).
type NodeInventory struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// node is the owning node name; it matches metadata.name.
	Node string `json:"node"`
	// entries summarizes the node's live sandboxes.
	// +optional
	Entries []InventoryEntry `json:"entries,omitempty"`
	// address is the node's sandboxd advertise address ("host:port"); the
	// aggregated apiserver routes a claim to this node's sandboxd through it.
	// +optional
	Address string `json:"address,omitempty"`
	// pools is the node's per-pool warm capacity, used to pick a node that
	// already holds a warm microVM for a requested (template, net, size).
	// +optional
	Pools []PoolCapacity `json:"pools,omitempty"`
}

// +kubebuilder:object:root=true

// NodeInventoryList contains a list of NodeInventory.
type NodeInventoryList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NodeInventory `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, &NodeInventory{}, &NodeInventoryList{})
		return nil
	})
}
