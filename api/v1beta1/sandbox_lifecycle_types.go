package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
)

// +kubebuilder:object:root=true

// SandboxPauseOptions is the body of POST sandboxes/{name}/pause. Pausing
// snapshots the guest's memory and stops its VM, so it costs time proportional
// to that memory — unlike resume, which takes the mmap restore fast path.
type SandboxPauseOptions struct {
	metav1.TypeMeta `json:",inline"`
}

// +kubebuilder:object:root=true

// SandboxResumeOptions is the body of POST sandboxes/{name}/resume. Resuming a
// paused sandbox restores it through cocoon's mmap fast path and is idempotent
// on one that is already running.
type SandboxResumeOptions struct {
	metav1.TypeMeta `json:",inline"`
}

// +kubebuilder:object:root=true

// SandboxForkOptions is the body of POST sandboxes/{name}/fork. The source is
// checkpointed in place and keeps running; each child is a brand-new sandbox
// with its own id and lease, not a replica of the source's identity.
type SandboxForkOptions struct {
	metav1.TypeMeta `json:",inline"`

	// count is how many children to branch. Defaults to 1, and is bounded by
	// the owning node's configured fork limit.
	// +optional
	Count int32 `json:"count,omitempty"`

	// ttlSeconds is each child's lease. Children never inherit the parent's
	// remaining lease — a lease is a per-sandbox resource bound. Zero takes the
	// node's default.
	// +optional
	TTLSeconds int32 `json:"ttlSeconds,omitempty"`
}

// +kubebuilder:object:root=true

// SandboxForkResult is the reply to a fork: one entry per child, in request
// order.
type SandboxForkResult struct {
	metav1.TypeMeta `json:",inline"`

	// children are the branched sandboxes.
	Children []ForkedSandbox `json:"children"`
}

// ForkedSandbox identifies one child of a fork.
type ForkedSandbox struct {
	// sandboxID is the child's node-local claim id.
	SandboxID string `json:"sandboxID"`
	// nodeName is the node that owns the child. A fork is node-local, so every
	// child lands on the source's node.
	NodeName string `json:"nodeName"`
	// address is the child's connection address, when the node published one.
	// +optional
	Address string `json:"address,omitempty"`
}

// +kubebuilder:object:root=true

// SandboxSnapshotOptions is the body of POST sandboxes/{name}/snapshot. The
// source keeps running; the checkpoint is an immutable state later sandboxes
// can branch from.
type SandboxSnapshotOptions struct {
	metav1.TypeMeta `json:",inline"`

	// name labels the checkpoint. Optional; the node assigns an id regardless.
	// +optional
	Name string `json:"name,omitempty"`
}

// +kubebuilder:object:root=true

// SandboxSnapshotResult is the reply to a snapshot.
type SandboxSnapshotResult struct {
	metav1.TypeMeta `json:",inline"`

	// snapshotID is the checkpoint's node-local id.
	SnapshotID string `json:"snapshotID"`
	// name echoes the requested label, when one was given.
	// +optional
	Name string `json:"name,omitempty"`
	// nodeName is the node holding the checkpoint. Checkpoints are node-local,
	// so branching from or deleting one requires knowing its node.
	NodeName string `json:"nodeName"`
	// creationTimestamp is when the node captured the checkpoint.
	// +optional
	CreationTimestamp metav1.Time `json:"creationTimestamp,omitempty"`
}

// AddLifecycleToScheme registers the action bodies above in upstream's Sandbox
// GroupVersion, where the subresources that carry them are served.
func AddLifecycleToScheme(s *runtime.Scheme) error {
	s.AddKnownTypes(sandboxv1beta1.GroupVersion,
		&SandboxPauseOptions{},
		&SandboxResumeOptions{},
		&SandboxForkOptions{},
		&SandboxForkResult{},
		&SandboxSnapshotOptions{},
		&SandboxSnapshotResult{},
	)
	return nil
}
