package apiserver

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"

	cocoonv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

// The subresources take the pods/eviction shape: a synchronous verb with nothing to GET.
var (
	_ rest.Storage                  = &lifecycleREST{}
	_ rest.Scoper                   = &lifecycleREST{}
	_ rest.NamedCreater             = &lifecycleREST{}
	_ rest.GroupVersionKindProvider = &lifecycleREST{}
)

// lifecycleREST is the shared plumbing of the action subresources: resolve the
// named sandbox through the store, then run one verb against its owning node.
type lifecycleREST struct {
	store      scale.SandboxStore
	newOptions func() runtime.Object
	verb       func(ctx context.Context, store scale.SandboxStore, sb *sandboxv1beta1.Sandbox, opts runtime.Object) (runtime.Object, error)
}

func (r *lifecycleREST) New() runtime.Object { return r.newOptions() }

func (r *lifecycleREST) Destroy() {}

func (r *lifecycleREST) NamespaceScoped() bool { return true }

func (r *lifecycleREST) GroupVersionKind(schema.GroupVersion) schema.GroupVersionKind {
	gvks, _, err := Scheme.ObjectKinds(r.newOptions())
	if err != nil || len(gvks) == 0 {
		return schema.GroupVersionKind{}
	}
	return gvks[0]
}

func (r *lifecycleREST) Create(ctx context.Context, name string, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	if options != nil && len(options.DryRun) > 0 {
		return nil, apierrors.NewBadRequest(dryRunUnsupported)
	}
	if createValidation != nil {
		if err := createValidation(ctx, obj); err != nil {
			return nil, err
		}
	}
	namespace := genericapirequest.NamespaceValue(ctx)
	sb, err := r.store.Get(ctx, namespace, name)
	if err != nil {
		// NotFound propagates unchanged so IsNotFound stays true for the caller.
		return nil, err
	}
	if sb.Status.NodeName == "" {
		return nil, apierrors.NewInternalError(fmt.Errorf(
			"sandbox %s/%s has no owning node; cannot run a lifecycle verb against it", namespace, name))
	}
	if claimID(sb) == "" {
		// releasing or pausing by k8s name would target the wrong claim
		return nil, apierrors.NewInternalError(fmt.Errorf(
			"sandbox %s/%s carries no %s (node-local claim id); refusing to act by name",
			namespace, name, ClaimIDAnnotation))
	}
	return r.verb(ctx, r.store, sb, obj)
}

// NewSandboxPauseREST serves sandboxes/pause.
func NewSandboxPauseREST(store scale.SandboxStore) rest.Storage {
	return &lifecycleREST{
		store:      store,
		newOptions: func() runtime.Object { return &cocoonv1beta1.SandboxPauseOptions{} },
		verb: func(ctx context.Context, st scale.SandboxStore, sb *sandboxv1beta1.Sandbox, _ runtime.Object) (runtime.Object, error) {
			if err := st.Pause(ctx, sb.Status.NodeName, claimID(sb)); err != nil {
				return nil, verbError("pause", sb, err)
			}
			return &cocoonv1beta1.SandboxPauseOptions{}, nil
		},
	}
}

// NewSandboxResumeREST serves sandboxes/resume.
func NewSandboxResumeREST(store scale.SandboxStore) rest.Storage {
	return &lifecycleREST{
		store:      store,
		newOptions: func() runtime.Object { return &cocoonv1beta1.SandboxResumeOptions{} },
		verb: func(ctx context.Context, st scale.SandboxStore, sb *sandboxv1beta1.Sandbox, _ runtime.Object) (runtime.Object, error) {
			if err := st.Resume(ctx, sb.Status.NodeName, claimID(sb)); err != nil {
				return nil, verbError("resume", sb, err)
			}
			return &cocoonv1beta1.SandboxResumeOptions{}, nil
		},
	}
}

// NewSandboxForkREST serves sandboxes/fork.
func NewSandboxForkREST(store scale.SandboxStore) rest.Storage {
	return &lifecycleREST{
		store:      store,
		newOptions: func() runtime.Object { return &cocoonv1beta1.SandboxForkOptions{} },
		verb: func(ctx context.Context, st scale.SandboxStore, sb *sandboxv1beta1.Sandbox, obj runtime.Object) (runtime.Object, error) {
			opts, ok := obj.(*cocoonv1beta1.SandboxForkOptions)
			if !ok {
				return nil, apierrors.NewBadRequest(fmt.Sprintf("expected SandboxForkOptions, got %T", obj))
			}
			count := int(opts.Count)
			count = cmp.Or(count, 1)
			if count < 0 {
				return nil, apierrors.NewBadRequest(fmt.Sprintf("count must be >= 1, got %d", count))
			}
			children, err := st.Fork(ctx, sb.Status.NodeName, claimID(sb), count, int(opts.TTLSeconds))
			if err != nil {
				return nil, verbError("fork", sb, err)
			}
			out := &cocoonv1beta1.SandboxForkResult{Children: make([]cocoonv1beta1.ForkedSandbox, 0, len(children))}
			for _, c := range children {
				out.Children = append(out.Children, cocoonv1beta1.ForkedSandbox{
					SandboxID: c.SandboxName,
					NodeName:  c.Node,
					Address:   c.Address,
				})
			}
			return out, nil
		},
	}
}

// NewSandboxSnapshotREST serves sandboxes/snapshot.
func NewSandboxSnapshotREST(store scale.SandboxStore) rest.Storage {
	return &lifecycleREST{
		store:      store,
		newOptions: func() runtime.Object { return &cocoonv1beta1.SandboxSnapshotOptions{} },
		verb: func(ctx context.Context, st scale.SandboxStore, sb *sandboxv1beta1.Sandbox, obj runtime.Object) (runtime.Object, error) {
			opts, ok := obj.(*cocoonv1beta1.SandboxSnapshotOptions)
			if !ok {
				return nil, apierrors.NewBadRequest(fmt.Sprintf("expected SandboxSnapshotOptions, got %T", obj))
			}
			name, err := scale.CheckpointName(sb.Namespace, opts.Name)
			if err != nil {
				return nil, err
			}
			snap, err := st.Snapshot(ctx, sb.Status.NodeName, claimID(sb), name)
			if err != nil {
				return nil, verbError("snapshot", sb, err)
			}
			return &cocoonv1beta1.SandboxSnapshotResult{
				SnapshotID:        snap.ID,
				Name:              opts.Name,
				NodeName:          snap.Node,
				CreationTimestamp: metav1.NewTime(snap.CreatedAt),
			}, nil
		},
	}
}

func claimID(sb *sandboxv1beta1.Sandbox) string { return sb.Annotations[ClaimIDAnnotation] }

func verbError(verb string, sb *sandboxv1beta1.Sandbox, err error) error {
	if se, ok := errors.AsType[*apierrors.StatusError](err); ok && se.ErrStatus.Code < http.StatusInternalServerError {
		return se
	}
	return apierrors.NewInternalError(fmt.Errorf("%s sandbox %s/%s: %w", verb, sb.Namespace, sb.Name, err))
}
