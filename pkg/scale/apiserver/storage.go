package apiserver

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/watch"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

// Aggregated-Sandbox annotations. Create stamps the claim id + delivered address
// onto the returned object; the synthesized read path stamps the same claim id
// from node inventory, and Delete reads it to release exactly the node-local
// microVM that was handed over.
const (
	// ClaimIDAnnotation carries the sandboxd claim id of a claimed sandbox.
	ClaimIDAnnotation = scale.ClaimIDAnnotation
	// DeadlineAnnotation carries the node-granted lease expiry (RFC3339).
	DeadlineAnnotation = scale.DeadlineAnnotation
	// AddressAnnotation carries the delivered sandbox connection address.
	AddressAnnotation = "sandbox.cocoonstack.io/address"
	// TokenAnnotation carries the per-sandbox ownership token so a caller can
	// exec/agent into the sandbox it claimed via the L3 apiserver.
	TokenAnnotation = scale.TokenAnnotation
	// NetAnnotation selects the pool network mode on Create (default "none").
	NetAnnotation = scale.NetAnnotation
	// TTLSecondsAnnotation bounds the claim lease in whole seconds on Create for
	// clients that cannot set spec.shutdownTime (0 = the owning node's default).
	TTLSecondsAnnotation = "sandbox.cocoonstack.io/ttl-seconds"

	dryRunUnsupported = "dry-run is not supported for sandbox mutations"
)

var (
	_ rest.Storage              = (*sandboxREST)(nil)
	_ rest.Scoper               = (*sandboxREST)(nil)
	_ rest.Lister               = (*sandboxREST)(nil)
	_ rest.Getter               = (*sandboxREST)(nil)
	_ rest.Watcher              = (*sandboxREST)(nil)
	_ rest.Creater              = (*sandboxREST)(nil) //nolint:misspell // rest.Creater is the upstream Kubernetes interface name.
	_ rest.GracefulDeleter      = (*sandboxREST)(nil)
	_ rest.TableConvertor       = (*sandboxREST)(nil)
	_ rest.SingularNameProvider = (*sandboxREST)(nil)
)

// sandboxREST backs sandboxes.agents.x-k8s.io via scale.SandboxStore (scatter-gather), not the etcd generic registry: List/Get/Watch synthesize from live nodes, Create/Delete are synchronous node-local claim/release.
type sandboxREST struct {
	store          scale.SandboxStore
	tableConvertor rest.TableConvertor
}

// NewSandboxREST builds the sandboxes REST storage over store.
func NewSandboxREST(store scale.SandboxStore) rest.Storage {
	return &sandboxREST{
		store:          store,
		tableConvertor: rest.NewDefaultTableConvertor(sandboxv1beta1.Resource("sandboxes")),
	}
}

func (r *sandboxREST) New() runtime.Object { return &sandboxv1beta1.Sandbox{} }

func (r *sandboxREST) NewList() runtime.Object { return &sandboxv1beta1.SandboxList{} }

func (r *sandboxREST) Destroy() {}

func (r *sandboxREST) NamespaceScoped() bool { return true }

func (r *sandboxREST) GetSingularName() string { return "sandbox" }

func (r *sandboxREST) List(ctx context.Context, options *metainternalversion.ListOptions) (runtime.Object, error) {
	return r.store.List(ctx, toScaleListOptions(ctx, options))
}

func (r *sandboxREST) Get(ctx context.Context, name string, _ *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, genericapirequest.NamespaceValue(ctx), name)
}

func (r *sandboxREST) Watch(ctx context.Context, options *metainternalversion.ListOptions) (watch.Interface, error) {
	return r.store.Watch(ctx, toScaleListOptions(ctx, options))
}

func (r *sandboxREST) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	if options != nil && len(options.DryRun) > 0 {
		return nil, apierrors.NewBadRequest(dryRunUnsupported)
	}
	sb, ok := obj.(*sandboxv1beta1.Sandbox)
	if !ok {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("expected a Sandbox object, got %T", obj))
	}
	if createValidation != nil {
		if err := createValidation(ctx, obj); err != nil {
			return nil, err
		}
	}

	namespace := cmp.Or(genericapirequest.NamespaceValue(ctx), sb.Namespace)
	name := sb.Name
	if name == "" && sb.GenerateName != "" {
		name = names.SimpleNameGenerator.GenerateName(sb.GenerateName)
	}
	if name == "" {
		return nil, apierrors.NewBadRequest("Sandbox has neither metadata.name nor metadata.generateName")
	}

	pool := poolKeyForSandbox(sb)
	ttlSeconds, err := ttlSecondsForSandbox(sb, time.Now())
	if err != nil {
		return nil, apierrors.NewBadRequest(err.Error())
	}
	assignment, err := r.store.Claim(ctx, namespace, name, pool, ttlSeconds)
	if err != nil {
		if scale.IsNoWarmCapacity(err) {
			return nil, apierrors.NewServiceUnavailable(fmt.Sprintf(
				"no warm sandbox available for pool (template=%q net=%q size=%q); retry as warm capacity refills",
				pool.Template, pool.Net, pool.Size))
		}
		return nil, apierrors.NewInternalError(fmt.Errorf("claim sandbox %s/%s: %w", namespace, name, err))
	}
	return synthesizeClaimedSandbox(namespace, name, sb, assignment), nil
}

func (r *sandboxREST) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	if options != nil && len(options.DryRun) > 0 {
		return nil, false, apierrors.NewBadRequest(dryRunUnsupported)
	}
	// Owner-authorized teardown of the Sandbox resource only; pod state never reaches here.
	namespace := genericapirequest.NamespaceValue(ctx)
	sb, err := r.store.Get(ctx, namespace, name)
	if err != nil {
		// NotFound propagates unchanged so IsNotFound stays true for the caller.
		return nil, false, err
	}
	if deleteValidation != nil {
		if err := deleteValidation(ctx, sb); err != nil {
			return nil, false, err
		}
	}

	node := sb.Status.NodeName
	if node == "" {
		// Reporting success here would leak the microVM to its TTL.
		return nil, false, apierrors.NewInternalError(fmt.Errorf(
			"cannot delete sandbox %s/%s: inventory entry names no owning node; refusing to report it released",
			namespace, name))
	}
	claimID := sb.Annotations[ClaimIDAnnotation]
	if claimID == "" {
		return nil, false, apierrors.NewInternalError(fmt.Errorf(
			"cannot delete sandbox %s/%s: node %q inventory carries no %s (sandboxd claim id); refusing to release by name",
			namespace, name, node, ClaimIDAnnotation))
	}
	if err := r.store.Release(ctx, node, claimID); err != nil {
		return nil, false, apierrors.NewInternalError(
			fmt.Errorf("release sandbox %s/%s (node %q id %q): %w", namespace, name, node, claimID, err))
	}
	return sb, true, nil
}

func (r *sandboxREST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return r.tableConvertor.ConvertToTable(ctx, object, tableOptions)
}

// toScaleListOptions lifts the request namespace and selectors into the store's
// ListOptions. The namespace comes from the request path (empty = all namespaces).
func toScaleListOptions(ctx context.Context, options *metainternalversion.ListOptions) scale.ListOptions {
	o := scale.ListOptions{Namespace: genericapirequest.NamespaceValue(ctx)}
	if options != nil {
		if options.LabelSelector != nil {
			o.LabelSelector = options.LabelSelector.String()
		}
		if options.FieldSelector != nil {
			o.FieldSelector = options.FieldSelector.String()
		}
	}
	return o
}

// poolKeyForSandbox derives the pool key via scale.PoolKeyFor/NetForAnnotations; the resolved lane must appear in a template's pod-template annotation for the warm-pool driver to provision it.
func poolKeyForSandbox(sb *sandboxv1beta1.Sandbox) scale.PoolKey {
	net := scale.NetForAnnotations(sb.Annotations, sb.Spec.PodTemplate.ObjectMeta.Annotations)
	return scale.PoolKeyFor(sb.Spec.PodTemplate.Spec.Containers, net)
}

// ttlSecondsForSandbox derives the claim lease: spec.shutdownTime wins, the
// ttl-seconds annotation is the fallback, 0 asks for the node default.
func ttlSecondsForSandbox(sb *sandboxv1beta1.Sandbox, now time.Time) (int, error) {
	if t := sb.Spec.ShutdownTime; t != nil {
		left := t.Sub(now)
		if left <= 0 {
			return 0, fmt.Errorf("spec.shutdownTime %s is not in the future", t.Format(time.RFC3339))
		}
		return int(math.Ceil(left.Seconds())), nil
	}
	raw := sb.Annotations[TTLSecondsAnnotation]
	if raw == "" {
		return 0, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 0 {
		return 0, fmt.Errorf("invalid %s=%q: want a non-negative integer of seconds", TTLSecondsAnnotation, raw)
	}
	return v, nil
}

// synthesizeClaimedSandbox builds Create's response Sandbox (never persisted): the submitted spec echoed back with a fresh identity and the claim's annotations/status.
func synthesizeClaimedSandbox(namespace, name string, in *sandboxv1beta1.Sandbox, a scale.Assignment) *sandboxv1beta1.Sandbox {
	out := in.DeepCopy()
	out.Namespace = namespace
	out.Name = name
	out.GenerateName = ""
	out.UID = uuid.NewUUID()
	out.CreationTimestamp = metav1.Now()
	out.ResourceVersion = ""
	if out.Annotations == nil {
		out.Annotations = map[string]string{}
	}
	out.Annotations[ClaimIDAnnotation] = a.SandboxName
	if a.Address != "" {
		out.Annotations[AddressAnnotation] = a.Address
	}
	if a.Token != "" {
		out.Annotations[TokenAnnotation] = a.Token
	}
	if !a.Deadline.IsZero() {
		out.Annotations[DeadlineAnnotation] = a.Deadline.UTC().Format(time.RFC3339)
	}
	out.Status = sandboxv1beta1.SandboxStatus{
		NodeName: a.Node,
		PodIPs:   scale.AddressIPs(a.Address),
	}
	apimeta.SetStatusCondition(&out.Status.Conditions, metav1.Condition{
		Type:    string(sandboxv1beta1.SandboxConditionReady),
		Status:  metav1.ConditionTrue,
		Reason:  sandboxv1beta1.SandboxReasonDependenciesReady,
		Message: fmt.Sprintf("warm sandbox %q delivered by node %q", a.SandboxName, a.Node),
	})
	return out
}
