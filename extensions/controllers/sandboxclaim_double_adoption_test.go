package controllers

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	extensionsv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/internal/hash"
	asmetrics "github.com/cocoonstack/sandbox-operator/internal/metrics"
	"github.com/cocoonstack/sandbox-operator/internal/queue"
)

func TestARequeuedCandidateIsAdoptedByOneClaimOnly(t *testing.T) {
	scheme := newScheme(t)
	claimA, claimB := doubleAdoptionClaim("claim-a"), doubleAdoptionClaim("claim-b")
	q := queue.NewSimpleSandboxQueue()
	qName := queue.GetNamespacedWarmPoolName("default", "pool")
	q.Add(qName, queue.SandboxKey{Namespace: "default", Name: "pool-abcde"})

	base := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(doubleAdoptionTemplate(), doubleAdoptionPool(), claimA, claimB, doubleAdoptionWarmSandbox()).
		WithStatusSubresource(claimA, claimB, &sandboxv1beta1.Sandbox{}).Build()

	var (
		interleaved atomic.Bool
		rB          *SandboxClaimReconciler
		handovers   atomic.Int64
	)
	c := interceptor.NewClient(base, interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
			sb, isSandbox := obj.(*sandboxv1beta1.Sandbox)
			if isSandbox && sb.Name == "pool-abcde" && interleaved.CompareAndSwap(false, true) {
				settled := doubleAdoptionWarmSandbox()
				settled.Status.NodeName = "node-1"
				(&sandboxEventHandler{sandboxQueue: q}).Update(ctx, event.UpdateEvent{ObjectOld: doubleAdoptionWarmSandbox(), ObjectNew: settled}, nil)
				if _, err := rB.Reconcile(context.WithoutCancel(ctx), ctrl.Request{Namespace: "default", Name: "claim-b"}); err != nil {
					t.Errorf("claim-b reconcile inside claim-a's hand-over: %v", err)
				}
			}
			err := cl.Patch(ctx, obj, p, opts...)
			if isSandbox && sb.Name == "pool-abcde" && err == nil {
				handovers.Add(1)
			}
			return err
		},
	})
	rA := &SandboxClaimReconciler{Client: c, Scheme: scheme, WarmSandboxQueue: q, Tracer: asmetrics.NewNoOp()}
	rB = &SandboxClaimReconciler{Client: c, Scheme: scheme, WarmSandboxQueue: q, Tracer: asmetrics.NewNoOp()}

	if _, err := rA.Reconcile(t.Context(), ctrl.Request{Namespace: "default", Name: "claim-a"}); err != nil {
		t.Fatalf("claim-a reconcile: %v", err)
	}

	if got := handovers.Load(); got != 1 {
		t.Fatalf("the warm sandbox changed hands %d times, want exactly 1: a stale-base transfer must be rejected", got)
	}
	if _, err := rA.Reconcile(t.Context(), ctrl.Request{Namespace: "default", Name: "claim-a"}); err != nil {
		t.Fatalf("claim-a second pass: %v", err)
	}
	boundA, boundB := boundSandbox(t, base, "claim-a"), boundSandbox(t, base, "claim-b")
	if boundA != "claim-a" || boundB != "pool-abcde" {
		t.Fatalf("claim-a bound to %q and claim-b to %q: the winner keeps the warm sandbox and the loser cold-starts", boundA, boundB)
	}
}

func TestAStaleCandidateReadFinishesTheWarmAdoptionOnALaterPass(t *testing.T) {
	scheme := newScheme(t)
	claim := doubleAdoptionClaim("claim-a")
	q := queue.NewSimpleSandboxQueue()
	q.Add(queue.GetNamespacedWarmPoolName("default", "pool"), queue.SandboxKey{Namespace: "default", Name: "pool-abcde"})
	base := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(doubleAdoptionTemplate(), doubleAdoptionPool(), claim, doubleAdoptionWarmSandbox()).
		WithStatusSubresource(claim, &sandboxv1beta1.Sandbox{}).Build()

	var lagging atomic.Bool
	lagging.Store(true)
	c := interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := cl.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if sb, ok := obj.(*sandboxv1beta1.Sandbox); ok && sb.Name == "pool-abcde" && lagging.Load() {
				sb.ResourceVersion = "1"
			}
			return nil
		},
	})
	r := &SandboxClaimReconciler{Client: c, Scheme: scheme, WarmSandboxQueue: q, Tracer: asmetrics.NewNoOp()}
	req := ctrl.Request{Namespace: "default", Name: "claim-a"}

	if _, err := r.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("reconcile against a lagging cache: %v", err)
	}
	if err := base.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: "claim-a"}, &sandboxv1beta1.Sandbox{}); !k8errors.IsNotFound(err) {
		t.Fatalf("the claim cold-started with a warm sandbox assigned to it: get sandbox claim-a = %v", err)
	}

	lagging.Store(false)
	for range 2 {
		if _, err := r.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("reconcile after the cache converged: %v", err)
		}
	}
	if got := boundSandbox(t, base, "claim-a"); got != "pool-abcde" {
		t.Fatalf("claim bound to %q, want the warm sandbox pool-abcde", got)
	}
}

func TestAConflictedAssignmentIsSettledLaterEvenWhenTheQueueRunsDry(t *testing.T) {
	scheme := newScheme(t)
	claim := doubleAdoptionClaim("claim-a")
	q := queue.NewSimpleSandboxQueue()
	q.Add(queue.GetNamespacedWarmPoolName("default", "pool"), queue.SandboxKey{Namespace: "default", Name: "pool-abcde"})
	base := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(doubleAdoptionTemplate(), doubleAdoptionPool(), claim, doubleAdoptionWarmSandbox()).
		WithStatusSubresource(claim, &sandboxv1beta1.Sandbox{}).Build()

	var conflicted atomic.Bool
	c := interceptor.NewClient(base, interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
			if sb, ok := obj.(*sandboxv1beta1.Sandbox); ok && sb.Name == "pool-abcde" && conflicted.CompareAndSwap(false, true) {
				return k8errors.NewConflict(sandboxv1beta1.Resource("sandboxes"), sb.Name, errors.New("another writer"))
			}
			return cl.Patch(ctx, obj, p, opts...)
		},
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := cl.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if sb, ok := obj.(*sandboxv1beta1.Sandbox); ok && sb.Name == "pool-abcde" && conflicted.Load() {
				sb.DeletionTimestamp = &metav1.Time{Time: time.Now()}
			}
			return nil
		},
	})
	r := &SandboxClaimReconciler{Client: c, Scheme: scheme, WarmSandboxQueue: q, Tracer: asmetrics.NewNoOp()}
	req := ctrl.Request{Namespace: "default", Name: "claim-a"}

	if _, err := r.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := base.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: "claim-a"}, &sandboxv1beta1.Sandbox{}); !k8errors.IsNotFound(err) {
		t.Fatalf("the claim cold-started in the pass whose hand-over conflicted: get sandbox claim-a = %v", err)
	}
	got := &extensionsv1beta1.SandboxClaim{}
	if err := base.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: "claim-a"}, got); err != nil {
		t.Fatalf("get claim: %v", err)
	}
	if v := got.Annotations[extensionsv1beta1.AssignedSandboxNameAnnotation]; v != "pool-abcde" {
		t.Fatalf("recorded assignment = %q, want pool-abcde kept for the next pass", v)
	}

	if _, err := r.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if got := boundSandbox(t, base, "claim-a"); got != "claim-a" {
		t.Fatalf("claim bound to %q after the candidate went away, want its own cold-started sandbox", got)
	}
}

func TestAReferenceToAnotherClaimsSandboxIsCleared(t *testing.T) {
	scheme := newScheme(t)
	claim := doubleAdoptionClaim("claim-a")
	claim.Annotations = map[string]string{extensionsv1beta1.AssignedSandboxNameAnnotation: "pool-abcde"}
	taken := doubleAdoptionWarmSandbox()
	taken.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: extensionsv1beta1.GroupVersion.String(), Kind: "SandboxClaim",
		Name: "claim-b", UID: "claim-b-uid", Controller: new(true),
	}}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(doubleAdoptionTemplate(), doubleAdoptionPool(), claim, taken).
		WithStatusSubresource(claim).Build()
	r := &SandboxClaimReconciler{Client: c, Scheme: scheme, WarmSandboxQueue: queue.NewSimpleSandboxQueue(), Tracer: asmetrics.NewNoOp()}

	if _, err := r.Reconcile(t.Context(), ctrl.Request{Namespace: "default", Name: "claim-a"}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := &extensionsv1beta1.SandboxClaim{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: "claim-a"}, got); err != nil {
		t.Fatalf("get claim: %v", err)
	}
	if v, ok := got.Annotations[extensionsv1beta1.AssignedSandboxNameAnnotation]; ok {
		t.Fatalf("claim still references %q, a sandbox another claim owns; every later pass would re-read it", v)
	}
	if boundSandbox(t, c, "claim-a") != "claim-a" {
		t.Fatalf("claim did not cold-start its own sandbox: bound to %q", boundSandbox(t, c, "claim-a"))
	}
}

func doubleAdoptionClaim(name string) *extensionsv1beta1.SandboxClaim {
	return &extensionsv1beta1.SandboxClaim{
		Name: name, Namespace: "default", UID: types.UID(name + "-uid"),
		Spec: extensionsv1beta1.SandboxClaimSpec{WarmPoolRef: extensionsv1beta1.SandboxWarmPoolRef{Name: "pool"}},
	}
}

func doubleAdoptionTemplate() *extensionsv1beta1.SandboxTemplate {
	return &extensionsv1beta1.SandboxTemplate{
		Name: "tpl", Namespace: "default",
		Spec: extensionsv1beta1.SandboxTemplateSpec{SandboxBlueprint: sandboxv1beta1.SandboxBlueprint{PodTemplate: sandboxv1beta1.PodTemplate{
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "img"}}},
		}}},
	}
}

func doubleAdoptionPool() *extensionsv1beta1.SandboxWarmPool {
	return &extensionsv1beta1.SandboxWarmPool{
		Name: "pool", Namespace: "default", UID: "pool-uid",
		Spec: extensionsv1beta1.SandboxWarmPoolSpec{TemplateRef: extensionsv1beta1.SandboxTemplateRef{Name: "tpl"}},
	}
}

func doubleAdoptionWarmSandbox() *sandboxv1beta1.Sandbox {
	return &sandboxv1beta1.Sandbox{
		Name: "pool-abcde", Namespace: "default",
		CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Minute)),
		Labels: map[string]string{
			sandboxv1beta1.SandboxWarmPoolLabel:        hash.Name("pool"),
			sandboxv1beta1.SandboxTemplateRefHashLabel: SandboxTemplateRefHash("tpl"),
		},
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: extensionsv1beta1.GroupVersion.String(), Kind: "SandboxWarmPool",
			Name: "pool", UID: "pool-uid", Controller: new(true),
		}},
		Spec: sandboxv1beta1.SandboxSpec{SandboxBlueprint: sandboxv1beta1.SandboxBlueprint{PodTemplate: sandboxv1beta1.PodTemplate{
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "img"}}},
		}}},
	}
}

func boundSandbox(t *testing.T, c client.Client, claim string) string {
	t.Helper()
	got := &extensionsv1beta1.SandboxClaim{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: claim}, got); err != nil {
		t.Fatalf("get claim %s: %v", claim, err)
	}
	return got.Status.SandboxStatus.Name
}
