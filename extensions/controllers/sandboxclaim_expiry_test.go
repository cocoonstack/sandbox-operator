package controllers

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	extensionsv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
	asmetrics "github.com/cocoonstack/sandbox-operator/internal/metrics"
	"github.com/cocoonstack/sandbox-operator/internal/queue"
)

func TestAnExpiredRetainClaimDeletesTheSandboxItsAnnotationNames(t *testing.T) {
	scheme := newScheme(t)
	past := metav1.NewTime(time.Now().Add(-time.Minute))
	claim := &extensionsv1beta1.SandboxClaim{
		Name: "claim", Namespace: "default", UID: "claim-uid",
		Annotations: map[string]string{extensionsv1beta1.AssignedSandboxNameAnnotation: "pool-abcde"},
		Spec: extensionsv1beta1.SandboxClaimSpec{
			WarmPoolRef: extensionsv1beta1.SandboxWarmPoolRef{Name: "pool"},
			Lifecycle:   &extensionsv1beta1.Lifecycle{ShutdownTime: &past, ShutdownPolicy: extensionsv1beta1.ShutdownPolicyRetain},
		},
	}
	adopted := &sandboxv1beta1.Sandbox{
		Name: "pool-abcde", Namespace: "default",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: extensionsv1beta1.GroupVersion.String(), Kind: "SandboxClaim",
			Name: "claim", UID: "claim-uid", Controller: new(true),
		}},
		Spec: sandboxv1beta1.SandboxSpec{SandboxBlueprint: sandboxv1beta1.SandboxBlueprint{PodTemplate: sandboxv1beta1.PodTemplate{
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "img"}}},
		}}},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(claim, adopted).WithStatusSubresource(claim).Build()
	r := &SandboxClaimReconciler{Client: c, Scheme: scheme, WarmSandboxQueue: queue.NewSimpleSandboxQueue(), Tracer: asmetrics.NewNoOp()}

	req := ctrl.Request{Namespace: "default", Name: "claim"}
	for range 2 {
		if _, err := r.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
	}

	err := c.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: "pool-abcde"}, &sandboxv1beta1.Sandbox{})
	if !k8errors.IsNotFound(err) {
		t.Fatalf("the adopted Sandbox survived the claim's expiry (err=%v); with Retain nothing else ever deletes it", err)
	}
}
