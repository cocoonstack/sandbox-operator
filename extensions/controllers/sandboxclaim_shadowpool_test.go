package controllers

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	extensionsv1alpha1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1alpha1"
	extensionsv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
	asmetrics "github.com/cocoonstack/sandbox-operator/internal/metrics"
	"github.com/cocoonstack/sandbox-operator/internal/queue"
)

func TestAShadowPoolClaimColdStartsFromItsTemplate(t *testing.T) {
	scheme := newScheme(t)
	template := &extensionsv1beta1.SandboxTemplate{
		Name: "tpl", Namespace: "default",
		Spec: extensionsv1beta1.SandboxTemplateSpec{SandboxBlueprint: sandboxv1beta1.SandboxBlueprint{PodTemplate: sandboxv1beta1.PodTemplate{
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "img"}}},
		}}},
	}
	claim := shadowPoolClaim("legacy", extensionsv1alpha1.ShadowPoolPrefix+"tpl")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(template, claim).WithStatusSubresource(claim).Build()
	r := &SandboxClaimReconciler{Client: c, Scheme: scheme, WarmSandboxQueue: queue.NewSimpleSandboxQueue(), Tracer: asmetrics.NewNoOp()}

	if _, err := r.Reconcile(t.Context(), ctrl.Request{Namespace: "default", Name: "legacy"}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	sandbox := &sandboxv1beta1.Sandbox{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: "legacy"}, sandbox); err != nil {
		t.Fatalf("a v1alpha1 claim without a pool name must cold-start a Sandbox from its template: %v", err)
	}
	if !metav1.IsControlledBy(sandbox, claim) {
		t.Fatalf("sandbox owner = %v, want the claim", sandbox.OwnerReferences)
	}
	if got := sandbox.Spec.PodTemplate.Spec.Containers[0].Image; got != "img" {
		t.Fatalf("sandbox image = %q, want the template's", got)
	}
}

func TestAShadowPoolClaimReportsAMissingTemplateNotAMissingPool(t *testing.T) {
	scheme := newScheme(t)
	claim := shadowPoolClaim("legacy", extensionsv1alpha1.ShadowPoolPrefix+"missing")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(claim).WithStatusSubresource(claim).Build()
	r := &SandboxClaimReconciler{Client: c, Scheme: scheme, WarmSandboxQueue: queue.NewSimpleSandboxQueue(), Tracer: asmetrics.NewNoOp()}

	if _, err := r.Reconcile(t.Context(), ctrl.Request{Namespace: "default", Name: "legacy"}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := &extensionsv1beta1.SandboxClaim{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: "legacy"}, got); err != nil {
		t.Fatalf("get claim: %v", err)
	}
	ready := meta.FindStatusCondition(got.Status.Conditions, string(sandboxv1beta1.SandboxConditionReady))
	if ready == nil || ready.Reason != reasonTemplateNotFound {
		t.Fatalf("Ready condition = %+v, want reason %s", ready, reasonTemplateNotFound)
	}
}

func shadowPoolClaim(name, poolRef string) *extensionsv1beta1.SandboxClaim {
	return &extensionsv1beta1.SandboxClaim{
		Name: name, Namespace: "default", UID: types.UID(name + "-uid"),
		Spec: extensionsv1beta1.SandboxClaimSpec{WarmPoolRef: extensionsv1beta1.SandboxWarmPoolRef{Name: poolRef}},
	}
}
