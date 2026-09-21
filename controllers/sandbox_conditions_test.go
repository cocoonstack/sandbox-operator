package controllers

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	asmetrics "github.com/cocoonstack/sandbox-operator/internal/metrics"
)

func TestSandboxResumeClearsSuspendedCondition(t *testing.T) {
	sandbox := &sandboxv1beta1.Sandbox{
		Name:       "resume",
		Namespace:  "default",
		UID:        sandboxUID,
		Generation: 1,
		Spec: sandboxv1beta1.SandboxSpec{
			OperatingMode: sandboxv1beta1.SandboxOperatingModeSuspended,
			PodTemplate: sandboxv1beta1.PodTemplate{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "agent", Image: "image"}}},
			},
		},
	}
	kube := newFakeClient(sandbox)
	r := &SandboxReconciler{Client: kube, Scheme: Scheme, Tracer: asmetrics.NewNoOp()}
	ctx := t.Context()
	key := types.NamespacedName{Name: sandbox.Name, Namespace: sandbox.Namespace}
	req := ctrl.Request{NamespacedName: key}

	_, err := r.Reconcile(ctx, req)
	require.NoError(t, err)
	require.NoError(t, kube.Get(ctx, key, sandbox))
	require.True(t, meta.IsStatusConditionTrue(sandbox.Status.Conditions, string(sandboxv1beta1.SandboxConditionSuspended)))

	sandbox.Spec.OperatingMode = sandboxv1beta1.SandboxOperatingModeRunning
	sandbox.Generation++
	require.NoError(t, kube.Update(ctx, sandbox))
	for range 2 {
		_, err = r.Reconcile(ctx, req)
		require.NoError(t, err)
		require.NoError(t, kube.Get(ctx, key, sandbox))
		require.Nil(t, meta.FindStatusCondition(sandbox.Status.Conditions, string(sandboxv1beta1.SandboxConditionSuspended)))
		ready := meta.FindStatusCondition(sandbox.Status.Conditions, string(sandboxv1beta1.SandboxConditionReady))
		require.NotNil(t, ready)
		require.Equal(t, sandbox.Generation, ready.ObservedGeneration)
	}
	var pod corev1.Pod
	require.NoError(t, kube.Get(ctx, key, &pod))
	require.True(t, metav1.IsControlledBy(&pod, sandbox))
}
