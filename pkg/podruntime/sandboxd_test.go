package podruntime

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

func TestMutateSandboxdRoutesToHotPool(t *testing.T) {
	m, err := NewMutator(ModeSandboxd)
	if err != nil {
		t.Fatalf("NewMutator(sandboxd): %v", err)
	}
	sandbox := &sandboxv1beta1.Sandbox{Name: "sb", Namespace: "ns"}
	pod := sandboxdPod("base:24.04")

	if err := m.MutatePod(t.Context(), sandbox, pod); err != nil {
		t.Fatalf("MutatePod: %v", err)
	}

	if got := pod.Spec.NodeSelector[sandboxdNodeLabelKey]; got != sandboxdNodeLabelValue {
		t.Fatalf("nodeSelector %s=%q, want %q", sandboxdNodeLabelKey, got, sandboxdNodeLabelValue)
	}
	if got := pod.Spec.NodeSelector[vkNodeLabelKey]; got != "" {
		t.Fatalf("sandboxd pod must NOT carry the vk-cocoon selector, got %s=%q", vkNodeLabelKey, got)
	}
	if pod.Annotations[RuntimeAnnotation] != ModeSandboxd {
		t.Fatalf("runtime annotation = %q, want sandboxd", pod.Annotations[RuntimeAnnotation])
	}
	if pod.Annotations[sandboxdTemplateAnnotation] != "base:24.04" {
		t.Fatalf("template default = %q, want base:24.04", pod.Annotations[sandboxdTemplateAnnotation])
	}

	if _, ok := pod.Annotations[cocoonModeAnnotation]; ok {
		t.Fatal("sandboxd pod must not carry cocoon vk-cocoon annotations")
	}
	if !toleratesVKProvider(pod.Spec.Tolerations) {
		t.Fatal("sandboxd pod must tolerate the vk-provider taint")
	}
}

func TestMutateSandboxdClaimsUnderThePoolKeyTheDriverProvisions(t *testing.T) {
	m, _ := NewMutator(ModeSandboxd)
	sandbox := &sandboxv1beta1.Sandbox{Name: "sb", Namespace: "ns"}
	pod := sandboxdPod("base:24.04")
	pod.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("4"),
		corev1.ResourceMemory: resource.MustParse("8Gi"),
	}
	if err := m.MutatePod(t.Context(), sandbox, pod); err != nil {
		t.Fatalf("MutatePod: %v", err)
	}
	want := scale.PoolKeyFor(pod.Spec.Containers, "")
	got := scale.PoolKey{
		Template: pod.Annotations[sandboxdTemplateAnnotation],
		Net:      pod.Annotations[sandboxdNetAnnotation],
		Size:     pod.Annotations[sandboxdSizeAnnotation],
	}
	if got != want {
		t.Fatalf("pod claims %+v, the warm-pool driver provisions %+v: a claim outside the provisioned key never hits warm capacity", got, want)
	}
	if got.Size != scale.SizeClassMedium || got.Net != scale.NetDefault {
		t.Fatalf("derived key %+v, want net=%s size=%s", got, scale.NetDefault, scale.SizeClassMedium)
	}
}

func TestMutateSandboxdKeepsAnExplicitNet(t *testing.T) {
	m, _ := NewMutator(ModeSandboxd)
	sandbox := &sandboxv1beta1.Sandbox{Name: "sb", Namespace: "ns"}
	pod := sandboxdPod("base:24.04")
	pod.Annotations = map[string]string{sandboxdNetAnnotation: scale.NetEgress}
	if err := m.MutatePod(t.Context(), sandbox, pod); err != nil {
		t.Fatalf("MutatePod: %v", err)
	}
	if pod.Annotations[sandboxdNetAnnotation] != scale.NetEgress {
		t.Fatalf("net = %q, want the pod's own %s", pod.Annotations[sandboxdNetAnnotation], scale.NetEgress)
	}
}

func TestMutateSandboxdRespectsExplicitTemplate(t *testing.T) {
	m, _ := NewMutator(ModeStandard)
	sandbox := &sandboxv1beta1.Sandbox{Name: "sb", Namespace: "ns"}
	pod := sandboxdPod("ignored:latest")
	pod.Annotations = map[string]string{
		RuntimeAnnotation:          ModeSandboxd,
		sandboxdTemplateAnnotation: "myproj:v1",
	}
	if err := m.MutatePod(t.Context(), sandbox, pod); err != nil {
		t.Fatalf("MutatePod: %v", err)
	}
	if pod.Annotations[sandboxdTemplateAnnotation] != "myproj:v1" {
		t.Fatalf("explicit template overwritten: %q", pod.Annotations[sandboxdTemplateAnnotation])
	}
}

func TestMutateSandboxdRejectsPinnedNode(t *testing.T) {
	m, _ := NewMutator(ModeSandboxd)
	sandbox := &sandboxv1beta1.Sandbox{Name: "sb", Namespace: "ns"}
	pod := sandboxdPod("base:24.04")
	pod.Spec.NodeName = "cocoon-bd1"
	if err := m.MutatePod(t.Context(), sandbox, pod); err == nil {
		t.Fatal("expected error for pinned nodeName in sandboxd mode")
	}
}

func TestNewMutatorAcceptsSandboxd(t *testing.T) {
	if _, err := NewMutator(ModeSandboxd); err != nil {
		t.Fatalf("NewMutator must accept sandboxd: %v", err)
	}
	if _, err := NewMutator("nonsense"); err == nil {
		t.Fatal("NewMutator must reject unknown modes")
	}
}

func sandboxdPod(image string) *corev1.Pod {
	return &corev1.Pod{
		Name: "sb", Namespace: "ns",
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "agent", Image: image}},
		},
	}
}
