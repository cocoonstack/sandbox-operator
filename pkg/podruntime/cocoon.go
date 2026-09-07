// Package podruntime adapts agent-sandbox Pods to Cocoon runtime backends.
package podruntime

import (
	"cmp"
	"context"
	"crypto/sha256"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
)

const (
	// RuntimeAnnotation selects the runtime integration for one Sandbox Pod.
	RuntimeAnnotation = "sandbox.cocoonstack.io/runtime"

	ModeVKCocoon = "vk-cocoon"
	ModeStandard = "standard"
	// ModeSandboxd routes a Sandbox Pod to the sandboxd hot-pool data plane —
	// the vk-sandbox virtual node, which serves the claim from a
	// node-local sandboxd (github.com/cocoonstack/sandbox) in sub-millisecond
	// time. This is the successor to ModeVKCocoon for agent-sandbox workloads;
	// vk-cocoon no longer answers sandbox Pods when this mode is selected.
	ModeSandboxd = "sandboxd"
	// DefaultMode keeps ordinary kubelet scheduling unless a Sandbox explicitly
	// opts into a virtual-node contract.
	DefaultMode = ModeStandard

	vkProviderTaintKey = "virtual-kubelet.io/provider"
	vkNodeLabelKey     = "node.kubernetes.io/instance-type"
	vkNodeLabelValue   = "virtual-node"

	// sandboxd virtual-node contract: vk-sandbox advertises this label
	// and taints with vkProviderTaintKey (Exists toleration covers both planes).
	sandboxdNodeLabelKey   = "sandbox.cocoonstack.io/runtime"
	sandboxdNodeLabelValue = "sandboxd"
	// sandboxd claim axes — mirror pkg/scale selector keys and the
	// vk-sandbox provider's AnnTemplate/AnnNet/AnnSize contract.
	sandboxdTemplateAnnotation = "sandbox.cocoonstack.io/template"

	cocoonModeAnnotation    = "cocoonset.cocoonstack.io/mode"
	cocoonManagedAnnotation = "cocoonset.cocoonstack.io/managed"
	cocoonImageAnnotation   = "cocoonset.cocoonstack.io/image"
	cocoonOSAnnotation      = "cocoonset.cocoonstack.io/os"
	cocoonVMNameAnnotation  = "vm.cocoonstack.io/name"
)

// Mutator applies the selected Cocoon runtime defaults to newly-created Pods.
type Mutator struct {
	defaultMode string
}

// NewMutator validates defaultMode and returns a runtime Pod mutator.
func NewMutator(defaultMode string) (*Mutator, error) {
	mode := strings.TrimSpace(defaultMode)
	switch mode {
	case ModeVKCocoon, ModeStandard, ModeSandboxd:
		return &Mutator{defaultMode: mode}, nil
	default:
		return nil, fmt.Errorf("unsupported default runtime %q", defaultMode)
	}
}

// MutatePod applies runtime defaults without replacing user-supplied values.
func (m *Mutator) MutatePod(_ context.Context, sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod) error {
	if sandbox == nil {
		return fmt.Errorf("sandbox is nil")
	}
	if pod == nil {
		return fmt.Errorf("pod is nil")
	}

	mode, explicit := requestedMode(pod, m.defaultMode)
	switch mode {
	case ModeStandard:
		return nil
	case ModeVKCocoon:
		return m.mutateVKCocoon(sandbox, pod, explicit)
	case ModeSandboxd:
		return m.mutateSandboxd(pod, explicit)
	default:
		return fmt.Errorf("unsupported %s value %q", RuntimeAnnotation, mode)
	}
}

// mutateVKCocoon routes the Pod to a vk-cocoon virtual node (cocoon MicroVM).
func (m *Mutator) mutateVKCocoon(sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod, explicit bool) error {
	if pod.Spec.RuntimeClassName != nil {
		if explicit {
			return fmt.Errorf("%s=%s conflicts with spec.runtimeClassName", RuntimeAnnotation, ModeVKCocoon)
		}
		return nil
	}
	if err := routeToVirtualNode(pod, ModeVKCocoon, vkNodeLabelKey, vkNodeLabelValue); err != nil {
		return err
	}

	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	setDefault(pod.Annotations, RuntimeAnnotation, ModeVKCocoon)
	setDefault(pod.Annotations, cocoonModeAnnotation, "run")
	setDefault(pod.Annotations, cocoonManagedAnnotation, "true")
	setDefault(pod.Annotations, cocoonVMNameAnnotation, stableVMName(sandbox.Namespace, sandbox.Name))
	if len(pod.Spec.Containers) > 0 {
		setDefault(pod.Annotations, cocoonImageAnnotation, pod.Spec.Containers[0].Image)
	}
	osName := string(corev1.Linux)
	if pod.Spec.OS != nil && pod.Spec.OS.Name != "" {
		osName = string(pod.Spec.OS.Name)
	}
	setDefault(pod.Annotations, cocoonOSAnnotation, strings.ToLower(osName))
	return nil
}

// mutateSandboxd routes the Pod to a vk-sandbox virtual node (sandboxd
// hot pool). It sets the sandboxd claim template axis from the first container
// image when unset, so the node provider always has a template to claim.
func (m *Mutator) mutateSandboxd(pod *corev1.Pod, explicit bool) error {
	if pod.Spec.RuntimeClassName != nil {
		if explicit {
			return fmt.Errorf("%s=%s conflicts with spec.runtimeClassName", RuntimeAnnotation, ModeSandboxd)
		}
		return nil
	}
	if err := routeToVirtualNode(pod, ModeSandboxd, sandboxdNodeLabelKey, sandboxdNodeLabelValue); err != nil {
		return err
	}

	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	setDefault(pod.Annotations, RuntimeAnnotation, ModeSandboxd)
	if len(pod.Spec.Containers) > 0 {
		setDefault(pod.Annotations, sandboxdTemplateAnnotation, pod.Spec.Containers[0].Image)
	}
	return nil
}

// routeToVirtualNode pins the Pod to the virtual node identified by
// labelKey=labelValue and adds the shared vk-provider toleration. A pinned
// NodeName bypasses the selector entirely and would misroute the sandbox, so it
// is rejected rather than silently mis-scheduled.
func routeToVirtualNode(pod *corev1.Pod, mode, labelKey, labelValue string) error {
	if pod.Spec.NodeName != "" {
		return fmt.Errorf("%s=%s conflicts with spec.nodeName=%s", RuntimeAnnotation, mode, pod.Spec.NodeName)
	}
	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = make(map[string]string)
	}
	if value, found := pod.Spec.NodeSelector[labelKey]; found && value != labelValue {
		return fmt.Errorf("node selector %s=%s conflicts with %s value %s", labelKey, value, mode, labelValue)
	}
	pod.Spec.NodeSelector[labelKey] = labelValue

	if !toleratesVKCocoon(pod.Spec.Tolerations) {
		pod.Spec.Tolerations = append(pod.Spec.Tolerations, corev1.Toleration{
			Key:      vkProviderTaintKey,
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoSchedule,
		})
	}
	return nil
}

func requestedMode(pod *corev1.Pod, defaultMode string) (string, bool) {
	if pod.Annotations != nil {
		if value := strings.TrimSpace(pod.Annotations[RuntimeAnnotation]); value != "" {
			return value, true
		}
	}
	if pod.Spec.RuntimeClassName != nil {
		return ModeStandard, false
	}
	return defaultMode, false
}

func toleratesVKCocoon(tolerations []corev1.Toleration) bool {
	return slices.ContainsFunc(tolerations, func(t corev1.Toleration) bool {
		return t.Key == vkProviderTaintKey && t.Operator == corev1.TolerationOpExists &&
			(t.Effect == "" || t.Effect == corev1.TaintEffectNoSchedule)
	})
}

func setDefault(values map[string]string, key, value string) {
	if _, found := values[key]; !found && value != "" {
		values[key] = value
	}
}

func stableVMName(namespace, name string) string {
	raw := strings.ToLower(strings.Trim(strings.Join([]string{"sandbox", namespace, name}, "-"), "-"))
	var normalized strings.Builder
	lastDash := false
	for _, char := range raw {
		if char >= 'a' && char <= 'z' || char >= '0' && char <= '9' {
			normalized.WriteRune(char)
			lastDash = false
			continue
		}
		if !lastDash {
			normalized.WriteByte('-')
			lastDash = true
		}
	}
	prefix := strings.Trim(normalized.String(), "-")
	prefix = cmp.Or(prefix, "sandbox")
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(namespace+"/"+name)))[:8]
	const maxPrefix = 63 - 1 - 8
	if len(prefix) > maxPrefix {
		prefix = strings.Trim(prefix[:maxPrefix], "-")
	}
	return prefix + "-" + digest
}
