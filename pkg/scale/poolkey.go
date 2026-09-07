package scale

import (
	"cmp"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	SizeClassSmall  = "small"
	SizeClassMedium = "medium"
	SizeClassLarge  = "large"

	// NetDefault is the mode used when none is annotated: the NIC-less Firecracker lane.
	NetDefault = "none"
	// NetEgress is the egress-capable network lane.
	NetEgress = "egress"
)

// Coarse, documented t-shirt-size thresholds mapping a container's CPU/memory
// request (falling back to its limit) onto the warm pool size classes.
var (
	sizeCPUMedium = resource.MustParse("1")
	sizeMemMedium = resource.MustParse("2Gi")
	sizeCPULarge  = resource.MustParse("4")
	sizeMemLarge  = resource.MustParse("8Gi")
)

// PoolKeyFor derives the warm-pool key for a workload. It is the SINGLE source of
// pool-key truth: the aggregated apiserver derives it from a Sandbox's
// podTemplate on Create, and the SandboxWarmPool driver derives it from a
// SandboxTemplate's podTemplate when setting node warm targets. Both MUST agree
// or a Create would never match the warm capacity the pool driver provisions
// (perpetual 503). Template = the first container image; Net = the given net
// (defaulted to NetDefault); Size = the first container's t-shirt class.
func PoolKeyFor(containers []corev1.Container, net string) PoolKey {
	var template string
	if len(containers) > 0 {
		template = containers[0].Image
	}
	net = cmp.Or(net, NetDefault)
	return PoolKey{Template: template, Net: net, Size: SizeClassForContainers(containers)}
}

// NetForAnnotations resolves the pool network axis from an object's own
// annotations, falling back to its pod template's. Create and the warm-pool
// driver resolve it through this one function so both derive the same key.
func NetForAnnotations(object, podTemplate map[string]string) string {
	return cmp.Or(object[NetAnnotation], podTemplate[NetAnnotation])
}

// SizeClassForContainers maps the first container's CPU/memory onto small|medium|
// large. It prefers requests, falls back to limits, and defaults to "small" when
// neither is set. Thresholds: >4 CPU or >8Gi -> large; >1 CPU or >2Gi -> medium.
func SizeClassForContainers(containers []corev1.Container) string {
	if len(containers) == 0 {
		return SizeClassSmall
	}
	res := containers[0].Resources
	cpu := res.Requests.Cpu()
	if cpu.IsZero() {
		cpu = res.Limits.Cpu()
	}
	mem := res.Requests.Memory()
	if mem.IsZero() {
		mem = res.Limits.Memory()
	}
	switch {
	case cpu.Cmp(sizeCPULarge) > 0 || mem.Cmp(sizeMemLarge) > 0:
		return SizeClassLarge
	case cpu.Cmp(sizeCPUMedium) > 0 || mem.Cmp(sizeMemMedium) > 0:
		return SizeClassMedium
	default:
		return SizeClassSmall
	}
}
