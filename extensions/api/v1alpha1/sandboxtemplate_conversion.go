// Copyright 2026 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v1alpha1

import (
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/conversion"

	sandboxv1alpha1 "github.com/cocoonstack/sandbox-operator/api/v1alpha1"
	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	v1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
)

const v1alpha1SandboxTemplateStateAnnotation = "api.agents.x-k8s.io/v1alpha1-sandboxtemplate-state"

// ConvertTo converts this SandboxTemplate to the Hub version (v1beta1).
func (s *SandboxTemplate) ConvertTo(dstRaw conversion.Hub) error {
	dst := dstRaw.(*v1beta1.SandboxTemplate)

	s.ObjectMeta.DeepCopyInto(&dst.ObjectMeta)
	convertTemplateSpecTo(&s.Spec, &dst.Spec)

	if policy, ok := s.Annotations["api.agents.x-k8s.io/v1beta1-volume-claim-templates-policy"]; ok {
		switch v1beta1.VolumeClaimTemplatesPolicy(policy) {
		case v1beta1.VolumeClaimTemplatesPolicyDisallowed, v1beta1.VolumeClaimTemplatesPolicyAllowed, v1beta1.VolumeClaimTemplatesPolicyOverrides:
			dst.Spec.VolumeClaimTemplatesPolicy = v1beta1.VolumeClaimTemplatesPolicy(policy)
		default:
			return fmt.Errorf("invalid VolumeClaimTemplatesPolicy annotation value: %q", policy)
		}
		if dst.Annotations != nil {
			delete(dst.Annotations, "api.agents.x-k8s.io/v1beta1-volume-claim-templates-policy")
		}
	}

	delete(dst.Annotations, v1alpha1SandboxTemplateStateAnnotation)
	return nil
}

// ConvertFrom converts from the Hub version (v1beta1) to this SandboxTemplate.
func (s *SandboxTemplate) ConvertFrom(srcRaw conversion.Hub) error {
	src := srcRaw.(*v1beta1.SandboxTemplate)

	src.ObjectMeta.DeepCopyInto(&s.ObjectMeta)
	convertTemplateSpecFrom(&src.Spec, &s.Spec)

	// Strip the state annotation if present so it doesn't leak to clients and get sent back on updates
	delete(s.Annotations, v1alpha1SandboxTemplateStateAnnotation)

	if src.Spec.VolumeClaimTemplatesPolicy != "" {
		if s.Annotations == nil {
			s.Annotations = make(map[string]string)
		}
		s.Annotations["api.agents.x-k8s.io/v1beta1-volume-claim-templates-policy"] = string(src.Spec.VolumeClaimTemplatesPolicy)
	} else if s.Annotations != nil {
		delete(s.Annotations, "api.agents.x-k8s.io/v1beta1-volume-claim-templates-policy")
	}

	return nil
}

func convertTemplateSpecTo(src *SandboxTemplateSpec, dst *v1beta1.SandboxTemplateSpec) {
	sandboxv1alpha1.ConvertPodTemplateTo(&src.PodTemplate, &dst.PodTemplate)

	if src.VolumeClaimTemplates != nil {
		dst.VolumeClaimTemplates = make([]sandboxv1beta1.PersistentVolumeClaimTemplate, len(src.VolumeClaimTemplates))
		for i := range src.VolumeClaimTemplates {
			sandboxv1alpha1.ConvertPVCClaimTemplateTo(&src.VolumeClaimTemplates[i], &dst.VolumeClaimTemplates[i])
		}
	} else {
		dst.VolumeClaimTemplates = nil
	}

	if src.NetworkPolicy != nil {
		dst.NetworkPolicy = &v1beta1.NetworkPolicySpec{
			Ingress: src.NetworkPolicy.Ingress,
			Egress:  src.NetworkPolicy.Egress,
		}
	} else {
		dst.NetworkPolicy = nil
	}

	dst.NetworkPolicyManagement = v1beta1.NetworkPolicyManagement(src.NetworkPolicyManagement)
	dst.EnvVarsInjectionPolicy = v1beta1.EnvVarsInjectionPolicy(src.EnvVarsInjectionPolicy)
	dst.Service = src.Service
}

func convertTemplateSpecFrom(src *v1beta1.SandboxTemplateSpec, dst *SandboxTemplateSpec) {
	sandboxv1alpha1.ConvertPodTemplateFrom(&src.PodTemplate, &dst.PodTemplate)

	if src.VolumeClaimTemplates != nil {
		dst.VolumeClaimTemplates = make([]sandboxv1alpha1.PersistentVolumeClaimTemplate, len(src.VolumeClaimTemplates))
		for i := range src.VolumeClaimTemplates {
			sandboxv1alpha1.ConvertPVCClaimTemplateFrom(&src.VolumeClaimTemplates[i], &dst.VolumeClaimTemplates[i])
		}
	} else {
		dst.VolumeClaimTemplates = nil
	}

	if src.NetworkPolicy != nil {
		dst.NetworkPolicy = &NetworkPolicySpec{
			Ingress: src.NetworkPolicy.Ingress,
			Egress:  src.NetworkPolicy.Egress,
		}
	} else {
		dst.NetworkPolicy = nil
	}

	dst.NetworkPolicyManagement = NetworkPolicyManagement(src.NetworkPolicyManagement)
	dst.EnvVarsInjectionPolicy = EnvVarsInjectionPolicy(src.EnvVarsInjectionPolicy)
	dst.Service = src.Service
}
