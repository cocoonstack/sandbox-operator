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
	"sigs.k8s.io/controller-runtime/pkg/conversion"

	v1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
)

const v1alpha1SandboxWarmPoolStateAnnotation = "api.agents.x-k8s.io/v1alpha1-sandboxwarmpool-state"

// ConvertTo converts this SandboxWarmPool to the Hub version (v1beta1).
func (s *SandboxWarmPool) ConvertTo(dstRaw conversion.Hub) error {
	dst := dstRaw.(*v1beta1.SandboxWarmPool)

	s.ObjectMeta.DeepCopyInto(&dst.ObjectMeta)
	convertWarmPoolSpecTo(&s.Spec, &dst.Spec)
	dst.Status = v1beta1.SandboxWarmPoolStatus(s.Status)

	delete(dst.Annotations, v1alpha1SandboxWarmPoolStateAnnotation)
	return nil
}

// ConvertFrom converts from the Hub version (v1beta1) to this SandboxWarmPool.
func (s *SandboxWarmPool) ConvertFrom(srcRaw conversion.Hub) error {
	src := srcRaw.(*v1beta1.SandboxWarmPool)

	src.ObjectMeta.DeepCopyInto(&s.ObjectMeta)
	convertWarmPoolSpecFrom(&src.Spec, &s.Spec)
	s.Status = SandboxWarmPoolStatus(src.Status)

	// Strip the state annotation if present so it doesn't leak to clients and get sent back on updates
	delete(s.Annotations, v1alpha1SandboxWarmPoolStateAnnotation)

	return nil
}

func convertWarmPoolSpecTo(src *SandboxWarmPoolSpec, dst *v1beta1.SandboxWarmPoolSpec) {
	dst.Replicas = new(src.Replicas)
	dst.TemplateRef = v1beta1.SandboxTemplateRef{
		Name: src.TemplateRef.Name,
	}

	if src.UpdateStrategy != nil {
		dst.UpdateStrategy = &v1beta1.SandboxWarmPoolUpdateStrategy{
			Type: v1beta1.SandboxWarmPoolUpdateStrategyType(src.UpdateStrategy.Type),
		}
	} else {
		dst.UpdateStrategy = nil
	}
}

func convertWarmPoolSpecFrom(src *v1beta1.SandboxWarmPoolSpec, dst *SandboxWarmPoolSpec) {
	if src.Replicas != nil {
		dst.Replicas = *src.Replicas
	} else {
		dst.Replicas = 1
	}
	dst.TemplateRef = SandboxTemplateRef{
		Name: src.TemplateRef.Name,
	}

	if src.UpdateStrategy != nil {
		dst.UpdateStrategy = &SandboxWarmPoolUpdateStrategy{
			Type: SandboxWarmPoolUpdateStrategyType(src.UpdateStrategy.Type),
		}
	} else {
		dst.UpdateStrategy = nil
	}
}
