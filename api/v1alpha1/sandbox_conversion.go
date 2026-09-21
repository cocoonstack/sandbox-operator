// Copyright 2025 The Kubernetes Authors.
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
	"encoding/json"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/conversion"

	v1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
)

const v1alpha1SandboxStateAnnotation = "api.agents.x-k8s.io/v1alpha1-sandbox-state"

// v1alpha1State is the round-trip payload for the fields v1beta1 cannot represent.
type v1alpha1State struct {
	Spec struct {
		Replicas *int32 `json:"replicas,omitempty"`
	} `json:"spec"`
}

// ConvertTo converts this Sandbox to the Hub version (v1beta1).
func (s *Sandbox) ConvertTo(dstRaw conversion.Hub) error {
	dst := dstRaw.(*v1beta1.Sandbox)

	s.ObjectMeta.DeepCopyInto(&dst.ObjectMeta)
	ConvertSpecTo(&s.Spec, &dst.Spec)
	ConvertStatusTo(&s.Status, &dst.Status)

	// Preserve the fields v1beta1 cannot represent for lossless round-tripping
	if dst.Annotations == nil {
		dst.Annotations = make(map[string]string)
	}
	var state v1alpha1State
	state.Spec.Replicas = s.Spec.Replicas
	stateJSON, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal v1alpha1 sandbox state: %w", err)
	}
	dst.Annotations[v1alpha1SandboxStateAnnotation] = string(stateJSON)

	return nil
}

// ConvertFrom converts from the Hub version (v1beta1) to this Sandbox.
func (s *Sandbox) ConvertFrom(srcRaw conversion.Hub) error {
	src := srcRaw.(*v1beta1.Sandbox)

	src.ObjectMeta.DeepCopyInto(&s.ObjectMeta)
	ConvertSpecFrom(&src.Spec, &s.Spec)
	ConvertStatusFrom(&src.Status, &s.Status)

	// Set best-effort default for Status.Replicas based on OperatingMode.
	if src.Spec.OperatingMode == v1beta1.SandboxOperatingModeSuspended {
		s.Status.Replicas = 0
	} else {
		s.Status.Replicas = 1
	}

	// Restore original v1alpha1 state if present to ensure lossless conversion
	if stateJSON, ok := s.Annotations[v1alpha1SandboxStateAnnotation]; ok {
		// Strip the state annotation so it doesn't leak to clients and get sent back on updates
		delete(s.Annotations, v1alpha1SandboxStateAnnotation)

		var original v1alpha1State
		if err := json.Unmarshal([]byte(stateJSON), &original); err != nil {
			return fmt.Errorf("unmarshal v1alpha1 sandbox state: %w", err)
		}

		// Restore replicas field from original if OperatingMode matches original intent
		switch src.Spec.OperatingMode {
		case v1beta1.SandboxOperatingModeSuspended:
			s.Spec.Replicas = new(int32(0))
		case v1beta1.SandboxOperatingModeRunning:
			if original.Spec.Replicas == nil || *original.Spec.Replicas != 0 {
				s.Spec.Replicas = original.Spec.Replicas
			} else {
				s.Spec.Replicas = new(int32(1))
			}
		}
	}

	return nil
}

func ConvertSpecTo(src *SandboxSpec, dst *v1beta1.SandboxSpec) {
	ConvertPodTemplateTo(&src.PodTemplate, &dst.PodTemplate)

	if src.VolumeClaimTemplates != nil {
		dst.VolumeClaimTemplates = make([]v1beta1.PersistentVolumeClaimTemplate, len(src.VolumeClaimTemplates))
		for i := range src.VolumeClaimTemplates {
			ConvertPVCClaimTemplateTo(&src.VolumeClaimTemplates[i], &dst.VolumeClaimTemplates[i])
		}
	} else {
		dst.VolumeClaimTemplates = nil
	}

	ConvertLifecycleTo(&src.Lifecycle, &dst.Lifecycle)

	if src.Replicas != nil && *src.Replicas == 0 {
		dst.OperatingMode = v1beta1.SandboxOperatingModeSuspended
	} else {
		dst.OperatingMode = v1beta1.SandboxOperatingModeRunning
	}

	dst.Service = src.Service
}

func ConvertSpecFrom(src *v1beta1.SandboxSpec, dst *SandboxSpec) {
	ConvertPodTemplateFrom(&src.PodTemplate, &dst.PodTemplate)

	if src.VolumeClaimTemplates != nil {
		dst.VolumeClaimTemplates = make([]PersistentVolumeClaimTemplate, len(src.VolumeClaimTemplates))
		for i := range src.VolumeClaimTemplates {
			ConvertPVCClaimTemplateFrom(&src.VolumeClaimTemplates[i], &dst.VolumeClaimTemplates[i])
		}
	} else {
		dst.VolumeClaimTemplates = nil
	}

	ConvertLifecycleFrom(&src.Lifecycle, &dst.Lifecycle)

	if src.OperatingMode == v1beta1.SandboxOperatingModeSuspended {
		dst.Replicas = new(int32(0))
	} else {
		dst.Replicas = new(int32(1))
	}

	dst.Service = src.Service
}

func ConvertStatusTo(src *SandboxStatus, dst *v1beta1.SandboxStatus) {
	dst.ServiceFQDN = src.ServiceFQDN
	dst.Service = src.Service
	dst.Conditions = src.Conditions
	dst.LabelSelector = src.LabelSelector
	dst.PodIPs = src.PodIPs
	dst.NodeName = "" // NodeName is new in v1beta1 and does not exist in v1alpha1
}

func ConvertStatusFrom(src *v1beta1.SandboxStatus, dst *SandboxStatus) {
	dst.ServiceFQDN = src.ServiceFQDN
	dst.Service = src.Service
	dst.Conditions = src.Conditions
	dst.LabelSelector = src.LabelSelector
	dst.PodIPs = src.PodIPs
}

func ConvertPodTemplateTo(src *PodTemplate, dst *v1beta1.PodTemplate) {
	dst.Spec = src.Spec
	ConvertPodMetadataTo(&src.ObjectMeta, &dst.ObjectMeta)
}

func ConvertPodTemplateFrom(src *v1beta1.PodTemplate, dst *PodTemplate) {
	dst.Spec = src.Spec
	ConvertPodMetadataFrom(&src.ObjectMeta, &dst.ObjectMeta)
}

func ConvertPodMetadataTo(src *PodMetadata, dst *v1beta1.PodMetadata) {
	*dst = v1beta1.PodMetadata(*src)
}

func ConvertPodMetadataFrom(src *v1beta1.PodMetadata, dst *PodMetadata) {
	*dst = PodMetadata(*src)
}

func ConvertPVCClaimTemplateTo(src *PersistentVolumeClaimTemplate, dst *v1beta1.PersistentVolumeClaimTemplate) {
	dst.Spec = src.Spec
	dst.EmbeddedObjectMetadata = v1beta1.EmbeddedObjectMetadata(src.EmbeddedObjectMetadata)
}

func ConvertPVCClaimTemplateFrom(src *v1beta1.PersistentVolumeClaimTemplate, dst *PersistentVolumeClaimTemplate) {
	dst.Spec = src.Spec
	dst.EmbeddedObjectMetadata = EmbeddedObjectMetadata(src.EmbeddedObjectMetadata)
}

func ConvertLifecycleTo(src *Lifecycle, dst *v1beta1.Lifecycle) {
	dst.ShutdownTime = src.ShutdownTime
	if src.ShutdownPolicy != nil {
		dst.ShutdownPolicy = new(v1beta1.ShutdownPolicy(*src.ShutdownPolicy))
	} else {
		dst.ShutdownPolicy = nil
	}
}

func ConvertLifecycleFrom(src *v1beta1.Lifecycle, dst *Lifecycle) {
	dst.ShutdownTime = src.ShutdownTime
	if src.ShutdownPolicy != nil {
		dst.ShutdownPolicy = new(ShutdownPolicy(*src.ShutdownPolicy))
	} else {
		dst.ShutdownPolicy = nil
	}
}
