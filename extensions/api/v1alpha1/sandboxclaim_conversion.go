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
	"encoding/json"
	"fmt"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/conversion"

	sandboxv1alpha1 "github.com/cocoonstack/sandbox-operator/api/v1alpha1"
	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	v1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
)

const (
	v1alpha1SandboxClaimStateAnnotation = "api.agents.x-k8s.io/v1alpha1-sandboxclaim-state"

	// v1beta1SandboxClaimVolumeClaimTemplatesAnnotation preserves the v1beta1-only
	// spec.volumeClaimTemplates field across the lossy v1alpha1 representation, so a
	// v1beta1-authored claim keeps its PVCs (and forced cold-start) after a round trip
	// through any v1alpha1 client. Symmetric to the SandboxTemplate
	// VolumeClaimTemplatesPolicy annotation.
	v1beta1SandboxClaimVolumeClaimTemplatesAnnotation = "api.agents.x-k8s.io/v1beta1-sandboxclaim-volume-claim-templates"

	v1beta1SandboxClaimWarmPoolRefAnnotation = "api.agents.x-k8s.io/v1beta1-sandboxclaim-warm-pool-ref"
)

// ConvertTo converts this SandboxClaim to the Hub version (v1beta1).
func (s *SandboxClaim) ConvertTo(dstRaw conversion.Hub) error {
	dst := dstRaw.(*v1beta1.SandboxClaim)

	s.ObjectMeta.DeepCopyInto(&dst.ObjectMeta)
	convertClaimSpecTo(&s.Spec, &dst.Spec, s.Name, s.Status.SandboxStatus.Name)
	convertClaimStatusTo(&s.Status, &dst.Status)

	// Restore the v1beta1-only volumeClaimTemplates from its preservation
	// annotation, so a v1beta1-authored claim keeps its PVCs after a v1alpha1 hop.
	if raw, ok := s.Annotations[v1beta1SandboxClaimVolumeClaimTemplatesAnnotation]; ok {
		var vcts []sandboxv1beta1.PersistentVolumeClaimTemplate
		if err := json.Unmarshal([]byte(raw), &vcts); err != nil {
			return fmt.Errorf("unmarshal v1beta1 SandboxClaim volumeClaimTemplates: %w", err)
		}
		dst.Spec.VolumeClaimTemplates = vcts
		if dst.Annotations != nil {
			delete(dst.Annotations, v1beta1SandboxClaimVolumeClaimTemplatesAnnotation)
		}
	}

	if hubPool, ok := s.Annotations[v1beta1SandboxClaimWarmPoolRefAnnotation]; ok {
		if s.Spec.WarmPool != nil && string(*s.Spec.WarmPool) == hubPool {
			dst.Spec.WarmPoolRef.Name = hubPool
		}
		delete(dst.Annotations, v1beta1SandboxClaimWarmPoolRefAnnotation)
	}

	return stashClaimState(dst, s.DeepCopy())
}

// ConvertFrom converts from the Hub version (v1beta1) to this SandboxClaim.
func (s *SandboxClaim) ConvertFrom(srcRaw conversion.Hub) error {
	src := srcRaw.(*v1beta1.SandboxClaim)

	src.ObjectMeta.DeepCopyInto(&s.ObjectMeta)
	convertClaimSpecFrom(&src.Spec, &s.Spec)
	convertClaimStatusFrom(&src.Status, &s.Status)

	if err := restoreV1alpha1Spec(s, src); err != nil {
		return err
	}

	// Preserve the v1beta1-only volumeClaimTemplates across the v1alpha1 hop.
	// v1alpha1 SandboxClaim has no such field, so carry it in an annotation that
	// ConvertTo decodes back into spec.volumeClaimTemplates.
	if src.Spec.VolumeClaimTemplates != nil {
		raw, err := json.Marshal(src.Spec.VolumeClaimTemplates)
		if err != nil {
			return fmt.Errorf("marshal v1beta1 SandboxClaim volumeClaimTemplates: %w", err)
		}
		if s.Annotations == nil {
			s.Annotations = make(map[string]string)
		}
		s.Annotations[v1beta1SandboxClaimVolumeClaimTemplatesAnnotation] = string(raw)
	} else if s.Annotations != nil {
		delete(s.Annotations, v1beta1SandboxClaimVolumeClaimTemplatesAnnotation)
	}

	if pool := src.Spec.WarmPoolRef.Name; pool != "" && !WarmPoolPolicy(pool).IsSpecificPool() {
		if s.Annotations == nil {
			s.Annotations = make(map[string]string)
		}
		s.Annotations[v1beta1SandboxClaimWarmPoolRefAnnotation] = pool
	} else if s.Annotations != nil {
		delete(s.Annotations, v1beta1SandboxClaimWarmPoolRefAnnotation)
	}

	return nil
}

// restoreV1alpha1Spec replays the v1alpha1 spec stashed on the way up, so a
// round trip through the hub is lossless. The stash annotation is consumed here
// rather than returned to clients, who would send it back on the next update.
func restoreV1alpha1Spec(s *SandboxClaim, src *v1beta1.SandboxClaim) error {
	stateJSON, ok := s.Annotations[v1alpha1SandboxClaimStateAnnotation]
	if !ok {
		return nil
	}
	delete(s.Annotations, v1alpha1SandboxClaimStateAnnotation)

	var original SandboxClaim
	if err := json.Unmarshal([]byte(stateJSON), &original); err != nil {
		return fmt.Errorf("unmarshal v1alpha1 SandboxClaim state: %w", err)
	}

	// The template ref is kept either way: when the hub's warm pool changed there
	// is no way to derive the new one, so the stashed value is the best fallback.
	s.Spec.TemplateRef = original.Spec.TemplateRef

	expected := original.Spec.TemplateRef.Name
	if original.Spec.WarmPool != nil && original.Spec.WarmPool.IsSpecificPool() {
		expected = string(*original.Spec.WarmPool)
	}
	if isWarmPoolRefMatching(src.Spec.WarmPoolRef.Name, expected, src.Status.SandboxStatus.Name) {
		s.Spec.WarmPool = original.Spec.WarmPool
		return nil
	}
	s.Spec.WarmPool = new(WarmPoolPolicy(src.Spec.WarmPoolRef.Name))
	return nil
}

func isWarmPoolRefMatching(actualName, expectedName, sandboxName string) bool {
	if actualName == expectedName {
		return true
	}
	if actualName == ShadowPoolPrefix+expectedName {
		return true
	}
	if sandboxName != "" && (actualName == sandboxName || actualName == stripRandomSuffix(sandboxName)) {
		return true
	}
	return false
}

func stripRandomSuffix(name string) string {
	if head, _, ok := strings.CutLast(name, "-"); ok {
		return head
	}
	return name
}

func convertClaimSpecTo(src *SandboxClaimSpec, dst *v1beta1.SandboxClaimSpec, claimName, sandboxName string) {
	if src.Lifecycle != nil {
		dst.Lifecycle = &v1beta1.Lifecycle{
			ShutdownTime:            src.Lifecycle.ShutdownTime,
			TTLSecondsAfterFinished: src.Lifecycle.TTLSecondsAfterFinished,
			ShutdownPolicy:          v1beta1.ShutdownPolicy(src.Lifecycle.ShutdownPolicy),
		}
	} else {
		dst.Lifecycle = nil
	}

	if src.WarmPool != nil && src.WarmPool.IsSpecificPool() {
		dst.WarmPoolRef = v1beta1.SandboxWarmPoolRef{
			Name: string(*src.WarmPool),
		}
	} else {
		if sandboxName != "" && claimName != sandboxName {
			dst.WarmPoolRef = v1beta1.SandboxWarmPoolRef{
				Name: stripRandomSuffix(sandboxName),
			}
		} else {
			dst.WarmPoolRef = v1beta1.SandboxWarmPoolRef{
				Name: ShadowPoolPrefix + src.TemplateRef.Name,
			}
		}
	}

	sandboxv1alpha1.ConvertPodMetadataTo(&src.AdditionalPodMetadata, &dst.AdditionalPodMetadata)

	if src.Env != nil {
		dst.Env = make([]v1beta1.EnvVar, len(src.Env))
		for i := range src.Env {
			dst.Env[i] = v1beta1.EnvVar{
				Name:          src.Env[i].Name,
				Value:         src.Env[i].Value,
				ContainerName: src.Env[i].ContainerName,
			}
		}
	} else {
		dst.Env = nil
	}
}

func convertClaimSpecFrom(src *v1beta1.SandboxClaimSpec, dst *SandboxClaimSpec) {
	if src.Lifecycle != nil {
		dst.Lifecycle = &Lifecycle{
			ShutdownTime:            src.Lifecycle.ShutdownTime,
			TTLSecondsAfterFinished: src.Lifecycle.TTLSecondsAfterFinished,
			ShutdownPolicy:          ShutdownPolicy(src.Lifecycle.ShutdownPolicy),
		}
	} else {
		dst.Lifecycle = nil
	}

	if templateName, ok := strings.CutPrefix(src.WarmPoolRef.Name, ShadowPoolPrefix); ok {
		dst.TemplateRef = SandboxTemplateRef{
			Name: templateName,
		}
		dst.WarmPool = new(WarmPoolPolicyDefault)
	} else {
		dst.WarmPool = new(WarmPoolPolicy(src.WarmPoolRef.Name))
		dst.TemplateRef = SandboxTemplateRef{
			Name: src.WarmPoolRef.Name,
		}
	}

	sandboxv1alpha1.ConvertPodMetadataFrom(&src.AdditionalPodMetadata, &dst.AdditionalPodMetadata)

	if src.Env != nil {
		dst.Env = make([]EnvVar, len(src.Env))
		for i := range src.Env {
			dst.Env[i] = EnvVar{
				Name:          src.Env[i].Name,
				Value:         src.Env[i].Value,
				ContainerName: src.Env[i].ContainerName,
			}
		}
	} else {
		dst.Env = nil
	}
}

func convertClaimStatusTo(src *SandboxClaimStatus, dst *v1beta1.SandboxClaimStatus) {
	dst.Conditions = src.Conditions
	dst.SandboxStatus = v1beta1.SandboxStatus{
		Name:   src.SandboxStatus.Name,
		PodIPs: src.SandboxStatus.PodIPs,
	}
}

func convertClaimStatusFrom(src *v1beta1.SandboxClaimStatus, dst *SandboxClaimStatus) {
	dst.Conditions = src.Conditions
	dst.SandboxStatus = SandboxStatus{
		Name:   src.SandboxStatus.Name,
		PodIPs: src.SandboxStatus.PodIPs,
	}
}

func stashClaimState(dst *v1beta1.SandboxClaim, sCopy *SandboxClaim) error {
	delete(sCopy.Annotations, v1alpha1SandboxClaimStateAnnotation)
	stateJSON, err := json.Marshal(sCopy)
	if err != nil {
		return fmt.Errorf("marshal v1alpha1 SandboxClaim state: %w", err)
	}
	if dst.Annotations == nil {
		dst.Annotations = map[string]string{}
	}
	dst.Annotations[v1alpha1SandboxClaimStateAnnotation] = string(stateJSON)
	return nil
}
