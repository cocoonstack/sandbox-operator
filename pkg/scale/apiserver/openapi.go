package apiserver

import (
	openapicommon "k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/validation/spec"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
)

// The canonical Go package names openapinamer derives from the Scheme for the
// aggregated types; the model map MUST be keyed by them so the namer resolves
// the sandboxes resource and its action subresources to these definitions.
const (
	sandboxDefPrefix = "sigs.k8s.io/agent-sandbox/api/v1beta1."
	actionDefPrefix  = "github.com/cocoonstack/sandbox-operator/api/v1beta1."
)

// gvkExtension is the x-kubernetes-group-version-kind marker the managed-fields
// TypeConverter (managedfields/internal.indexModels) reads to map a GVK to its
// model. WITHOUT it, ObjectToTyped(sandbox) returns NoCorrespondingTypeError,
// which the create handler swallows as a "[SHOULD NOT HAPPEN] failed to update
// managedFields" error on every write. This is the whole reason this file exists.
func gvkExtension(kind string) spec.Extensions {
	return spec.Extensions{
		"x-kubernetes-group-version-kind": []any{
			map[string]any{
				"group":   sandboxv1beta1.GroupVersion.Group,
				"version": sandboxv1beta1.GroupVersion.Version,
				"kind":    kind,
			},
		},
	}
}

func stringSchema() spec.Schema {
	return spec.Schema{Type: spec.StringOrArray{"string"}}
}

// preserveUnknownObject is an object whose interior is left untyped. The
// aggregated server synthesizes Sandboxes and stores NOTHING in etcd, so
// server-side-apply field tracking is meaningless here — a coarse schema that
// lets the TypeConverter build a valid model for the Sandbox GVK is all the
// field manager needs. Full-fidelity per-field SSA would require openapi-gen
// over the embedded core/v1.PodSpec graph and buy us nothing.
func preserveUnknownObject() spec.Schema {
	return spec.Schema{
		Type:       spec.StringOrArray{"object"},
		Extensions: spec.Extensions{"x-kubernetes-preserve-unknown-fields": true},
	}
}

// sandboxOpenAPIDefinitions supplies just enough OpenAPI to register the
// Sandbox and SandboxList GVKs with the managed-fields TypeConverter.
func sandboxOpenAPIDefinitions(ref openapicommon.ReferenceCallback) map[string]openapicommon.OpenAPIDefinition {
	sandbox := openapicommon.OpenAPIDefinition{
		Schema: spec.Schema{
			VendorExtensible: spec.VendorExtensible{Extensions: gvkExtension("Sandbox")},
			SchemaProps: spec.SchemaProps{
				Type: spec.StringOrArray{"object"},
				Properties: map[string]spec.Schema{
					"kind":       stringSchema(),
					"apiVersion": stringSchema(),
					"metadata":   preserveUnknownObject(),
					"spec":       preserveUnknownObject(),
					"status":     preserveUnknownObject(),
				},
			},
		},
	}
	list := openapicommon.OpenAPIDefinition{
		Schema: spec.Schema{
			VendorExtensible: spec.VendorExtensible{Extensions: gvkExtension("SandboxList")},
			SchemaProps: spec.SchemaProps{
				Type: spec.StringOrArray{"object"},
				Properties: map[string]spec.Schema{
					"kind":       stringSchema(),
					"apiVersion": stringSchema(),
					"metadata":   preserveUnknownObject(),
					"items": {
						SchemaProps: spec.SchemaProps{
							Type: spec.StringOrArray{"array"},
							Items: &spec.SchemaOrArray{
								Schema: &spec.Schema{
									Ref: ref(sandboxDefPrefix + "Sandbox"),
								},
							},
						},
					},
				},
			},
		},
		Dependencies: []string{sandboxDefPrefix + "Sandbox"},
	}
	defs := map[string]openapicommon.OpenAPIDefinition{
		sandboxDefPrefix + "Sandbox":     sandbox,
		sandboxDefPrefix + "SandboxList": list,
	}
	// The action-subresource bodies embed metav1.TypeMeta, and the definition
	// builder walks embedded field types through this map, not the Scheme.
	// Without this entry it aborts the whole group install with "cannot find
	// model definition for ...meta.v1.TypeMeta" — a crash loop on rollout, not
	// a degraded feature.
	typeMeta := openapicommon.OpenAPIDefinition{
		Schema: spec.Schema{
			SchemaProps: spec.SchemaProps{
				Type: spec.StringOrArray{"object"},
				Properties: map[string]spec.Schema{
					"kind":       stringSchema(),
					"apiVersion": stringSchema(),
				},
			},
		},
	}
	// Registered under BOTH spellings: the namer keys models by Go package path,
	// while the definition builder resolves embedded fields by their canonical
	// ("friendly") OpenAPI name. Only one of them being present still aborts the
	// group install.
	defs["k8s.io/apimachinery/pkg/apis/meta/v1.TypeMeta"] = typeMeta
	defs["io.k8s.apimachinery.pkg.apis.meta.v1.TypeMeta"] = typeMeta
	for _, kind := range []string{
		"SandboxPauseOptions",
		"SandboxResumeOptions",
		"SandboxForkOptions",
		"SandboxForkResult",
		"SandboxSnapshotOptions",
		"SandboxSnapshotResult",
	} {
		defs[actionDefPrefix+kind] = actionDefinition(kind)
	}
	return defs
}

// actionDefinition is the model for one action-subresource body. Like the
// Sandbox model it is deliberately coarse: these types are never persisted, so
// per-field server-side-apply tracking buys nothing — the model exists so the
// TypeConverter can resolve the GVK at all.
func actionDefinition(kind string) openapicommon.OpenAPIDefinition {
	ext := gvkExtension(kind)
	// These bodies carry their own fields (count, ttlSeconds, children, ...).
	// Declaring only kind/apiVersion would make every other field "unknown" to
	// the field manager, so the interior is preserved rather than enumerated —
	// the same trade the Sandbox model makes, and for the same reason: nothing
	// here is persisted, so per-field tracking buys nothing.
	ext["x-kubernetes-preserve-unknown-fields"] = true
	return openapicommon.OpenAPIDefinition{
		Schema: spec.Schema{
			VendorExtensible: spec.VendorExtensible{Extensions: ext},
			SchemaProps: spec.SchemaProps{
				Type: spec.StringOrArray{"object"},
				Properties: map[string]spec.Schema{
					"kind":       stringSchema(),
					"apiVersion": stringSchema(),
				},
			},
		},
	}
}
