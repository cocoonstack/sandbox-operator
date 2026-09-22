package apiserver

import (
	openapicommon "k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/validation/spec"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
)

// sandboxDefPrefix is the Go package path the namer derives for Sandbox and SandboxList; the model map is keyed by it.
const sandboxDefPrefix = "sigs.k8s.io/agent-sandbox/api/v1beta1."

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
	// The action bodies embed only metav1.TypeMeta, whose generated
	// OpenAPIModelName is promoted, so the namer resolves every subresource to
	// this model; without it the whole group install aborts.
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
	return defs
}
