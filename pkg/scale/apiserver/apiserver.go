package apiserver

import (
	"fmt"

	openapinamer "k8s.io/apiserver/pkg/endpoints/openapi"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	openapicommon "k8s.io/kube-openapi/pkg/common"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

// NewOpenAPIV3Config returns the OpenAPIV3 config the aggregated server installs
// with. A non-nil config is mandatory for InstallAPIGroup (managed fields).
//
// The sandboxes group is WRITABLE (rest.Creater), so its path must NOT be
// ignored: getResourceNamesForGroup skips ignored paths, which would leave the
// managed-fields TypeConverter without a Sandbox model and make every Create log
// "[SHOULD NOT HAPPEN] failed to update managedFields". sandboxOpenAPIDefinitions
// supplies the minimal model (with the x-kubernetes-group-version-kind marker)
// the TypeConverter needs.
func NewOpenAPIV3Config() *openapicommon.OpenAPIV3Config {
	cfg := genericapiserver.DefaultOpenAPIV3Config(sandboxOpenAPIDefinitions, openapinamer.NewDefinitionNamer(Scheme))
	cfg.Info.Title = "sandbox-aggregated-apiserver"
	return cfg
}

// InstallSandboxAPI installs the sandboxes.agents.x-k8s.io group (version
// v1beta1, resource "sandboxes") into server, backed by the scatter-gather
// store. This is the metrics.k8s.io-style wiring: a custom rest.Storage rather
// than the etcd generic registry.
func InstallSandboxAPI(server *genericapiserver.GenericAPIServer, store scale.SandboxStore) error {
	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(sandboxv1beta1.GroupVersion.Group, Scheme, ParameterCodec, Codecs)
	apiGroupInfo.VersionedResourcesStorageMap[sandboxv1beta1.GroupVersion.Version] = map[string]rest.Storage{
		"sandboxes": NewSandboxREST(store),
		// Action subresources. A map key with a slash installs the tail as a
		// subresource of the head, so the standard Sandbox schema is untouched.
		"sandboxes/pause":    NewSandboxPauseREST(store),
		"sandboxes/resume":   NewSandboxResumeREST(store),
		"sandboxes/fork":     NewSandboxForkREST(store),
		"sandboxes/snapshot": NewSandboxSnapshotREST(store),
	}
	if err := server.InstallAPIGroup(&apiGroupInfo); err != nil {
		return fmt.Errorf("apiserver: install sandboxes API group: %w", err)
	}
	return nil
}
