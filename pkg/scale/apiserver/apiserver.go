package apiserver

import (
	"fmt"

	openapinamer "k8s.io/apiserver/pkg/endpoints/openapi"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	restclient "k8s.io/client-go/rest"
	basecompatibility "k8s.io/component-base/compatibility"
	openapicommon "k8s.io/kube-openapi/pkg/common"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
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

// NewInProcessServer builds a minimal GenericAPIServer with the sandboxes API
// installed, suitable for in-process serving via httptest. Authentication and
// authorization are left nil (the generic handler chain treats nil as
// "disabled", i.e. pass-through) so the aggregation acceptance harness can
// exercise the real client-go → apiserver storage code path without TLS or
// certificate machinery.
func NewInProcessServer(name string, store scale.SandboxStore) (*genericapiserver.GenericAPIServer, error) {
	config := genericapiserver.NewConfig(Codecs)
	config.ExternalAddress = "localhost:443"
	config.LoopbackClientConfig = &restclient.Config{}
	config.EffectiveVersion = basecompatibility.NewEffectiveVersionFromString("", "", "")
	config.OpenAPIV3Config = NewOpenAPIV3Config()

	server, err := config.Complete(nil).New(name, genericapiserver.NewEmptyDelegate())
	if err != nil {
		return nil, fmt.Errorf("apiserver: build generic server: %w", err)
	}
	if err := InstallSandboxAPI(server, store); err != nil {
		return nil, err
	}
	return server, nil
}
