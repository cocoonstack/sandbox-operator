package apiserver

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/managedfields"
	genericapiserver "k8s.io/apiserver/pkg/server"
	apiservercompatibility "k8s.io/apiserver/pkg/util/compatibility"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kube-openapi/pkg/builder3"
	"k8s.io/kube-openapi/pkg/validation/spec"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"

	cocoonv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
)

func TestManagedFieldsTypeConverterResolvesSandbox(t *testing.T) {
	cfg := NewOpenAPIV3Config()
	models, err := builder3.BuildOpenAPIDefinitionsForResources(cfg,
		sandboxDefPrefix+"Sandbox",
		sandboxDefPrefix+"SandboxList",
	)
	if err != nil {
		t.Fatalf("build openapi models: %v", err)
	}
	tc, err := managedfields.NewTypeConverter(models, false)
	if err != nil {
		t.Fatalf("new type converter: %v", err)
	}

	sb := &sandboxv1beta1.Sandbox{}
	sb.SetGroupVersionKind(sandboxv1beta1.GroupVersion.WithKind("Sandbox"))
	sb.Name = "probe"
	sb.Namespace = "default"
	if _, err := tc.ObjectToTyped(sb); err != nil {
		t.Fatalf("ObjectToTyped(Sandbox) must succeed (root cause of [SHOULD NOT HAPPEN]): %v", err)
	}

	list := &sandboxv1beta1.SandboxList{}
	list.SetGroupVersionKind(sandboxv1beta1.GroupVersion.WithKind("SandboxList"))
	if _, err := tc.ObjectToTyped(list); err != nil {
		t.Fatalf("ObjectToTyped(SandboxList) must succeed: %v", err)
	}
}

func TestEveryServedTypeHasAnOpenAPIModelUnderItsOwnGoPackage(t *testing.T) {
	defs := sandboxOpenAPIDefinitions(func(path string) spec.Ref { return spec.Ref{} })

	for _, obj := range []runtime.Object{
		&sandboxv1beta1.Sandbox{},
		&sandboxv1beta1.SandboxList{},
		&cocoonv1beta1.SandboxPauseOptions{},
		&cocoonv1beta1.SandboxResumeOptions{},
		&cocoonv1beta1.SandboxForkOptions{},
		&cocoonv1beta1.SandboxForkResult{},
		&cocoonv1beta1.SandboxSnapshotOptions{},
		&cocoonv1beta1.SandboxSnapshotResult{},
	} {
		typ := reflect.TypeOf(obj).Elem()
		kind := typ.Name()
		key := typ.PkgPath() + "." + kind
		def, ok := defs[key]
		if !ok {
			t.Errorf("no OpenAPI model for %s; InstallAPIGroup will fail to start the apiserver", key)
			continue
		}
		gvks, ok := def.Schema.Extensions["x-kubernetes-group-version-kind"]
		if !ok {
			t.Errorf("%s has no x-kubernetes-group-version-kind; the managed-fields TypeConverter cannot map it", key)
			continue
		}
		entries, ok := gvks.([]any)
		if !ok || len(entries) != 1 {
			t.Errorf("%s gvk extension = %v, want exactly one entry", key, gvks)
			continue
		}
		m, _ := entries[0].(map[string]any)
		gv := sandboxv1beta1.GroupVersion
		if m["group"] != gv.Group || m["version"] != gv.Version || m["kind"] != kind {
			t.Errorf("%s declares %v/%v %v, want %s %s", key, m["group"], m["version"], m["kind"], gv, kind)
		}
	}
}

func TestOnlySandboxKindsUseTheUpstreamDefinitionPrefix(t *testing.T) {
	defs := sandboxOpenAPIDefinitions(func(path string) spec.Ref { return spec.Ref{} })

	var got []string
	for key := range defs {
		if kind, ok := strings.CutPrefix(key, sandboxDefPrefix); ok {
			got = append(got, kind)
		}
	}
	slices.Sort(got)
	if want := []string{"Sandbox", "SandboxList"}; !slices.Equal(got, want) {
		t.Errorf("definitions under %q = %v, want %v", sandboxDefPrefix, got, want)
	}
}

func TestInstallSandboxAPISucceeds(t *testing.T) {
	cfg := genericapiserver.NewConfig(Codecs)
	cfg.EffectiveVersion = apiservercompatibility.DefaultBuildEffectiveVersion()
	cfg.OpenAPIV3Config = NewOpenAPIV3Config()
	cfg.ExternalAddress = "localhost:6443"
	cfg.LoopbackClientConfig = &restclient.Config{Host: "localhost:6443"}

	server, err := cfg.Complete(nil).New("test-apiserver", genericapiserver.NewEmptyDelegate())
	if err != nil {
		t.Fatalf("build generic apiserver: %v", err)
	}
	if err := InstallSandboxAPI(server, nil); err != nil {
		t.Fatalf("InstallSandboxAPI: %v", err)
	}
}
