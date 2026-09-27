// Command sandbox-apiserver is the L3 aggregated apiserver for
// sandboxes.agents.x-k8s.io. It serves the resource by scatter-gathering
// per-node NodeInventory objects (the metrics.k8s.io pattern) and stores NO
// per-sandbox object in etcd. It is registered with the kube-apiserver via the
// APIService the helm chart installs, exactly as metrics-server is.
package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/projecteru2/core/log"
	"github.com/projecteru2/core/types"
	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/runtime"
	genericapiserver "k8s.io/apiserver/pkg/server"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	apiservercompatibility "k8s.io/apiserver/pkg/util/compatibility"
	restclient "k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	extv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	cocoonv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/e2bcompat"
	"github.com/cocoonstack/sandbox-operator/pkg/logbridge"
	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
	sandboxapiserver "github.com/cocoonstack/sandbox-operator/pkg/scale/apiserver"
	"github.com/cocoonstack/sandbox-operator/pkg/scale/kubeinventory"
	"github.com/cocoonstack/sandbox-operator/pkg/scale/warmpool"
	"github.com/cocoonstack/sandbox-operator/version"
)

// options has no etcd option because this server stores nothing.
type options struct {
	SecureServing  *genericoptions.SecureServingOptionsWithLoopback
	Authentication *genericoptions.DelegatingAuthenticationOptions
	Authorization  *genericoptions.DelegatingAuthorizationOptions
	Features       *genericoptions.FeatureOptions

	SandboxdToken     string
	SandboxdTokenFile string

	WarmPoolDriver   bool
	WarmPoolInterval time.Duration

	E2BAPI bool
	E2B    *e2bcompat.Flags
}

func newOptions() *options {
	o := &options{
		SecureServing:  genericoptions.NewSecureServingOptions().WithLoopback(),
		Authentication: genericoptions.NewDelegatingAuthenticationOptions(),
		Authorization:  genericoptions.NewDelegatingAuthorizationOptions(),
		Features:       genericoptions.NewFeatureOptions(),
		WarmPoolDriver: true,
		E2B:            e2bcompat.NewFlags(),
	}
	o.SecureServing.BindPort = 6443
	o.Features.EnablePriorityAndFairness = false
	// Allow running without a remote kubeconfig (in-cluster service account).
	o.Authentication.RemoteKubeConfigFileOptional = true
	o.Authorization.RemoteKubeConfigFileOptional = true
	return o
}

func (o *options) addFlags(fs *pflag.FlagSet) {
	o.SecureServing.AddFlags(fs)
	o.Authentication.AddFlags(fs)
	o.Authorization.AddFlags(fs)
	o.Features.AddFlags(fs)
	fs.StringVar(&o.SandboxdToken, "sandboxd-token", o.SandboxdToken,
		"Uniform fleet-wide sandboxd api_token presented on node-local claim/release. Prefer --sandboxd-token-file for a Secret mount.")
	fs.StringVar(&o.SandboxdTokenFile, "sandboxd-token-file", o.SandboxdTokenFile,
		"Path to a file (Secret mount) holding the sandboxd api_token; overrides --sandboxd-token when set.")
	fs.BoolVar(&o.WarmPoolDriver, "enable-warm-pool-driver", o.WarmPoolDriver,
		"Run the in-process SandboxWarmPool → sandboxd pool reconcile loop (control-plane warm-capacity surface; pool-level, never per-sandbox).")
	fs.DurationVar(&o.WarmPoolInterval, "warm-pool-sync-interval", o.WarmPoolInterval,
		"Resync cadence for the SandboxWarmPool driver, and with it the sampling period of the warm count in pool status (0 = default 5s).")
	fs.BoolVar(&o.E2BAPI, "enable-e2b-api", o.E2BAPI,
		"Serve the e2b-compatible REST surface, so an unmodified e2b SDK can claim from the same warm pools (point E2B_API_URL at it).")
	o.E2B.AddFlags(fs)
}

// serverConfig assembles a GenericAPIServer config from the options.
func (o *options) serverConfig() (*genericapiserver.Config, error) {
	if err := o.SecureServing.MaybeDefaultWithSelfSignedCerts("localhost", nil, nil); err != nil {
		return nil, fmt.Errorf("create self-signed certificates: %w", err)
	}
	cfg := genericapiserver.NewConfig(sandboxapiserver.Codecs)
	cfg.EffectiveVersion = apiservercompatibility.DefaultBuildEffectiveVersion()
	cfg.OpenAPIV3Config = sandboxapiserver.NewOpenAPIV3Config()
	if err := o.Features.ApplyTo(cfg, nil, nil); err != nil {
		return nil, fmt.Errorf("apply features: %w", err)
	}
	if err := o.SecureServing.ApplyTo(&cfg.SecureServing, &cfg.LoopbackClientConfig); err != nil {
		return nil, fmt.Errorf("apply secure serving: %w", err)
	}
	if err := o.Authentication.ApplyTo(&cfg.Authentication, cfg.SecureServing, nil); err != nil {
		return nil, fmt.Errorf("apply authentication: %w", err)
	}
	if err := o.Authorization.ApplyTo(&cfg.Authorization); err != nil {
		return nil, fmt.Errorf("apply authorization: %w", err)
	}
	return cfg, nil
}

func run() error {
	o := newOptions()
	fs := pflag.NewFlagSet("sandbox-apiserver", pflag.ExitOnError)
	o.addFlags(fs)
	_ = fs.Parse(os.Args[1:])
	ctx, fail := context.WithCancelCause(genericapiserver.SetupSignalContext())
	defer fail(nil)
	level := cmp.Or(os.Getenv("OPERATOR_LOG_LEVEL"), "info")
	if err := log.SetupLog(ctx, &types.ServerLogConfig{Level: level, UseJSON: !stderrIsTerminal()}, ""); err != nil {
		return fmt.Errorf("setup log: %w", err)
	}
	ctrl.SetLogger(logbridge.New(ctx))
	klog.SetLogger(logbridge.New(ctx).WithName("klog"))
	log.WithFunc("main.run").Infof(ctx, "starting sandbox-apiserver version=%s revision=%s builtAt=%s", version.VERSION, version.REVISION, version.BUILTAT)

	restCfg, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("load kube config: %w", err)
	}
	reader, err := kubeinventory.NewCache(ctx, restCfg)
	if err != nil {
		return err
	}
	token, err := sandboxd.TokenFrom(o.SandboxdToken, o.SandboxdTokenFile)
	if err != nil {
		return err
	}
	invSource := kubeinventory.New(reader)
	store := scale.NewScatterGatherStore(
		invSource,
		scale.WithClaimRouting(token, scale.NewSandboxdClientFactory()),
	)

	if o.WarmPoolDriver {
		if err = startWarmPoolDriver(ctx, fail, restCfg, token, o.WarmPoolInterval, invSource); err != nil {
			return err
		}
	}
	stopE2B := func() {}
	if o.E2BAPI {
		if stopE2B, err = startE2BServer(ctx, o, store, invSource); err != nil {
			return err
		}
	}

	cfg, err := o.serverConfig()
	if err != nil {
		return err
	}
	server, err := cfg.Complete(nil).New("sandbox-apiserver", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return fmt.Errorf("build generic apiserver: %w", err)
	}
	if err = sandboxapiserver.InstallSandboxAPI(server, store); err != nil {
		return err
	}
	err = server.PrepareRun().RunWithContext(ctx)
	stopE2B()
	if cause := context.Cause(ctx); err == nil && !errors.Is(cause, context.Canceled) {
		return cause
	}
	return err
}

// startWarmPoolDriver takes the cache-fed inv because the manager client reads NodeInventory unstructured, uncached.
func startWarmPoolDriver(ctx context.Context, fail context.CancelCauseFunc, restCfg *restclient.Config, token string, interval time.Duration, inv scale.InventorySource) error {
	scheme := runtime.NewScheme()
	if err := extv1beta1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("register extensions scheme: %w", err)
	}
	if err := cocoonv1beta1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("register node inventory scheme: %w", err)
	}
	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme:                  scheme,
		Metrics:                 metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress:  "0",
		LeaderElection:          true,
		LeaderElectionID:        "cocoon-warmpool-driver",
		LeaderElectionNamespace: currentNamespace(),
	})
	if err != nil {
		return fmt.Errorf("build warm-pool manager: %w", err)
	}
	driver := warmpool.New(mgr.GetClient(), inv, token, warmpool.NewSandboxdFactory(), warmpool.Options{Interval: interval})
	if err := driver.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("set up warm-pool controller: %w", err)
	}
	go func() {
		if err := mgr.Start(ctx); err != nil {
			fail(fmt.Errorf("warm-pool manager: %w", err))
		}
	}()
	return nil
}

// startE2BServer shares the aggregated apiserver's store, so a claim made here is the same node-local claim, released
// the same way, and listed by the same scatter-gather read.
func startE2BServer(ctx context.Context, o *options, store scale.SandboxStore, inv scale.InventorySource) (func(), error) {
	opts, err := o.E2B.ServerOptions(inv)
	if err != nil {
		return nil, err
	}
	srv, err := e2bcompat.NewServer(store, opts)
	if err != nil {
		return nil, err
	}
	return srv.Serve(ctx, o.E2B.Addr)
}

// currentNamespace returns the pod's namespace (for the leader-election lease),
// read from the service-account mount, defaulting to the deployment namespace.
func currentNamespace() string {
	if b, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		if ns := strings.TrimSpace(string(b)); ns != "" {
			return ns
		}
	}
	return "sandbox-system"
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "sandbox-apiserver:", err)
		os.Exit(1)
	}
}

func stderrIsTerminal() bool {
	fi, err := os.Stderr.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}
