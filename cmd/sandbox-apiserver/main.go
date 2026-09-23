// Command sandbox-apiserver is the L3 aggregated apiserver for
// sandboxes.agents.x-k8s.io. It serves the resource by scatter-gathering
// per-node NodeInventory objects (the metrics.k8s.io pattern) and stores NO
// per-sandbox object in etcd. It is registered with the kube-apiserver via the
// APIService the helm chart installs, exactly as metrics-server is.
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

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
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
	sandboxapiserver "github.com/cocoonstack/sandbox-operator/pkg/scale/apiserver"
	"github.com/cocoonstack/sandbox-operator/pkg/scale/warmpool"
	"github.com/cocoonstack/sandbox-operator/version"
)

const (
	e2bReadHeaderTimeout = 10 * time.Second
	e2bShutdownTimeout   = 10 * time.Second
)

// options are the standard aggregated-apiserver options: secure serving plus
// delegated authentication/authorization (token/SAR review against the host
// kube-apiserver). There is deliberately no etcd option — this server stores
// nothing. The sandboxd token wires the node-local claim/release write path.
type options struct {
	SecureServing  *genericoptions.SecureServingOptionsWithLoopback
	Authentication *genericoptions.DelegatingAuthenticationOptions
	Authorization  *genericoptions.DelegatingAuthorizationOptions
	Features       *genericoptions.FeatureOptions

	SandboxdToken     string
	SandboxdTokenFile string

	WarmPoolDriver   bool
	WarmPoolInterval time.Duration

	E2BAPI            bool
	E2BAddr           string
	E2BNamespace      string
	E2BDomain         string
	E2BEnvdVersion    string
	E2BTimeoutSeconds int
	E2BAPIKeyFile     string
	E2BAllowAnonymous bool
}

func newOptions() *options {
	o := &options{
		SecureServing:  genericoptions.NewSecureServingOptions().WithLoopback(),
		Authentication: genericoptions.NewDelegatingAuthenticationOptions(),
		Authorization:  genericoptions.NewDelegatingAuthorizationOptions(),
		Features:       genericoptions.NewFeatureOptions(),
		WarmPoolDriver: true,
		E2BAddr:        ":8080",
		E2BNamespace:   "default",
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
	fs.StringVar(&o.E2BAddr, "e2b-bind-address", o.E2BAddr,
		"Address the e2b-compatible surface listens on.")
	fs.StringVar(&o.E2BNamespace, "e2b-namespace", o.E2BNamespace,
		"Namespace a key that names none claims in, and where anonymous claims land; e2b has no namespace concept.")
	fs.StringVar(&o.E2BDomain, "e2b-domain", o.E2BDomain,
		"Base domain the SDK derives the in-sandbox envd host from, as {port}-{sandboxID}.{domain}. Required with --enable-e2b-api: without it a created sandbox has no reachable data plane.")
	fs.StringVar(&o.E2BEnvdVersion, "e2b-envd-version", o.E2BEnvdVersion,
		"envd version reported to the SDK. It must name the envd actually installed in the pool's image; the SDK version-compares it and kills the sandbox when it cannot parse one.")
	fs.IntVar(&o.E2BTimeoutSeconds, "e2b-default-timeout", o.E2BTimeoutSeconds,
		"Lease in seconds granted to a create that names no timeout, and the lease an SDK refresh renews for.")
	fs.StringVar(&o.E2BAPIKeyFile, "e2b-api-key-file", o.E2BAPIKeyFile,
		"Path to a file (Secret mount) of accepted e2b API keys, one per line as \"key\" or \"key namespace\", presented by the SDK as X-API-KEY; a key sees only the sandboxes and snapshots of its namespace, --e2b-namespace when none is given.")
	fs.BoolVar(&o.E2BAllowAnonymous, "e2b-allow-anonymous", o.E2BAllowAnonymous,
		"Serve the e2b surface with NO API key. Development only: it leaves the claim endpoint open to anyone who can reach the port.")
}

// e2bAPIKeys reads the accepted e2b API keys from the key file, one per line as
// "key" or "key namespace". Blank lines and #-comments are ignored.
func (o *options) e2bAPIKeys() ([]string, error) {
	if o.E2BAPIKeyFile == "" {
		return nil, nil
	}
	b, err := os.ReadFile(o.E2BAPIKeyFile)
	if err != nil {
		return nil, fmt.Errorf("read e2b api key file %q: %w", o.E2BAPIKeyFile, err)
	}
	var keys []string
	for line := range strings.SplitSeq(string(b), "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			keys = append(keys, line)
		}
	}
	return keys, nil
}

// resolveSandboxdToken returns the sandboxd token, reading it from the token file
// (a Secret mount) when one is configured, else the literal flag value.
func (o *options) resolveSandboxdToken() (string, error) {
	if o.SandboxdTokenFile == "" {
		return o.SandboxdToken, nil
	}
	b, err := os.ReadFile(o.SandboxdTokenFile)
	if err != nil {
		return "", fmt.Errorf("read sandboxd token file %q: %w", o.SandboxdTokenFile, err)
	}
	return strings.TrimSpace(string(b)), nil
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
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	klog.InfoS("starting sandbox-apiserver", "version", version.VERSION, "revision", version.REVISION, "builtAt", version.BUILTAT)

	// Route the warm-pool driver's controller-runtime logs into the apiserver's own stream.
	ctrl.SetLogger(klog.NewKlogr())
	ctx, fail := context.WithCancelCause(genericapiserver.SetupSignalContext())
	defer fail(nil)

	restCfg, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("load kube config: %w", err)
	}
	reader, err := scale.NewInventoryCache(ctx, restCfg)
	if err != nil {
		return err
	}
	token, err := o.resolveSandboxdToken()
	if err != nil {
		return err
	}
	invSource := scale.NewClientInventorySource(reader)
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

// startWarmPoolDriver runs the SandboxWarmPool driver as a controller inside a
// controller-runtime manager: it WATCHES SandboxWarmPool and NodeInventory, so a
// `kubectl apply/patch/delete` reconciles in milliseconds instead of waiting for
// a poll tick (the only latency that ever mattered — the node side fills a pool
// in under a second). Leader election makes exactly one of the apiserver replicas
// drive the pools. The manager's own metrics/health servers are disabled; the
// aggregated apiserver owns the serving port. inv is the process-wide cache-fed
// inventory source; the manager's own client would read NodeInventory
// unstructured and so bypass its cache on every node read.
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
	driver := warmpool.New(nil, inv, token, warmpool.NewSandboxdFactory(), warmpool.Options{
		Interval: interval,
		Log:      ctrl.Log.WithName("warmpool"),
	})
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
	keys, err := o.e2bAPIKeys()
	if err != nil {
		return nil, err
	}
	srv, err := e2bcompat.NewServer(store, e2bcompat.Options{
		Namespace:             o.E2BNamespace,
		Domain:                o.E2BDomain,
		EnvdVersion:           o.E2BEnvdVersion,
		DefaultTimeoutSeconds: o.E2BTimeoutSeconds,
		Inventory:             inv,
		APIKeys:               keys,
		AllowAnonymous:        o.E2BAllowAnonymous,
		Log:                   ctrl.Log.WithName("e2b"),
	})
	if err != nil {
		return nil, err
	}
	httpSrv := &http.Server{
		Addr:              o.E2BAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: e2bReadHeaderTimeout,
	}
	ln, err := net.Listen("tcp", o.E2BAddr)
	if err != nil {
		return nil, fmt.Errorf("listen on e2b address %q: %w", o.E2BAddr, err)
	}
	klog.InfoS("serving e2b-compatible API", "address", o.E2BAddr, "namespace", o.E2BNamespace,
		"authenticated", len(keys) > 0)
	go func() {
		if err := httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			klog.ErrorS(err, "e2b-compatible API server exited")
		}
	}()
	e2bCtx, stop := context.WithCancel(ctx)
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		<-e2bCtx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), e2bShutdownTimeout)
		defer cancel()
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			klog.ErrorS(err, "e2b-compatible API server shutdown")
		}
	}()
	return func() { stop(); <-drained }, nil
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
