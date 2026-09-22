// Command sandbox-apiserver is the L3 aggregated apiserver for
// sandboxes.agents.x-k8s.io. It serves the resource by scatter-gathering
// per-node NodeInventory objects (the metrics.k8s.io pattern) and stores NO
// per-sandbox object in etcd. It is registered with the kube-apiserver via the
// APIService in config/apiservice/, exactly as metrics-server is.
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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	genericapiserver "k8s.io/apiserver/pkg/server"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	apiservercompatibility "k8s.io/apiserver/pkg/util/compatibility"
	restclient "k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	extv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	cocoonv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/e2bcompat"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
	sandboxapiserver "github.com/cocoonstack/sandbox-operator/pkg/scale/apiserver"
	"github.com/cocoonstack/sandbox-operator/pkg/scale/warmpool"
)

const (
	// inventoryCacheSyncTimeout bounds the startup wait for the NodeInventory
	// informer to sync. If the NodeInventory CRD is not installed or the
	// kube-apiserver is unreachable, the binary fails loud instead of serving
	// empty sandbox lists.
	inventoryCacheSyncTimeout = 2 * time.Minute

	// e2bReadHeaderTimeout bounds how long a client may take to send its request
	// headers on the e2b surface, so a stalled connection cannot pin a handler.
	e2bReadHeaderTimeout = 10 * time.Second
	// e2bShutdownTimeout bounds the graceful drain of in-flight e2b requests.
	e2bShutdownTimeout = 10 * time.Second
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

	// SandboxdToken is the uniform fleet-wide sandboxd api_token presented on the
	// node-local claim/release verbs. SandboxdTokenFile, when set, is read at
	// startup (a Secret mount) and takes precedence. When both are empty the
	// Create/Delete write path stays disabled (fails closed).
	SandboxdToken     string
	SandboxdTokenFile string

	// WarmPoolDriver enables the in-process SandboxWarmPool → sandboxd pool
	// reconcile loop (the control-plane surface for warm capacity). Pool-level,
	// O(pools+nodes); never per-sandbox. WarmPoolInterval is its resync cadence.
	WarmPoolDriver   bool
	WarmPoolInterval time.Duration

	// E2B* configure the optional e2b-compatible REST surface, which lets an
	// unmodified e2b SDK drive the same warm pools. It is off by default and
	// serves on its own address, so the aggregated API is never affected.
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
		"Namespace e2b claims are made in; e2b has no namespace concept, so every compat claim lands here.")
	fs.StringVar(&o.E2BDomain, "e2b-domain", o.E2BDomain,
		"Base domain the SDK derives the in-sandbox envd host from, as {port}-{sandboxID}.{domain}. Required with --enable-e2b-api: without it a created sandbox has no reachable data plane.")
	fs.StringVar(&o.E2BEnvdVersion, "e2b-envd-version", o.E2BEnvdVersion,
		"envd version reported to the SDK. It must name the envd actually installed in the pool's image; the SDK version-compares it and kills the sandbox when it cannot parse one.")
	fs.IntVar(&o.E2BTimeoutSeconds, "e2b-default-timeout", o.E2BTimeoutSeconds,
		"Lease in seconds granted to a create that names no timeout, and the lease an SDK refresh renews for.")
	fs.StringVar(&o.E2BAPIKeyFile, "e2b-api-key-file", o.E2BAPIKeyFile,
		"Path to a file (Secret mount) of accepted e2b API keys, one per line, presented by the SDK as X-API-KEY.")
	fs.BoolVar(&o.E2BAllowAnonymous, "e2b-allow-anonymous", o.E2BAllowAnonymous,
		"Serve the e2b surface with NO API key. Development only: it leaves the claim endpoint open to anyone who can reach the port.")
}

// e2bAPIKeys reads the accepted e2b API keys from the key file, one per line.
// Blank lines and #-comments are ignored.
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

	// Route controller-runtime logs (the warm-pool driver's) through klog so they
	// land in the apiserver's own log stream instead of being silently discarded.
	ctrl.SetLogger(klog.NewKlogr())
	ctx := genericapiserver.SetupSignalContext()

	// The store reads NodeInventory objects through a cache-fed reader: the
	// O(nodes) enumeration is served from an informer, never a hot-path LIST
	// against the kube-apiserver/etcd.
	restCfg, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("load kube config: %w", err)
	}
	reader, err := startInventoryCache(ctx, restCfg)
	if err != nil {
		return err
	}
	token, err := o.resolveSandboxdToken()
	if err != nil {
		return err
	}
	// WithClaimRouting wires the node-local claim/release write path: the uniform
	// fleet token plus a per-node sandboxd HTTP client keyed on each node's
	// advertised address (NodeInventory.Address). Empty token leaves it fail-closed.
	invSource := scale.NewClientInventorySource(reader)
	store := scale.NewScatterGatherStore(
		invSource,
		scale.WithClaimRouting(token, scale.NewSandboxdClientFactory()),
	)

	// Optional in-process components sharing the apiserver's inventory cache.
	if o.WarmPoolDriver {
		if err = startWarmPoolDriver(ctx, restCfg, token, o.WarmPoolInterval, invSource); err != nil {
			return err
		}
	}
	if o.E2BAPI {
		if err = startE2BServer(ctx, o, store, invSource); err != nil {
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
	if err := sandboxapiserver.InstallSandboxAPI(server, store); err != nil {
		return err
	}
	return server.PrepareRun().RunWithContext(ctx)
}

// startInventoryCache builds, starts, and syncs a controller-runtime cache
// scoped to exactly the NodeInventory GVK, returning it as the store's reader.
// ReaderFailOnMissingInformer makes a read of any other GVK fail loudly instead
// of silently spawning a cluster-wide informer, so the cache watches nothing
// but the O(nodes) NodeInventory objects.
func startInventoryCache(ctx context.Context, restCfg *restclient.Config) (cache.Cache, error) {
	inv := &unstructured.Unstructured{}
	inv.SetGroupVersionKind(scale.NodeInventoryGVK)
	invCache, err := cache.New(restCfg, cache.Options{
		ByObject:                    map[client.Object]cache.ByObject{inv: {}},
		ReaderFailOnMissingInformer: true,
	})
	if err != nil {
		return nil, fmt.Errorf("build inventory cache: %w", err)
	}
	if _, err := invCache.GetInformer(ctx, inv); err != nil {
		return nil, fmt.Errorf("register node inventory informer: %w", err)
	}
	cacheErr := make(chan error, 1)
	go func() { cacheErr <- invCache.Start(ctx) }()
	syncCtx, cancel := context.WithTimeout(ctx, inventoryCacheSyncTimeout)
	defer cancel()
	if !invCache.WaitForCacheSync(syncCtx) {
		select {
		case err := <-cacheErr:
			if err != nil {
				return nil, fmt.Errorf("run inventory cache: %w", err)
			}
		default:
		}
		return nil, fmt.Errorf("node inventory cache did not sync within %s (is the %s CRD installed?)",
			inventoryCacheSyncTimeout, scale.NodeInventoryGVK.GroupKind())
	}
	return invCache, nil
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
func startWarmPoolDriver(ctx context.Context, restCfg *restclient.Config, token string, interval time.Duration, inv scale.InventorySource) error {
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
			klog.ErrorS(err, "warm-pool manager exited")
		}
	}()
	return nil
}

// startE2BServer shares the aggregated apiserver's store, so a claim made here is the same node-local claim, released
// the same way, and listed by the same scatter-gather read.
func startE2BServer(ctx context.Context, o *options, store scale.SandboxStore, inv scale.InventorySource) error {
	keys, err := o.e2bAPIKeys()
	if err != nil {
		return err
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
		return err
	}
	httpSrv := &http.Server{
		Addr:              o.E2BAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: e2bReadHeaderTimeout,
	}
	ln, err := net.Listen("tcp", o.E2BAddr)
	if err != nil {
		return fmt.Errorf("listen on e2b address %q: %w", o.E2BAddr, err)
	}
	klog.InfoS("serving e2b-compatible API", "address", o.E2BAddr, "namespace", o.E2BNamespace,
		"authenticated", len(keys) > 0)
	go func() {
		if err := httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			klog.ErrorS(err, "e2b-compatible API server exited")
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), e2bShutdownTimeout)
		defer cancel()
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			klog.ErrorS(err, "e2b-compatible API server shutdown")
		}
	}()
	return nil
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
