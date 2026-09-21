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

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/felixge/fgprof"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	_ "k8s.io/client-go/plugin/pkg/client/auth" // every kubeconfig auth plugin an operator may be handed
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/controllers"
	extensionsv1alpha1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1alpha1"
	extensionsv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
	extensionscontrollers "github.com/cocoonstack/sandbox-operator/extensions/controllers"
	asmetrics "github.com/cocoonstack/sandbox-operator/internal/metrics"
	"github.com/cocoonstack/sandbox-operator/internal/queue"
	"github.com/cocoonstack/sandbox-operator/internal/version"
	"github.com/cocoonstack/sandbox-operator/pkg/podruntime"
	//+kubebuilder:scaffold:imports
)

var setupLog = ctrl.Log.WithName("setup")

func main() {
	var o options
	o.bindFlags()
	flag.Parse()

	if o.printVersion {
		fmt.Println(version.Print("sandbox-operator"))
		return
	}
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&o.zap)))
	if err := o.run(); err != nil {
		setupLog.Error(err, "sandbox-operator exited")
		os.Exit(1)
	}
}

// options holds every operator flag. It is one struct so main stays a
// sequence of named phases rather than a 400-line body.
type options struct {
	metricsAddr             string
	probeAddr               string
	clusterDomain           string
	defaultRuntime          string
	leaderElectionNamespace string
	enableLeaderElection    bool
	extensions              bool
	printVersion            bool

	webhookPort        int
	webhookCertDir     string
	webhookServiceName string
	webhookNamespace   string
	manageWebhookCerts bool

	enableTracing             bool
	enablePprof               bool
	enablePprofDebug          bool
	pprofBlockProfileRate     int
	pprofMutexProfileFraction int

	kubeAPIQPS   float64
	kubeAPIBurst int

	sandboxWorkers          int
	claimWorkers            int
	warmPoolWorkers         int
	templateWorkers         int
	warmPoolMaxBatchSize    int
	enableWarmPoolEviction  bool
	warmPoolDisableCRManage bool

	zap zap.Options
}

func (o *options) bindFlags() {
	flag.BoolVar(&o.printVersion, "version", false, "Print version information and exit.")
	flag.IntVar(&o.webhookPort, "webhook-port", 9443, "The port the webhook server binds to.")
	flag.StringVar(&o.webhookCertDir, "webhook-cert-dir", "/tmp/k8s-webhook-server/serving-certs", "The directory that contains the certificates.")
	flag.StringVar(&o.webhookServiceName, "webhook-service-name", "sandbox-webhook-service", "The name of the webhook service.")
	flag.StringVar(&o.webhookNamespace, "webhook-namespace", "sandbox-system", "The namespace of the webhook service.")
	flag.BoolVar(&o.manageWebhookCerts, "manage-webhook-certs", true, "Manage webhook serving certs and patch CRD conversion caBundles on startup. Set to false when certs and CRD/webhook configuration are managed externally (e.g., GKE Dynamic Certificate Delivery).")
	flag.StringVar(&o.clusterDomain, "cluster-domain", "cluster.local", "Kubernetes cluster domain for service FQDN generation")
	flag.StringVar(&o.metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&o.probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&o.enableLeaderElection, "leader-elect", true,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.StringVar(&o.leaderElectionNamespace, "leader-election-namespace", "", "The namespace in which the leader election resource will be created.")
	flag.BoolVar(&o.extensions, "extensions", true, "Enable SandboxTemplate, SandboxWarmPool, and SandboxClaim controllers.")
	flag.StringVar(&o.defaultRuntime, "default-runtime", podruntime.DefaultMode, "Default Pod runtime: standard, vk-cocoon, or sandboxd. sandboxd routes sandbox Pods to the vk-sandbox hot-pool virtual node. Set the Pod runtime annotation to override; an explicit runtimeClassName selects standard kubelet.")
	flag.BoolVar(&o.enableTracing, "enable-tracing", false, "Enable OpenTelemetry tracing via OTLP.")
	flag.BoolVar(&o.enablePprof, "enable-pprof", false,
		"Enable CPU profiling endpoint (/debug/pprof/profile) on the metrics server.")
	flag.BoolVar(&o.enablePprofDebug, "enable-pprof-debug", false,
		"Enable all pprof endpoints including sensitive ones (cmdline, symbol, heap, goroutine, etc). "+
			"Implies --enable-pprof. WARNING: May expose sensitive information and comes with performance overhead.")
	flag.IntVar(&o.pprofBlockProfileRate, "pprof-block-profile-rate", 1000000,
		"Block profile sampling rate for /debug/pprof/block when --enable-pprof-debug is set. "+
			"<=0 disables; 1 samples all blocking events; >=2 sets the rate in nanoseconds (e.g. 1000000 ~= 1ms).")
	flag.IntVar(&o.pprofMutexProfileFraction, "pprof-mutex-profile-fraction", 10,
		"Mutex contention sampling rate for /debug/pprof/mutex when --enable-pprof-debug is set. "+
			"<=0 disables; 1 samples all events; N>1 samples ~1/N events (e.g. 10 ~= 1/10, 100 ~= 1/100).")
	flag.Float64Var(&o.kubeAPIQPS, "kube-api-qps", 200, "Client-side QPS limit for the Kubernetes API client; a negative value disables client-side rate limiting entirely, which also makes --kube-api-burst meaningless.")
	flag.IntVar(&o.kubeAPIBurst, "kube-api-burst", 400, "Burst for client-side throttling of the Kubernetes API client. Ignored when --kube-api-qps is negative.")
	flag.IntVar(&o.sandboxWorkers, "sandbox-concurrent-workers", 16, "Max concurrent reconciles for the Sandbox controller")
	flag.IntVar(&o.claimWorkers, "sandbox-claim-concurrent-workers", 50, "Max concurrent reconciles for the SandboxClaim controller")
	flag.IntVar(&o.warmPoolWorkers, "sandbox-warm-pool-concurrent-workers", 8, "Max concurrent reconciles for the SandboxWarmPool controller")
	flag.IntVar(&o.templateWorkers, "sandbox-template-concurrent-workers", 1, "Max concurrent reconciles for the SandboxTemplate controller")
	flag.IntVar(&o.warmPoolMaxBatchSize, "sandbox-warm-pool-max-batch-size", 300, "Max batch size for parallel sandbox creation and deletion in SandboxWarmPool controller. Default is 300.")
	flag.BoolVar(&o.enableWarmPoolEviction, "enable-warm-pool-eviction", true, "Mark pods created by a warm pool as ready-to-evict by default.")
	flag.BoolVar(&o.warmPoolDisableCRManage, "sandbox-warm-pool-disable-cr-management", false,
		"Disable per-CR Sandbox create/delete in the SandboxWarmPool controller (status-only). "+
			"Use with the L3 writable-aggregation design, where warm capacity is driven per-node by sandboxd "+
			"and CR-based replenishment would fight the aggregated node-local claim path.")
	o.zap.BindFlags(flag.CommandLine)
}

func (o *options) validate() error {
	if o.sandboxWorkers <= 0 || o.claimWorkers <= 0 || o.warmPoolWorkers <= 0 {
		return fmt.Errorf("concurrent workers must be greater than 0")
	}
	if o.warmPoolMaxBatchSize <= 0 {
		return fmt.Errorf("sandbox-warm-pool-max-batch-size must be greater than 0")
	}
	if o.kubeAPIBurst <= 0 {
		return fmt.Errorf("kube-api-burst must be greater than 0")
	}
	return nil
}

func (o *options) run() error {
	if err := o.validate(); err != nil {
		return err
	}
	podMutator, err := podruntime.NewMutator(o.defaultRuntime)
	if err != nil {
		return fmt.Errorf("invalid runtime configuration: %w", err)
	}
	o.logSettings()

	ctx := ctrl.SetupSignalHandler()
	instrumenter, cleanup, err := setupTracing(ctx, o.enableTracing)
	if err != nil {
		return fmt.Errorf("initialize tracing: %w", err)
	}
	defer cleanup()

	// net/http/pprof registers on DefaultServeMux at import time, so reset it.
	http.DefaultServeMux = http.NewServeMux()

	scheme := controllers.Scheme
	utilruntime.Must(apiextensionsv1.AddToScheme(scheme))
	if o.extensions {
		utilruntime.Must(extensionsv1alpha1.AddToScheme(scheme))
		utilruntime.Must(extensionsv1beta1.AddToScheme(scheme))
	}

	restConfig := ctrl.GetConfigOrDie()
	restConfig.QPS = float32(o.kubeAPIQPS)
	restConfig.Burst = o.kubeAPIBurst

	if err = o.prepareWebhookCerts(ctx, restConfig, scheme); err != nil {
		return fmt.Errorf("webhook certificate setup: %w", err)
	}

	cacheByObject, err := controllers.CacheByObject()
	if err != nil {
		return err
	}
	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme:                  scheme,
		Cache:                   cache.Options{ByObject: cacheByObject},
		Metrics:                 metricsserver.Options{BindAddress: o.metricsAddr, ExtraHandlers: o.pprofHandlers()},
		HealthProbeBindAddress:  o.probeAddr,
		LeaderElection:          o.enableLeaderElection,
		LeaderElectionNamespace: o.leaderElectionNamespace,
		LeaderElectionID:        "sandbox-operator.agents.x-k8s.io",
		WebhookServer: webhook.NewServer(webhook.Options{
			Port:    o.webhookPort,
			CertDir: o.webhookCertDir,
			TLSOpts: []func(*tls.Config){
				func(cfg *tls.Config) { cfg.ClientAuth = tls.NoClientCert },
			},
		}),
	})
	if err != nil {
		return fmt.Errorf("start manager: %w", err)
	}

	if err := asmetrics.Register(metrics.Registry); err != nil {
		return err
	}
	asmetrics.RegisterSandboxCollector(ctx, mgr.GetClient(), mgr.GetLogger().WithName("sandbox-collector"))

	if err := o.setupControllers(mgr, instrumenter, podMutator); err != nil {
		return err
	}

	//+kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("set up health check: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("set up ready check: %w", err)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("run manager: %w", err)
	}
	return nil
}

func (o *options) logSettings() {
	setupLog.Info("Concurrency settings",
		"sandbox", o.sandboxWorkers,
		"sandboxClaim", o.claimWorkers,
		"sandboxWarmPool", o.warmPoolWorkers,
		"sandboxTemplate", o.templateWorkers,
		"sandboxWarmPoolMaxBatchSize", o.warmPoolMaxBatchSize,
	)
	total := o.sandboxWorkers + o.claimWorkers + o.warmPoolWorkers + o.templateWorkers
	if total > 1000 {
		setupLog.Info("Warning: total concurrent workers exceeds 1000, which could lead to resource exhaustion", "total", total)
	}
	if o.kubeAPIQPS > 0 && total > o.kubeAPIBurst {
		setupLog.Info("Warning: Total concurrent workers exceeds the kube API burst limit. Workers may experience client-side throttling.",
			"totalWorkers", total, "kubeAPIBurst", o.kubeAPIBurst)
	}
	if o.enableLeaderElection && o.leaderElectionNamespace == "" {
		setupLog.V(1).Info("leader election is enabled (--leader-elect=true), but --leader-election-namespace is empty; attempting auto-detection")
	}
}

func (o *options) pprofHandlers() map[string]http.Handler {
	if !o.enablePprof && !o.enablePprofDebug {
		return nil
	}
	setupLog.Info("pprof enabled", "debug", o.enablePprofDebug)
	h := map[string]http.Handler{"/debug/pprof/profile": http.HandlerFunc(pprof.Profile)}
	if !o.enablePprofDebug {
		return h
	}

	setupLog.Info("pprof debug endpoints enabled")
	blockRate := max(o.pprofBlockProfileRate, 0)
	mutexFraction := max(o.pprofMutexProfileFraction, 0)
	if blockRate != o.pprofBlockProfileRate {
		setupLog.Info("invalid pprof block profile rate; clamping to 0", "rate", o.pprofBlockProfileRate)
	}
	if mutexFraction != o.pprofMutexProfileFraction {
		setupLog.Info("invalid pprof mutex profile fraction; clamping to 0", "fraction", o.pprofMutexProfileFraction)
	}
	runtime.SetBlockProfileRate(blockRate)
	runtime.SetMutexProfileFraction(mutexFraction)
	setupLog.Info("pprof sampling configured", "blockProfileRateNs", blockRate, "mutexProfileFraction", mutexFraction)

	h["/debug/pprof/"] = http.HandlerFunc(pprof.Index)
	h["/debug/pprof/cmdline"] = http.HandlerFunc(pprof.Cmdline)
	h["/debug/pprof/symbol"] = http.HandlerFunc(pprof.Symbol)
	h["/debug/pprof/heap"] = pprof.Handler("heap")
	h["/debug/pprof/goroutine"] = pprof.Handler("goroutine")
	h["/debug/pprof/allocs"] = pprof.Handler("allocs")
	h["/debug/pprof/block"] = pprof.Handler("block")
	h["/debug/pprof/mutex"] = pprof.Handler("mutex")
	h["/debug/pprof/trace"] = http.HandlerFunc(pprof.Trace)
	h["/debug/fgprof"] = fgprof.Handler()
	return h
}

func (o *options) prepareWebhookCerts(ctx context.Context, restConfig *rest.Config, scheme *apiruntime.Scheme) error {
	if !o.manageWebhookCerts {
		setupLog.Info("Webhook cert management and CRD conversion caBundle patching disabled; expecting existing tls.crt/tls.key in certDir and CRDs patched externally",
			"certDir", o.webhookCertDir, "serviceName", o.webhookServiceName, "namespace", o.webhookNamespace)
		for _, f := range []string{tlsCertKey, tlsPrivateKey} {
			p := filepath.Join(o.webhookCertDir, f)
			if _, err := os.Stat(p); err != nil {
				return fmt.Errorf("required webhook cert file %s missing: %w (pre-provision tls.crt/tls.key via cert-manager, GKE, or similar)", p, err)
			}
		}
		return nil
	}

	tempClient, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("create temporary client: %w", err)
	}
	setupLog.Info("Preparing webhook certificates", "certDir", o.webhookCertDir)
	caPEM, err := generateWebhookCerts(ctx, tempClient, o.webhookCertDir, o.webhookServiceName, o.webhookNamespace, o.clusterDomain)
	if err != nil {
		return fmt.Errorf("prepare webhook certificates: %w", err)
	}
	setupLog.Info("Patching CRDs with generated CA bundle")
	if err := patchCRDs(ctx, tempClient, caPEM, o.webhookServiceName, o.webhookNamespace, o.extensions); err != nil {
		return fmt.Errorf("patch CRDs with CA bundle: %w", err)
	}
	return nil
}

func (o *options) setupControllers(mgr ctrl.Manager, instrumenter asmetrics.Instrumenter, podMutator controllers.PodMutator) error {
	if err := (&controllers.SandboxReconciler{
		Client:        mgr.GetClient(),
		Scheme:        mgr.GetScheme(),
		Tracer:        instrumenter,
		ClusterDomain: o.clusterDomain,
		PodMutator:    podMutator,
	}).SetupWithManager(mgr, o.sandboxWorkers); err != nil {
		return fmt.Errorf("controller Sandbox: %w", err)
	}
	if err := ctrl.NewWebhookManagedBy(mgr, &sandboxv1beta1.Sandbox{}).Complete(); err != nil {
		return fmt.Errorf("webhook Sandbox: %w", err)
	}
	if !o.extensions {
		return nil
	}
	return o.setupExtensionControllers(mgr, instrumenter)
}

func (o *options) setupExtensionControllers(mgr ctrl.Manager, instrumenter asmetrics.Instrumenter) error {
	allowedDomains, err := readAllowedLabelDomains()
	if err != nil {
		return err
	}
	if err := (&extensionscontrollers.SandboxClaimReconciler{
		Client:              mgr.GetClient(),
		Scheme:              mgr.GetScheme(),
		WarmSandboxQueue:    queue.NewSimpleSandboxQueue(),
		Recorder:            mgr.GetEventRecorder("sandboxclaim-controller"),
		Tracer:              instrumenter,
		AllowedLabelDomains: allowedDomains,
	}).SetupWithManager(mgr, o.claimWorkers); err != nil {
		return fmt.Errorf("controller SandboxClaim: %w", err)
	}
	if err := (&extensionscontrollers.SandboxTemplateReconciler{
		Client:          mgr.GetClient(),
		Scheme:          mgr.GetScheme(),
		Tracer:          instrumenter,
		RouterNamespace: o.webhookNamespace,
	}).SetupWithManager(mgr, o.templateWorkers); err != nil {
		return fmt.Errorf("controller SandboxTemplate: %w", err)
	}
	if err := (&extensionscontrollers.SandboxWarmPoolReconciler{
		Client:                     mgr.GetClient(),
		Scheme:                     mgr.GetScheme(),
		MaxBatchSize:               o.warmPoolMaxBatchSize,
		EnableWarmPoolEviction:     o.enableWarmPoolEviction,
		DisableSandboxCRManagement: o.warmPoolDisableCRManage,
		Tracer:                     instrumenter,
	}).SetupWithManager(mgr, o.warmPoolWorkers); err != nil {
		return fmt.Errorf("controller SandboxWarmPool: %w", err)
	}
	for _, w := range []client.Object{
		&extensionsv1beta1.SandboxClaim{},
		&extensionsv1beta1.SandboxTemplate{},
		&extensionsv1beta1.SandboxWarmPool{},
	} {
		if err := ctrl.NewWebhookManagedBy(mgr, w).Complete(); err != nil {
			return fmt.Errorf("webhook %T: %w", w, err)
		}
	}
	return nil
}

func setupTracing(ctx context.Context, enabled bool) (asmetrics.Instrumenter, func(), error) {
	if !enabled {
		return asmetrics.NewNoOp(), func() {}, nil
	}
	initCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return asmetrics.SetupOTel(initCtx, "sandbox-operator")
}

// readAllowedLabelDomains reads the optional label-domain allowlist mounted by
// the deployment; a missing file leaves the allowlist empty.
func readAllowedLabelDomains() ([]string, error) {
	const configPath = "/etc/sandbox-config/allowed-label-domains"
	data, err := os.ReadFile(configPath)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", configPath, err)
	}
	var domains []string
	for d := range strings.FieldsFuncSeq(strings.TrimSpace(string(data)), func(c rune) bool {
		return c == ',' || c == '\n' || c == '\r'
	}) {
		if d = strings.ToLower(strings.TrimSpace(d)); d != "" {
			domains = append(domains, d)
		}
	}
	return domains, nil
}
