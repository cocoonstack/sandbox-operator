// sandbox-loadgen measures the real "claim a pre-warmed sandbox from the
// warmpool" latency (the checkout path, NOT create-from-scratch) and exports it
// as Prometheus metrics.
//
// It stands up a SandboxTemplate + SandboxWarmPool (replicas=--replicas), waits
// for the pool to warm, then SERIALLY (one outstanding at a time) issues
// SandboxClaims against the pool, fast-polls each to Ready (a warm sandbox has
// been delivered), records the claim latency at ms resolution, and releases the
// claim (delete) so the pool refills. Because it drives real SandboxClaims, the
// operator's own agent_sandbox_claim_startup_latency_ms is populated too.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	extv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
	asmetrics "github.com/cocoonstack/sandbox-operator/internal/metrics"
	"github.com/cocoonstack/sandbox-operator/pkg/podruntime"
)

const (
	runtimeAnnotation = podruntime.RuntimeAnnotation
	templateName      = "loadgen-tpl"
	// releaseTimeout bounds the detached release so shutdown cannot hang on it.
	releaseTimeout = 10 * time.Second
	warmPoolName   = "loadgen-pool"
	// observedAtAnnotation is read by the operator to compute the claim-startup
	// latency (time.Since). Stamping it at create-time makes the operator record
	// agent_sandbox_claim_startup_latency_ms for our claims.
	observedAtAnnotation = asmetrics.ObservabilityAnnotation
)

var (
	// claimSeconds: fine buckets — checkout is typically sub-second.
	claimSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "sandbox_loadgen_claim_seconds",
		Help:    "Time to claim a pre-warmed sandbox from the warmpool (create SandboxClaim -> Ready).",
		Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.2, 0.35, 0.5, 0.75, 1, 1.5, 2, 3, 5, 10, 30},
	})
	claimsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sandbox_loadgen_claims_total", Help: "SandboxClaims issued.",
	})
	boundTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sandbox_loadgen_bound_total", Help: "SandboxClaims served (Ready) from the warmpool.",
	})
	claimFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sandbox_loadgen_claim_failed_total", Help: "Failed claim operations by reason.",
	}, []string{"reason"})
	inflight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sandbox_loadgen_inflight", Help: "Claims currently outstanding (serial => 0 or 1).",
	})
	poolReady = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sandbox_loadgen_pool_ready", Help: "SandboxWarmPool readyReplicas (warm sandboxes available).",
	})
	poolTarget = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sandbox_loadgen_pool_target", Help: "SandboxWarmPool desired replicas.",
	})
	buildInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sandbox_loadgen_build_info", Help: "Loadgen run parameters, constant 1.",
	}, []string{"namespace", "warmpool", "template"})
)

type options struct {
	replicas      int
	namespace     string
	image         string
	createdBy     string
	metricsAddr   string
	concurrency   int
	total         int
	claimInterval time.Duration
	poll          time.Duration
	claimTimeout  time.Duration
	poolPoll      time.Duration
}

func main() {
	o := options{}
	flag.IntVar(&o.replicas, "replicas", 100, "SandboxWarmPool size (pre-warmed sandboxes)")
	flag.StringVar(&o.namespace, "namespace", "sandboxd-test", "namespace")
	flag.StringVar(&o.image, "template", "ghcr.io/cocoonstack/sandbox/rt@sha256:c8cab53a1e1684e6c0c95a06855001b0535be2862b7d4f72658f9f0e784c8778", "sandbox container image (digest ref)")
	flag.StringVar(&o.createdBy, "created-by", "loadgen", "value for the agents.x-k8s.io/created-by label")
	flag.StringVar(&o.metricsAddr, "metrics-addr", ":9090", "Prometheus metrics listen address")
	flag.IntVar(&o.concurrency, "concurrency", 1, "number of parallel claim workers (in-flight claims)")
	flag.IntVar(&o.total, "total", 0, "stop after this many claims (0 = run until terminated)")
	flag.DurationVar(&o.claimInterval, "claim-interval", 1*time.Second, "per-worker gap between claims (0 = as fast as possible)")
	flag.DurationVar(&o.poll, "poll", 25*time.Millisecond, "readiness poll interval (ms resolution)")
	flag.DurationVar(&o.claimTimeout, "claim-timeout", 60*time.Second, "max wait for a claim to be served")
	flag.DurationVar(&o.poolPoll, "pool-poll", 10*time.Second, "how often to refresh the warmpool gauge")
	flag.Parse()

	logf.SetLogger(zap.New(zap.UseDevMode(false)))
	log := logf.Log.WithName("sandbox-loadgen")

	// pre-initialize the failure series so panels render 0 instead of "No data".
	for _, r := range []string{"create", "timeout", "delete"} {
		claimFailed.WithLabelValues(r).Add(0)
	}
	poolTarget.Set(float64(o.replicas))
	buildInfo.WithLabelValues(o.namespace, warmPoolName, o.image).Set(1)

	cfg, err := config.GetConfig()
	if err != nil {
		log.Error(err, "config")
		os.Exit(1)
	}
	cl, err := client.New(cfg, client.Options{})
	if err != nil {
		log.Error(err, "client")
		os.Exit(1)
	}

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
		log.Info("serving metrics", "addr", o.metricsAddr)
		srv := &http.Server{Addr: o.metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		if err := srv.ListenAndServe(); err != nil {
			log.Error(err, "metrics server exited")
		}
	}()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	lg := &loadgen{cl: cl, o: o, log: log}
	if err := lg.setup(ctx); err != nil {
		log.Error(err, "setup template/warmpool")
		os.Exit(1) //nolint:gocritic // startup failure: the process dies before the deferred cleanup matters
	}
	go lg.poolPollLoop(ctx)
	lg.waitPoolWarm(ctx)
	lg.claimLoop(ctx)
	// Keep serving /metrics after a bounded (--total) run completes so the final
	// latency histogram stays scrapeable instead of vanishing on pod restart.
	if o.total > 0 {
		log.Info("claim benchmark finished; serving metrics until terminated")
		<-ctx.Done()
	}
	log.Info("shutting down")
}

type loadgen struct {
	cl  client.Client
	o   options
	log logr.Logger
}

// setup creates the SandboxTemplate + SandboxWarmPool (idempotent).
func (l *loadgen) setup(ctx context.Context) error {
	tpl := &unstructured.Unstructured{}
	tpl.SetGroupVersionKind(gvk("SandboxTemplate"))
	tpl.SetNamespace(l.o.namespace)
	tpl.SetName(templateName)
	tpl.SetLabels(map[string]string{sandboxv1beta1.CreatedByLabel: l.o.createdBy})
	tpl.Object["spec"] = map[string]any{
		"podTemplate": map[string]any{
			"metadata": map[string]any{
				"annotations": map[string]any{runtimeAnnotation: "sandboxd"},
				"labels":      map[string]any{"app": "loadgen-warm"},
			},
			"spec": map[string]any{
				"containers": []any{map[string]any{
					"name": "main", "image": l.o.image,
					"resources": map[string]any{"requests": map[string]any{"cpu": "50m", "memory": "64Mi"}},
				}},
			},
		},
	}
	if err := l.cl.Create(ctx, tpl); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create template: %w", err)
	}

	pool := &unstructured.Unstructured{}
	pool.SetGroupVersionKind(gvk("SandboxWarmPool"))
	pool.SetNamespace(l.o.namespace)
	pool.SetName(warmPoolName)
	pool.SetLabels(map[string]string{sandboxv1beta1.CreatedByLabel: l.o.createdBy})
	pool.Object["spec"] = map[string]any{
		"replicas":           int64(l.o.replicas),
		"sandboxTemplateRef": map[string]any{"name": templateName},
	}
	if err := l.cl.Create(ctx, pool); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create warmpool: %w", err)
	}
	l.log.Info("template + warmpool ensured", "replicas", l.o.replicas)
	return nil
}

func (l *loadgen) waitPoolWarm(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		if r := l.poolReadyReplicas(ctx); r >= 1 {
			l.log.Info("warmpool has ready sandboxes; starting claim benchmark", "ready", r)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// claimLoop runs --concurrency parallel workers that each claim a warm sandbox,
// time it, and release, until --total claims are done (or forever if total==0).
// A shared atomic counter hands out claim indices so exactly --total claims are
// issued regardless of worker count.
func (l *loadgen) claimLoop(ctx context.Context) {
	workers := max(l.o.concurrency, 1)
	var issued atomic.Int64
	var served atomic.Int64
	start := time.Now()

	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for {
				if ctx.Err() != nil {
					return
				}
				n := issued.Add(1)
				if l.o.total > 0 && n > int64(l.o.total) {
					return
				}
				l.claimOnce(ctx, fmt.Sprintf("loadgen-claim-%d", n))
				served.Add(1)
				if l.o.claimInterval > 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(l.o.claimInterval):
					}
				}
			}
		})
	}

	if l.o.total > 0 {
		wg.Wait()
		l.log.Info("claim benchmark complete",
			"claims", served.Load(), "concurrency", workers, "elapsed", time.Since(start).String())
		return
	}
	<-ctx.Done()
	wg.Wait()
}

// claimOnce creates a SandboxClaim, fast-polls it to Ready (warm sandbox
// delivered), records the claim latency, then deletes (releases) it.
func (l *loadgen) claimOnce(ctx context.Context, name string) {
	inflight.Inc()
	defer inflight.Dec()

	claim := &unstructured.Unstructured{}
	claim.SetGroupVersionKind(gvk("SandboxClaim"))
	claim.SetNamespace(l.o.namespace)
	claim.SetName(name)
	claim.SetLabels(map[string]string{sandboxv1beta1.CreatedByLabel: l.o.createdBy})
	claim.SetAnnotations(map[string]string{observedAtAnnotation: time.Now().UTC().Format(time.RFC3339Nano)})
	claim.Object["spec"] = map[string]any{
		"warmPoolRef": map[string]any{"name": warmPoolName},
	}

	start := time.Now()
	if err := l.cl.Create(ctx, claim); err != nil {
		claimFailed.WithLabelValues("create").Inc()
		l.log.Error(err, "create claim", "name", name)
		return
	}
	claimsTotal.Inc()
	// Deferred and detached from ctx: an early return on shutdown would otherwise
	// strand the claim, and with it the warm sandbox, until its own expiry.
	defer func() {
		rel := &unstructured.Unstructured{}
		rel.SetGroupVersionKind(gvk("SandboxClaim"))
		rel.SetNamespace(l.o.namespace)
		rel.SetName(name)
		relCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
		defer cancel()
		if err := l.cl.Delete(relCtx, rel); err != nil && !apierrors.IsNotFound(err) {
			claimFailed.WithLabelValues("delete").Inc()
		}
	}()

	poll := time.NewTicker(l.o.poll)
	defer poll.Stop()
	deadline := start.Add(l.o.claimTimeout)
	for {
		cur := &unstructured.Unstructured{}
		cur.SetGroupVersionKind(gvk("SandboxClaim"))
		if err := l.cl.Get(ctx, client.ObjectKey{Namespace: l.o.namespace, Name: name}, cur); err == nil {
			if isReady(cur) {
				claimSeconds.Observe(time.Since(start).Seconds())
				boundTotal.Inc()
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-poll.C:
			if time.Now().After(deadline) {
				claimFailed.WithLabelValues("timeout").Inc()
				l.log.Info("claim not served before timeout", "name", name)
			}
		}
		if time.Now().After(deadline) {
			break
		}
	}
}

func (l *loadgen) poolPollLoop(ctx context.Context) {
	t := time.NewTicker(l.o.poolPoll)
	defer t.Stop()
	for {
		poolReady.Set(float64(l.poolReadyReplicas(ctx)))
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (l *loadgen) poolReadyReplicas(ctx context.Context) int64 {
	pool := &unstructured.Unstructured{}
	pool.SetGroupVersionKind(gvk("SandboxWarmPool"))
	if err := l.cl.Get(ctx, client.ObjectKey{Namespace: l.o.namespace, Name: warmPoolName}, pool); err != nil {
		return 0
	}
	rr, _, _ := unstructured.NestedInt64(pool.Object, "status", "readyReplicas")
	return rr
}

func gvk(kind string) schema.GroupVersionKind { return extv1beta1.GroupVersion.WithKind(kind) }

func isReady(u *unstructured.Unstructured) bool {
	conds, found, err := unstructured.NestedSlice(u.Object, "status", "conditions")
	if err != nil || !found {
		return false
	}
	for _, c := range conds {
		m, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if m["type"] == "Ready" && m["status"] == string(metav1.ConditionTrue) {
			return true
		}
	}
	return false
}
