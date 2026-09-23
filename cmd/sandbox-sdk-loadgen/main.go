// Command sandbox-sdk-loadgen measures the REAL create latency of the writable
// L3 aggregated apiserver by creating agents.x-k8s.io Sandboxes through a single
// persistent typed client — the same path the official agent-sandbox Go client
// uses. Unlike `kubectl create` in a loop (which re-runs discovery/OpenAPI on
// every invocation and inflated p50 to ~12s), the client here builds its
// RESTMapper once, so the histogram reflects the apiserver round-trip (warm
// claim), not client bootstrap cost.
//
// Safety properties (hard, by construction):
//   - EXACTLY --total creates are issued, then the run stops. --total is
//     REQUIRED and positive; there is no unbounded mode.
//   - With --cleanup (default true), each worker BLOCKS until its sandbox's
//     release is confirmed before creating the next one, so live claims never
//     exceed --concurrency. A delete that returns NotFound is NOT success —
//     it means the read view has not published the object yet (vk lag, ~10s);
//     the worker retries until the delete lands or --release-timeout expires,
//     and an expiry is counted in sandbox_sdk_leaked_total and logged loudly.
//
// Cycle mode (--wave-size > 0) replaces the single bounded run with a
// soak cycle: create --wave-size sandboxes at --concurrency, pause
// --wave-pause, repeat until --target creates have been issued this cycle;
// then delete every sandbox this loadgen owns (its namespace + name prefix)
// at --delete-concurrency with the same confirm-or-leak release semantics;
// then (with --loop) start the next cycle. Live claims stay bounded by
// --target: waves never issue past it and the delete phase re-lists until the
// read view shows the namespace empty before the next cycle begins.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
	"github.com/cocoonstack/sandbox-operator/pkg/scale/apiserver"
	"github.com/cocoonstack/sandbox-operator/version"
	sdk "github.com/cocoonstack/sandbox/sdk/go"
)

// L3 Create stamps these onto the returned Sandbox: the delivered connection
// address, per-sandbox exec token, and sandboxd claim id. Together they let the
// loadgen exec into exactly what it claimed (create -> exec latency).
const (
	addressAnnotation = apiserver.AddressAnnotation
	tokenAnnotation   = apiserver.TokenAnnotation
	claimIDAnnotation = scale.ClaimIDAnnotation

	// metricsReadHeaderTimeout bounds how long a client may take to send its
	// request headers, so a stalled connection cannot pin a handler.
	metricsReadHeaderTimeout = 5 * time.Second

	// The read view lags a wave by a publish interval, so the delete phase re-lists until two passes come back empty.
	deleteSettleTimeout = 2 * time.Minute
	deleteResettle      = 10 * time.Second
)

var (
	// sdkClient is shared; per-sandbox handles come from Attach, with no lookup round-trip.
	sdkClient *sdk.Client

	failReasons = []string{"no-warm-503", "throttled-429", "internal-500", "timeout", "conflict", "other"}

	// logMu/lastLog rate-limit logSampled to one line per key per 5s so load-test rates cannot flood the pod log.
	logMu   sync.Mutex
	lastLog = map[string]time.Time{}

	createSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "sandbox_sdk_create_seconds",
		Help: "Latency of Create(Sandbox) against the aggregated apiserver (warm claim round-trip).",
		// 1ms .. ~32s: warm hits land in the low-ms buckets, cold/clone fallback in seconds.
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
	})
	deleteSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "sandbox_sdk_delete_seconds",
		Help:    "Latency of the FIRST successful Delete call (exact-VM release), excluding read-view NotFound retries.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
	})
	// execSeconds is the metric that matters: the real "usable" latency.
	execSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "sandbox_sdk_exec_seconds",
		Help:    "Latency from Create(Sandbox) start to first successful in-sandbox exec (create->exec).",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
	})
	execsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sandbox_sdk_execs_total",
		Help: "Sandboxes where a post-create exec succeeded.",
	})
	execFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sandbox_sdk_exec_failed_total",
		Help: "Sandboxes where the post-create exec failed.",
	})
	createsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sandbox_sdk_creates_total",
		Help: "Sandboxes successfully created.",
	})
	deletesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sandbox_sdk_deletes_total",
		Help: "Sandboxes whose release was CONFIRMED (Delete accepted by the apiserver).",
	})
	deleteRetries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sandbox_sdk_delete_notfound_retries_total",
		Help: "Delete attempts that hit NotFound because the read view had not published the object yet (vk lag).",
	})
	leakedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sandbox_sdk_leaked_total",
		Help: "Sandboxes created but whose release could NOT be confirmed within --release-timeout. Anything >0 means node claims were left for the TTL reaper — investigate.",
	})
	createFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sandbox_sdk_create_failed_total",
		Help: "Create failures by reason.",
	}, []string{"reason"})
	inflight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sandbox_sdk_inflight",
		Help: "In-flight Create calls.",
	})
	cyclesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sandbox_sdk_cycles_total",
		Help: "Completed create->delete cycles (cycle mode).",
	})
	phaseGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sandbox_sdk_phase",
		Help: "Current cycle-mode phase: 0 idle, 1 create wave, 2 wave pause, 3 delete.",
	})
	buildInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sandbox_sdk_build_info",
		Help: "Loadgen build info; value is always 1.",
	}, []string{"version"})
)

type options struct {
	namespace      string
	image          string
	namegen        string
	concurrency    int
	total          int
	interval       time.Duration
	timeout        time.Duration
	cleanup        bool
	releaseTimeout time.Duration
	metricsAddr    string

	// Cycle mode (enabled by waveSize > 0).
	waveSize          int
	wavePause         time.Duration
	target            int
	deleteConcurrency int
	loop              bool
}

// validateMode enforces the flags each run mode requires and settles the
// derived ones. Cycle mode owns deletion itself, so per-create cleanup is off.
func (o *options) validateMode() {
	if o.waveSize > 0 {
		if o.target <= 0 {
			fatalf("--target is required and must be > 0 in cycle mode (got %d)", o.target)
		}
		if o.deleteConcurrency <= 0 {
			fatalf("--delete-concurrency must be > 0 (got %d)", o.deleteConcurrency)
		}
		o.cleanup = false
		return
	}
	if o.total <= 0 {
		fatalf("--total is required and must be > 0 (got %d): this loadgen issues exactly --total creates and refuses to run unbounded", o.total)
	}
	o.concurrency = min(o.concurrency, o.total)
}

func main() {
	var o options
	flag.StringVar(&o.namespace, "namespace", "sandbox-sdk-loadgen", "namespace to create Sandboxes in")
	flag.StringVar(&o.image, "template", "ghcr.io/cocoonstack/sandbox/rt@sha256:c8cab53a1e1684e6c0c95a06855001b0535be2862b7d4f72658f9f0e784c8778", "container image; picks the warm pool (image=template, no resources=size small, no net annotation=none)")
	flag.StringVar(&o.namegen, "name-prefix", "sdklg", "generated Sandbox name prefix")
	flag.IntVar(&o.concurrency, "concurrency", 1, "parallel create workers; with --cleanup this is also the max live claims")
	flag.IntVar(&o.total, "total", 0, "REQUIRED: issue exactly this many creates, then stop (must be > 0; there is no unbounded mode)")
	flag.DurationVar(&o.interval, "interval", 0, "per-worker gap between creates (0 = as fast as possible)")
	flag.DurationVar(&o.timeout, "timeout", 30*time.Second, "per-create context timeout")
	flag.BoolVar(&o.cleanup, "cleanup", true, "delete each Sandbox after creating it and BLOCK until the release is confirmed (bounds live claims to --concurrency)")
	flag.DurationVar(&o.releaseTimeout, "release-timeout", 120*time.Second, "max time to retry a Delete past the read-view publish lag before counting the sandbox as leaked")
	flag.StringVar(&o.metricsAddr, "metrics-addr", ":9090", "Prometheus metrics listen address")
	flag.IntVar(&o.waveSize, "wave-size", 0, "cycle mode: creates per wave (0 disables cycle mode)")
	flag.DurationVar(&o.wavePause, "wave-pause", 3*time.Minute, "cycle mode: pause between create waves")
	flag.IntVar(&o.target, "target", 0, "cycle mode: creates issued per cycle before the delete phase (required with --wave-size)")
	flag.IntVar(&o.deleteConcurrency, "delete-concurrency", 10, "cycle mode: parallel delete workers in the delete phase")
	flag.BoolVar(&o.loop, "loop", true, "cycle mode: start the next cycle after the delete phase (false = one cycle)")
	flag.Parse()

	// Hard gate: never start an unbounded or ill-configured run. The one time
	// this binary ran with an unbounded total it drained the fleet's warm pools
	// and leaked ~19k claims. Exactly --total (or, in cycle mode, at most
	// --target live per cycle with a delete phase in between), or nothing.
	if o.concurrency <= 0 {
		fatalf("--concurrency must be > 0 (got %d)", o.concurrency)
	}
	o.validateMode()

	buildInfo.WithLabelValues(version.VERSION).Set(1)
	for _, r := range failReasons {
		createFailed.WithLabelValues(r).Add(0) // pre-init so panels render 0 not "No data"
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(sandboxv1beta1.AddToScheme(scheme))

	cfg, err := config.GetConfig()
	if err != nil {
		fatalf("load kubeconfig: %v", err)
	}
	// Raise client-side throttling well above the default 5 QPS so the loadgen,
	// not client-go's rate limiter, sets the offered load.
	cfg.QPS = 2000
	cfg.Burst = 4000

	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		fatalf("build client: %v", err)
	}

	// Shared silkd data-plane client. The entry addr is unused for exec (Attach
	// binds each handle to the claim's own owner address); only the HTTP client
	// it carries matters for the relayed silkd dial.
	sdkClient, err = sdk.Connect("127.0.0.1:7777")
	if err != nil {
		fatalf("build sdk client: %v", err)
	}

	// Serve metrics for the whole run (and after the bounded run finishes) so
	// vmagent always has a live endpoint to scrape.
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
		srv := &http.Server{
			Addr:              o.metricsAddr,
			Handler:           mux,
			ReadHeaderTimeout: metricsReadHeaderTimeout,
		}
		if err := srv.ListenAndServe(); err != nil {
			fatalf("metrics server: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if o.waveSize > 0 {
		fmt.Printf("sandbox-sdk-loadgen %s: CYCLE mode ns=%s target=%d wave=%d pause=%s create-concurrency=%d delete-concurrency=%d loop=%v image=%s\n",
			version.VERSION, o.namespace, o.target, o.waveSize, o.wavePause, o.concurrency, o.deleteConcurrency, o.loop, o.image)
		runCycles(ctx, cl, &o)
		if ctx.Err() != nil {
			return
		}
		// --loop=false single cycle finished: keep the histograms scrapeable.
		fmt.Println("cycle complete; serving /metrics until terminated")
		<-ctx.Done()
		return
	}

	fmt.Printf("sandbox-sdk-loadgen %s: ns=%s total=%d concurrency=%d cleanup=%v release-timeout=%s image=%s\n",
		version.VERSION, o.namespace, o.total, o.concurrency, o.cleanup, o.releaseTimeout, o.image)

	var seq int64
	summary := createBatch(ctx, cl, &o, &seq, o.total)
	fmt.Println(summary)
	if summary.leaked > 0 {
		fmt.Printf("ERROR: %d sandbox(es) leaked — their node claims were left for the TTL reaper. Do NOT scale this run up until the leak is explained.\n", summary.leaked)
	}

	fmt.Println("run complete; serving /metrics until terminated")
	<-ctx.Done()
}

// runSummary is the end-of-run accounting printed to stdout.
type runSummary struct {
	issued, created, failed, released, leaked int64
	elapsed                                   time.Duration
}

func (s runSummary) String() string {
	return fmt.Sprintf("summary: issued=%d created=%d failed=%d released=%d leaked=%d elapsed=%s",
		s.issued, s.created, s.failed, s.released, s.leaked, s.elapsed.Round(time.Millisecond))
}

// runCycles drives create-in-waves -> delete-all cycles until the context is
// canceled (or after one cycle with --loop=false). Sandbox names use a
// process-lifetime sequence so a cycle never collides with an undeleted
// leftover from a previous one.
func runCycles(ctx context.Context, cl client.Client, o *options) {
	defer phaseGauge.Set(0)
	var nameSeq int64
	for cycle := 1; ctx.Err() == nil; cycle++ {
		fmt.Printf("cycle %d: create phase (target=%d wave=%d concurrency=%d)\n", cycle, o.target, o.waveSize, o.concurrency)
		issued := 0
		for issued < o.target && ctx.Err() == nil {
			n := min(o.target-issued, o.waveSize)
			phaseGauge.Set(1)
			s := createBatch(ctx, cl, o, &nameSeq, n)
			issued += int(s.issued)
			fmt.Printf("cycle %d: wave done (%s), issued %d/%d\n", cycle, s, issued, o.target)
			if issued >= o.target || ctx.Err() != nil {
				break
			}
			phaseGauge.Set(2)
			sleepCtx(ctx, o.wavePause)
		}
		if ctx.Err() != nil {
			return
		}
		phaseGauge.Set(3)
		listed, deleted, leaked := deleteAll(ctx, cl, o)
		fmt.Printf("cycle %d: delete phase done: listed=%d deleted=%d leaked=%d\n", cycle, listed, deleted, leaked)
		cyclesTotal.Inc()
		if !o.loop {
			return
		}
	}
}

// createBatch issues exactly n creates at o.concurrency, drawing names from
// seq and recording each outcome (release included under --cleanup). Failures
// consume their slot and are not retried.
func createBatch(ctx context.Context, cl client.Client, o *options, seq *int64, n int) runSummary {
	var s runSummary
	var issued atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	for range o.concurrency {
		wg.Go(func() {
			for {
				if ctx.Err() != nil {
					return
				}
				if issued.Add(1) > int64(n) {
					return
				}
				name := fmt.Sprintf("%s-%d", o.namegen, atomic.AddInt64(seq, 1))
				sb, err := createOne(ctx, cl, o, name)
				recordOutcome(ctx, cl, o, &s, sb, err)
				if o.interval > 0 {
					sleepCtx(ctx, o.interval)
				}
			}
		})
	}
	wg.Wait()
	s.issued = min(issued.Load(), int64(n))
	s.elapsed = time.Since(start)
	return s
}

// deleteAll releases every sandbox this loadgen owns — its namespace, filtered
// by the generated name prefix (labels are not relied on: the L3 read view
// synthesizes objects from node inventory and does not guarantee label
// round-trip) — at o.deleteConcurrency with confirm-or-leak semantics.
func deleteAll(ctx context.Context, cl client.Client, o *options) (listed, deleted, leaked int64) {
	prefix := o.namegen + "-"
	done := map[string]struct{}{}
	deadline := time.Now().Add(deleteSettleTimeout)
	for empty := 0; empty < 2 && ctx.Err() == nil; {
		var list sandboxv1beta1.SandboxList
		if err := cl.List(ctx, &list, client.InNamespace(o.namespace)); err != nil {
			fmt.Printf("delete phase: list %s failed: %v\n", o.namespace, err)
			return listed, deleted, leaked
		}
		var mine []*sandboxv1beta1.Sandbox
		for i := range list.Items {
			sb := &list.Items[i]
			if _, seen := done[sb.Name]; seen || !strings.HasPrefix(sb.Name, prefix) {
				continue
			}
			done[sb.Name] = struct{}{}
			mine = append(mine, sb)
		}
		if len(mine) == 0 {
			empty++
		} else {
			empty = 0
			listed += int64(len(mine))
			d, l := releaseAll(ctx, cl, o, mine)
			deleted += d
			leaked += l
		}
		if time.Now().After(deadline) {
			break
		}
		if empty < 2 {
			sleepCtx(ctx, deleteResettle)
		}
	}
	return listed, deleted, leaked
}

func releaseAll(ctx context.Context, cl client.Client, o *options, sbs []*sandboxv1beta1.Sandbox) (deleted, leaked int64) {
	work := make(chan *sandboxv1beta1.Sandbox)
	var wg sync.WaitGroup
	for range o.deleteConcurrency {
		wg.Go(func() {
			for sb := range work {
				if releaseWithRetry(ctx, cl, o, sb) {
					atomic.AddInt64(&deleted, 1)
				} else {
					atomic.AddInt64(&leaked, 1)
				}
			}
		})
	}
	for _, sb := range sbs {
		if ctx.Err() != nil {
			break
		}
		work <- sb
	}
	close(work)
	wg.Wait()
	return deleted, leaked
}

func sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// recordOutcome tallies one create attempt and, with --cleanup, its release. A
// release that could not be confirmed counts as leaked, never as released.
func recordOutcome(ctx context.Context, cl client.Client, o *options, s *runSummary, sb *sandboxv1beta1.Sandbox, err error) {
	if err != nil {
		atomic.AddInt64(&s.failed, 1)
		return
	}
	atomic.AddInt64(&s.created, 1)
	if !o.cleanup {
		return
	}
	if releaseWithRetry(ctx, cl, o, sb) {
		atomic.AddInt64(&s.released, 1)
		return
	}
	atomic.AddInt64(&s.leaked, 1)
}

// createOne issues a single Create, records latency and outcome, and returns
// the created object (for cleanup) or the error.
func createOne(ctx context.Context, cl client.Client, o *options, name string) (*sandboxv1beta1.Sandbox, error) {
	sb := newSandbox(o.namespace, name, o.image)

	cctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()

	inflight.Inc()
	start := time.Now()
	err := cl.Create(cctx, sb)
	elapsed := time.Since(start).Seconds()
	inflight.Dec()

	if err != nil {
		reason := classify(err)
		createFailed.WithLabelValues(reason).Inc()
		logSampledf(reason, "create %s/%s failed: %v", o.namespace, name, err)
		return nil, err
	}
	createSeconds.Observe(elapsed)
	createsTotal.Inc()

	// create -> exec: exec a trivial command in the delivered sandbox to measure
	// the real usable latency (not just the Create round-trip). The token/address
	// ride back as annotations on the L3-created object.
	addr := sb.Annotations[addressAnnotation]
	id := sb.Annotations[claimIDAnnotation]
	tok := sb.Annotations[tokenAnnotation]
	if addr != "" && id != "" && tok != "" {
		ectx, ecancel := context.WithTimeout(ctx, o.timeout)
		out, xerr := sdkClient.Attach(addr, id, tok).Exec(ectx, "true")
		ecancel()
		if xerr == nil {
			execSeconds.Observe(time.Since(start).Seconds())
			execsTotal.Inc()
		} else {
			execFailed.Inc()
			logSampledf("exec", "exec %s/%s (addr=%s id=%s) failed: %v out=%q", o.namespace, name, addr, id, xerr, out)
		}
	} else {
		execFailed.Inc()
		logSampledf("exec", "exec %s/%s: missing addr/id/token annotations (addr=%q id=%q tok?=%v)", o.namespace, name, addr, id, tok != "")
	}
	return sb, nil
}

// releaseWithRetry deletes sb and BLOCKS until the apiserver accepts the
// delete, retrying NotFound: a freshly created sandbox is not deletable until
// the node's vk publishes it into the read view (~10s), and a NotFound before
// then means "not yet", NOT "already gone". Returns false — and counts the
// sandbox as leaked — only after --release-timeout.
func releaseWithRetry(ctx context.Context, cl client.Client, o *options, sb *sandboxv1beta1.Sandbox) bool {
	deadline := time.Now().Add(o.releaseTimeout)
	for attempt := 0; ; attempt++ {
		dctx, cancel := context.WithTimeout(ctx, o.timeout)
		start := time.Now()
		err := cl.Delete(dctx, sb)
		cancel()
		switch {
		case err == nil:
			deleteSeconds.Observe(time.Since(start).Seconds())
			deletesTotal.Inc()
			return true
		case apierrors.IsNotFound(err):
			deleteRetries.Inc()
		default:
			logSampledf("delete", "delete %s/%s failed (attempt %d): %v", sb.Namespace, sb.Name, attempt, err)
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			leakedTotal.Inc()
			fmt.Printf("ERROR: sandbox %s/%s leaked: release not confirmed within %s (last err: %v)\n",
				sb.Namespace, sb.Name, o.releaseTimeout, err)
			return false
		}
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
		}
	}
}

// newSandbox builds a Sandbox whose derived pool is (image, net=none, size=small)
// — matching the warm pool sandboxd maintains — so a create is a warm hit.
func newSandbox(ns, name, image string) *sandboxv1beta1.Sandbox {
	return &sandboxv1beta1.Sandbox{
		Namespace: ns,
		Name:      name,
		Labels:    map[string]string{sandboxv1beta1.CreatedByLabel: "sdk-loadgen"},
		Spec: sandboxv1beta1.SandboxSpec{
			SandboxBlueprint: sandboxv1beta1.SandboxBlueprint{
				PodTemplate: sandboxv1beta1.PodTemplate{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{
							Name:  "sandbox",
							Image: image,
						}},
					},
				},
			},
		},
	}
}

// classify buckets a Create error for the failed-by-reason counter. no-warm is
// the meaningful signal: the apiserver returns 503 ServiceUnavailable when the
// derived pool has no warm VM (NewServiceUnavailable in storage.Create); claim
// plumbing failures surface as 500 InternalError.
func classify(err error) string {
	switch {
	case apierrors.IsServiceUnavailable(err):
		return "no-warm-503"
	case apierrors.IsTooManyRequests(err):
		return "throttled-429"
	case apierrors.IsInternalError(err):
		return "internal-500"
	case apierrors.IsServerTimeout(err), apierrors.IsTimeout(err), errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case apierrors.IsConflict(err), apierrors.IsAlreadyExists(err):
		return "conflict"
	default:
		return "other"
	}
}

func logSampledf(key, format string, args ...any) {
	logMu.Lock()
	defer logMu.Unlock()
	if time.Since(lastLog[key]) < 5*time.Second {
		return
	}
	lastLog[key] = time.Now()
	fmt.Printf(format+"\n", args...)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "sandbox-sdk-loadgen: "+format+"\n", args...)
	os.Exit(1)
}
