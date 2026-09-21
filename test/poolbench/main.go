//go:build poolbench

// Command poolbench measures SandboxWarmPool behavior against the live operator
// via the Kubernetes SDK: (A) pool fill throughput (scale a pool and time how
// fast sandboxes reach Ready) and (B) warm-claim latency (fire SandboxClaims at
// a pre-warmed pool and measure claim -> bound), which is the apples-to-apples
// comparison point against snapshot-resume sandbox services.
//
//	Run: KUBECONFIG=<vke-my> go run -tags poolbench ./test/poolbench \
//	       -ns sandbox-pool -pool 60 -claims 50 -claim-conc 25 -out results.json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	extv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/test/benchutil"
)

const image = "m.daocloud.io/docker.io/library/alpine:3.20"

var (
	ns        = flag.String("ns", "sandbox-pool", "namespace")
	poolSize  = flag.Int("pool", 60, "warm pool desired replicas to fill and measure")
	claims    = flag.Int("claims", 50, "number of claims to fire for latency")
	claimConc = flag.Int("claim-conc", 25, "claim concurrency")
	fillWait  = flag.Int("fill-timeout", 300, "seconds to wait for pool fill")
	out       = flag.String("out", "/tmp/poolbench.json", "results json")
	sbRuntime = flag.String("runtime", "standard", "standard|vk-cocoon (vk-cocoon = real MicroVM)")
	imageFlag = flag.String("image", image, "sandbox container/VM image")
	nodeSelKV = flag.String("node-selector", "sandbox.cocoonstack.io/schedulable=true", "pod nodeSelector key=value")
	cpuReq    = flag.String("cpu", "10m", "cpu request")
	memReq    = flag.String("mem", "16Mi", "memory request")
	snapPol   = flag.String("snapshot-policy", "", "cocoonset.cocoonstack.io/snapshot-policy (never for churn)")
	mode      = flag.String("mode", "both", "both|fill|claim (claim = measure latency against an existing pool)")
	cl        ctrlclient.Client
	wcl       ctrlclient.WithWatch
	cfg       *rest.Config
	scheme    = runtime.NewScheme()
)

const (
	tmplName = "poolbench-tmpl"
	poolName = "poolbench-pool"
)

func main() {
	flag.Parse()
	ctx := context.Background()
	benchutil.Must(clientgoscheme.AddToScheme(scheme))
	benchutil.Must(sandboxv1beta1.AddToScheme(scheme))
	benchutil.Must(extv1beta1.AddToScheme(scheme))
	var err error
	cfg, err = clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
	benchutil.Must(err)
	cfg.QPS, cfg.Burst = 400, 800
	cl, err = ctrlclient.New(cfg, ctrlclient.Options{Scheme: scheme})
	benchutil.Must(err)
	wcl, err = ctrlclient.NewWithWatch(cfg, ctrlclient.Options{Scheme: scheme})
	benchutil.Must(err)

	result := map[string]any{"ns": *ns, "poolTarget": *poolSize}

	if *mode == "claim" {
		// Claim-only: measure latency against a pool that an external orchestrator filled.
		claimRes := claimLatency(ctx, *claims, *claimConc)
		result["claim"] = claimRes
		fmt.Printf("[claim] n=%d conc=%d warmHits=%d succ=%.1f%% latency p50=%.0fms p95=%.0fms p99=%.0fms max=%.0fms\n",
			*claims, *claimConc, claimRes["warmHits"], claimRes["successRate"].(float64)*100, claimRes["p50Ms"], claimRes["p95Ms"], claimRes["p99Ms"], claimRes["maxMs"])
		b, _ := json.MarshalIndent(result, "", "  ")
		_ = os.WriteFile(*out, b, 0o644)
		fmt.Printf("wrote %s\n", *out)
		return
	}

	benchutil.EnsureNamespace(ctx, cl, *ns, map[string]string{"cocoon-e2e-run": "g0129-pool"})
	ensureTemplate(ctx)

	fill := fillPool(ctx, *poolSize)
	result["fill"] = fill
	fmt.Printf("[fill] target=%d reachedReady=%d in %.1fs -> %.2f ready/s; createToReady p50=%.0fms p95=%.0fms\n",
		*poolSize, fill["reachedReady"], fill["elapsedSec"], fill["throughputPerSec"], fill["p50Ms"], fill["p95Ms"])

	if *mode == "fill" {
		b, _ := json.MarshalIndent(result, "", "  ")
		_ = os.WriteFile(*out, b, 0o644)
		fmt.Printf("wrote %s\n", *out)
		return
	}

	// Gate: the pool must be fully warm and steady, else claims race replenishment.
	waitPoolStable(ctx, *poolSize, 120)

	claimRes := claimLatency(ctx, *claims, *claimConc)
	result["claim"] = claimRes
	fmt.Printf("[claim] n=%d conc=%d warmHits=%d succ=%.1f%% latency p50=%.0fms p95=%.0fms p99=%.0fms max=%.0fms\n",
		*claims, *claimConc, claimRes["warmHits"], claimRes["successRate"].(float64)*100, claimRes["p50Ms"], claimRes["p95Ms"], claimRes["p99Ms"], claimRes["maxMs"])

	b, _ := json.MarshalIndent(result, "", "  ")
	_ = os.WriteFile(*out, b, 0o644)
	fmt.Printf("wrote %s\n", *out)
}

func ensureTemplate(ctx context.Context) {
	nodeSel := map[string]string{}
	if kv := strings.SplitN(*nodeSelKV, "=", 2); len(kv) == 2 && kv[0] != "" {
		nodeSel[kv[0]] = kv[1]
	}
	podAnnotations := map[string]string{}
	container := corev1.Container{
		Name: "agent", Image: *imageFlag, ImagePullPolicy: corev1.PullIfNotPresent,
		Command: []string{"/bin/sh", "-c", "sleep 100000"},
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(*cpuReq),
			corev1.ResourceMemory: resource.MustParse(*memReq),
		}},
	}
	if *sbRuntime == "vk-cocoon" {
		// Real cocoon MicroVM: select the vk-cocoon backend; the operator's podruntime
		// mutator adds mode=run/managed/image/os + virtual-node selector + toleration.
		podAnnotations["sandbox.cocoonstack.io/runtime"] = "vk-cocoon"
		// vk-cocoon boots the VM from an image; no in-container command applies.
		container.Command = nil
	}
	if *snapPol != "" {
		podAnnotations["cocoonset.cocoonstack.io/snapshot-policy"] = *snapPol
	}
	benchutil.EnsureTemplate(ctx, cl, *ns, tmplName, podAnnotations, corev1.PodSpec{
		NodeSelector: nodeSel,
		Containers:   []corev1.Container{container},
	})
}

func ensurePool(ctx context.Context, replicas int32) {
	benchutil.EnsurePool(ctx, cl, *ns, poolName, tmplName, replicas, nil)
}

func readySandboxes(ctx context.Context) (total, ready int) {
	return benchutil.ReadySandboxes(ctx, cl, *ns, poolName)
}

func fillPool(ctx context.Context, target int) map[string]any {
	ensurePool(ctx, int32(target))
	start := time.Now()
	deadline := start.Add(time.Duration(*fillWait) * time.Second)
	series := []map[string]any{}
	lastReady := 0
	for time.Now().Before(deadline) {
		total, ready := readySandboxes(ctx)
		series = append(series, map[string]any{"t": int(time.Since(start).Seconds()), "total": total, "ready": ready})
		if ready > lastReady {
			lastReady = ready
		}
		if ready >= target {
			break
		}
		time.Sleep(2 * time.Second)
	}
	elapsed := time.Since(start).Seconds()
	// createToReady per sandbox: creationTimestamp -> Ready condition lastTransitionTime
	var c2r []float64
	sl := &sandboxv1beta1.SandboxList{}
	_ = cl.List(ctx, sl, ctrlclient.InNamespace(*ns))
	for i := range sl.Items {
		s := &sl.Items[i]
		for _, c := range s.Status.Conditions {
			if c.Type == "Ready" && c.Status == metav1.ConditionTrue && !c.LastTransitionTime.IsZero() {
				d := c.LastTransitionTime.Sub(s.CreationTimestamp.Time).Seconds() * 1000
				if d >= 0 {
					c2r = append(c2r, d)
				}
			}
		}
	}
	thr := 0.0
	if elapsed > 0 {
		thr = float64(lastReady) / elapsed
	}
	return map[string]any{
		"reachedReady":     lastReady,
		"elapsedSec":       benchutil.Round1(elapsed),
		"throughputPerSec": benchutil.Round2(thr),
		"p50Ms":            pct(c2r, 50),
		"p95Ms":            pct(c2r, 95),
		"p99Ms":            pct(c2r, 99),
		"series":           series,
	}
}

// waitPoolStable blocks until the pool has `target` Ready sandboxes for 3
// consecutive checks (no in-flight replenishment), so the claim measurement is
// not racing pod pulls.
func waitPoolStable(ctx context.Context, target, timeoutSec int) {
	deadline := time.Now().Add(time.Duration(timeoutSec) * time.Second)
	stable := 0
	for time.Now().Before(deadline) {
		_, ready := readySandboxes(ctx)
		if ready >= target {
			stable++
			if stable >= 3 {
				fmt.Printf("[pool] stable at %d ready\n", ready)
				return
			}
		} else {
			stable = 0
		}
		time.Sleep(2 * time.Second)
	}
	_, ready := readySandboxes(ctx)
	fmt.Printf("[pool] WARN: not fully stable, proceeding at %d/%d ready\n", ready, target)
}

// claimLatency measures claim -> Bound latency using a single watch stream
// (event-driven, no polling load on the apiserver, so the measurement does not
// contend with the controller it is measuring).
func claimLatency(ctx context.Context, n, conc int) map[string]any {
	base := fmt.Sprintf("plc-%d", time.Now().Unix()%100000)
	names := make([]string, n)
	track := make(map[string]bool, n)
	for i := range names {
		names[i] = fmt.Sprintf("%s-%d", base, i)
		track[names[i]] = true
	}

	// Establish the watch before creating claims so no Bound event is missed.
	seed := &extv1beta1.SandboxClaimList{}
	_ = wcl.List(ctx, seed, ctrlclient.InNamespace(*ns))
	w, err := wcl.Watch(ctx, &extv1beta1.SandboxClaimList{}, ctrlclient.InNamespace(*ns))
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	defer w.Stop()

	var mu sync.Mutex
	createTimes := make(map[string]time.Time, n)
	lat := make(map[string]float64, n)
	allBound := make(chan struct{})

	go func() {
		for ev := range w.ResultChan() {
			c, ok := ev.Object.(*extv1beta1.SandboxClaim)
			if !ok || !track[c.Name] || c.Status.SandboxStatus.Name == "" {
				continue
			}
			recv := time.Now()
			mu.Lock()
			if ct, have := createTimes[c.Name]; have && !ct.IsZero() {
				if _, done := lat[c.Name]; !done {
					lat[c.Name] = float64(recv.Sub(ct).Microseconds()) / 1000
					if len(lat) == n {
						mu.Unlock()
						close(allBound)
						return
					}
				}
			}
			mu.Unlock()
		}
	}()

	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	fails := 0
	for i := range n {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			claim := &extv1beta1.SandboxClaim{
				ObjectMeta: metav1.ObjectMeta{Name: names[i], Namespace: *ns, Labels: map[string]string{"cocoon-e2e-run": "g0129-pool"}},
				Spec:       extv1beta1.SandboxClaimSpec{WarmPoolRef: extv1beta1.SandboxWarmPoolRef{Name: poolName}},
			}
			mu.Lock()
			createTimes[names[i]] = time.Now()
			mu.Unlock()
			if err := cl.Create(ctx, claim); err != nil {
				mu.Lock()
				createTimes[names[i]] = time.Time{}
				fails++
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	select {
	case <-allBound:
	case <-time.After(90 * time.Second):
	}
	w.Stop()

	for _, nm := range names {
		_ = cl.Delete(ctx, &extv1beta1.SandboxClaim{ObjectMeta: metav1.ObjectMeta{Name: nm, Namespace: *ns}})
	}

	mu.Lock()
	defer mu.Unlock()
	var xs []float64
	for _, v := range lat {
		xs = append(xs, v)
	}
	rate := 0.0
	if n > 0 {
		rate = float64(len(xs)) / float64(n)
	}
	return map[string]any{
		"total": n, "successes": len(xs), "warmHits": len(xs), "successRate": rate,
		"p50Ms": pct(xs, 50), "p95Ms": pct(xs, 95), "p99Ms": pct(xs, 99), "maxMs": pct(xs, 100),
		"createFails": fails, "measurement": "watch-based",
	}
}

func pct(xs []float64, p float64) float64 { return benchutil.Pct(xs, p, benchutil.Round1) }
