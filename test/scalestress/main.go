//go:build scalestress

// Command scalestress ramps a warm pool of real microVMs across a set of
// vk-cocoon nodes and watches whether growing the cluster's object count wedges
// the apiserver's LIST path. The concern it probes: APF prices a LIST seat by the
// resource's *total* object count, so even if the nodes under test run a
// cache-fed (L0-fixed) vk, the OTHER nodes' older vk still issue bare
// cluster-wide LIST pods whose width — and seat-seconds — grow with every
// sandbox added anywhere. VKE serves those LISTs from a dedicated priority level
// (`vke-list-limit`), so this tool samples that level's seat use, queue depth,
// and rejections at each ramp step, alongside the global LIST pods/sandboxes
// latency. Production desktops on the target nodes are a hard guard: if their pod
// count changes, it aborts.
//
//	Run: KUBECONFIG=<vke-my> go run -tags scalestress ./test/scalestress \
//	       -hosts cocoon-bd23,cocoon-bd24,cocoon-bd25,cocoon-bd26 \
//	       -steps 50,100,150,200 -out stress.json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	extv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/test/benchutil"
)

var (
	ns        = flag.String("ns", "g0131-stress", "namespace for the run")
	hostsCSV  = flag.String("hosts", "cocoon-bd23,cocoon-bd24,cocoon-bd25,cocoon-bd26", "vk-cocoon nodes to spread across")
	stepsCSV  = flag.String("steps", "50,100,150,200", "cumulative warm-pool sizes (kept <=200)")
	prodNS    = flag.String("prod-ns", "cloud-desktop", "namespace whose pods on the target nodes must not change")
	sbImage   = flag.String("image", "ghcr.io/cocoonstack/sandbox/rt:24.04", "sandbox VM image")
	plName    = flag.String("priority-level", "vke-list-limit", "APF priority level that carries vk LIST")
	listN     = flag.Int("list-samples", 15, "client LIST latency samples per step")
	windowN   = flag.Int("window-samples", 10, "apiserver metric samples per step (peak detection)")
	windowGap = flag.Int("window-gap-ms", 1500, "gap between apiserver metric samples")
	stepWait  = flag.Int("step-timeout", 900, "seconds to wait for each ramp step to reach ready")
	settle    = flag.Int("settle", 6, "seconds to let the pool settle before measuring")
	out       = flag.String("out", "/tmp/scalestress.json", "results json path")
	keepAfter = flag.Bool("keep", false, "skip cleanup (debug)")

	cl     ctrlclient.Client
	cs     *kubernetes.Clientset
	scheme = runtime.NewScheme()
)

const (
	tmplName = "g0131-stress-tmpl"
	poolName = "g0131-stress-pool"
	runLabel = "cocoon-e2e-run"
	runVal   = "g0131-stress"
)

func main() {
	flag.Parse()
	benchutil.Must(clientgoscheme.AddToScheme(scheme))
	benchutil.Must(sandboxv1beta1.AddToScheme(scheme))
	benchutil.Must(extv1beta1.AddToScheme(scheme))
	cfg, err := clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
	benchutil.Must(err)
	cfg.QPS, cfg.Burst = 200, 400
	cl, err = ctrlclient.New(cfg, ctrlclient.Options{Scheme: scheme})
	benchutil.Must(err)
	cs, err = kubernetes.NewForConfig(cfg)
	benchutil.Must(err)

	hosts := splitCSV(*hostsCSV)
	steps := parseSteps(*stepsCSV)
	for _, n := range steps {
		if n > 200 {
			benchutil.Must(fmt.Errorf("refusing step %d: this run is capped at 200 sandboxes", n))
		}
	}
	ctx := context.Background()

	prodBase := prodPods(ctx, *prodNS, hosts)
	fmt.Printf("[prod] baseline on %v = %d pods (hard guard)\n", hosts, prodBase)
	if prodBase <= 0 {
		benchutil.Must(fmt.Errorf("refusing to run: prod baseline non-positive (%d)", prodBase))
	}

	benchutil.EnsureNamespace(ctx, cl, *ns, map[string]string{runLabel: runVal})
	ensureTemplate(ctx, hosts)

	result := map[string]any{
		"goal": "G-0131", "layer": "stress-apiserver", "date": time.Now().UTC().Format("2006-01-02"),
		"hosts": hosts, "prod_baseline": prodBase, "steps": steps, "priority_level": *plName,
		"probe": "does growing the cluster object count wedge apiserver LIST (APF seat-seconds on the vk LIST priority level)? new vk (these nodes) is cache-fed; old vk elsewhere does bare cluster-wide LIST.",
	}
	var rounds []map[string]any
	abort := ""

	// A pre-fill snapshot so step 1's deltas are measured against the idle cluster.
	prev := scrape(ctx)
	prevT := time.Now()

	for _, n := range steps {
		fmt.Printf("\n=== ramp to %d ===\n", n)
		benchutil.EnsurePool(ctx, cl, *ns, poolName, tmplName, int32(n), map[string]string{runLabel: runVal})
		ready := benchutil.WaitReady(ctx, cl, *ns, poolName, n, *stepWait)
		time.Sleep(time.Duration(*settle) * time.Second)

		if prodNow := prodPods(ctx, *prodNS, hosts); prodNow != prodBase {
			abort = fmt.Sprintf("prod pod count on target nodes changed %d -> %d at step %d", prodBase, prodNow, n)
			fmt.Printf("[ABORT] %s\n", abort)
			break
		}

		// Sampling window: catch peak seat use / queue depth while vk nodes LIST.
		peakInqueue, peakInUse := 0.0, 0.0
		var last map[string]float64
		for range *windowN {
			s := scrape(ctx)
			peakInqueue = max(peakInqueue, s["vke_inqueue"])
			peakInUse = max(peakInUse, s["vke_inuse"])
			last = s
			time.Sleep(time.Duration(*windowGap) * time.Millisecond)
		}
		curT := time.Now()

		sbLat := listLatency(ctx, *listN, func() (int, error) { return listSandboxes(ctx) })
		podLat := listLatency(ctx, *listN, func() (int, error) { return listPods(ctx) })
		dist := nodeDistribution(ctx)
		prodNow := prodPods(ctx, *prodNS, hosts)

		round := map[string]any{
			"target": n, "ready": ready,
			"sandbox_cr_total":                sbLat.count,
			"cluster_pod_total":               podLat.count,
			"client_list_sandboxes":           msStats(sbLat),
			"client_list_pods":                msStats(podLat),
			"apiserver_list_sandboxes_avg_ms": benchutil.Round2(deltaAvgMs(prev, last, "list_sandboxes")),
			"apiserver_list_pods_avg_ms":      benchutil.Round2(deltaAvgMs(prev, last, "list_pods")),
			"vke_seats_nominal":               last["vke_nominal"],
			"vke_seats_inuse_peak":            benchutil.Round2(peakInUse),
			"vke_inqueue_peak":                benchutil.Round2(peakInqueue),
			"vke_wait_avg_ms":                 benchutil.Round2(deltaAvgMs(prev, last, "vke_wait")),
			"vke_dispatched_delta":            benchutil.Round0(last["vke_dispatched"] - prev["vke_dispatched"]),
			"vke_rejected_timeout_delta":      benchutil.Round0(last["vke_rej_timeout"] - prev["vke_rej_timeout"]),
			"vke_rejected_cancelled_delta":    benchutil.Round0(last["vke_rej_cancelled"] - prev["vke_rej_cancelled"]),
			"node_distribution":               dist,
			"prod_intact":                     prodNow,
			"window_sec":                      benchutil.Round1(curT.Sub(prevT).Seconds()),
		}
		rounds = append(rounds, round)
		fmt.Printf("[N=%d] sbCR=%d pods=%d | client LIST sb p50=%.0f/p95=%.0fms pods p50=%.0f/p95=%.0fms | apiserver LIST sb=%.0fms pods=%.1fms | %s seats %.0f/%.0f peak inq=%.0f wait=%.0fms disp+%.0f REJECT_timeout+%.0f | dist=%v prod=%d\n",
			n, sbLat.count, podLat.count, msP(sbLat, 50), msP(sbLat, 95), msP(podLat, 50), msP(podLat, 95),
			deltaAvgMs(prev, last, "list_sandboxes"), deltaAvgMs(prev, last, "list_pods"),
			*plName, peakInUse, last["vke_nominal"], peakInqueue, deltaAvgMs(prev, last, "vke_wait"),
			last["vke_dispatched"]-prev["vke_dispatched"], last["vke_rej_timeout"]-prev["vke_rej_timeout"],
			dist, prodNow)
		prev, prevT = last, curT
	}

	result["rounds"] = rounds
	result["aborted"] = abort

	// A wedge tendency shows up as: any NEW time-out rejection on
	// the vk LIST level during the ramp, seat use approaching the nominal limit, or
	// LIST latency climbing materially with object count.
	newRejections, maxInUse := 0.0, 0.0
	var firstSbP95, lastSbP95 float64
	prodOK := abort == ""
	for i, r := range rounds {
		newRejections += r["vke_rejected_timeout_delta"].(float64)
		maxInUse = max(maxInUse, r["vke_seats_inuse_peak"].(float64))
		if r["prod_intact"].(int) != prodBase {
			prodOK = false
		}
		p95 := r["client_list_sandboxes"].(map[string]any)["p95_ms"].(float64)
		if i == 0 {
			firstSbP95 = p95
		}
		lastSbP95 = p95
	}
	ratio := 0.0
	if firstSbP95 > 0 {
		ratio = lastSbP95 / firstSbP95
	}
	nominal := 79.0
	if len(rounds) > 0 {
		if v, ok := rounds[0]["vke_seats_nominal"].(float64); ok && v > 0 {
			nominal = v
		}
	}
	wedgeObserved := newRejections > 0 || (nominal > 0 && maxInUse >= 0.8*nominal)
	result["prod_intact_throughout"] = prodOK
	result["new_list_timeout_rejections"] = newRejections
	result["peak_vke_seats_inuse"] = maxInUse
	result["vke_seats_nominal"] = nominal
	result["client_list_sandboxes_p95_ratio"] = benchutil.Round2(ratio)
	result["wedge_observed"] = wedgeObserved
	// This is an exploratory probe, not an acceptance gate: "pass" just means the
	// run completed cleanly and did not harm production. The wedge finding is the
	// payload, reported separately.
	result["completed_clean"] = abort == "" && len(rounds) == len(steps) && prodOK

	b, _ := json.MarshalIndent(result, "", "  ")
	benchutil.Must(os.WriteFile(*out, b, 0o644))
	fmt.Printf("\n== new LIST time-out rejections during ramp: %.0f | peak %s seats in use: %.0f/%.0f | client LIST-sb p95 x%.2f | wedge_observed=%v | prod intact=%v ==\n",
		newRejections, *plName, maxInUse, nominal, ratio, wedgeObserved, prodOK)
	fmt.Printf("wrote %s\n", *out)

	if !*keepAfter {
		cleanup(ctx, prodBase, hosts)
	}
	if abort != "" {
		os.Exit(1)
	}
}

func ensureTemplate(ctx context.Context, hosts []string) {
	container := corev1.Container{
		Name: "agent", Image: *sbImage, ImagePullPolicy: corev1.PullIfNotPresent,
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("10m"),
			corev1.ResourceMemory: resource.MustParse("16Mi"),
		}},
	}
	benchutil.EnsureTemplate(ctx, cl, *ns, tmplName,
		map[string]string{"sandbox.cocoonstack.io/runtime": "vk-cocoon"},
		corev1.PodSpec{
			Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      "kubernetes.io/hostname",
							Operator: corev1.NodeSelectorOpIn,
							Values:   hosts,
						}},
					}},
				},
			}},
			Containers: []corev1.Container{container},
		})
}

func ourReady(ctx context.Context) (total, ready int) {
	return benchutil.ReadySandboxes(ctx, cl, *ns, poolName)
}

func nodeDistribution(ctx context.Context) map[string]int {
	dist := map[string]int{}
	sl := &sandboxv1beta1.SandboxList{}
	if err := cl.List(ctx, sl, ctrlclient.InNamespace(*ns)); err != nil {
		return dist
	}
	for i := range sl.Items {
		if nn := sl.Items[i].Status.NodeName; nn != "" {
			dist[nn]++
		}
	}
	return dist
}

func prodPods(ctx context.Context, namespace string, hosts []string) int {
	pl := &corev1.PodList{}
	if err := cl.List(ctx, pl, ctrlclient.InNamespace(namespace)); err != nil {
		return -1
	}
	set := map[string]bool{}
	for _, h := range hosts {
		set[h] = true
	}
	n := 0
	for i := range pl.Items {
		if set[pl.Items[i].Spec.NodeName] {
			n++
		}
	}
	return n
}

type latSamples struct {
	ms    []float64
	count int
}

func listLatency(ctx context.Context, samples int, do func() (int, error)) latSamples {
	var s latSamples
	for range samples {
		start := time.Now()
		c, err := do()
		d := float64(time.Since(start).Microseconds()) / 1000
		if err == nil {
			s.ms = append(s.ms, d)
			s.count = c
		}
		time.Sleep(150 * time.Millisecond)
	}
	return s
}

func listSandboxes(ctx context.Context) (int, error) {
	sl := &sandboxv1beta1.SandboxList{}
	if err := cl.List(ctx, sl); err != nil {
		return 0, err
	}
	return len(sl.Items), nil
}

func listPods(ctx context.Context) (int, error) {
	pl := &corev1.PodList{}
	if err := cl.List(ctx, pl); err != nil {
		return 0, err
	}
	return len(pl.Items), nil
}

// scrape pulls /metrics once and aggregates the LIST request-duration sums and
// the vk LIST priority level's APF state.
func scrape(ctx context.Context) map[string]float64 {
	m := map[string]float64{}
	raw, err := cs.RESTClient().Get().AbsPath("/metrics").DoRaw(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[warn] scrape /metrics: %v\n", err)
		return m
	}
	plTag := `priority_level="` + *plName + `"`
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || line[0] == '#' {
			continue
		}
		val := lineValue(line)
		switch {
		case strings.HasPrefix(line, "apiserver_request_duration_seconds_sum{") && strings.Contains(line, `verb="LIST"`):
			if strings.Contains(line, `resource="pods"`) {
				m["list_pods_sum"] += val
			} else if strings.Contains(line, `resource="sandboxes"`) {
				m["list_sandboxes_sum"] += val
			}
		case strings.HasPrefix(line, "apiserver_request_duration_seconds_count{") && strings.Contains(line, `verb="LIST"`):
			if strings.Contains(line, `resource="pods"`) {
				m["list_pods_count"] += val
			} else if strings.Contains(line, `resource="sandboxes"`) {
				m["list_sandboxes_count"] += val
			}
		case !strings.Contains(line, plTag):
			// only the vk LIST priority level below
		case strings.HasPrefix(line, "apiserver_flowcontrol_nominal_limit_seats{"):
			m["vke_nominal"] = val
		case strings.HasPrefix(line, "apiserver_flowcontrol_request_concurrency_in_use{"):
			m["vke_inuse"] += val
		case strings.HasPrefix(line, "apiserver_flowcontrol_current_inqueue_requests{"):
			m["vke_inqueue"] += val
		case strings.HasPrefix(line, "apiserver_flowcontrol_dispatched_requests_total{"):
			m["vke_dispatched"] += val
		case strings.HasPrefix(line, "apiserver_flowcontrol_request_wait_duration_seconds_sum{"):
			m["vke_wait_sum"] += val
		case strings.HasPrefix(line, "apiserver_flowcontrol_request_wait_duration_seconds_count{"):
			m["vke_wait_count"] += val
		case strings.HasPrefix(line, "apiserver_flowcontrol_rejected_requests_total{"):
			if strings.Contains(line, `reason="time-out"`) {
				m["vke_rej_timeout"] += val
			} else if strings.Contains(line, `reason="cancelled"`) {
				m["vke_rej_cancelled"] += val
			}
		}
	}
	return m
}

func lineValue(line string) float64 {
	i := strings.LastIndexByte(line, ' ')
	if i < 0 {
		return 0
	}
	v, _ := strconv.ParseFloat(strings.TrimSpace(line[i+1:]), 64)
	return v
}

// deltaAvgMs returns the mean duration in ms over the window for a *_sum/*_count
// series pair (list_sandboxes | list_pods | vke_wait).
func deltaAvgMs(prev, cur map[string]float64, series string) float64 {
	sumK, cntK := series+"_sum", series+"_count"
	dc := cur[cntK] - prev[cntK]
	if dc <= 0 {
		return 0
	}
	return (cur[sumK] - prev[sumK]) / dc * 1000
}

func msP(s latSamples, p float64) float64 { return pct(s.ms, p) }

func msStats(s latSamples) map[string]any {
	return map[string]any{
		"p50_ms": pct(s.ms, 50), "p95_ms": pct(s.ms, 95), "max_ms": pct(s.ms, 100), "samples": len(s.ms),
	}
}

func pct(xs []float64, p float64) float64 { return benchutil.Pct(xs, p, benchutil.Round2) }

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func parseSteps(s string) []int {
	var out []int
	for _, p := range splitCSV(s) {
		v, err := strconv.Atoi(p)
		benchutil.Must(err)
		out = append(out, v)
	}
	return out
}

func cleanup(ctx context.Context, prodBase int, hosts []string) {
	fmt.Printf("[cleanup] deleting pool/template/ns %s\n", *ns)
	_ = cl.Delete(ctx, &extv1beta1.SandboxWarmPool{ObjectMeta: metav1.ObjectMeta{Name: poolName, Namespace: *ns}})
	_ = cl.Delete(ctx, &extv1beta1.SandboxTemplate{ObjectMeta: metav1.ObjectMeta{Name: tmplName, Namespace: *ns}})
	deadline := time.Now().Add(300 * time.Second)
	for time.Now().Before(deadline) {
		total, _ := ourReady(ctx)
		if total == 0 {
			break
		}
		time.Sleep(3 * time.Second)
	}
	_ = cl.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: *ns}})
	total, _ := ourReady(ctx)
	prodNow := prodPods(ctx, *prodNS, hosts)
	fmt.Printf("[cleanup] residual sandboxes=%d, prod on target nodes=%d (baseline %d)\n", total, prodNow, prodBase)
}
