//go:build e2e

// Command e2e drives the deployed sandbox-operator through real
// agents.x-k8s.io v1beta1 / extensions / v1alpha1 conversion scenarios against a
// live cluster, using only the Kubernetes SDK (controller-runtime client +
// client-go exec) — no kubectl, no mocks, no direct sandboxd.
//
// Run: KUBECONFIG=<vke-my> go run -tags e2e ./test/e2e -ns g0129-e2e -out /path/results.json
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	sandboxv1alpha1 "github.com/cocoonstack/sandbox-operator/api/v1alpha1"
	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	extv1alpha1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1alpha1"
	extv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/test/benchutil"
)

const image = "m.daocloud.io/docker.io/library/alpine:3.20"

var (
	ns     = flag.String("ns", "g0129-e2e", "test namespace")
	out    = flag.String("out", "/tmp/phase3-e2e-results.json", "results json path")
	run    = flag.String("run", "g0129", "run id label")
	only   = flag.String("only", "", "comma list of scenario names to run (default all)")
	cl     ctrlclient.Client
	cs     *kubernetes.Clientset
	cfg    *rest.Config
	scheme = runtime.NewScheme()
)

type result struct {
	Name     string `json:"name"`
	Passed   bool   `json:"passed"`
	Detail   string `json:"detail"`
	Err      string `json:"err,omitempty"`
	DurMs    int64  `json:"dur_ms"`
	Evidence string `json:"evidence,omitempty"`
}

func main() {
	flag.Parse()
	rootCtx := context.Background()
	benchutil.Must(clientgoscheme.AddToScheme(scheme))
	benchutil.Must(sandboxv1beta1.AddToScheme(scheme))
	benchutil.Must(sandboxv1alpha1.AddToScheme(scheme))
	benchutil.Must(extv1beta1.AddToScheme(scheme))
	benchutil.Must(extv1alpha1.AddToScheme(scheme))

	var err error
	cfg, err = clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
	benchutil.Must(err)
	cl, err = ctrlclient.New(cfg, ctrlclient.Options{Scheme: scheme})
	benchutil.Must(err)
	cs, err = kubernetes.NewForConfig(cfg)
	benchutil.Must(err)

	ensureNS(rootCtx, *ns)

	scenarios := []struct {
		name string
		fn   func(context.Context) (string, error)
	}{
		{"core_create_ready_standard_node", scCoreCreateReady},
		{"core_exec_workload", scExec},
		{"core_service_fqdn_conditions", scServiceFQDN},
		{"core_no_vk_injection", scNoVKInjection},
		{"core_suspend_resume", scSuspendResume},
		{"core_shutdown_retain_expired", scShutdownRetain},
		{"core_pvc", scPVC},
		{"core_delete_cleanup", scDeleteCleanup},
		{"ext_template_crud", scTemplateCRUD},
		{"ext_warmpool_scale", scWarmPoolScale},
		{"ext_claim_warm_hit", scClaimWarmHit},
		{"ext_conversion_v1alpha1_v1beta1", scConversion},
	}

	var results []result
	allPassed := true
	for _, s := range scenarios {
		if *only != "" && !strings.Contains(","+*only+",", ","+s.name+",") {
			continue
		}
		ctx, cancel := context.WithTimeout(rootCtx, 6*time.Minute)
		start := time.Now()
		detail, err := s.fn(ctx)
		cancel()
		r := result{Name: s.name, Passed: err == nil, Detail: detail, DurMs: time.Since(start).Milliseconds()}
		if err != nil {
			r.Err = err.Error()
			allPassed = false
		}
		results = append(results, r)
		status := "PASS"
		if err != nil {
			status = "FAIL"
		}
		fmt.Printf("[%s] %s (%dms) %s %s\n", status, s.name, r.DurMs, detail, r.Err)
	}

	summary := map[string]any{
		"all_passed": allPassed,
		"namespace":  *ns,
		"scenarios":  results,
		"count":      len(results),
	}
	b, _ := json.MarshalIndent(summary, "", "  ")
	_ = os.WriteFile(*out, b, 0o644)
	fmt.Printf("\nwrote %s (all_passed=%v)\n", *out, allPassed)
	if !allPassed {
		os.Exit(1)
	}
}

func ensureNS(ctx context.Context, n string) {
	nsObj := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: n, Labels: map[string]string{"cocoon-e2e-run": *run}}}
	_ = cl.Create(ctx, nsObj)
}

func labels() map[string]string { return map[string]string{"cocoon-e2e-run": *run} }

func newSandbox(name, cmd string, svc bool) *sandboxv1beta1.Sandbox {
	b := svc
	del := sandboxv1beta1.ShutdownPolicyDelete
	return &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: *ns, Labels: labels()},
		Spec: sandboxv1beta1.SandboxSpec{
			SandboxBlueprint: sandboxv1beta1.SandboxBlueprint{
				Service: &b,
				PodTemplate: sandboxv1beta1.PodTemplate{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{
							Name:    "agent",
							Image:   image,
							Command: []string{"sh", "-c", cmd},
						}},
					},
				},
			},
			Lifecycle: sandboxv1beta1.Lifecycle{ShutdownPolicy: &del},
		},
	}
}

func readyCond(s *sandboxv1beta1.Sandbox) *metav1.Condition {
	for i := range s.Status.Conditions {
		if s.Status.Conditions[i].Type == "Ready" {
			return &s.Status.Conditions[i]
		}
	}
	return nil
}

func waitSandboxReady(ctx context.Context, name string) (*sandboxv1beta1.Sandbox, error) {
	var last *sandboxv1beta1.Sandbox
	for {
		select {
		case <-ctx.Done():
			return last, fmt.Errorf("timeout waiting Ready")
		default:
		}
		s := &sandboxv1beta1.Sandbox{}
		if err := cl.Get(ctx, types.NamespacedName{Namespace: *ns, Name: name}, s); err != nil {
			return last, err
		}
		last = s
		if c := readyCond(s); c != nil && c.Status == metav1.ConditionTrue {
			return s, nil
		}
		time.Sleep(2 * time.Second)
	}
}

// isStandardNode returns true if the node is NOT a vk-cocoon virtual node.
func isStandardNode(ctx context.Context, nodeName string) (bool, error) {
	n := &corev1.Node{}
	if err := cl.Get(ctx, types.NamespacedName{Name: nodeName}, n); err != nil {
		return false, err
	}
	if n.Labels["node.kubernetes.io/instance-type"] == "virtual-node" {
		return false, nil
	}
	// vk-cocoon nodes carry the provider taint.
	if slices.ContainsFunc(n.Spec.Taints, func(t corev1.Taint) bool {
		return t.Key == "virtual-kubelet.io/provider"
	}) {
		return false, nil
	}
	return true, nil
}

func execInPod(ctx context.Context, pod, container string, cmd []string) (string, error) {
	req := cs.CoreV1().RESTClient().Post().Resource("pods").Name(pod).Namespace(*ns).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container, Command: cmd, Stdout: true, Stderr: true,
		}, clientgoscheme.ParameterCodec)
	exec, err := remotecommand.NewSPDYExecutor(cfg, "POST", req.URL())
	if err != nil {
		return "", err
	}
	var stdout, stderr strings.Builder
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr})
	return stdout.String() + stderr.String(), err
}

// WithoutCancel: cleanup defers run after the scenario ctx's timeout.
func deleteSandbox(ctx context.Context, name string) {
	s := &sandboxv1beta1.Sandbox{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: *ns}}
	_ = cl.Delete(context.WithoutCancel(ctx), s)
}

func deleteClaim(ctx context.Context, name string) {
	c := &extv1beta1.SandboxClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: *ns}}
	_ = cl.Delete(context.WithoutCancel(ctx), c)
}

func podForSandbox(ctx context.Context, name string) (*corev1.Pod, error) {
	// The operator names the backing Pod after the Sandbox.
	p := &corev1.Pod{}
	err := cl.Get(ctx, types.NamespacedName{Namespace: *ns, Name: name}, p)
	return p, err
}

func scCoreCreateReady(ctx context.Context) (string, error) {
	name := "e2e-core"
	deleteSandbox(ctx, name)
	time.Sleep(2 * time.Second)
	if err := cl.Create(ctx, newSandbox(name, "echo up; sleep 3600", true)); err != nil {
		return "", err
	}
	s, err := waitSandboxReady(ctx, name)
	if err != nil {
		return "", err
	}
	if s.Status.NodeName == "" || len(s.Status.PodIPs) == 0 {
		return "", fmt.Errorf("empty nodeName/podIPs: node=%q ips=%v", s.Status.NodeName, s.Status.PodIPs)
	}
	std, err := isStandardNode(ctx, s.Status.NodeName)
	if err != nil {
		return "", err
	}
	if !std {
		return "", fmt.Errorf("scheduled on vk-cocoon node %s, expected standard kubelet", s.Status.NodeName)
	}
	pod, err := podForSandbox(ctx, name)
	if err != nil {
		return "", fmt.Errorf("backing pod not found: %w", err)
	}
	owned := slices.ContainsFunc(pod.OwnerReferences, func(o metav1.OwnerReference) bool {
		return o.Kind == "Sandbox" && o.Name == name
	})
	if !owned {
		return "", fmt.Errorf("pod not owned by Sandbox")
	}
	return fmt.Sprintf("node=%s(standard) podIP=%s ownerRef=Sandbox/%s", s.Status.NodeName, s.Status.PodIPs[0], name), nil
}

func scExec(ctx context.Context) (string, error) {
	pod, err := podForSandbox(ctx, "e2e-core")
	if err != nil {
		return "", err
	}
	out, err := execInPod(ctx, pod.Name, "agent", []string{"sh", "-c", "echo exec-ok $((6*7))"})
	if err != nil {
		return "", err
	}
	if !strings.Contains(out, "exec-ok 42") {
		return "", fmt.Errorf("unexpected exec output: %q", out)
	}
	return "exec output: " + strings.TrimSpace(out), nil
}

func scServiceFQDN(ctx context.Context) (string, error) {
	s := &sandboxv1beta1.Sandbox{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: *ns, Name: "e2e-core"}, s); err != nil {
		return "", err
	}
	if s.Status.Service == "" || s.Status.ServiceFQDN == "" {
		return "", fmt.Errorf("missing service/FQDN: %q %q", s.Status.Service, s.Status.ServiceFQDN)
	}
	svc := &corev1.Service{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: *ns, Name: s.Status.Service}, svc); err != nil {
		return "", fmt.Errorf("service object missing: %w", err)
	}
	c := readyCond(s)
	if c == nil {
		return "", fmt.Errorf("no Ready condition")
	}
	return fmt.Sprintf("service=%s fqdn=%s reason=%s", s.Status.Service, s.Status.ServiceFQDN, c.Reason), nil
}

func scNoVKInjection(ctx context.Context) (string, error) {
	pod, err := podForSandbox(ctx, "e2e-core")
	if err != nil {
		return "", err
	}
	if v := pod.Spec.NodeSelector["node.kubernetes.io/instance-type"]; v == "virtual-node" {
		return "", fmt.Errorf("vk nodeSelector injected")
	}
	if slices.ContainsFunc(pod.Spec.Tolerations, func(t corev1.Toleration) bool {
		return t.Key == "virtual-kubelet.io/provider"
	}) {
		return "", fmt.Errorf("vk toleration injected")
	}
	for k := range pod.Annotations {
		if strings.HasPrefix(k, "cocoonset.cocoonstack.io/") || strings.HasPrefix(k, "vm.cocoonstack.io/") {
			return "", fmt.Errorf("cocoon annotation injected: %s", k)
		}
	}
	return "no vk nodeSelector/toleration/cocoonset annotations on standard pod", nil
}

func scSuspendResume(ctx context.Context) (string, error) {
	name := "e2e-suspend"
	deleteSandbox(ctx, name)
	time.Sleep(2 * time.Second)
	sb := newSandbox(name, "sleep 3600", false)
	if err := cl.Create(ctx, sb); err != nil {
		return "", err
	}
	defer deleteSandbox(ctx, name)
	if _, err := waitSandboxReady(ctx, name); err != nil {
		return "", err
	}
	s := &sandboxv1beta1.Sandbox{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: *ns, Name: name}, s); err != nil {
		return "", err
	}
	s.Spec.OperatingMode = sandboxv1beta1.SandboxOperatingModeSuspended
	if err := cl.Update(ctx, s); err != nil {
		return "", err
	}
	if err := waitPodGone(ctx, name, 90*time.Second); err != nil {
		return "", fmt.Errorf("pod not removed after suspend: %w", err)
	}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: *ns, Name: name}, s); err != nil {
		return "", err
	}
	s.Spec.OperatingMode = sandboxv1beta1.SandboxOperatingModeRunning
	if err := cl.Update(ctx, s); err != nil {
		return "", err
	}
	if _, err := waitSandboxReady(ctx, name); err != nil {
		return "", fmt.Errorf("not Ready after resume: %w", err)
	}
	return "suspend removed pod; resume recreated + Ready", nil
}

// pollDetail retries probe on a 3s tick until it yields evidence or d elapses; an empty string means keep polling.
func pollDetail(d time.Duration, timeoutMsg string, probe func() (string, error)) (string, error) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		detail, err := probe()
		if err != nil {
			return "", err
		}
		if detail != "" {
			return detail, nil
		}
		time.Sleep(3 * time.Second)
	}
	return "", errors.New(timeoutMsg)
}

func waitPodGone(ctx context.Context, name string, d time.Duration) error {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		p := &corev1.Pod{}
		if apierrors.IsNotFound(cl.Get(ctx, types.NamespacedName{Namespace: *ns, Name: name}, p)) {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("still present after %s", d)
}

func scShutdownRetain(ctx context.Context) (string, error) {
	name := "e2e-shutdown"
	deleteSandbox(ctx, name)
	time.Sleep(2 * time.Second)
	sb := newSandbox(name, "sleep 3600", false)
	retain := sandboxv1beta1.ShutdownPolicyRetain
	past := metav1.NewTime(time.Now().Add(-1 * time.Minute))
	sb.Spec.Lifecycle.ShutdownPolicy = &retain
	sb.Spec.Lifecycle.ShutdownTime = &past
	if err := cl.Create(ctx, sb); err != nil {
		return "", err
	}
	defer deleteSandbox(ctx, name)
	// With Retain + past shutdownTime, the sandbox should end up Expired (not deleted).
	return pollDetail(90*time.Second, "no Expired condition within deadline", func() (string, error) {
		s := &sandboxv1beta1.Sandbox{}
		if err := cl.Get(ctx, types.NamespacedName{Namespace: *ns, Name: name}, s); err != nil {
			return "", err
		}
		for _, c := range s.Status.Conditions {
			if strings.EqualFold(c.Type, "Expired") && c.Status == metav1.ConditionTrue {
				return "sandbox reached Expired=True under Retain policy", nil
			}
			if strings.Contains(strings.ToLower(c.Reason), "expire") {
				return "expiry reason observed: " + c.Reason, nil
			}
		}
		return "", nil
	})
}

func scPVC(ctx context.Context) (string, error) {
	name := "e2e-pvc"
	deleteSandbox(ctx, name)
	time.Sleep(2 * time.Second)
	sb := newSandbox(name, "sleep 3600", false)
	sb.Spec.VolumeClaimTemplates = []sandboxv1beta1.PersistentVolumeClaimTemplate{{
		EmbeddedObjectMetadata: sandboxv1beta1.EmbeddedObjectMetadata{Name: "data"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("64Mi")},
			},
		},
	}}
	if err := cl.Create(ctx, sb); err != nil {
		return "", err
	}
	defer deleteSandbox(ctx, name)
	return pollDetail(120*time.Second, "no PVC created for sandbox", func() (string, error) {
		pvcs := &corev1.PersistentVolumeClaimList{}
		if err := cl.List(ctx, pvcs, ctrlclient.InNamespace(*ns)); err == nil {
			for _, p := range pvcs.Items {
				if strings.Contains(p.Name, name) {
					return "PVC created: " + p.Name + " phase=" + string(p.Status.Phase), nil
				}
			}
		}
		return "", nil
	})
}

func scDeleteCleanup(ctx context.Context) (string, error) {
	name := "e2e-core"
	deleteSandbox(ctx, name)
	return pollDetail(90*time.Second, "resources not fully cleaned up", func() (string, error) {
		s := &sandboxv1beta1.Sandbox{}
		errS := cl.Get(ctx, types.NamespacedName{Namespace: *ns, Name: name}, s)
		p := &corev1.Pod{}
		errP := cl.Get(ctx, types.NamespacedName{Namespace: *ns, Name: name}, p)
		svc := &corev1.Service{}
		errSvc := cl.Get(ctx, types.NamespacedName{Namespace: *ns, Name: name}, svc)
		if apierrors.IsNotFound(errS) && apierrors.IsNotFound(errP) && apierrors.IsNotFound(errSvc) {
			return "sandbox+pod+service fully garbage-collected", nil
		}
		return "", nil
	})
}

func newTemplate(name string) *extv1beta1.SandboxTemplate {
	return &extv1beta1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: *ns, Labels: labels()},
		Spec: extv1beta1.SandboxTemplateSpec{
			SandboxBlueprint: sandboxv1beta1.SandboxBlueprint{
				PodTemplate: sandboxv1beta1.PodTemplate{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "agent", Image: image, Command: []string{"sh", "-c", "sleep 3600"}}},
					},
				},
			},
			NetworkPolicyManagement: "Unmanaged",
		},
	}
}

func scTemplateCRUD(ctx context.Context) (string, error) {
	name := "e2e-tmpl"
	_ = cl.Delete(ctx, &extv1beta1.SandboxTemplate{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: *ns}})
	time.Sleep(2 * time.Second)
	t := newTemplate(name)
	if err := cl.Create(ctx, t); err != nil {
		return "", err
	}
	got := &extv1beta1.SandboxTemplate{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: *ns, Name: name}, got); err != nil {
		return "", err
	}
	got.Spec.EnvVarsInjectionPolicy = "Allowed"
	if err := cl.Update(ctx, got); err != nil {
		return "", fmt.Errorf("update failed: %w", err)
	}
	return "template created + updated (envVarsInjectionPolicy=Allowed)", nil
}

func scWarmPoolScale(ctx context.Context) (string, error) {
	tmpl := "e2e-tmpl"
	name := "e2e-wp"
	_ = cl.Delete(ctx, &extv1beta1.SandboxWarmPool{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: *ns}})
	time.Sleep(2 * time.Second)
	two := int32(2)
	wp := &extv1beta1.SandboxWarmPool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: *ns, Labels: labels()},
		Spec: extv1beta1.SandboxWarmPoolSpec{
			Replicas:    &two,
			TemplateRef: extv1beta1.SandboxTemplateRef{Name: tmpl},
		},
	}
	if err := cl.Create(ctx, wp); err != nil {
		return "", err
	}
	return pollDetail(150*time.Second, "warm pool did not reach desired replicas", func() (string, error) {
		sl := &sandboxv1beta1.SandboxList{}
		if err := cl.List(ctx, sl, ctrlclient.InNamespace(*ns)); err == nil {
			warm := 0
			for _, s := range sl.Items {
				for _, or := range s.OwnerReferences {
					if or.Kind == "SandboxWarmPool" && or.Name == name {
						warm++
					}
				}
			}
			if warm >= 2 {
				return fmt.Sprintf("warm pool provisioned %d sandboxes", warm), nil
			}
		}
		return "", nil
	})
}

func scClaimWarmHit(ctx context.Context) (string, error) {
	name := "e2e-claim"
	_ = cl.Delete(ctx, &extv1beta1.SandboxClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: *ns}})
	time.Sleep(2 * time.Second)
	claim := &extv1beta1.SandboxClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: *ns, Labels: labels()},
		Spec: extv1beta1.SandboxClaimSpec{
			WarmPoolRef: extv1beta1.SandboxWarmPoolRef{Name: "e2e-wp"},
		},
	}
	if err := cl.Create(ctx, claim); err != nil {
		return "", err
	}
	defer deleteClaim(ctx, name)
	return pollDetail(120*time.Second, "claim did not bind to a warm sandbox", func() (string, error) {
		got := &extv1beta1.SandboxClaim{}
		if err := cl.Get(ctx, types.NamespacedName{Namespace: *ns, Name: name}, got); err == nil {
			if got.Status.SandboxStatus.Name != "" {
				return "claim bound to sandbox " + got.Status.SandboxStatus.Name, nil
			}
			for _, c := range got.Status.Conditions {
				if c.Status == metav1.ConditionTrue && (strings.Contains(strings.ToLower(c.Type), "bound") || strings.Contains(strings.ToLower(c.Type), "ready")) {
					return "claim condition " + c.Type + "=True", nil
				}
			}
		}
		return "", nil
	})
}

func scConversion(ctx context.Context) (string, error) {
	name := "e2e-conv"
	_ = cl.Delete(ctx, &sandboxv1alpha1.Sandbox{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: *ns}})
	time.Sleep(2 * time.Second)
	a := &sandboxv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: *ns, Labels: labels()},
		Spec: sandboxv1alpha1.SandboxSpec{
			PodTemplate: sandboxv1alpha1.PodTemplate{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "agent", Image: image, Command: []string{"sh", "-c", "sleep 3600"}}}},
			},
		},
	}
	if err := cl.Create(ctx, a); err != nil {
		return "", fmt.Errorf("create v1alpha1: %w", err)
	}
	defer deleteSandbox(ctx, name)
	b := &sandboxv1beta1.Sandbox{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: *ns, Name: name}, b); err != nil {
		return "", fmt.Errorf("read as v1beta1: %w", err)
	}
	if len(b.Spec.PodTemplate.Spec.Containers) == 0 || b.Spec.PodTemplate.Spec.Containers[0].Image != image {
		return "", fmt.Errorf("conversion lost podTemplate")
	}
	a2 := &sandboxv1alpha1.Sandbox{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: *ns, Name: name}, a2); err != nil {
		return "", fmt.Errorf("read as v1alpha1: %w", err)
	}
	return "v1alpha1 create → read as v1beta1 (podTemplate preserved) and v1alpha1 round-trip", nil
}
