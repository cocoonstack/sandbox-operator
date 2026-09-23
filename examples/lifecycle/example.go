// Command example exercises every sandbox operation this operator serves,
// through BOTH of its surfaces, against a live cluster:
//
//   - the Kubernetes API (controller-runtime client + the action subresources)
//   - the e2b-compatible REST API (the same warm pools, no Kubernetes client)
//
// It is a runnable acceptance walk-through, not a unit test: each step prints
// what it did so the output doubles as evidence.
//
//	go run ./examples/lifecycle \
//	  -kubeconfig ~/.kube/config -namespace default \
//	  -template <image> \
//	  -e2b-url http://localhost:8080 -e2b-key <key>
//
// Omit -e2b-url to run only the Kubernetes half; omit -template and it is
// discovered from the fleet's advertised warm pools.
//
// Two behaviors of this system shape the code below and are worth reading
// before copying it:
//
//   - Lists are eventually consistent. Sandbox objects are synthesized from
//     per-node NodeInventory, which nodes republish on a ~30s cadence; a Get by
//     name asks the node only on the apiserver replica that served the create.
//     waitVisible below covers a Get that reaches another replica.
//   - Latency is not uniform. resume takes cocoon's mmap restore fast path and
//     fork clones a node-local snapshot, but pause and snapshot write the
//     guest's memory out and therefore cost time proportional to its size.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cocoonv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

const (
	// visibilityTimeout bounds the wait for the read view to publish a sandbox.
	// It is generous relative to the ~30s inventory cadence so a single slow
	// publish does not fail the walk-through.
	visibilityTimeout = 90 * time.Second

	claimIDAnnotation = scale.ClaimIDAnnotation
)

type options struct {
	kubeconfig string
	namespace  string
	template   string
	e2bURL     string
	e2bKey     string
	keep       bool
}

func main() {
	var o options
	flag.StringVar(&o.kubeconfig, "kubeconfig", os.Getenv("KUBECONFIG"), "path to a kubeconfig; empty uses in-cluster config")
	flag.StringVar(&o.namespace, "namespace", "default", "namespace to create sandboxes in")
	flag.StringVar(&o.template, "template", "", "container image selecting the warm pool; empty discovers one from NodeInventory")
	flag.StringVar(&o.e2bURL, "e2b-url", "", "base URL of the e2b-compatible surface; empty skips the e2b half")
	flag.StringVar(&o.e2bKey, "e2b-key", "", "X-API-KEY for the e2b surface")
	flag.BoolVar(&o.keep, "keep", false, "leave the created sandboxes running instead of deleting them")
	flag.Parse()

	if err := run(context.Background(), o); err != nil {
		fmt.Fprintln(os.Stderr, "FAILED:", err)
		os.Exit(1)
	}
	fmt.Println("\nAll operations completed.")
}

func run(ctx context.Context, o options) error {
	scheme, err := newScheme()
	if err != nil {
		return err
	}
	c, err := newClient(o.kubeconfig, scheme)
	if err != nil {
		return fmt.Errorf("build Kubernetes client: %w", err)
	}
	rc, err := newRESTClient(o.kubeconfig, scheme)
	if err != nil {
		return fmt.Errorf("build REST client: %w", err)
	}
	if o.template == "" {
		if o.template, err = discoverTemplate(ctx, c); err != nil {
			return err
		}
		fmt.Printf("discovered template from the fleet: %s\n", o.template)
	}
	checkpoint, err := runKubernetes(ctx, c, rc, o)
	if err != nil {
		return fmt.Errorf("kubernetes surface: %w", err)
	}
	if o.e2bURL == "" {
		fmt.Printf("\n-e2b-url not set; skipping the e2b surface. Checkpoint %s stays on its node: only the e2b surface deletes checkpoints (DELETE /templates/{id}).\n", checkpoint)
		return nil
	}
	if err := runE2B(ctx, o, checkpoint); err != nil {
		return fmt.Errorf("e2b surface: %w", err)
	}
	return nil
}

func loadConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig == "" {
		return rest.InClusterConfig()
	}
	return clientcmd.BuildConfigFromFlags("", kubeconfig)
}

func newScheme() (*runtime.Scheme, error) {
	scheme := runtime.NewScheme()
	if err := sandboxv1beta1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	if err := cocoonv1beta1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	return scheme, nil
}

// newClient builds a controller-runtime client that knows this operator's
// types. Any Kubernetes client works — client-go, the dynamic client, or
// kubectl; nothing here is specific to controller-runtime.
func newClient(kubeconfig string, scheme *runtime.Scheme) (client.Client, error) {
	cfg, err := loadConfig(kubeconfig)
	if err != nil {
		return nil, err
	}
	return client.New(cfg, client.Options{Scheme: scheme})
}

// discoverTemplate reads the fleet's advertised warm pools and returns a
// template that actually has capacity, so the walk-through does not depend on
// a hard-coded image.
func discoverTemplate(ctx context.Context, c client.Client) (string, error) {
	var inventories cocoonv1beta1.NodeInventoryList
	if err := c.List(ctx, &inventories); err != nil {
		return "", fmt.Errorf("list NodeInventory: %w", err)
	}
	for _, inv := range inventories.Items {
		for _, pool := range inv.Pools {
			if pool.Template != "" {
				return pool.Template, nil
			}
		}
	}
	return "", errors.New("no node advertises a warm pool; pass -template explicitly")
}

func runKubernetes(ctx context.Context, c client.Client, rc rest.Interface, o options) (string, error) {
	section("Kubernetes API")

	name := fmt.Sprintf("example-%d", time.Now().UnixNano()%1e9)
	sb := &sandboxv1beta1.Sandbox{
		Name: name, Namespace: o.namespace,
	}
	sb.Spec.PodTemplate.Spec.Containers = []corev1.Container{{Name: "agent", Image: o.template}}

	if err := c.Create(ctx, sb); err != nil {
		return "", fmt.Errorf("create Sandbox: %w", err)
	}
	stepf("create", "Sandbox %s/%s", o.namespace, name)

	live, err := waitVisible(ctx, c, o.namespace, name)
	if err != nil {
		return "", err
	}
	stepf("get", "node=%s claimID=%s", live.Status.NodeName, live.Annotations[claimIDAnnotation])

	var list sandboxv1beta1.SandboxList
	if err := c.List(ctx, &list, client.InNamespace(o.namespace)); err != nil {
		return "", fmt.Errorf("list Sandboxes: %w", err)
	}
	stepf("list", "%d sandbox(es) in %s", len(list.Items), o.namespace)

	snap := &cocoonv1beta1.SandboxSnapshotResult{}
	if err := post(ctx, rc, o.namespace, name, "snapshot",
		&cocoonv1beta1.SandboxSnapshotOptions{Name: "example-checkpoint"}, snap); err != nil {
		return snap.SnapshotID, fmt.Errorf("snapshot: %w", err)
	}
	stepf("snapshot", "snapshotID=%s on node=%s", snap.SnapshotID, snap.NodeName)

	forked := &cocoonv1beta1.SandboxForkResult{}
	if err := post(ctx, rc, o.namespace, name, "fork",
		&cocoonv1beta1.SandboxForkOptions{Count: 2, TTLSeconds: 600}, forked); err != nil {
		return snap.SnapshotID, fmt.Errorf("fork: %w", err)
	}
	for i, child := range forked.Children {
		stepf("fork", "child[%d] sandboxID=%s node=%s", i, child.SandboxID, child.NodeName)
	}

	start := time.Now()
	if err := post(ctx, rc, o.namespace, name, "pause", &cocoonv1beta1.SandboxPauseOptions{}, nil); err != nil {
		return snap.SnapshotID, fmt.Errorf("pause: %w", err)
	}
	stepf("pause", "took %s (proportional to guest memory)", time.Since(start).Round(time.Millisecond))

	start = time.Now()
	if err := post(ctx, rc, o.namespace, name, "resume", &cocoonv1beta1.SandboxResumeOptions{}, nil); err != nil {
		return snap.SnapshotID, fmt.Errorf("resume: %w", err)
	}
	stepf("resume", "took %s (mmap restore fast path)", time.Since(start).Round(time.Millisecond))

	if o.keep {
		stepf("delete", "skipped (-keep)")
		return snap.SnapshotID, nil
	}
	if err := c.Delete(ctx, live); err != nil && !apierrors.IsNotFound(err) {
		return snap.SnapshotID, fmt.Errorf("delete Sandbox: %w", err)
	}
	stepf("delete", "released %s/%s", o.namespace, name)
	return snap.SnapshotID, nil
}

func deleteCheckpoints(ctx context.Context, e *e2bClient, ids ...string) error {
	for _, ck := range ids {
		if ck == "" {
			continue
		}
		code, err := e.status(ctx, http.MethodDelete, "/templates/"+ck, nil)
		if err != nil {
			return err
		}
		if code != http.StatusNoContent {
			return fmt.Errorf("delete checkpoint %s returned %d, want 204", ck, code)
		}
		stepf("snapshot", "deleted checkpoint %s", ck)
	}
	return nil
}

// waitVisible polls until Get resolves the sandbox: at once on the replica that
// served the create, at the node's next publish on another.
func waitVisible(ctx context.Context, c client.Client, ns, name string) (*sandboxv1beta1.Sandbox, error) {
	var sb sandboxv1beta1.Sandbox
	err := pollVisible(ctx, fmt.Sprintf("sandbox %s/%s", ns, name), func() (bool, error) {
		err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &sb)
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("get Sandbox: %w", err)
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return &sb, nil
}

// post invokes an action subresource. These are POST-only verbs (the
// pods/eviction shape), which is why they are not fields on SandboxSpec: the
// standard agent-sandbox schema stays untouched, so an unmodified upstream
// client keeps working against this server.
//
// A raw REST client is used rather than controller-runtime's SubResource
// helper because these actions have DIFFERENT request and response types
// (SandboxForkOptions in, SandboxForkResult out); the helper decodes the reply
// back into the object it was given, which cannot express that.
func post(ctx context.Context, rc rest.Interface, ns, name, sub string, body, out runtime.Object) error {
	req := rc.Post().
		Namespace(ns).
		Resource("sandboxes").
		Name(name).
		SubResource(sub).
		Body(body)
	if out == nil {
		return req.Do(ctx).Error()
	}
	return req.Do(ctx).Into(out)
}

// newRESTClient builds a REST client for the sandboxes group, the transport the
// action subresources are invoked over.
func newRESTClient(kubeconfig string, scheme *runtime.Scheme) (rest.Interface, error) {
	cfg, err := loadConfig(kubeconfig)
	if err != nil {
		return nil, err
	}
	gv := sandboxv1beta1.GroupVersion
	cfg.GroupVersion = &gv
	cfg.APIPath = "/apis"
	cfg.NegotiatedSerializer = serializer.NewCodecFactory(scheme).WithoutConversion()
	return rest.RESTClientFor(cfg)
}

func runE2B(ctx context.Context, o options, k8sCheckpoint string) error {
	section("e2b-compatible REST API")
	e := &e2bClient{base: strings.TrimRight(o.e2bURL, "/"), key: o.e2bKey}

	if err := e.health(ctx); err != nil {
		return err
	}
	stepf("health", "reachable")

	var templates []map[string]any
	if err := e.do(ctx, http.MethodGet, "/templates", nil, &templates); err != nil {
		return fmt.Errorf("list templates: %w", err)
	}
	stepf("templates", "%d available", len(templates))

	var created map[string]any
	if err := e.do(ctx, http.MethodPost, "/sandboxes",
		map[string]any{"templateID": o.template, "timeout": 600}, &created); err != nil {
		return fmt.Errorf("create: %w", err)
	}
	id, _ := created["sandboxID"].(string)
	stepf("create", "sandboxID=%s envdVersion=%v", id, created["envdVersion"])

	// The published id is a DNS-label-safe rendering of the node's claim id:
	// the SDK builds "{port}-{sandboxID}.{domain}", and an underscore there
	// would produce a host that cannot resolve.
	if strings.Contains(id, "_") {
		return fmt.Errorf("sandboxID %q is not DNS-label safe", id)
	}

	var listed []map[string]any
	if err := e.do(ctx, http.MethodGet, "/sandboxes", nil, &listed); err != nil {
		return fmt.Errorf("list: %w", err)
	}
	stepf("list", "%d sandbox(es)", len(listed))

	if err := e.waitVisible(ctx, id); err != nil {
		return err
	}
	stepf("get", "%s is in the read view", id)

	var metrics []map[string]any
	if err := e.do(ctx, http.MethodGet, "/sandboxes/"+id+"/metrics", nil, &metrics); err != nil {
		return fmt.Errorf("metrics: %w", err)
	}
	if len(metrics) > 0 {
		stepf("metrics", "cpuCount=%v memTotal=%v", metrics[0]["cpuCount"], metrics[0]["memTotal"])
	}

	var snap map[string]any
	if err := e.do(ctx, http.MethodPost, "/sandboxes/"+id+"/snapshots",
		map[string]any{"name": "example-e2b-snap"}, &snap); err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	stepf("snapshot", "snapshotID=%v", snap["snapshotID"])

	var snaps []map[string]any
	if err := e.do(ctx, http.MethodGet, "/snapshots", nil, &snaps); err != nil {
		return fmt.Errorf("list snapshots: %w", err)
	}
	stepf("snapshots", "%d checkpoint(s) fleet-wide", len(snaps))

	var forks []map[string]any
	if err := e.do(ctx, http.MethodPost, "/sandboxes/"+id+"/fork",
		map[string]any{"count": 2, "timeout": 600}, &forks); err != nil {
		return fmt.Errorf("fork: %w", err)
	}
	stepf("fork", "%d child sandbox(es)", len(forks))

	if code, err := e.status(ctx, http.MethodPost, "/sandboxes/"+id+"/pause", nil); err != nil {
		return err
	} else if code != http.StatusNoContent {
		return fmt.Errorf("pause returned %d, want 204", code)
	}
	stepf("pause", "204")

	if code, err := e.status(ctx, http.MethodPost, "/sandboxes/"+id+"/pause", nil); err != nil {
		return err
	} else if code != http.StatusConflict {
		return fmt.Errorf("repeated pause returned %d, want 409 (already paused)", code)
	}
	stepf("pause", "409 on repeat — the already-paused contract holds")

	if code, err := e.status(ctx, http.MethodPost, "/sandboxes/"+id+"/connect",
		map[string]any{"timeout": 600}); err != nil {
		return err
	} else if code != http.StatusCreated {
		return fmt.Errorf("connect on a paused sandbox returned %d, want 201", code)
	}
	stepf("connect", "201 — restored via the mmap fast path")

	if code, err := e.status(ctx, http.MethodPost, "/sandboxes/"+id+"/connect",
		map[string]any{"timeout": 600}); err != nil {
		return err
	} else if code != http.StatusOK {
		return fmt.Errorf("connect on a running sandbox returned %d, want 200", code)
	}
	stepf("connect", "200 — already running, no restore")

	if code, err := e.status(ctx, http.MethodPost, "/sandboxes/"+id+"/timeout",
		map[string]any{"timeout": 900}); err != nil {
		return err
	} else if code/100 != 2 {
		return fmt.Errorf("timeout returned %d", code)
	}
	stepf("timeout", "acknowledged (the TTL is fixed at claim time)")

	if code, err := e.status(ctx, http.MethodPost, "/sandboxes/"+id+"/refreshes",
		map[string]any{"duration": 60}); err != nil {
		return err
	} else if code/100 != 2 {
		return fmt.Errorf("refreshes returned %d", code)
	}
	stepf("refreshes", "keepalive accepted")

	if o.keep {
		stepf("delete", "skipped (-keep)")
		return nil
	}
	if err := deleteCheckpoints(ctx, e, fmt.Sprint(snap["snapshotID"]), k8sCheckpoint); err != nil {
		return err
	}
	if code, err := e.status(ctx, http.MethodDelete, "/sandboxes/"+id, nil); err != nil {
		return err
	} else if code != http.StatusNoContent {
		return fmt.Errorf("delete returned %d, want 204", code)
	}
	stepf("delete", "released %s", id)
	return nil
}

// e2bClient is a minimal client for the e2b REST contract. The real e2b SDKs
// (JS and Python) speak exactly this and need no changes — point E2B_API_URL at
// the server. This exists because there is no official Go SDK.
type e2bClient struct {
	base string
	key  string
	hc   http.Client
}

func (e *e2bClient) health(ctx context.Context) error {
	code, err := e.status(ctx, http.MethodGet, "/health", nil)
	if err != nil {
		return err
	}
	if code/100 != 2 {
		return fmt.Errorf("health returned %d", code)
	}
	return nil
}

// waitVisible polls until the read view publishes the sandbox — the same
// eventual consistency the Kubernetes surface has, for the same reason.
func (e *e2bClient) waitVisible(ctx context.Context, id string) error {
	return pollVisible(ctx, "sandbox "+id, func() (bool, error) {
		code, err := e.status(ctx, http.MethodGet, "/sandboxes/"+id, nil)
		if err != nil {
			return false, err
		}
		return code == http.StatusOK, nil
	})
}

func (e *e2bClient) request(ctx context.Context, method, path string, body any) (*http.Request, error) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, e.base+path, rdr)
	if err != nil {
		return nil, err
	}
	if e.key != "" {
		req.Header.Set("X-API-KEY", e.key)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}

// do performs a request and decodes a 2xx JSON reply into out.
func (e *e2bClient) do(ctx context.Context, method, path string, body, out any) error {
	req, err := e.request(ctx, method, path, body)
	if err != nil {
		return err
	}
	resp, err := e.hc.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	payload, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s %s: HTTP %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(payload)))
	}
	if out == nil || len(payload) == 0 {
		return nil
	}
	return json.Unmarshal(payload, out)
}

// status performs a request and returns only its status code, for the verbs
// whose contract IS the status code.
func (e *e2bClient) status(ctx context.Context, method, path string, body any) (int, error) {
	req, err := e.request(ctx, method, path, body)
	if err != nil {
		return 0, err
	}
	resp, err := e.hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

// pollVisible retries probe on a 3s tick until the subject is visible or visibilityTimeout elapses.
func pollVisible(ctx context.Context, subject string, probe func() (bool, error)) error {
	deadline := time.Now().Add(visibilityTimeout)
	for {
		visible, err := probe()
		if err != nil {
			return err
		}
		if visible {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s not visible within %s", subject, visibilityTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
}

func section(name string) { fmt.Printf("\n=== %s ===\n", name) }

func stepf(verb, format string, args ...any) {
	fmt.Printf("  %-10s %s\n", verb, fmt.Sprintf(format, args...))
}
