//go:build l3bench

// Command l3bench proves the L3 aggregated-apiserver acceptance criteria of
// docs/scaling-design.md:
//
//   - kubectl_get_works: sandboxes.agents.x-k8s.io is served by a real
//     genericapiserver.GenericAPIServer with a scatter-gather rest.Storage (no
//     etcd generic registry). The server's http.Handler is served in-process via
//     httptest, and a real client-go REST client issues
//     GET /apis/agents.x-k8s.io/v1beta1/namespaces/<ns>/sandboxes (and the
//     cluster-scoped list, a per-item Get, and a watch) — exactly kubectl's code
//     path (client-go REST + codec decode). If the fanned-out list decodes into a
//     typed SandboxList with the expected items, the fact holds.
//
//   - etcd_scales_with = "pools+nodes": the durable objects backing the served
//     sandboxes are exactly the O(nodes) NodeInventory objects (server-side-applied
//     by the publisher, one apply per node) plus the O(pools) WarmPool intent
//     objects — ZERO per-sandbox etcd objects — while List still returns every
//     sandbox.
//
// It runs entirely in-process (no etcd, no cluster, no TLS): the "durable store"
// is a StaticInventorySource the publisher applies into and the store reads from,
// standing in for a cache-fed NodeInventory client. So it measures the aggregation
// contract and the object-count invariant, not real microVM state.
//
//	Run: go run -tags l3bench ./test/l3bench \
//	       -out /path/to/l3-aggregation.json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	restclient "k8s.io/client-go/rest"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
	sandboxapiserver "github.com/cocoonstack/sandbox-operator/pkg/scale/apiserver"
)

var (
	outFlag        = flag.String("out", "/tmp/l3-aggregation.json", "results json path")
	nodesFlag      = flag.Int("nodes", 3, "number of node inventories to publish")
	perNodeFlag    = flag.Int("per-node", 1000, "sandbox entries summarized per node")
	poolsFlag      = flag.Int("pools", 5, "number of WarmPool intent objects (etcd O(pools))")
	namespacesFlag = flag.Int("namespaces", 3, "number of namespaces to spread sandboxes across")
)

// sliceLiveSource is a node's own live sandbox state (the sandboxd inventory /
// L0 node cache stand-in) — NOT a cluster-wide LIST.
type sliceLiveSource []scale.InventoryEntry

func (s sliceLiveSource) LiveSandboxes(context.Context) ([]scale.InventoryEntry, error) {
	return []scale.InventoryEntry(s), nil
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FAIL: "+format+"\n", args...)
	os.Exit(1)
}

func must(err error) {
	if err != nil {
		fail("%v", err)
	}
}

func namespaceName(i int) string { return fmt.Sprintf("l3bench-ns-%d", i) }

func main() {
	flag.Parse()
	ctx := context.Background()

	nodes, perNode, pools, numNS := *nodesFlag, *perNodeFlag, *poolsFlag, *namespacesFlag
	if nodes <= 0 || perNode <= 0 || pools <= 0 || numNS <= 0 {
		fail("nodes/per-node/pools/namespaces must all be > 0")
	}
	wantSandboxes := nodes * perNode

	// The source is both the publisher's apply target (the O(nodes) write path)
	// and the store's read source (cache-fed NodeInventory read). No per-sandbox
	// object is ever created.
	source := scale.NewStaticInventorySource()

	// One WarmPool per pool expresses desired replicas as pure intent; these are
	// the O(pools) durable objects (a single spec can mean a million sandboxes).
	warmPools := make([]client.Object, 0, pools)
	for p := range pools {
		warmPools = append(warmPools, &extv1beta1.SandboxWarmPool{
			Name:      fmt.Sprintf("pool-%d", p),
			Namespace: namespaceName(p % numNS),
			Spec:      extv1beta1.SandboxWarmPoolSpec{Replicas: new(int32(perNode))},
		})
	}
	scheme := runtime.NewScheme()
	must(extv1beta1.AddToScheme(scheme))
	intent := fake.NewClientBuilder().WithScheme(scheme).WithObjects(warmPools...).Build()

	// Publish one NodeInventory per node from that node's own live state.
	var sampleNS, sampleName string
	for k := range nodes {
		node := fmt.Sprintf("node-%d", k)
		entries := make(sliceLiveSource, 0, perNode)
		for i := range perNode {
			g := k*perNode + i
			ns := namespaceName(g % numNS)
			name := fmt.Sprintf("sb-%06d", g)
			if sampleName == "" {
				sampleNS, sampleName = ns, name
			}
			entries = append(entries, scale.InventoryEntry{
				Name:     ns + "/" + name,
				Phase:    "Running",
				ClaimRef: ns + "/claim-" + name,
				Address:  fmt.Sprintf("10.%d.%d.%d:7777", k, (i>>8)&0xff, i&0xff),
			})
		}
		must(source.Apply(ctx, &scale.NodeInventory{
			Kind:       scale.NodeInventoryGVK.Kind,
			APIVersion: scale.NodeInventoryGVK.GroupVersion().String(),
			Name:       node,
			Node:       node,
			Entries:    entries,
		}))
		if n := len(entries); n != perNode {
			fail("published %d entries for %s, want %d", n, node, perNode)
		}
	}

	// etcd object accounting: NodeInventory (O(nodes)) + WarmPool (O(pools)).
	var stored extv1beta1.SandboxWarmPoolList
	must(intent.List(ctx, &stored))
	etcdObjectCount := source.ObjectCount() + len(stored.Items)
	if source.ObjectCount() != nodes {
		fail("expected %d NodeInventory objects, got %d", nodes, source.ObjectCount())
	}
	if source.ApplyCount() != nodes {
		fail("expected %d server-side-apply writes (O(nodes)), got %d", nodes, source.ApplyCount())
	}
	if etcdObjectCount != nodes+pools {
		fail("etcd object count %d != nodes+pools (%d+%d)", etcdObjectCount, nodes, pools)
	}

	store := scale.NewScatterGatherStore(source, scale.WithLogger(logr.Discard()), scale.WithWatchPollInterval(50*time.Millisecond))
	server, err := sandboxapiserver.NewInProcessServer("l3bench-apiserver", store)
	must(err)
	ts := httptest.NewServer(server.Handler)
	defer ts.Close()

	rc := newRESTClient(ts.URL)

	allList := &sandboxv1beta1.SandboxList{}
	if err := rc.Get().Resource("sandboxes").Do(ctx).Into(allList); err != nil {
		fail("client-go cluster-scoped list failed: %v", err)
	}
	if len(allList.Items) != wantSandboxes {
		fail("cluster list returned %d sandboxes, want %d", len(allList.Items), wantSandboxes)
	}

	nsList := &sandboxv1beta1.SandboxList{}
	if err := rc.Get().Namespace(sampleNS).Resource("sandboxes").Do(ctx).Into(nsList); err != nil {
		fail("client-go namespaced list failed: %v", err)
	}
	wantNS, err := store.List(ctx, scale.ListOptions{Namespace: sampleNS})
	must(err)
	if len(nsList.Items) != len(wantNS.Items) || len(nsList.Items) == 0 {
		fail("namespaced list returned %d, want %d (>0)", len(nsList.Items), len(wantNS.Items))
	}

	got := &sandboxv1beta1.Sandbox{}
	if err := rc.Get().Namespace(sampleNS).Resource("sandboxes").Name(sampleName).Do(ctx).Into(got); err != nil {
		fail("client-go get failed: %v", err)
	}
	if got.Name != sampleName || got.Namespace != sampleNS {
		fail("get returned %s/%s, want %s/%s", got.Namespace, got.Name, sampleNS, sampleName)
	}

	labelList := &sandboxv1beta1.SandboxList{}
	if err := rc.Get().Resource("sandboxes").Param("labelSelector", scale.NodeLabel+"=node-0").Do(ctx).Into(labelList); err != nil {
		fail("client-go label-selected list failed: %v", err)
	}
	if len(labelList.Items) != perNode {
		fail("label-selected list returned %d, want %d", len(labelList.Items), perNode)
	}

	watchEvents, watchOK := exerciseWatch(ctx, rc, sampleNS, sampleName)

	kubectlGetWorks := len(allList.Items) == wantSandboxes &&
		len(nsList.Items) == len(wantNS.Items) &&
		got.Name == sampleName &&
		len(labelList.Items) == perNode

	out := map[string]any{
		"kubectl_get_works":        kubectlGetWorks,
		"etcd_scales_with":         "pools+nodes",
		"sandbox_count":            len(allList.Items),
		"etcd_object_count":        etcdObjectCount,
		"nodes":                    nodes,
		"pools":                    pools,
		"list_via_client_go":       true,
		"namespaced_list":          len(nsList.Items),
		"label_selected":           len(labelList.Items),
		"get_via_client_go":        got.Name == sampleName && got.Namespace == sampleNS,
		"watch_via_client_go":      watchOK,
		"watch_events":             watchEvents,
		"ssa_writes":               source.ApplyCount(),
		"per_sandbox_etcd_objects": 0,
		"goal":                     "G-0131",
		"layer":                    "L3",
		"substrate":                "in-process genericapiserver via httptest + client-go",
	}
	b, err := json.MarshalIndent(out, "", "  ")
	must(err)
	must(os.MkdirAll(filepath.Dir(*outFlag), 0o755))
	must(os.WriteFile(*outFlag, b, 0o644))

	fmt.Printf("sandboxes served=%d | etcd objects=%d (nodes=%d + pools=%d) | ssa writes=%d | per-sandbox etcd objects=0\n",
		len(allList.Items), etcdObjectCount, nodes, pools, source.ApplyCount())
	fmt.Printf("client-go: cluster-list=%d ns-list=%d get=%v label-list=%d watch=%v(events=%d) | kubectl_get_works=%v\n",
		len(allList.Items), len(nsList.Items), got.Name == sampleName, len(labelList.Items), watchOK, watchEvents, kubectlGetWorks)
	fmt.Printf("wrote %s\n", *outFlag)

	if !kubectlGetWorks {
		fail("kubectl_get_works is false")
	}
}

// newRESTClient builds a real client-go REST client (kubectl's transport) against
// the in-process aggregated apiserver, decoding with the served group's codec.
func newRESTClient(host string) *restclient.RESTClient {
	cfg := &restclient.Config{Host: host}
	cfg.APIPath = "/apis"
	cfg.GroupVersion = &sandboxv1beta1.GroupVersion
	cfg.NegotiatedSerializer = sandboxapiserver.Codecs.WithoutConversion()
	cfg.ContentType = "application/json"
	rc, err := restclient.RESTClientFor(cfg)
	must(err)
	return rc
}

// exerciseWatch opens a client-go watch and reads events until the initial sync
// delivers at least one, or a short deadline elapses. Best-effort: List is the
// required acceptance, watch is a bonus that proves the merge stream.
func exerciseWatch(ctx context.Context, rc *restclient.RESTClient, ns, name string) (int, bool) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	w, err := rc.Get().Namespace(ns).Resource("sandboxes").
		Param("fieldSelector", "metadata.name="+name).
		Param("watch", "true").Watch(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "watch open failed (non-fatal): %v\n", err)
		return 0, false
	}
	defer w.Stop()
	events := 0
	deadline := time.After(3 * time.Second)
	for {
		select {
		case ev, ok := <-w.ResultChan():
			if !ok {
				return events, events > 0
			}
			if ev.Type == watch.Added || ev.Type == watch.Modified || ev.Type == watch.Deleted {
				events++
				if events >= 1 {
					return events, true
				}
			}
		case <-deadline:
			return events, events > 0
		}
	}
}
