//go:build l2bench

// Command l2bench proves the L2 node-local claim gateway acceptance criteria of
// docs/scaling-design.md:
//
//   - claim p50 is sub-millisecond on the sandboxd tier (the gateway's own
//     overhead is not a bottleneck), and
//   - orphan bindings — deliveries whose async Bound record was lost to a gateway
//     crash — converge to 0 via the OrphanReconciler, which adopts (records) them
//     and destroys NO VM (the delete-authorization contract).
//
// It runs entirely against a fake substrate: an httptest sandboxd that hands over
// a fresh sandbox in sub-millisecond time (standing in for sandboxd's 0.2–0.7 ms
// ownership transfer) and the controller-runtime fake client as the Bound-record
// store. So it measures gateway algorithm/overhead and the orphan-GC invariant,
// not real microVM boot.
//
//	Run: GOTOOLCHAIN=go1.26.3 go run -tags l2bench ./test/l2bench \
//	       -out /path/to/l2-gateway.json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	extv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
	"github.com/cocoonstack/sandbox-operator/test/benchutil"
)

const (
	ns   = "l2bench"
	node = "l2-node"
)

var (
	outFlag     = flag.String("out", "/tmp/l2-gateway.json", "results json path")
	itersFlag   = flag.Int("iters", 5000, "claim latency iterations")
	orphansFlag = flag.Int("orphans", 200, "orphan bindings to inject and reconcile")
)

// fakeSandboxd is an httptest-backed sandboxd: /v1/claim hands over a fresh
// sandbox fast; /v1/sandboxes/{id}/release counts VM destroys (must stay 0).
type fakeSandboxd struct {
	srv      *httptest.Server
	releases atomic.Int64
	nextID   atomic.Int64
}

func newFakeSandboxd() *fakeSandboxd {
	f := &fakeSandboxd{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/claim", func(w http.ResponseWriter, _ *http.Request) {
		id := fmt.Sprintf("sb_%d", f.nextID.Add(1))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(sandboxd.ClaimResult{
			ID: id, Token: "tok_" + id,
			Deadline:  time.Date(2026, 7, 6, 0, 5, 0, 0, time.UTC),
			OwnerAddr: "10.0.0.5:7777",
		})
	})
	mux.HandleFunc("/v1/sandboxes/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/release") {
			f.releases.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	f.srv = httptest.NewServer(mux)
	return f
}

type allowAuthorizer struct{}

func (allowAuthorizer) Authorize(context.Context, scale.ClaimRequest) error { return nil }

// countingRecorder records nothing durably; it just proves the async record path
// runs. Used only in the latency loop.
type countingRecorder struct{ n atomic.Int64 }

func (c *countingRecorder) RecordBound(context.Context, string, string, scale.Assignment) error {
	c.n.Add(1)
	return nil
}

// failingRecorder models the async record lost to a gateway crash → orphan binding.
type failingRecorder struct{}

func (failingRecorder) RecordBound(context.Context, string, string, scale.Assignment) error {
	return fmt.Errorf("simulated record failure")
}

type sliceInventory []scale.Delivery

func (s sliceInventory) LiveDeliveries(context.Context) ([]scale.Delivery, error) {
	return []scale.Delivery(s), nil
}

func newClaim(name string) *extv1beta1.SandboxClaim {
	return &extv1beta1.SandboxClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       extv1beta1.SandboxClaimSpec{WarmPoolRef: extv1beta1.SandboxWarmPoolRef{Name: "base:24.04"}},
	}
}

func pct(xs []float64, p float64) float64 { return benchutil.Pct(xs, p, benchutil.Round4) }

// claimWaiter is the gateway surface the latency loop needs: Claim plus Wait to
// drain async Bound records. The value scale.NewGateway returns satisfies it.
type claimWaiter interface {
	Claim(ctx context.Context, req scale.ClaimRequest) (scale.Assignment, error)
	Wait()
}

// measureClaimLatency times gw.Claim over many iterations against the fake
// sandboxd, returning per-call latencies in milliseconds.
func measureClaimLatency(ctx context.Context, gw claimWaiter, iters int) []float64 {
	lat := make([]float64, 0, iters)
	for i := range iters {
		req := scale.ClaimRequest{Namespace: ns, ClaimName: fmt.Sprintf("lat-%d", i), WarmPool: "base:24.04"}
		start := time.Now()
		a, err := gw.Claim(ctx, req)
		d := time.Since(start)
		benchutil.Must(err)
		if a.SandboxName == "" {
			benchutil.Must(fmt.Errorf("empty assignment at iter %d", i))
		}
		lat = append(lat, float64(d.Nanoseconds())/1e6)
	}
	gw.Wait()
	return lat
}

// injectAndReconcileOrphans delivers `orphans` claims through a gateway whose async
// record always fails (leaving orphan bindings), then runs the OrphanReconciler and
// returns (reconciled, remainingOrphans).
func injectAndReconcileOrphans(ctx context.Context, fs *fakeSandboxd, orphans int) (int, int) {
	objs := make([]ctrlclient.Object, 0, orphans)
	for i := range orphans {
		objs = append(objs, newClaim(fmt.Sprintf("orphan-%d", i)))
	}
	fc := fake.NewClientBuilder().
		WithScheme(benchutil.NewScheme()).
		WithObjects(objs...).
		WithStatusSubresource(&extv1beta1.SandboxClaim{}).
		Build()

	badGW := scale.NewGateway(scale.GatewayConfig{
		Node: node, Client: sandboxd.New(fs.srv.URL, "root-token"),
		Authorizer: allowAuthorizer{}, Recorder: failingRecorder{},
		BaseContext: ctx, Logger: logr.Discard(),
	})
	inv := make(sliceInventory, 0, orphans)
	for i := range orphans {
		name := fmt.Sprintf("orphan-%d", i)
		a, err := badGW.Claim(ctx, scale.ClaimRequest{Namespace: ns, ClaimName: name, WarmPool: "base:24.04"})
		benchutil.Must(err)
		inv = append(inv, scale.Delivery{
			SandboxName: a.SandboxName, Node: a.Node, Address: a.Address, ClaimNS: ns, ClaimName: name,
		})
	}
	badGW.Wait() // every async record has failed → `orphans` orphan bindings

	orc := scale.NewOrphanReconciler(node, inv, fc, scale.NewClaimRecorder(fc), logr.Discard())
	reconciled, err := orc.Reconcile(ctx)
	benchutil.Must(err)

	remaining := 0
	for i := range orphans {
		cur := &extv1beta1.SandboxClaim{}
		benchutil.Must(fc.Get(ctx, types.NamespacedName{Namespace: ns, Name: fmt.Sprintf("orphan-%d", i)}, cur))
		if cur.Status.SandboxStatus.Name == "" {
			remaining++
		}
	}
	return reconciled, remaining
}

func main() {
	flag.Parse()
	ctx := context.Background()

	fs := newFakeSandboxd()
	defer fs.srv.Close()

	gw := scale.NewGateway(scale.GatewayConfig{
		Node: node, Client: sandboxd.New(fs.srv.URL, "root-token"),
		Authorizer: allowAuthorizer{}, Recorder: &countingRecorder{},
		TTLSeconds: 300, BaseContext: ctx, Logger: logr.Discard(),
	})
	lat := measureClaimLatency(ctx, gw, *itersFlag)
	p50 := pct(lat, 50)
	p95 := pct(lat, 95)

	reconciled, remaining := injectAndReconcileOrphans(ctx, fs, *orphansFlag)
	destroys := fs.releases.Load()

	// pass ONLY if every orphan converged, none remain, no VM was destroyed, and
	// the gateway's claim p50 is sub-millisecond. Values are measured, not fixed.
	pass := remaining == 0 && reconciled == *orphansFlag && destroys == 0 && p50 < 1.0

	out := map[string]any{
		"pass":               pass,
		"orphan_bindings":    remaining,
		"claim_p50_ms":       p50,
		"claim_p95_ms":       p95,
		"orphans_injected":   *orphansFlag,
		"orphans_reconciled": reconciled,
		"vm_destroy_calls":   destroys,
		"goal":               "G-0131",
		"layer":              "L2",
		"substrate":          "httptest fake sandboxd + fake k8s recorder",
		"claim_iters":        *itersFlag,
	}
	b, err := json.MarshalIndent(out, "", "  ")
	benchutil.Must(err)
	benchutil.Must(os.MkdirAll(filepath.Dir(*outFlag), 0o755))
	benchutil.Must(os.WriteFile(*outFlag, b, 0o644))

	fmt.Printf("claim p50=%.4fms p95=%.4fms (iters=%d) | orphans injected=%d reconciled=%d remaining=%d | vm_destroy_calls=%d | pass=%v\n",
		p50, p95, *itersFlag, *orphansFlag, reconciled, remaining, destroys, pass)
	fmt.Printf("wrote %s\n", *outFlag)
	if !pass {
		os.Exit(1)
	}
}
