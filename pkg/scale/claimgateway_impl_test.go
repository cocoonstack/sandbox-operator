package scale

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr/testr"
	"github.com/stretchr/testify/require"
	authzv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cocoonv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
)

func TestClaimGatewayHappyPath(t *testing.T) {
	fs := newFakeSandboxd(t)
	fc := newClaimClient(t, "c1")

	gw := NewGateway(GatewayConfig{
		Node:        "node-a",
		Client:      fs.client(),
		Authorizer:  allowAuthorizer{},
		Recorder:    NewClaimRecorder(fc),
		TTLSeconds:  300,
		BaseContext: t.Context(),
		Logger:      testr.New(t),
	})

	a, err := gw.Claim(t.Context(), ClaimRequest{Namespace: "default", ClaimName: "c1", WarmPool: "base:24.04"})
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(a.SandboxName, "sb_"), "got %q", a.SandboxName)
	require.Equal(t, "node-a", a.Node)
	require.Equal(t, "10.0.0.5:7777", a.Address)

	gw.Wait()
	cur := getClaim(t, fc, "c1")
	require.Equal(t, a.SandboxName, cur.Status.SandboxStatus.Name, "async RecordBound should have set status.sandbox.name")
	require.Contains(t, []string{"10.0.0.5:7777"}, cur.Status.SandboxStatus.PodIPs[0])
	require.NotNil(t, findCond(cur, BoundConditionType))

	require.Equal(t, int64(1), fs.claims.Load(), "exactly one sandbox delivered")
	require.Equal(t, int64(0), fs.releases.Load(), "claim path must never destroy a VM")
}

func TestClaimGatewayOrphanBindingConverges(t *testing.T) {
	fs := newFakeSandboxd(t)
	fc := newClaimClient(t, "c-orphan")

	gw := NewGateway(GatewayConfig{
		Node: "node-a", Client: fs.client(), Authorizer: allowAuthorizer{},
		Recorder: failingRecorder{}, BaseContext: t.Context(), Logger: testr.New(t),
	})
	a, err := gw.Claim(t.Context(), ClaimRequest{Namespace: "default", ClaimName: "c-orphan", WarmPool: "base:24.04"})
	require.NoError(t, err)
	gw.Wait()

	require.Empty(t, getClaim(t, fc, "c-orphan").Status.SandboxStatus.Name)

	inv := sliceInventory{{SandboxName: a.SandboxName, Node: a.Node, Address: a.Address, ClaimNS: "default", ClaimName: "c-orphan"}}
	orc := NewOrphanReconciler("node-a", inv, fc, NewClaimRecorder(fc), testr.New(t))

	n, err := orc.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, n, "the single orphan binding should be reconciled")
	require.Equal(t, a.SandboxName, getClaim(t, fc, "c-orphan").Status.SandboxStatus.Name, "orphan binding converged to Bound")

	n, err = orc.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, 0, n, "orphan count must converge to 0")

	require.Equal(t, int64(0), fs.releases.Load(), "orphan GC must NEVER destroy a VM")
}

func TestClaimGatewayFallbackOnNoCapacity(t *testing.T) {
	fs := newFakeSandboxd(t)
	fs.forceStatus.Store(http.StatusTooManyRequests)
	fc := newClaimClient(t, "c2")

	gw := NewGateway(GatewayConfig{
		Node: "node-a", Client: fs.client(), Authorizer: allowAuthorizer{},
		Recorder: NewClaimRecorder(fc), BaseContext: t.Context(), Logger: testr.New(t),
	})

	a, err := gw.Claim(t.Context(), ClaimRequest{Namespace: "default", ClaimName: "c2", WarmPool: "base:24.04"})
	require.Error(t, err)
	require.True(t, IsFallback(err), "429 must map to a fallback signal, got %v", err)
	require.ErrorIs(t, err, ErrNoNodeCapacity)
	require.Equal(t, Assignment{}, a)
	require.Equal(t, int64(0), fs.releases.Load())
}

func TestClaimGatewayReleaseOwnerTeardownOnly(t *testing.T) {
	fs := newFakeSandboxd(t)

	gw := NewGateway(GatewayConfig{
		Node: "node-a", Client: fs.client(), Authorizer: allowAuthorizer{},
		Recorder: noopRecorder{}, BaseContext: t.Context(), Logger: testr.New(t),
	})

	a, err := gw.Claim(t.Context(), ClaimRequest{Namespace: "default", ClaimName: "c-rel", WarmPool: "base:24.04"})
	require.NoError(t, err)
	gw.Wait()

	require.NoError(t, gw.Release(t.Context(), a))
	require.Equal(t, int64(1), fs.releases.Load(), "owner teardown must destroy the delivered VM")

	err = gw.Release(t.Context(), Assignment{SandboxName: "sb_never_delivered", Node: "node-a"})
	require.Error(t, err)
	require.Equal(t, int64(1), fs.releases.Load(), "an undelivered Assignment must not trigger any destroy")

	err = gw.Release(t.Context(), a)
	require.Error(t, err)
	require.Equal(t, int64(1), fs.releases.Load())
}

func TestClaimGatewayAuthorizationRejectsInline(t *testing.T) {
	t.Run("deny", func(t *testing.T) {
		fs := newFakeSandboxd(t)
		gw := NewGateway(GatewayConfig{
			Node: "node-a", Client: fs.client(),
			Authorizer: &ReviewAuthorizer{Reviewer: fakeSAR{allow: false}},
			Recorder:   noopRecorder{}, BaseContext: t.Context(), Logger: testr.New(t),
		})
		req := ClaimRequest{
			Namespace: "default", ClaimName: "c3", WarmPool: "base:24.04",
			Selector: map[string]string{RequestUserSelectorKey: "alice"},
		}
		a, err := gw.Claim(t.Context(), req)
		require.Error(t, err)
		require.False(t, IsFallback(err), "an authz denial is a hard reject, not a fallback")
		require.Equal(t, Assignment{}, a)
		require.Equal(t, int64(0), fs.claims.Load(), "reject must happen before any delivery")
	})

	t.Run("missing-identity-fails-closed", func(t *testing.T) {
		fs := newFakeSandboxd(t)
		gw := NewGateway(GatewayConfig{
			Node: "node-a", Client: fs.client(),
			Authorizer: &ReviewAuthorizer{Reviewer: fakeSAR{allow: true}},
			Recorder:   noopRecorder{}, BaseContext: t.Context(), Logger: testr.New(t),
		})
		_, err := gw.Claim(t.Context(), ClaimRequest{Namespace: "default", ClaimName: "c4", WarmPool: "base:24.04"})
		require.Error(t, err, "no caller identity must fail closed")
		require.Equal(t, int64(0), fs.claims.Load())
	})

	t.Run("allow-delivers", func(t *testing.T) {
		fs := newFakeSandboxd(t)
		fc := newClaimClient(t, "c5")
		gw := NewGateway(GatewayConfig{
			Node: "node-a", Client: fs.client(),
			Authorizer: &ReviewAuthorizer{Reviewer: fakeSAR{allow: true}},
			Recorder:   NewClaimRecorder(fc), BaseContext: t.Context(), Logger: testr.New(t),
		})
		req := ClaimRequest{
			Namespace: "default", ClaimName: "c5", WarmPool: "base:24.04",
			Selector: map[string]string{RequestUserSelectorKey: "alice"},
		}
		a, err := gw.Claim(t.Context(), req)
		require.NoError(t, err)
		require.NotEmpty(t, a.SandboxName)
		require.Equal(t, int64(1), fs.claims.Load())
		gw.Wait()
	})
}

type fakeSandboxd struct {
	srv         *httptest.Server
	claims      atomic.Int64
	releases    atomic.Int64
	nextID      atomic.Int64
	forceStatus atomic.Int64
}

func newFakeSandboxd(t *testing.T) *fakeSandboxd {
	t.Helper()
	f := &fakeSandboxd{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/claim", func(w http.ResponseWriter, _ *http.Request) {
		f.claims.Add(1)
		if s := f.forceStatus.Load(); s != 0 {
			w.WriteHeader(int(s))
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "node at max_claims"})
			return
		}
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
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeSandboxd) client() *sandboxd.Client { return sandboxd.New(f.srv.URL, "root-token") }

type allowAuthorizer struct{}

func (allowAuthorizer) Authorize(context.Context, ClaimRequest) error { return nil }

type noopRecorder struct{}

func (noopRecorder) RecordBound(context.Context, string, string, Assignment) error { return nil }

type failingRecorder struct{}

func (failingRecorder) RecordBound(context.Context, string, string, Assignment) error {
	return fmt.Errorf("simulated record failure (gateway crashed before recording Bound)")
}

type sliceInventory []Delivery

func (s sliceInventory) LiveDeliveries(context.Context) ([]Delivery, error) {
	return []Delivery(s), nil
}

type fakeSAR struct {
	allow bool
	deny  bool
	err   error
}

func (f fakeSAR) Create(_ context.Context, sar *authzv1.SubjectAccessReview, _ metav1.CreateOptions) (*authzv1.SubjectAccessReview, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := sar.DeepCopy()
	out.Status = authzv1.SubjectAccessReviewStatus{Allowed: f.allow, Denied: f.deny, Reason: "test"}
	return out, nil
}

func newScaleScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, sandboxv1beta1.AddToScheme(s))
	require.NoError(t, extv1beta1.AddToScheme(s))
	require.NoError(t, cocoonv1beta1.AddToScheme(s))
	return s
}

func newClaimClient(t *testing.T, names ...string) client.Client {
	t.Helper()
	b := fake.NewClientBuilder().WithScheme(newScaleScheme(t)).WithStatusSubresource(&extv1beta1.SandboxClaim{})
	for _, n := range names {
		b = b.WithObjects(&extv1beta1.SandboxClaim{
			Name: n, Namespace: "default",
			Spec: extv1beta1.SandboxClaimSpec{WarmPoolRef: extv1beta1.SandboxWarmPoolRef{Name: "base:24.04"}},
		})
	}
	return b.Build()
}

func getClaim(t *testing.T, c client.Client, name string) *extv1beta1.SandboxClaim {
	t.Helper()
	cur := &extv1beta1.SandboxClaim{}
	require.NoError(t, c.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: name}, cur))
	return cur
}

func findCond(c *extv1beta1.SandboxClaim, t string) *metav1.Condition {
	for i := range c.Status.Conditions {
		if c.Status.Conditions[i].Type == t {
			return &c.Status.Conditions[i]
		}
	}
	return nil
}
