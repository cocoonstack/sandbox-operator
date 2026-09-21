package scale

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	authzv1 "k8s.io/api/authorization/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	extv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
)

const (
	// Selector keys a ClaimRequest may carry to override the ClaimSpec axes.
	SelectorTemplateKey = "sandbox.cocoonstack.io/template"
	SelectorNetKey      = "sandbox.cocoonstack.io/net"
	SelectorSizeKey     = "sandbox.cocoonstack.io/size"

	// BoundConditionType marks a SandboxClaim whose warm sandbox has been delivered.
	BoundConditionType = "Bound"

	// RequestUserSelectorKey names the Selector entry carrying the caller identity the
	// default Authorizer evaluates in its SubjectAccessReview. Absent, the default
	// Authorizer fails closed.
	RequestUserSelectorKey = "authorization.cocoonstack.io/user"
)

// ErrNoNodeCapacity is the sentinel Claim returns when the node has no warm VM to
// hand over (sandboxd answered 429/draining, or redirected to a peer). The caller
// falls back to the L1 Kubernetes path (create a new Sandbox). Test it with
// IsFallback rather than comparing directly, so future wrapped causes still match.
var ErrNoNodeCapacity = errors.New("scale: node has no warm capacity; fall back to L1 claim path")

// IsFallback reports whether err means the L2 node-local fast path declined and
// the caller should fall back to the L1 Kubernetes claim path.
func IsFallback(err error) bool { return errors.Is(err, ErrNoNodeCapacity) }

// SandboxdClient is the subset of the sandboxd HTTP client the gateway needs,
// kept as an interface so tests inject a fake without a live node. *sandboxd.Client
// satisfies it.
type SandboxdClient interface {
	Claim(ctx context.Context, spec sandboxd.ClaimSpec) (sandboxd.ClaimResult, error)
	Release(ctx context.Context, id, token string) error

	// The lifecycle verbs address an already-delivered sandbox by id. They all
	// take sandboxd's operator path, authorized by the fleet api_token the
	// client already carries, so the control plane needs no per-sandbox secret.
	Hibernate(ctx context.Context, id string) error
	Wake(ctx context.Context, id string) error
	Renew(ctx context.Context, id string, spec sandboxd.RenewSpec) (time.Time, error)
	Fork(ctx context.Context, id string, spec sandboxd.ForkSpec) (sandboxd.ForkResult, error)
	Checkpoint(ctx context.Context, id string, spec sandboxd.CheckpointSpec) (sandboxd.Checkpoint, error)
	Checkpoints(ctx context.Context) ([]sandboxd.Checkpoint, error)
	DeleteCheckpoint(ctx context.Context, checkpointID string) error
	Stats(ctx context.Context, id string) (sandboxd.SandboxStats, error)
}

// Authorizer checks a claim inline before delivery.
type Authorizer interface {
	Authorize(ctx context.Context, req ClaimRequest) error
}

// ClaimRecorder durably records the "Bound" outcome of a delivered claim onto the
// SandboxClaim object. It is invoked asynchronously after Claim returns (kubelet
// static-Pod semantics: the node acts first, the apiserver records after) and
// again by the OrphanReconciler when an async record was lost.
type ClaimRecorder interface {
	RecordBound(ctx context.Context, claimNS, claimName string, a Assignment) error
}

// GatewayConfig configures NewGateway.
type GatewayConfig struct {
	// Node is the name of the node this gateway fronts (stamped into Assignments).
	Node string
	// Client delivers and releases sandboxes on this node.
	Client SandboxdClient
	// Authorizer checks claims inline.
	Authorizer Authorizer
	// Recorder durably records Bound asynchronously after delivery.
	Recorder ClaimRecorder

	// DefaultTemplate/DefaultNet/DefaultSize/TTLSeconds fill a ClaimSpec when the
	// request Selector does not override them. An empty DefaultTemplate falls back
	// to the WarmPool name as the template axis.
	DefaultTemplate string
	DefaultNet      string
	DefaultSize     string
	TTLSeconds      int

	// RecordTimeout bounds each async Bound-record attempt. Defaults to 10s.
	RecordTimeout time.Duration
	// BaseContext is the gateway's lifetime context; async record jobs derive from
	// it rather than the (already-returned) request context. Defaults to
	// context.Background().
	BaseContext context.Context
	// Logger records async-record failures (which the OrphanReconciler later
	// heals). The zero logr.Logger discards.
	Logger logr.Logger
}

// delivered is the ownership credential (sandboxd id + token) a claim holds; only the gateway holding it can Release the VM.
type delivered struct {
	id    string
	token string
}

var _ ClaimGateway = (*nodeClaimGateway)(nil)

// nodeClaimGateway is the concrete L2 ClaimGateway: it authorizes inline, delivers a sandbox, and records Bound asynchronously.
type nodeClaimGateway struct {
	node          string
	client        SandboxdClient
	authz         Authorizer
	recorder      ClaimRecorder
	tmpl          string
	net           string
	size          string
	ttl           int
	recordTimeout time.Duration
	baseCtx       context.Context
	log           logr.Logger

	mu       sync.Mutex
	holdings map[string]delivered // Assignment.SandboxName -> ownership credential
	wg       sync.WaitGroup       // tracks in-flight async Bound-record jobs
}

// NewGateway builds a node-local ClaimGateway from cfg. It returns the concrete
// type so callers can Wait on async record drain during graceful shutdown; the
// value satisfies ClaimGateway.
func NewGateway(cfg GatewayConfig) *nodeClaimGateway {
	g := &nodeClaimGateway{
		node:          cfg.Node,
		client:        cfg.Client,
		authz:         cfg.Authorizer,
		recorder:      cfg.Recorder,
		tmpl:          cfg.DefaultTemplate,
		net:           cfg.DefaultNet,
		size:          cfg.DefaultSize,
		ttl:           cfg.TTLSeconds,
		recordTimeout: cfg.RecordTimeout,
		baseCtx:       cfg.BaseContext,
		log:           cfg.Logger,
		holdings:      make(map[string]delivered),
	}
	if g.recordTimeout <= 0 {
		g.recordTimeout = 10 * time.Second
	}
	if g.baseCtx == nil {
		g.baseCtx = context.Background()
	}
	return g
}

// Claim authorizes the request inline, delivers a warm sandbox from the node's
// sandboxd, and returns the Assignment immediately. Recording the SandboxClaim as
// Bound is enqueued as an async job — the return does NOT block on the k8s write.
// When the node has no warm capacity the result is ErrNoNodeCapacity (IsFallback).
func (g *nodeClaimGateway) Claim(ctx context.Context, req ClaimRequest) (Assignment, error) {
	if g.authz != nil {
		if err := g.authz.Authorize(ctx, req); err != nil {
			return Assignment{}, fmt.Errorf("scale: claim %s/%s not authorized: %w", req.Namespace, req.ClaimName, err)
		}
	}

	res, err := g.client.Claim(ctx, g.specFor(req))
	if err != nil {
		if errors.Is(err, sandboxd.ErrNodeAtCapacity) {
			return Assignment{}, ErrNoNodeCapacity
		}
		return Assignment{}, fmt.Errorf("scale: sandboxd claim for %s/%s: %w", req.Namespace, req.ClaimName, err)
	}

	a := Assignment{SandboxName: res.ID, Node: g.node, Address: res.OwnerAddr, Token: res.Token, Deadline: res.Deadline}
	g.mu.Lock()
	g.holdings[a.SandboxName] = delivered{id: res.ID, token: res.Token}
	g.mu.Unlock()

	g.enqueueRecord(req.Namespace, req.ClaimName, a)
	return a, nil
}

// Release destroys the node-local microVM backing the Assignment.
// It runs only on owner-authorized teardown, never off pod state — the same
// delete-authorization contract the aggregated apiserver's Delete honors.
func (g *nodeClaimGateway) Release(ctx context.Context, a Assignment) error {
	g.mu.Lock()
	d, ok := g.holdings[a.SandboxName]
	delete(g.holdings, a.SandboxName) // no-op when absent
	g.mu.Unlock()
	if !ok {
		return fmt.Errorf("scale: release of sandbox %q not delivered by node %q; refusing to destroy", a.SandboxName, g.node)
	}
	if err := g.client.Release(ctx, d.id, d.token); err != nil {
		// Restore the holding so a retry of the owner teardown can find it.
		g.mu.Lock()
		g.holdings[a.SandboxName] = d
		g.mu.Unlock()
		return fmt.Errorf("scale: sandboxd release of %q: %w", a.SandboxName, err)
	}
	return nil
}

// Wait blocks until all in-flight async Bound-record jobs finish. Used by tests
// and graceful shutdown; the synchronous Claim path never calls it.
func (g *nodeClaimGateway) Wait() { g.wg.Wait() }

func (g *nodeClaimGateway) enqueueRecord(ns, name string, a Assignment) {
	g.wg.Go(func() {
		// Detach from the request context (which may be canceled once Claim
		// returns) but stay bounded by the gateway lifetime context and a timeout.
		rctx, cancel := context.WithTimeout(g.baseCtx, g.recordTimeout)
		defer cancel()
		if err := g.recorder.RecordBound(rctx, ns, name, a); err != nil {
			g.log.Error(err, "async RecordBound failed; orphan binding left for OrphanReconciler",
				"namespace", ns, "claim", name, "sandbox", a.SandboxName)
		}
	})
}

func (g *nodeClaimGateway) specFor(req ClaimRequest) sandboxd.ClaimSpec {
	return sandboxd.ClaimSpec{
		Template:   cmp.Or(req.Selector[SelectorTemplateKey], g.tmpl, req.WarmPool),
		Net:        cmp.Or(req.Selector[SelectorNetKey], g.net),
		Size:       cmp.Or(req.Selector[SelectorSizeKey], g.size),
		TTLSeconds: g.ttl,
	}
}

// clientClaimRecorder patches SandboxClaim status to Bound (idempotent), the same status.sandbox.name field the L1 path sets.
type clientClaimRecorder struct {
	c client.Client
}

// NewClaimRecorder returns the default ClaimRecorder backed by a Kubernetes client
// (a cache-fed client in production, the fake client in tests).
func NewClaimRecorder(c client.Client) ClaimRecorder { return &clientClaimRecorder{c: c} }

func (r *clientClaimRecorder) RecordBound(ctx context.Context, claimNS, claimName string, a Assignment) error {
	cur := &extv1beta1.SandboxClaim{}
	if err := r.c.Get(ctx, types.NamespacedName{Namespace: claimNS, Name: claimName}, cur); err != nil {
		return fmt.Errorf("get claim %s/%s: %w", claimNS, claimName, err)
	}
	if cur.Status.SandboxStatus.Name == a.SandboxName {
		return nil // already recorded — idempotent
	}
	orig := cur.DeepCopy()
	cur.Status.SandboxStatus.Name = a.SandboxName
	if a.Address != "" {
		cur.Status.SandboxStatus.PodIPs = []string{a.Address}
	}
	apimeta.SetStatusCondition(&cur.Status.Conditions, metav1.Condition{
		Type:    BoundConditionType,
		Status:  metav1.ConditionTrue,
		Reason:  "NodeLocalClaim",
		Message: fmt.Sprintf("sandbox %s delivered by node-local claim gateway", a.SandboxName),
	})
	return r.c.Status().Patch(ctx, cur, client.MergeFrom(orig))
}

// Delivery is one live sandbox a node currently holds, with the claim it was
// delivered to. This is the node's own live state (the L0 node-scoped cache /
// sandboxd inventory), not a cluster-wide LIST.
type Delivery struct {
	SandboxName string
	Node        string
	Address     string
	ClaimNS     string
	ClaimName   string
}

// NodeInventorySource enumerates the deliveries a node currently holds. A crashed
// gateway loses its in-memory holdings, but the node/sandboxd still hold the VMs,
// so this source (backed by sandboxd's own inventory) survives a gateway restart —
// which is exactly what lets the OrphanReconciler heal a lost Bound record.
type NodeInventorySource interface {
	LiveDeliveries(ctx context.Context) ([]Delivery, error)
}

// OrphanReconciler heals orphan bindings: deliveries that happened but whose
// asynchronous Bound record was lost (the gateway crashed after delivery, before
// recording). It is audit-and-adopt only — it records the missing Bound and NEVER
// destroys a VM. Per the delete-authorization contract, a live VM is only ever
// torn down by owner-authorized Release, never as a side effect of GC.
type OrphanReconciler struct {
	node     string
	inv      NodeInventorySource
	reader   client.Client
	recorder ClaimRecorder
	log      logr.Logger
}

// NewOrphanReconciler builds an OrphanReconciler. reader reads SandboxClaim Bound
// state with point Gets (no cluster-wide LIST); recorder adopts orphan bindings.
func NewOrphanReconciler(node string, inv NodeInventorySource, reader client.Client, recorder ClaimRecorder, logger logr.Logger) *OrphanReconciler {
	return &OrphanReconciler{node: node, inv: inv, reader: reader, recorder: recorder, log: logger}
}

// Reconcile scans the node's live deliveries, finds those whose owning
// SandboxClaim has no Bound record, and adopts them (records Bound). It returns the
// number of orphan bindings reconciled. It never releases or destroys a sandbox.
func (o *OrphanReconciler) Reconcile(ctx context.Context) (int, error) {
	deliveries, err := o.inv.LiveDeliveries(ctx)
	if err != nil {
		return 0, fmt.Errorf("scale: list node %q deliveries: %w", o.node, err)
	}
	reconciled := 0
	for _, d := range deliveries {
		cur := &extv1beta1.SandboxClaim{}
		// Point Get by name — the typed client resolves the resource via the
		// scheme's REST mapper (correct es/ies pluralization), never a naive
		// kind+"s" that could 404-misread a live claim as deleted.
		getErr := o.reader.Get(ctx, types.NamespacedName{Namespace: d.ClaimNS, Name: d.ClaimName}, cur)
		if getErr != nil {
			if k8serrors.IsNotFound(getErr) {
				o.log.V(1).Info("delivery has no SandboxClaim object; leaving VM intact (no destroy)",
					"namespace", d.ClaimNS, "claim", d.ClaimName, "sandbox", d.SandboxName)
				continue
			}
			return reconciled, fmt.Errorf("scale: get claim %s/%s: %w", d.ClaimNS, d.ClaimName, getErr)
		}
		if cur.Status.SandboxStatus.Name != "" {
			continue // already Bound — not an orphan
		}
		a := Assignment{SandboxName: d.SandboxName, Node: d.Node, Address: d.Address}
		if err := o.recorder.RecordBound(ctx, d.ClaimNS, d.ClaimName, a); err != nil {
			return reconciled, fmt.Errorf("scale: adopt orphan binding %s/%s: %w", d.ClaimNS, d.ClaimName, err)
		}
		reconciled++
	}
	return reconciled, nil
}

// SubjectAccessReviewer narrows client-go's SubjectAccessReviewInterface.Create so the gateway needs no direct authz-client dependency and tests can inject a fake.
type SubjectAccessReviewer interface {
	Create(ctx context.Context, sar *authzv1.SubjectAccessReview, opts metav1.CreateOptions) (*authzv1.SubjectAccessReview, error)
}

// ReviewAuthorizer checks SandboxClaim access through SubjectAccessReview.
type ReviewAuthorizer struct {
	Reviewer SubjectAccessReviewer
	// Group/Resource/Verb default to the SandboxClaim create check when empty.
	Group    string
	Resource string
	Verb     string
}

func (a *ReviewAuthorizer) Authorize(ctx context.Context, req ClaimRequest) error {
	user := req.Selector[RequestUserSelectorKey]
	if user == "" {
		return fmt.Errorf("missing caller identity (selector %q)", RequestUserSelectorKey)
	}
	sar := &authzv1.SubjectAccessReview{
		Spec: authzv1.SubjectAccessReviewSpec{
			User: user,
			ResourceAttributes: &authzv1.ResourceAttributes{
				Namespace: req.Namespace,
				Verb:      cmp.Or(a.Verb, "create"),
				Group:     cmp.Or(a.Group, extv1beta1.GroupVersion.Group),
				Resource:  cmp.Or(a.Resource, "sandboxclaims"),
				Name:      req.ClaimName,
			},
		},
	}
	got, err := a.Reviewer.Create(ctx, sar, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("subjectaccessreview: %w", err)
	}
	if got.Status.Denied || !got.Status.Allowed {
		return fmt.Errorf("user %q may not claim in %s: %s", user, req.Namespace, got.Status.Reason)
	}
	return nil
}
