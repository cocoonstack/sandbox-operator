// Package warmpool drives the official agents.x-k8s.io SandboxWarmPool CRD onto
// the L3 node-local warm pools. It is the control-plane surface for warm
// capacity: `kubectl apply` a SandboxWarmPool sets the target, editing
// spec.replicas scales it, deleting it drains — no SSH, no per-node poking.
//
// The design contract the whole L3 stack depends on: this reconcile is
// POOL-LEVEL, O(pools + nodes), NEVER O(sandboxes). It runs inside the
// aggregated apiserver process alongside the NodeInventory cache, so one
// component polls node state and writes back CR status. There is deliberately
// NO per-sandbox reconcile — individual Sandbox status is synthesized on read
// from NodeInventory, so the apiserver never carries a per-sandbox background
// load and cannot wedge as the sandbox count grows.
package warmpool

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/sync/errgroup"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	extv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

const (
	// defaultInterval is the pool resync cadence, and with it the sampling period of
	// the fleet-wide warm count reported in pool status. Pools are O(single-digit)
	// and every tick's node fan-out is the same PUT the driver already owes, so a 5s
	// loop costs no extra round trips — it buys a 5s-granularity view of total warm
	// capacity while still reconciling drift (a node that restarted, a target edited
	// out of band).
	defaultInterval = 5 * time.Second

	// maxNodeConcurrency bounds the per-node PUT /v1/pools fan-out per tick.
	maxNodeConcurrency = 16

	// The sandboxd client has no HTTP timeout; unbounded, one silent node
	// wedges the global reconcile forever.
	setPoolsTimeout = 10 * time.Second
)

var errNoTemplateRef = errors.New("spec.sandboxTemplateRef.name is required")

// PoolSetter is the sandboxd surface the driver needs: replace a node's whole
// warm-target set. The concrete *sandboxd.Client satisfies it; tests inject a fake.
type PoolSetter interface {
	SetPools(ctx context.Context, pools []sandboxd.PoolSpec) (*sandboxd.NodeInfo, error)
}

// ClientFactory builds a PoolSetter for one node's advertise address and the
// uniform fleet api_token.
type ClientFactory func(addr, token string) PoolSetter

// NewSandboxdFactory returns the production factory. It shares the store's
// address rendering and keep-alive client, so a node advertising a scheme is
// reachable here too.
func NewSandboxdFactory() ClientFactory {
	hc := scale.NewSandboxdHTTPClient()
	return func(addr, token string) PoolSetter {
		return sandboxd.New(scale.SandboxdBaseURL(addr), token, sandboxd.WithHTTPClient(hc))
	}
}

// Options configures a Driver.
type Options struct {
	Interval time.Duration
	Log      logr.Logger
}

// nodeView is one schedulable node: its name, sandboxd address, and current
// per-pool warm counts (for status write-back).
type nodeView struct {
	name   string
	addr   string
	warmBy map[scale.PoolKey]int
}

// desiredPool is a resolved SandboxWarmPool: its key and per-node target.
type desiredPool struct {
	pool    *extv1beta1.SandboxWarmPool
	key     scale.PoolKey
	targets map[string]int
}

// Driver reconciles SandboxWarmPool CRs into node-local sandboxd warm targets.
type Driver struct {
	// kube lists SandboxWarmPools, gets SandboxTemplates, and writes pool status.
	kube client.Client
	// inv is the cache-fed NodeInventory reader (node list + address + live warm).
	inv scale.InventorySource
	// token is the uniform fleet-wide sandboxd api_token used on every PUT /v1/pools.
	token   string
	factory ClientFactory

	interval time.Duration
	log      logr.Logger
}

// New builds a Driver. token empty leaves the driver fail-closed (it logs and
// sets no pools rather than driving sandboxd unauthenticated).
func New(kube client.Client, inv scale.InventorySource, token string, factory ClientFactory, opts Options) *Driver {
	if opts.Interval <= 0 {
		opts.Interval = defaultInterval
	}
	return &Driver{kube: kube, inv: inv, token: token, factory: factory, interval: opts.Interval, log: opts.Log}
}

func (d *Driver) Reconcile(ctx context.Context, _ ctrl.Request) (ctrl.Result, error) {
	if err := d.reconcileOnce(ctx); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: d.interval}, nil
}

// SetupWithManager registers the driver as a controller watching SandboxWarmPool
// (the desired-state object) and NodeInventory (the node set to spread across).
// It uses the manager's cached client for pool/template reads and status writes.
func (d *Driver) SetupWithManager(mgr ctrl.Manager) error {
	if d.kube == nil {
		d.kube = mgr.GetClient()
	}
	if d.inv == nil {
		d.inv = scale.NewClientInventorySource(mgr.GetClient())
	}
	// Any NodeInventory change (a node joined, restarted, changed address) must
	// re-spread every pool, so map it to a single global reconcile trigger.
	enqueueAll := handler.EnqueueRequestsFromMapFunc(syncRequest)
	// Generation-filtered so the loop's own writeStatus cannot re-trigger it
	// into a continuous back-to-back loop under claim churn; create/delete,
	// spec edits, NodeInventory events, and the RequeueAfter tick keep coverage.
	return ctrl.NewControllerManagedBy(mgr).
		Named("sandboxwarmpool").
		Watches(&extv1beta1.SandboxWarmPool{}, enqueueAll, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&extv1beta1.NodeInventory{}, enqueueAll).
		Complete(d)
}

// reconcileOnce is the whole loop: resolve pools, distribute targets, PUT every
// node's full pool set, write pool status from the warm those PUTs report back.
func (d *Driver) reconcileOnce(ctx context.Context) error {
	var pools extv1beta1.SandboxWarmPoolList
	if err := d.kube.List(ctx, &pools); err != nil {
		return fmt.Errorf("list SandboxWarmPools: %w", err)
	}
	nodes, err := d.schedulableNodes(ctx)
	if err != nil {
		return err
	}

	desired := make([]desiredPool, 0, len(pools.Items))
	for i := range pools.Items {
		p := &pools.Items[i]
		key, rerr := d.poolKey(ctx, p)
		if rerr != nil {
			if !k8serrors.IsNotFound(rerr) && !errors.Is(rerr, errNoTemplateRef) {
				return fmt.Errorf("resolve warm pool %s/%s template: %w", p.Namespace, p.Name, rerr)
			}
			d.log.Error(rerr, "resolve warm pool template", "pool", p.Namespace+"/"+p.Name)
			d.writeStatus(ctx, p, nodes, key) // status still reflects live warm (0 target)
			continue
		}
		replicas := int32(1)
		if p.Spec.Replicas != nil {
			replicas = *p.Spec.Replicas
		}
		desired = append(desired, desiredPool{
			pool:    p,
			key:     key,
			targets: distribute(replicas, nodes),
		})
	}

	d.applyToNodes(ctx, nodes, desired)

	// Status comes from the warm counts applyToNodes just refreshed off the PUT
	// responses; post-apply warm may still be refilling.
	for _, dp := range desired {
		d.writeStatus(ctx, dp.pool, nodes, dp.key)
	}
	return nil
}

// schedulableNodes reads NodeInventory (O(nodes), cache-fed) into node views,
// keeping only nodes that advertise a sandboxd address.
func (d *Driver) schedulableNodes(ctx context.Context) ([]nodeView, error) {
	names, err := d.inv.ListNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("list node inventories: %w", err)
	}
	views := make([]nodeView, 0, len(names))
	for _, name := range names {
		addr, pools, err := d.inv.NodeCapacity(ctx, name)
		if err != nil {
			d.log.V(1).Info("skip node without readable inventory", "node", name, "err", err.Error())
			continue
		}
		if addr == "" {
			continue
		}
		warmBy := make(map[scale.PoolKey]int, len(pools))
		for _, pc := range pools {
			warmBy[scale.PoolKey{Template: pc.Template, Net: pc.Net, Size: pc.Size}] = pc.Warm
		}
		views = append(views, nodeView{name: name, addr: addr, warmBy: warmBy})
	}
	slices.SortFunc(views, func(a, b nodeView) int { return cmp.Compare(a.name, b.name) })
	return views, nil
}

// poolKey resolves a SandboxWarmPool's SandboxTemplate and derives the pool key
// via the shared scale.PoolKeyFor — identical to the key a Create derives.
func (d *Driver) poolKey(ctx context.Context, p *extv1beta1.SandboxWarmPool) (scale.PoolKey, error) {
	name := p.Spec.TemplateRef.Name
	if name == "" {
		return scale.PoolKey{}, errNoTemplateRef
	}
	var tmpl extv1beta1.SandboxTemplate
	if err := d.kube.Get(ctx, types.NamespacedName{Namespace: p.Namespace, Name: name}, &tmpl); err != nil {
		return scale.PoolKey{}, fmt.Errorf("get SandboxTemplate %s/%s: %w", p.Namespace, name, err)
	}
	net := scale.NetForAnnotations(tmpl.Spec.PodTemplate.ObjectMeta.Annotations, nil)
	return scale.PoolKeyFor(tmpl.Spec.PodTemplate.Spec.Containers, net), nil
}

// applyToNodes builds each node's FULL desired pool set (one entry per pool) and
// PUTs it. The whole set goes in one request because sandboxd replaces its pool
// config wholesale — an omitted pool drains. Per-node failures are logged, not
// fatal: one unreachable node must not stall the rest.
func (d *Driver) applyToNodes(ctx context.Context, nodes []nodeView, desired []desiredPool) {
	if d.token == "" {
		d.log.Info("warm-pool driver has no sandboxd token; skipping pool apply (fail-closed)")
		return
	}
	var g errgroup.Group
	g.SetLimit(maxNodeConcurrency)
	for i := range nodes {
		node := nodes[i]
		// Aggregate targets BY KEY: two SandboxWarmPools may resolve to the same
		// (template,net,size) — their warm targets sum, and sandboxd rejects a PUT
		// that repeats a key ("duplicate pool"). One spec per distinct key.
		byKey := make(map[scale.PoolKey]int, len(desired))
		for _, dp := range desired {
			byKey[dp.key] += dp.targets[node.name]
		}
		specs := make([]sandboxd.PoolSpec, 0, len(byKey))
		for key, warm := range byKey {
			specs = append(specs, sandboxd.PoolSpec{Template: key.Template, Net: key.Net, Size: key.Size, Warm: warm})
		}
		slices.SortFunc(specs, func(a, b sandboxd.PoolSpec) int {
			return cmp.Or(
				cmp.Compare(a.Template, b.Template),
				cmp.Compare(a.Net, b.Net),
				cmp.Compare(a.Size, b.Size),
			)
		})
		g.Go(func() error {
			callCtx, cancel := context.WithTimeout(ctx, setPoolsTimeout)
			defer cancel()
			info, err := d.factory(node.addr, d.token).SetPools(callCtx, specs)
			if err != nil {
				d.log.Error(err, "set node warm pools", "node", node.name, "addr", node.addr)
				return nil
			}
			if info == nil {
				return nil
			}
			// The PUT response carries this node's live per-pool warm, so adopt it
			// as the status source: NodeInventory lags by up to one publish
			// interval, which would make the status sampling period
			// interval+publish instead of the resync interval. A node whose PUT
			// failed keeps its inventory-derived counts. Only this goroutine
			// touches nodes[i], so no lock is needed.
			nodes[i].warmBy = warmByFrom(info)
			return nil
		})
	}
	_ = g.Wait()
}

// writeStatus updates a SandboxWarmPool's status.replicas/readyReplicas from the
// live warm counts across all nodes for the pool's key — this tick's PUT
// responses for every node that answered, inventory for any that did not. Warm
// VMs are claim-ready, so readyReplicas == replicas. Best-effort; a conflict is
// retried next tick.
func (d *Driver) writeStatus(ctx context.Context, p *extv1beta1.SandboxWarmPool, nodes []nodeView, key scale.PoolKey) {
	total := 0
	for i := range nodes {
		total += nodes[i].warmBy[key]
	}
	selector := "agents.x-k8s.io/warm-pool=" + p.Name
	if p.Status.Replicas == int32(total) && p.Status.ReadyReplicas == int32(total) && p.Status.Selector == selector {
		return
	}
	fresh := p.DeepCopy()
	fresh.Status.Replicas = int32(total)
	fresh.Status.ReadyReplicas = int32(total)
	fresh.Status.Selector = selector
	if err := d.kube.Status().Update(ctx, fresh); err != nil {
		d.log.V(1).Info("warm-pool status update deferred", "pool", p.Namespace+"/"+p.Name, "err", err.Error())
	}
}

// warmByFrom indexes a sandboxd PUT /v1/pools response by pool key. A key the
// response omits is genuinely 0 warm — sandboxd echoes back every pool it holds,
// and one it no longer holds is drained.
func warmByFrom(info *sandboxd.NodeInfo) map[scale.PoolKey]int {
	warmBy := make(map[scale.PoolKey]int, len(info.Pools))
	for _, p := range info.Pools {
		warmBy[scale.PoolKey{Template: p.Key.Template, Net: p.Key.Net, Size: p.Key.Size}] = p.Warm
	}
	return warmBy
}

func syncRequest(context.Context, client.Object) []reconcile.Request {
	return []reconcile.Request{{Name: "sync"}}
}

// distribute spreads total warm targets evenly across nodes (base + remainder to the first nodes).
func distribute(replicas int32, nodes []nodeView) map[string]int {
	targets := make(map[string]int, len(nodes))
	if len(nodes) == 0 {
		return targets
	}
	total := max(int(replicas), 0)
	base := total / len(nodes)
	remainder := total % len(nodes)
	for i := range nodes {
		t := base
		if i < remainder {
			t++
		}
		targets[nodes[i].name] = t
	}
	return targets
}
