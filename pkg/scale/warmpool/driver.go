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

	"github.com/projecteru2/core/log"
	"golang.org/x/sync/errgroup"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	extv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

const (
	// defaultInterval is the pool resync cadence and the sampling period of the warm count in pool status; each tick's fan-out is the PUT the driver owes anyway.
	defaultInterval = 5 * time.Second

	// maxNodeConcurrency bounds the per-node PUT /v1/pools fan-out per tick.
	maxNodeConcurrency = 16
)

var errNoTemplateRef = errors.New("spec.sandboxTemplateRef.name is required")

// PoolSetter replaces a node's whole warm-target set; *sandboxd.Client satisfies it and tests inject a fake.
type PoolSetter interface {
	SetPools(ctx context.Context, pools []sandboxd.PoolSpec) (*sandboxd.NodeInfo, error)
}

// ClientFactory builds a PoolSetter for one node's advertise address and the fleet api_token.
type ClientFactory func(addr, token string) PoolSetter

// NewSandboxdFactory returns the production factory, which renders addresses as the store does so a node advertising a scheme is reachable.
func NewSandboxdFactory() ClientFactory {
	hc := scale.NewSandboxdHTTPClient()
	return func(addr, token string) PoolSetter {
		return sandboxd.New(scale.SandboxdBaseURL(addr), token, sandboxd.WithHTTPClient(hc))
	}
}

// Options configures a Driver.
type Options struct {
	Interval time.Duration
}

// nodeView is one schedulable node with its live per-pool warm counts.
type nodeView struct {
	name   string
	addr   string
	warmBy map[scale.PoolKey]int
}

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
}

// New builds a Driver; an empty token leaves it fail-closed, logging instead of driving sandboxd unauthenticated.
func New(kube client.Client, inv scale.InventorySource, token string, factory ClientFactory, opts Options) *Driver {
	if opts.Interval <= 0 {
		opts.Interval = defaultInterval
	}
	return &Driver{kube: kube, inv: inv, token: token, factory: factory, interval: opts.Interval}
}

func (d *Driver) Reconcile(ctx context.Context, _ ctrl.Request) (ctrl.Result, error) {
	if err := d.reconcileOnce(ctx); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: d.interval}, nil
}

// SetupWithManager registers the driver as a controller watching SandboxWarmPool and NodeInventory.
func (d *Driver) SetupWithManager(mgr ctrl.Manager) error {
	// Any NodeInventory change re-spreads every pool, so it maps to one global reconcile.
	enqueueAll := handler.EnqueueRequestsFromMapFunc(syncRequest)
	// Generation-filtered so the loop's own status writes cannot re-trigger it under claim churn.
	return ctrl.NewControllerManagedBy(mgr).
		Named("sandboxwarmpool").
		Watches(&extv1beta1.SandboxWarmPool{}, enqueueAll, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&scale.NodeInventory{}, enqueueAll).
		Complete(d)
}

// reconcileOnce resolves pools, spreads targets, PUTs every node's full set and writes status from what the PUTs report.
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
			log.WithFunc("warmpool.reconcileOnce").Errorf(ctx, rerr, "resolve warm pool template pool=%s/%s", p.Namespace, p.Name)
			d.writeStatus(ctx, p, 0)
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

	// Status comes from the warm the PUTs just reported; post-apply warm may still be refilling.
	for i, warm := range apportionWarm(desired, nodes) {
		d.writeStatus(ctx, desired[i].pool, warm)
	}
	return nil
}

// schedulableNodes keeps only the nodes that advertise a sandboxd address.
func (d *Driver) schedulableNodes(ctx context.Context) ([]nodeView, error) {
	nodes, err := d.inv.NodeCapacities(ctx)
	if err != nil {
		return nil, fmt.Errorf("list node inventories: %w", err)
	}
	views := make([]nodeView, 0, len(nodes))
	for _, n := range nodes {
		if n.Address != "" {
			views = append(views, nodeView{name: n.Node, addr: n.Address, warmBy: warmByKey(n.Pools)})
		}
	}
	slices.SortFunc(views, func(a, b nodeView) int { return cmp.Compare(a.name, b.name) })
	return views, nil
}

// poolKey derives the key through scale.PoolKeyFor, the derivation a Create uses.
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

// applyToNodes PUTs each node's full pool set in one request, since sandboxd drains any pool the set omits; a failed node is logged, not fatal.
func (d *Driver) applyToNodes(ctx context.Context, nodes []nodeView, desired []desiredPool) {
	logger := log.WithFunc("warmpool.applyToNodes")
	if d.token == "" {
		logger.Warn(ctx, "warm-pool driver has no sandboxd token; skipping pool apply (fail-closed)")
		return
	}
	var g errgroup.Group
	g.SetLimit(maxNodeConcurrency)
	for i := range nodes {
		node := nodes[i]
		// Targets sum by key: two SandboxWarmPools may resolve to one key, and sandboxd rejects a PUT that repeats one.
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
			info, err := d.factory(node.addr, d.token).SetPools(ctx, specs)
			if err != nil {
				logger.Errorf(ctx, err, "set node warm pools node=%s addr=%s", node.name, node.addr)
				return nil
			}
			if info == nil {
				return nil
			}
			// The PUT response carries the node's live warm, one publish interval fresher than inventory; only this goroutine touches nodes[i].
			nodes[i].warmBy = warmByKey(scale.PoolCapacityFromInfo(info))
			return nil
		})
	}
	_ = g.Wait()
}

// writeStatus sets replicas and readyReplicas to the pool's share of the live warm, which is claim-ready; a conflict waits for the next tick.
func (d *Driver) writeStatus(ctx context.Context, p *extv1beta1.SandboxWarmPool, warm int) {
	selector := "agents.x-k8s.io/warm-pool=" + p.Name
	if p.Status.Replicas == int32(warm) && p.Status.ReadyReplicas == int32(warm) && p.Status.Selector == selector {
		return
	}
	fresh := p.DeepCopy()
	fresh.Status.Replicas = int32(warm)
	fresh.Status.ReadyReplicas = int32(warm)
	fresh.Status.Selector = selector
	if err := d.kube.Status().Update(ctx, fresh); err != nil {
		log.WithFunc("warmpool.writeStatus").Debugf(ctx, "warm-pool status update deferred pool=%s/%s err=%v", p.Namespace, p.Name, err)
	}
}

// warmByKey indexes pools by pool key; a key a PUT /v1/pools response omits is genuinely 0 warm, since sandboxd echoes every pool it holds.
func warmByKey(pools []scale.PoolCapacity) map[scale.PoolKey]int {
	warmBy := make(map[scale.PoolKey]int, len(pools))
	for _, pc := range pools {
		warmBy[scale.PoolKey{Template: pc.Template, Net: pc.Net, Size: pc.Size}] = pc.Warm
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

func apportionWarm(desired []desiredPool, nodes []nodeView) []int {
	fleet := make(map[scale.PoolKey]int, len(desired))
	sumTarget := make(map[scale.PoolKey]int, len(desired))
	for _, dp := range desired {
		if _, seen := fleet[dp.key]; !seen {
			for i := range nodes {
				fleet[dp.key] += nodes[i].warmBy[dp.key]
			}
		}
		sumTarget[dp.key] += poolTarget(dp)
	}
	out := make([]int, len(desired))
	given := make(map[scale.PoolKey]int, len(fleet))
	for i, dp := range desired {
		if sumTarget[dp.key] > 0 {
			out[i] = fleet[dp.key] * poolTarget(dp) / sumTarget[dp.key]
			given[dp.key] += out[i]
		}
	}
	for i, dp := range desired {
		for given[dp.key] < fleet[dp.key] {
			out[i]++
			given[dp.key]++
			if sumTarget[dp.key] > 0 {
				break
			}
		}
	}
	return out
}

func poolTarget(dp desiredPool) int {
	total := 0
	for _, t := range dp.targets {
		total += t
	}
	return total
}
