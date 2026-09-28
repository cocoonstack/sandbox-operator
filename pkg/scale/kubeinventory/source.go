// Package kubeinventory is the Kubernetes InventorySource: NodeInventory objects read through a controller-runtime cache.
package kubeinventory

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"sync/atomic"
	"time"

	"github.com/spf13/pflag"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

const defaultStaleAfter = 90 * time.Second

var nodeInventories = schema.GroupResource{Group: scale.NodeInventoryGVK.Group, Resource: "nodeinventories"}

// Options tunes a Source.
type Options struct {
	// StaleAfter is how old publishedAt may get before its node leaves the fleet; zero means 90 s.
	StaleAfter time.Duration
}

// AddFlags registers the source flags on fs.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.DurationVar(&o.StaleAfter, "inventory-stale-after", cmp.Or(o.StaleAfter, defaultStaleAfter),
		"Drop a node from this process's inventory reads once its NodeInventory publishedAt trails the newest publish in the fleet by more than this; set the same value on every binary that reads inventory. An inventory without publishedAt is stale.")
}

type snapshot struct {
	caps   []scale.NodePools
	stamps []int64
	newest int64
	byNode map[string]*scale.NodeInventory
}

var _ scale.InventorySource = (*Source)(nil)

// Source is the production InventorySource: a snapshot of the informer's NodeInventory objects, rebuilt on every event.
// It never mutates what it holds, since the informer hands out its cached objects themselves.
type Source struct {
	staleAfter time.Duration
	snap       atomic.Pointer[snapshot]
	byNode     map[string]*scale.NodeInventory
}

// New builds a Source over the NodeInventory informer in informers and waits until its snapshot holds the synced fleet.
func New(ctx context.Context, informers cache.Informers, opts Options) (*Source, error) {
	s := &Source{staleAfter: cmp.Or(opts.StaleAfter, defaultStaleAfter), byNode: map[string]*scale.NodeInventory{}}
	s.snap.Store(&snapshot{})
	inf, err := informers.GetInformer(ctx, &scale.NodeInventory{})
	if err != nil {
		return nil, fmt.Errorf("kubeinventory: get node inventory informer: %w", err)
	}
	reg, err := inf.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc:    s.put,
		UpdateFunc: func(_, obj any) { s.put(obj) },
		DeleteFunc: s.drop,
	})
	if err != nil {
		return nil, fmt.Errorf("kubeinventory: watch node inventories: %w", err)
	}
	syncCtx, cancel := context.WithTimeout(ctx, cacheSyncTimeout)
	defer cancel()
	if !toolscache.WaitForCacheSync(syncCtx.Done(), reg.HasSynced) {
		return nil, fmt.Errorf("kubeinventory: node inventory snapshot did not sync within %s", cacheSyncTimeout)
	}
	return s, nil
}

func (s *Source) ListNodes(context.Context) ([]string, error) {
	snap, ref := s.current()
	nodes := make([]string, 0, len(snap.caps))
	for i, c := range snap.caps {
		if !s.stale(snap.stamps[i], ref) {
			nodes = append(nodes, c.Node)
		}
	}
	return nodes, nil
}

func (s *Source) NodeCapacities(context.Context) ([]scale.NodePools, error) {
	snap, ref := s.current()
	if !slices.ContainsFunc(snap.stamps, func(stamp int64) bool { return s.stale(stamp, ref) }) {
		return snap.caps, nil
	}
	out := make([]scale.NodePools, 0, len(snap.caps))
	for i, c := range snap.caps {
		if !s.stale(snap.stamps[i], ref) {
			out = append(out, c)
		}
	}
	return out, nil
}

func (s *Source) NodeInventory(_ context.Context, node string) (*scale.NodeInventory, error) {
	return s.get(node)
}

func (s *Source) NodeCapacity(_ context.Context, node string) (string, []scale.PoolCapacity, error) {
	inv, err := s.get(node)
	if err != nil {
		return "", nil, err
	}
	return inv.Address, inv.Pools, nil
}

func (s *Source) get(node string) (*scale.NodeInventory, error) {
	snap := s.snap.Load()
	inv := snap.byNode[node]
	if inv == nil {
		return nil, fmt.Errorf("kubeinventory: get node %q inventory: %w", node, k8serrors.NewNotFound(nodeInventories, node))
	}
	if s.stale(publishedAt(inv), min(snap.newest, time.Now().UnixNano())) {
		return nil, fmt.Errorf("kubeinventory: node %q inventory is stale: %w", node, k8serrors.NewNotFound(nodeInventories, node))
	}
	return inv, nil
}

func (s *Source) current() (*snapshot, int64) {
	snap := s.snap.Load()
	return snap, min(snap.newest, time.Now().UnixNano())
}

func (s *Source) stale(stamp, ref int64) bool {
	return ref-stamp > int64(s.staleAfter)
}

func (s *Source) put(obj any) {
	inv, ok := obj.(*scale.NodeInventory)
	if !ok {
		return
	}
	s.byNode[inv.Name] = inv
	s.publish()
}

func (s *Source) drop(obj any) {
	if tomb, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
		obj = tomb.Obj
	}
	inv, ok := obj.(*scale.NodeInventory)
	if !ok {
		return
	}
	delete(s.byNode, inv.Name)
	s.publish()
}

func (s *Source) publish() {
	names := slices.Sorted(maps.Keys(s.byNode))
	snap := &snapshot{caps: make([]scale.NodePools, len(names)), stamps: make([]int64, len(names)), byNode: maps.Clone(s.byNode)}
	for i, node := range names {
		inv := s.byNode[node]
		snap.caps[i] = scale.NodePools{Node: node, Address: inv.Address, Pools: inv.Pools, Templates: inv.Templates}
		snap.stamps[i] = publishedAt(inv)
		snap.newest = max(snap.newest, snap.stamps[i])
	}
	s.snap.Store(snap)
}

func publishedAt(inv *scale.NodeInventory) int64 {
	if inv.PublishedAt.IsZero() {
		return 0
	}
	return inv.PublishedAt.UnixNano()
}
