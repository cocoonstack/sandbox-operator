// Package kubeinventory is the Kubernetes InventorySource: NodeInventory objects read through a controller-runtime cache.
package kubeinventory

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/spf13/pflag"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

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
		"Drop a node from claims, lists, lookups, the warm-pool driver and the envd-proxy probe once its NodeInventory publishedAt is older than this. An inventory without publishedAt, from a vk-sandbox that predates the field, always stays.")
}

var _ scale.InventorySource = (*Source)(nil)

// Source is the production InventorySource over a cache-fed NodeInventory reader.
// It never mutates what it reads, since NewCache hands out its cached objects themselves.
type Source struct {
	reader     client.Reader
	staleAfter time.Duration
}

// New builds a Source over reader.
func New(reader client.Reader, opts Options) *Source {
	return &Source{reader: reader, staleAfter: cmp.Or(opts.StaleAfter, defaultStaleAfter)}
}

func (s *Source) ListNodes(ctx context.Context) ([]string, error) {
	ul := &unstructured.UnstructuredList{}
	ul.SetGroupVersionKind(scale.NodeInventoryGVK.GroupVersion().WithKind(scale.NodeInventoryGVK.Kind + "List"))
	if err := s.reader.List(ctx, ul); err != nil {
		return nil, fmt.Errorf("kubeinventory: list node inventories: %w", err)
	}
	nodes := make([]string, 0, len(ul.Items))
	now := time.Now()
	for i := range ul.Items {
		if !s.stale(ul.Items[i].Object, now) {
			nodes = append(nodes, ul.Items[i].GetName())
		}
	}
	slices.Sort(nodes)
	return nodes, nil
}

func (s *Source) NodeInventory(ctx context.Context, node string) (*scale.NodeInventory, error) {
	u, err := s.get(ctx, node)
	if err != nil {
		return nil, err
	}
	inv := &scale.NodeInventory{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, inv); err != nil {
		return nil, fmt.Errorf("kubeinventory: decode node %q inventory: %w", node, err)
	}
	return inv, nil
}

func (s *Source) NodeCapacity(ctx context.Context, node string) (string, []scale.PoolCapacity, error) {
	u, err := s.get(ctx, node)
	if err != nil {
		return "", nil, err
	}
	addr, _, err := unstructured.NestedString(u.Object, "address")
	if err != nil {
		return "", nil, fmt.Errorf("kubeinventory: decode node %q address: %w", node, err)
	}
	raw, _, err := unstructured.NestedSlice(u.Object, "pools")
	if err != nil {
		return "", nil, fmt.Errorf("kubeinventory: decode node %q pools: %w", node, err)
	}
	pools := make([]scale.PoolCapacity, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		pc := scale.PoolCapacity{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(m, &pc); err != nil {
			return "", nil, fmt.Errorf("kubeinventory: decode node %q pool capacity: %w", node, err)
		}
		pools = append(pools, pc)
	}
	return addr, pools, nil
}

func (s *Source) get(ctx context.Context, node string) (*unstructured.Unstructured, error) {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(scale.NodeInventoryGVK)
	if err := s.reader.Get(ctx, types.NamespacedName{Name: node}, u); err != nil {
		return nil, fmt.Errorf("kubeinventory: get node %q inventory: %w", node, err)
	}
	if s.stale(u.Object, time.Now()) {
		return nil, fmt.Errorf("kubeinventory: node %q inventory is stale: %w", node, k8serrors.NewNotFound(nodeInventories, node))
	}
	return u, nil
}

func (s *Source) stale(obj map[string]any, now time.Time) bool {
	raw, _ := obj["publishedAt"].(string)
	if raw == "" {
		return false
	}
	at, err := time.Parse(time.RFC3339, raw)
	return err == nil && now.Sub(at) > s.staleAfter
}
