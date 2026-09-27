// Package kubeinventory is the Kubernetes InventorySource: NodeInventory objects read through a controller-runtime cache.
package kubeinventory

import (
	"context"
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

var _ scale.InventorySource = (*Source)(nil)

// Source is the production InventorySource over a cache-fed NodeInventory reader.
// It never mutates what it reads, since NewCache hands out its cached objects themselves.
type Source struct {
	reader client.Reader
}

// New builds a Source over reader.
func New(reader client.Reader) *Source {
	return &Source{reader: reader}
}

func (s *Source) ListNodes(ctx context.Context) ([]string, error) {
	ul := &unstructured.UnstructuredList{}
	ul.SetGroupVersionKind(scale.NodeInventoryGVK.GroupVersion().WithKind(scale.NodeInventoryGVK.Kind + "List"))
	if err := s.reader.List(ctx, ul); err != nil {
		return nil, fmt.Errorf("kubeinventory: list node inventories: %w", err)
	}
	nodes := make([]string, 0, len(ul.Items))
	for i := range ul.Items {
		nodes = append(nodes, ul.Items[i].GetName())
	}
	slices.Sort(nodes)
	return nodes, nil
}

func (s *Source) NodeInventory(ctx context.Context, node string) (*scale.NodeInventory, error) {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(scale.NodeInventoryGVK)
	if err := s.reader.Get(ctx, types.NamespacedName{Name: node}, u); err != nil {
		return nil, fmt.Errorf("kubeinventory: get node %q inventory: %w", node, err)
	}
	inv := &scale.NodeInventory{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, inv); err != nil {
		return nil, fmt.Errorf("kubeinventory: decode node %q inventory: %w", node, err)
	}
	return inv, nil
}

func (s *Source) NodeCapacity(ctx context.Context, node string) (string, []scale.PoolCapacity, error) {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(scale.NodeInventoryGVK)
	if err := s.reader.Get(ctx, types.NamespacedName{Name: node}, u); err != nil {
		return "", nil, fmt.Errorf("kubeinventory: get node %q inventory: %w", node, err)
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
