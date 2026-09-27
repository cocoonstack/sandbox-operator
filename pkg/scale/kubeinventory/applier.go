package kubeinventory

import (
	"cmp"
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

var _ scale.InventoryApplier = (*ssaApplier)(nil)

type ssaApplier struct {
	c          client.Client
	fieldOwner string
}

// NewSSAApplier returns the default InventoryApplier, which resolves the resource through a RESTMapper.
func NewSSAApplier(c client.Client, fieldOwner string) scale.InventoryApplier {
	fieldOwner = cmp.Or(fieldOwner, "cocoon-node-inventory-publisher")
	return &ssaApplier{c: c, fieldOwner: fieldOwner}
}

func (a *ssaApplier) Apply(ctx context.Context, inv *scale.NodeInventory) error {
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(inv)
	if err != nil {
		return fmt.Errorf("kubeinventory: encode node inventory: %w", err)
	}
	u := &unstructured.Unstructured{Object: raw}
	u.SetGroupVersionKind(scale.NodeInventoryGVK)
	u.SetName(inv.Node)
	ac := client.ApplyConfigurationFromUnstructured(u)
	if err := a.c.Apply(ctx, ac, client.FieldOwner(a.fieldOwner), client.ForceOwnership); err != nil {
		return fmt.Errorf("kubeinventory: server-side-apply node inventory: %w", err)
	}
	return nil
}
