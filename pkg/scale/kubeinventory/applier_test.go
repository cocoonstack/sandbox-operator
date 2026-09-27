package kubeinventory

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cocoonv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

func TestSSAApplier_UpsertsOneObjectPerNode(t *testing.T) {
	ctx := t.Context()
	scheme := runtime.NewScheme()
	require.NoError(t, cocoonv1beta1.AddToScheme(scheme))
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	applier := NewSSAApplier(cli, "vk-test")

	require.NoError(t, applier.Apply(ctx, &scale.NodeInventory{
		Kind: scale.NodeInventoryGVK.Kind, APIVersion: scale.NodeInventoryGVK.GroupVersion().String(), Name: "n1", Node: "n1",
		Entries: []scale.InventoryEntry{{Name: "ns/a", Phase: "Running"}},
	}))
	got := &cocoonv1beta1.NodeInventory{}
	require.NoError(t, cli.Get(ctx, client.ObjectKey{Name: "n1"}, got))
	require.Len(t, got.Entries, 1)

	require.NoError(t, applier.Apply(ctx, &scale.NodeInventory{
		Kind: scale.NodeInventoryGVK.Kind, APIVersion: scale.NodeInventoryGVK.GroupVersion().String(), Name: "n1", Node: "n1",
		Entries: []scale.InventoryEntry{{Name: "ns/a", Phase: "Running"}, {Name: "ns/b", Phase: "Running"}},
	}))
	list := &cocoonv1beta1.NodeInventoryList{}
	require.NoError(t, cli.List(ctx, list))
	require.Len(t, list.Items, 1)
	assert.Len(t, list.Items[0].Entries, 2)
}

func TestSSAApplier_DeleteTreatsAMissingInventoryAsDone(t *testing.T) {
	ctx := t.Context()
	scheme := runtime.NewScheme()
	require.NoError(t, cocoonv1beta1.AddToScheme(scheme))
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	applier := NewSSAApplier(cli, "vk-test")
	require.NoError(t, applier.Apply(ctx, &scale.NodeInventory{
		Kind: scale.NodeInventoryGVK.Kind, APIVersion: scale.NodeInventoryGVK.GroupVersion().String(), Name: "n1", Node: "n1",
	}))

	require.NoError(t, applier.Delete(ctx, "n1"))
	err := cli.Get(ctx, client.ObjectKey{Name: "n1"}, &cocoonv1beta1.NodeInventory{})
	assert.True(t, k8serrors.IsNotFound(err), "inventory after delete: %v", err)
	require.NoError(t, applier.Delete(ctx, "n1"), "a missing inventory is success")
}
