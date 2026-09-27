package kubeinventory

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cocoonv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

func TestSourceDropsAnInventoryPastTheWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		src := New(fakeReader(t, inventory("fresh", metav1.Now()), inventory("unstamped", metav1.Time{})), Options{})

		time.Sleep(90 * time.Second)
		nodes, err := src.ListNodes(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{"fresh", "unstamped"}, nodes)
		_, err = src.NodeInventory(ctx, "fresh")
		require.NoError(t, err)
		addr, pools, err := src.NodeCapacity(ctx, "fresh")
		require.NoError(t, err)
		assert.Equal(t, "fresh:7777", addr)
		assert.Len(t, pools, 1)

		time.Sleep(500 * time.Millisecond)
		nodes, err = src.ListNodes(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{"unstamped"}, nodes)
		_, err = src.NodeInventory(ctx, "fresh")
		assert.True(t, k8serrors.IsNotFound(err), "NodeInventory on a stale node: %v", err)
		_, _, err = src.NodeCapacity(ctx, "fresh")
		assert.True(t, k8serrors.IsNotFound(err), "NodeCapacity on a stale node: %v", err)

		time.Sleep(time.Hour)
		_, _, err = src.NodeCapacity(ctx, "unstamped")
		require.NoError(t, err, "an inventory without publishedAt never goes stale")
	})
}

func TestSourceHonorsStaleAfter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := New(fakeReader(t, inventory("n1", metav1.Now())), Options{StaleAfter: 10 * time.Second})
		time.Sleep(11 * time.Second)
		nodes, err := src.ListNodes(t.Context())
		require.NoError(t, err)
		assert.Empty(t, nodes)
	})
}

func fakeReader(t *testing.T, objs ...client.Object) client.Reader {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, cocoonv1beta1.AddToScheme(scheme))
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

func inventory(node string, publishedAt metav1.Time) *cocoonv1beta1.NodeInventory {
	return &cocoonv1beta1.NodeInventory{
		Name:        node,
		Node:        node,
		Address:     node + ":7777",
		Pools:       []scale.PoolCapacity{{Template: "rt", Warm: 1, Target: 1}},
		PublishedAt: publishedAt,
	}
}
