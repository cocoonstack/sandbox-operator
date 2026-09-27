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

func TestSourceDropsADeadNodeAgainstTheNewestPublish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		c := fakeClient(t, inventory("live", metav1.Now()), inventory("dead", metav1.Now()), inventory("unstamped", metav1.Time{}))
		src := New(c, Options{})

		time.Sleep(90 * time.Second)
		republish(t, c, "live")
		nodes, err := src.ListNodes(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{"dead", "live", "unstamped"}, nodes)

		time.Sleep(time.Second)
		republish(t, c, "live")
		nodes, err = src.ListNodes(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{"live", "unstamped"}, nodes)
		_, err = src.NodeInventory(ctx, "dead")
		assert.True(t, k8serrors.IsNotFound(err), "NodeInventory on a stale node: %v", err)
		_, _, err = src.NodeCapacity(ctx, "dead")
		assert.True(t, k8serrors.IsNotFound(err), "NodeCapacity on a stale node: %v", err)
		addr, pools, err := src.NodeCapacity(ctx, "live")
		require.NoError(t, err)
		assert.Equal(t, "live:7777", addr)
		assert.Len(t, pools, 1)
	})
}

func TestSourceKeepsEveryNodeThroughAPublishOutage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		src := New(fakeClient(t, inventory("a", metav1.Now()), inventory("b", metav1.Now())), Options{})
		time.Sleep(10 * time.Minute)
		nodes, err := src.ListNodes(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{"a", "b"}, nodes)
		_, err = src.NodeInventory(ctx, "a")
		require.NoError(t, err, "a frozen fleet still answers lookups")
	})
}

func TestSourceCapsTheReferenceAtTheClock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ahead := metav1.NewTime(time.Now().Add(time.Hour))
		src := New(fakeClient(t, inventory("rogue", ahead), inventory("a", metav1.Now()), inventory("b", metav1.Now())), Options{})
		nodes, err := src.ListNodes(t.Context())
		require.NoError(t, err)
		assert.Equal(t, []string{"a", "b", "rogue"}, nodes, "a stamp ahead of the clock must not evict the others")
	})
}

func TestSourceLookupBeforeAnyListCountsFresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := New(fakeClient(t, inventory("a", metav1.Now())), Options{})
		time.Sleep(time.Hour)
		_, _, err := src.NodeCapacity(t.Context(), "a")
		require.NoError(t, err)
	})
}

func TestSourceHonorsStaleAfter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := fakeClient(t, inventory("live", metav1.Now()), inventory("dead", metav1.Now()))
		src := New(c, Options{StaleAfter: 10 * time.Second})
		time.Sleep(11 * time.Second)
		republish(t, c, "live")
		nodes, err := src.ListNodes(t.Context())
		require.NoError(t, err)
		assert.Equal(t, []string{"live"}, nodes)
	})
}

func TestPublishedAtTreatsAMissingOrUnparsableStampAsUnset(t *testing.T) {
	for _, obj := range []map[string]any{{}, {"publishedAt": nil}, {"publishedAt": "not a time"}} {
		assert.Zero(t, publishedAt(obj), "%v", obj)
	}
	assert.Equal(t, time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC).UnixNano(), publishedAt(map[string]any{"publishedAt": "2026-09-27T09:00:00Z"}))
}

func fakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, cocoonv1beta1.AddToScheme(scheme))
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

func republish(t *testing.T, c client.Client, node string) {
	t.Helper()
	inv := &cocoonv1beta1.NodeInventory{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Name: node}, inv))
	inv.PublishedAt = metav1.Now()
	require.NoError(t, c.Update(t.Context(), inv))
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
