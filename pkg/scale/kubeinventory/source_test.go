package kubeinventory

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	restclient "k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"

	cocoonv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

func TestSourceDropsADeadNodeAgainstTheNewestPublish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		src, w := fleet(t, Options{}, inventory("live", metav1.Now()), inventory("dead", metav1.Now()))

		time.Sleep(90 * time.Second)
		w.publish("live", metav1.Now())
		nodes, err := src.ListNodes(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{"dead", "live"}, nodes)

		time.Sleep(time.Second)
		w.publish("live", metav1.Now())
		nodes, err = src.ListNodes(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{"live"}, nodes)
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
		src, _ := fleet(t, Options{}, inventory("a", metav1.Now()), inventory("b", metav1.Now()))
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
		src, _ := fleet(t, Options{}, inventory("rogue", ahead), inventory("a", metav1.Now()), inventory("b", metav1.Now()))
		nodes, err := src.ListNodes(t.Context())
		require.NoError(t, err)
		assert.Equal(t, []string{"a", "b", "rogue"}, nodes, "a stamp ahead of the clock must not evict the others")
	})
}

func TestSourceLookupBeforeAnyListCountsFresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src, _ := fleet(t, Options{}, inventory("a", metav1.Now()))
		time.Sleep(time.Hour)
		_, _, err := src.NodeCapacity(t.Context(), "a")
		require.NoError(t, err)
	})
}

func TestSourceHonorsStaleAfter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src, w := fleet(t, Options{StaleAfter: 10 * time.Second}, inventory("live", metav1.Now()), inventory("dead", metav1.Now()))
		time.Sleep(11 * time.Second)
		w.publish("live", metav1.Now())
		nodes, err := src.ListNodes(t.Context())
		require.NoError(t, err)
		assert.Equal(t, []string{"live"}, nodes)
	})
}

func TestSourceSnapshotFollowsAddUpdateAndDelete(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		src, w := fleet(t, Options{}, inventory("a", metav1.Now()))

		w.apply(watch.Added, inventory("b", metav1.Now()))
		nodes, err := src.ListNodes(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{"a", "b"}, nodes)

		moved := inventory("b", metav1.Now())
		moved.Address = "b:8888"
		w.apply(watch.Modified, moved)
		addr, _, err := src.NodeCapacity(ctx, "b")
		require.NoError(t, err)
		assert.Equal(t, "b:8888", addr)

		w.apply(watch.Deleted, moved)
		nodes, err = src.ListNodes(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{"a"}, nodes)
		_, err = src.NodeInventory(ctx, "b")
		assert.True(t, k8serrors.IsNotFound(err), "NodeInventory on a deleted node: %v", err)

		src.drop(toolscache.DeletedFinalStateUnknown{Key: "a", Obj: inventory("a", metav1.Now())})
		nodes, err = src.ListNodes(ctx)
		require.NoError(t, err)
		assert.Empty(t, nodes, "a tombstone deletes its node")
	})
}

func TestSourceReadsStayConsistentWhileTheSnapshotRebuilds(t *testing.T) {
	src, w := fleet(t, Options{}, inventory("a", metav1.Now()), inventory("b", metav1.Now()))
	ctx := t.Context()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				nodes, err := src.ListNodes(ctx)
				assert.NoError(t, err)
				for _, node := range nodes {
					if _, pools, err := src.NodeCapacity(ctx, node); err == nil {
						assert.Len(t, pools, 1)
					}
				}
			}
		})
	}
	for range 200 {
		w.publish("a", metav1.Now())
		w.publish("b", metav1.Now())
	}
	close(stop)
	wg.Wait()
}

func TestNodeCapacitiesMatchesTheListAndPerNodeLookups(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		src, w := fleet(t, Options{}, inventory("live", metav1.Now()), inventory("dead", metav1.Now()))
		fresh, err := src.NodeCapacities(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{"dead", "live"}, nodeNames(fresh))
		time.Sleep(91 * time.Second)
		w.publish("live", metav1.Now())
		got, err := src.NodeCapacities(ctx)
		require.NoError(t, err)
		nodes, err := src.ListNodes(ctx)
		require.NoError(t, err)
		want := make([]scale.NodePools, 0, len(nodes))
		for _, n := range nodes {
			addr, pools, err := src.NodeCapacity(ctx, n)
			require.NoError(t, err)
			want = append(want, scale.NodePools{Node: n, Address: addr, Pools: pools})
		}
		assert.Equal(t, want, got)
		assert.Equal(t, []string{"live"}, nodes)
	})
}

func TestPublishedAtTreatsAZeroStampAsUnset(t *testing.T) {
	assert.Zero(t, publishedAt(inventory("a", metav1.Time{})))
	at := time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC)
	assert.Equal(t, at.UnixNano(), publishedAt(inventory("a", metav1.NewTime(at))))
}

type fakeWatch struct {
	t     *testing.T
	w     *watch.FakeWatcher
	objs  map[string]*cocoonv1beta1.NodeInventory
	src   *Source
	count int
}

func fleet(t *testing.T, opts Options, objs ...*cocoonv1beta1.NodeInventory) (*Source, *fakeWatch) {
	t.Helper()
	fw := &fakeWatch{t: t, w: watch.NewFake(), objs: map[string]*cocoonv1beta1.NodeInventory{}}
	list := &cocoonv1beta1.NodeInventoryList{ResourceVersion: "1"}
	for _, obj := range objs {
		fw.objs[obj.Name] = obj
		list.Items = append(list.Items, *obj)
	}
	scheme := runtime.NewScheme()
	require.NoError(t, cocoonv1beta1.AddToScheme(scheme))
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{scale.NodeInventoryGVK.GroupVersion()})
	mapper.Add(scale.NodeInventoryGVK, meta.RESTScopeRoot)
	var watched bool
	c, err := cache.New(&restclient.Config{Host: "http://127.0.0.1:1"}, cache.Options{
		Scheme: scheme,
		Mapper: mapper,
		NewInformer: func(_ toolscache.ListerWatcher, obj runtime.Object, resync time.Duration, indexers toolscache.Indexers) toolscache.SharedIndexInformer {
			return toolscache.NewSharedIndexInformer(&toolscache.ListWatch{
				ListWithContextFunc: func(context.Context, metav1.ListOptions) (runtime.Object, error) { return list, nil },
				WatchFuncWithContext: func(_ context.Context, opts metav1.ListOptions) (watch.Interface, error) {
					if opts.SendInitialEvents != nil && *opts.SendInitialEvents {
						return nil, k8serrors.NewBadRequest("watch-list is not served")
					}
					if watched {
						return watch.NewFake(), nil
					}
					watched = true
					return fw.w, nil
				},
			}, obj, resync, indexers)
		},
	})
	require.NoError(t, err)
	ctx := t.Context()
	_, err = c.GetInformer(ctx, &cocoonv1beta1.NodeInventory{})
	require.NoError(t, err)
	go func() { _ = c.Start(ctx) }()
	require.True(t, c.WaitForCacheSync(ctx))
	fw.src, err = New(ctx, c, opts)
	require.NoError(t, err)
	return fw.src, fw
}

func (f *fakeWatch) publish(node string, at metav1.Time) {
	next := f.objs[node].DeepCopy()
	next.PublishedAt = at.Rfc3339Copy()
	f.apply(watch.Modified, next)
}

func (f *fakeWatch) apply(kind watch.EventType, obj *cocoonv1beta1.NodeInventory) {
	f.t.Helper()
	f.count++
	obj = obj.DeepCopy()
	obj.ResourceVersion = "rv" + string(rune('a'+f.count%26)) + time.Now().Format("150405.000000000")
	f.objs[obj.Name] = obj
	before := f.src.snap.Load()
	f.w.Action(kind, obj)
	require.Eventually(f.t, func() bool { return f.src.snap.Load() != before }, 5*time.Second, time.Millisecond)
}

func inventory(node string, publishedAt metav1.Time) *cocoonv1beta1.NodeInventory {
	return &cocoonv1beta1.NodeInventory{
		Name:        node,
		Node:        node,
		Address:     node + ":7777",
		Pools:       []scale.PoolCapacity{{Template: "rt", Warm: 1, Target: 1}},
		PublishedAt: publishedAt.Rfc3339Copy(),
	}
}

func nodeNames(caps []scale.NodePools) []string {
	names := make([]string, len(caps))
	for i, c := range caps {
		names[i] = c.Node
	}
	return names
}
