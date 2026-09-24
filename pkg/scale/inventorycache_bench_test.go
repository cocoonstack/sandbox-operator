package scale

import (
	"context"
	"errors"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	restclient "k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// errWatchListUnserved sends the reflector down the LIST/WATCH path, which the fake ListWatch serves.
var errWatchListUnserved = errors.New("watch-list is not served")

func BenchmarkClientInventoryWarmCandidates(b *testing.B) {
	for _, fleet := range benchFleets {
		for _, arm := range []struct {
			name   string
			noCopy bool
		}{{"copy", false}, {"nocopy", true}} {
			b.Run(fleet.name+"/"+arm.name, func(b *testing.B) {
				store, pool := benchCachedStore(b, fleet.nodes, fleet.perNode, arm.noCopy)
				ctx := b.Context()
				b.ReportAllocs()
				for b.Loop() {
					candidates, err := store.warmCandidates(ctx, pool)
					if err != nil {
						b.Fatalf("warm candidates: %v", err)
					}
					if len(candidates) != fleet.nodes {
						b.Fatalf("got %d candidates, want %d", len(candidates), fleet.nodes)
					}
				}
			})
		}
	}
}

// benchCachedStore serves the fleet through a real informer-fed cache reader, the production read path.
func benchCachedStore(b *testing.B, nodes, perNode int, noCopy bool) (*scatterGatherStore, PoolKey) {
	b.Helper()
	invs, pool := benchInventories(nodes, perNode)
	list := &unstructured.UnstructuredList{}
	for _, inv := range invs {
		raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(inv)
		if err != nil {
			b.Fatalf("encode node inventory: %v", err)
		}
		u := unstructured.Unstructured{Object: raw}
		u.SetGroupVersionKind(NodeInventoryGVK)
		list.Items = append(list.Items, u)
	}
	inv := &unstructured.Unstructured{}
	inv.SetGroupVersionKind(NodeInventoryGVK)
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{NodeInventoryGVK.GroupVersion()})
	mapper.Add(NodeInventoryGVK, meta.RESTScopeRoot)
	invCache, err := cache.New(&restclient.Config{Host: "http://127.0.0.1:1"}, cache.Options{
		Mapper:   mapper,
		ByObject: map[client.Object]cache.ByObject{inv: {UnsafeDisableDeepCopy: &noCopy}},
		NewInformer: func(_ toolscache.ListerWatcher, obj runtime.Object, resync time.Duration, indexers toolscache.Indexers) toolscache.SharedIndexInformer {
			return toolscache.NewSharedIndexInformer(&toolscache.ListWatch{
				ListWithContextFunc: func(context.Context, metav1.ListOptions) (runtime.Object, error) { return list, nil },
				WatchFuncWithContext: func(_ context.Context, opts metav1.ListOptions) (watch.Interface, error) {
					if opts.SendInitialEvents != nil && *opts.SendInitialEvents {
						return nil, errWatchListUnserved
					}
					return watch.NewFake(), nil
				},
			}, obj, resync, indexers)
		},
	})
	if err != nil {
		b.Fatalf("build inventory cache: %v", err)
	}
	ctx, cancel := context.WithCancel(b.Context())
	b.Cleanup(cancel)
	if _, err := invCache.GetInformer(ctx, inv); err != nil {
		b.Fatalf("register node inventory informer: %v", err)
	}
	go func() { _ = invCache.Start(ctx) }()
	syncCtx, cancelSync := context.WithTimeout(ctx, inventoryCacheSyncTimeout)
	defer cancelSync()
	if !invCache.WaitForCacheSync(syncCtx) {
		b.Fatal("node inventory cache did not sync")
	}
	return NewScatterGatherStore(NewClientInventorySource(invCache)).(*scatterGatherStore), pool
}
