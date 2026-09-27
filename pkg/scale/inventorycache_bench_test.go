package scale_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	restclient "k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cocoonv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
	"github.com/cocoonstack/sandbox-operator/pkg/scale/kubeinventory"
)

const cacheSyncTimeout = 2 * time.Minute

// errWatchListUnserved sends the reflector down the LIST/WATCH path, which the fake ListWatch serves.
var errWatchListUnserved = errors.New("watch-list is not served")

func BenchmarkClientInventoryWarmCandidates(b *testing.B) {
	for _, fleet := range scale.BenchFleets {
		for _, arm := range []struct {
			name   string
			noCopy bool
		}{{"copy", false}, {"nocopy", true}} {
			b.Run(fleet.Name+"/"+arm.name, func(b *testing.B) {
				store, pool := benchCachedStore(b, fleet.Nodes, fleet.PerNode, arm.noCopy)
				ctx := b.Context()
				b.ReportAllocs()
				for b.Loop() {
					candidates, err := scale.WarmCandidates(store, ctx, pool)
					if err != nil {
						b.Fatalf("warm candidates: %v", err)
					}
					if len(candidates) != fleet.Nodes {
						b.Fatalf("got %d candidates, want %d", len(candidates), fleet.Nodes)
					}
				}
			})
		}
	}
}

func BenchmarkClientInventoryWatchTick(b *testing.B) {
	for _, fleet := range scale.BenchFleets {
		b.Run(fleet.Name, func(b *testing.B) {
			store, _ := benchCachedStore(b, fleet.Nodes, fleet.PerNode, true)
			ns, name := scale.SplitNamespacedName(fmt.Sprintf("default/sb-%s-%d", scale.BenchNodeName(fleet.Nodes-1), fleet.PerNode-1))
			labelSel, fieldSel, err := scale.ParseSelectors(scale.ListOptions{Namespace: ns, FieldSelector: "metadata.name=" + name})
			if err != nil {
				b.Fatalf("parse selectors: %v", err)
			}
			held, err := scale.LookupName(store, b.Context(), ns, name)
			if err != nil || held == nil {
				b.Fatalf("look up %s/%s: %v", ns, name, err)
			}
			for _, arm := range []struct {
				name string
				poll func(context.Context) ([]sandboxv1beta1.Sandbox, error)
			}{
				{"fleet", func(ctx context.Context) ([]sandboxv1beta1.Sandbox, error) {
					return scale.ListInventories(store, ctx, ns, labelSel, fieldSel)
				}},
				{"pinned", func(ctx context.Context) ([]sandboxv1beta1.Sandbox, error) {
					return scale.PollPinned(store, ctx, ns, name, held, labelSel, fieldSel)
				}},
			} {
				b.Run(arm.name, func(b *testing.B) {
					ctx := b.Context()
					b.ReportAllocs()
					for b.Loop() {
						if items, err := arm.poll(ctx); err != nil || len(items) != 1 {
							b.Fatalf("poll: %d items, %v", len(items), err)
						}
					}
				})
			}
		})
	}
}

// benchCachedStore serves the fleet through a real informer-fed cache reader, the production read path.
func benchCachedStore(b *testing.B, nodes, perNode int, noCopy bool) (*scale.ScatterGatherStore, scale.PoolKey) {
	b.Helper()
	invs, pool := scale.BenchInventories(nodes, perNode, 0)
	list := &cocoonv1beta1.NodeInventoryList{}
	for _, inv := range invs {
		inv.PublishedAt = metav1.Now().Rfc3339Copy()
		list.Items = append(list.Items, *inv)
	}
	scheme := runtime.NewScheme()
	if err := cocoonv1beta1.AddToScheme(scheme); err != nil {
		b.Fatalf("register node inventory scheme: %v", err)
	}
	inv := &cocoonv1beta1.NodeInventory{}
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{scale.NodeInventoryGVK.GroupVersion()})
	mapper.Add(scale.NodeInventoryGVK, meta.RESTScopeRoot)
	invCache, err := cache.New(&restclient.Config{Host: "http://127.0.0.1:1"}, cache.Options{
		Scheme:   scheme,
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
	syncCtx, cancelSync := context.WithTimeout(ctx, cacheSyncTimeout)
	defer cancelSync()
	if !invCache.WaitForCacheSync(syncCtx) {
		b.Fatal("node inventory cache did not sync")
	}
	src, err := kubeinventory.New(ctx, invCache, kubeinventory.Options{})
	if err != nil {
		b.Fatalf("build inventory source: %v", err)
	}
	return scale.NewScatterGatherStore(src).(*scale.ScatterGatherStore), pool
}
