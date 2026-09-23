package scale

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	restclient "k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// inventoryCacheSyncTimeout bounds the startup wait for the informer, so a missing CRD fails loud instead of serving an empty fleet.
const inventoryCacheSyncTimeout = 2 * time.Minute

// NewInventoryCache starts a cache scoped to NodeInventory alone and waits for it to sync; a read of any other kind fails instead of starting a cluster-wide informer.
func NewInventoryCache(ctx context.Context, restCfg *restclient.Config) (cache.Cache, error) {
	inv := &unstructured.Unstructured{}
	inv.SetGroupVersionKind(NodeInventoryGVK)
	invCache, err := cache.New(restCfg, cache.Options{
		ByObject:                    map[client.Object]cache.ByObject{inv: {UnsafeDisableDeepCopy: new(true)}},
		ReaderFailOnMissingInformer: true,
	})
	if err != nil {
		return nil, fmt.Errorf("scale: build inventory cache: %w", err)
	}
	if _, err := invCache.GetInformer(ctx, inv); err != nil {
		return nil, fmt.Errorf("scale: register node inventory informer: %w", err)
	}
	cacheErr := make(chan error, 1)
	go func() { cacheErr <- invCache.Start(ctx) }()
	syncCtx, cancel := context.WithTimeout(ctx, inventoryCacheSyncTimeout)
	defer cancel()
	if !invCache.WaitForCacheSync(syncCtx) {
		select {
		case err := <-cacheErr:
			if err != nil {
				return nil, fmt.Errorf("scale: run inventory cache: %w", err)
			}
		default:
		}
		return nil, fmt.Errorf("scale: node inventory cache did not sync within %s (is the %s CRD installed?)",
			inventoryCacheSyncTimeout, NodeInventoryGVK.GroupKind())
	}
	return invCache, nil
}
