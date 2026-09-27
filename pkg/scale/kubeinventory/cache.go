package kubeinventory

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	restclient "k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cocoonv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

// cacheSyncTimeout bounds the startup wait for the informer, so a missing CRD fails loud instead of serving an empty fleet.
const cacheSyncTimeout = 2 * time.Minute

// NewCache starts a cache scoped to NodeInventory alone and waits for it to sync; a read of any other kind fails instead of starting a cluster-wide informer.
func NewCache(ctx context.Context, restCfg *restclient.Config) (cache.Cache, error) {
	scheme := runtime.NewScheme()
	if err := cocoonv1beta1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("kubeinventory: register node inventory scheme: %w", err)
	}
	inv := &scale.NodeInventory{}
	invCache, err := cache.New(restCfg, cache.Options{
		Scheme:                      scheme,
		ByObject:                    map[client.Object]cache.ByObject{inv: {UnsafeDisableDeepCopy: new(true)}},
		ReaderFailOnMissingInformer: true,
	})
	if err != nil {
		return nil, fmt.Errorf("kubeinventory: build inventory cache: %w", err)
	}
	if _, err := invCache.GetInformer(ctx, inv); err != nil {
		return nil, fmt.Errorf("kubeinventory: register node inventory informer: %w", err)
	}
	cacheErr := make(chan error, 1)
	go func() { cacheErr <- invCache.Start(ctx) }()
	syncCtx, cancel := context.WithTimeout(ctx, cacheSyncTimeout)
	defer cancel()
	if !invCache.WaitForCacheSync(syncCtx) {
		select {
		case err := <-cacheErr:
			if err != nil {
				return nil, fmt.Errorf("kubeinventory: run inventory cache: %w", err)
			}
		default:
		}
		return nil, fmt.Errorf("kubeinventory: node inventory cache did not sync within %s (is the %s CRD installed?)",
			cacheSyncTimeout, scale.NodeInventoryGVK.GroupKind())
	}
	return invCache, nil
}
