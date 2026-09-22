package envdproxy

import (
	"context"
	"errors"
	"fmt"
	"strings"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/cocoonstack/sandbox-operator/pkg/e2bcompat"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

// ErrSandboxNotFound is what a Resolver returns for an id it cannot place. The
// proxy answers it like any other failure — a caller must not be able to probe
// which sandbox ids exist — but only this one is expected, so the rest are logged.
var ErrSandboxNotFound = errors.New("envdproxy: sandbox not found")

// Owner is where a sandbox's data plane lives: the node-local claim id and its
// sandboxd's internal address. The address is the node's advertise_addr, never
// client_advertise — the edge reaches nodes over the private network, and a
// client must not learn either.
type Owner struct {
	ClaimID string
	Address string
}

// Resolver locates the sandboxd that owns a published sandbox id.
type Resolver interface {
	Owner(ctx context.Context, sandboxID string) (Owner, error)
}

// storeResolver answers from the same cache-fed node inventories the aggregated
// apiserver reads, so a lookup costs no round trip to the kube-apiserver.
type storeResolver struct {
	claims    scale.ClaimIDResolver
	inventory scale.InventorySource
	namespace string
}

// NewResolver builds the inventory-backed Resolver. An empty namespace matches
// every namespace.
func NewResolver(store scale.SandboxStore, inventory scale.InventorySource, namespace string) (Resolver, error) {
	claims, ok := store.(scale.ClaimIDResolver)
	if !ok {
		return nil, errors.New("envdproxy: store does not implement scale.ClaimIDResolver")
	}
	if inventory == nil {
		return nil, errors.New("envdproxy: inventory source is required")
	}
	return &storeResolver{claims: claims, inventory: inventory, namespace: namespace}, nil
}

func (s *storeResolver) Owner(ctx context.Context, sandboxID string) (Owner, error) {
	if strings.TrimSpace(sandboxID) == "" {
		return Owner{}, ErrSandboxNotFound
	}
	sb, err := s.claims.GetByClaimID(ctx, s.namespace, sandboxID, func(claimID string) bool {
		return e2bcompat.MatchesID(claimID, sandboxID)
	})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return Owner{}, ErrSandboxNotFound
		}
		return Owner{}, err
	}
	node := sb.Status.NodeName
	if node == "" {
		return Owner{}, ErrSandboxNotFound
	}
	address, _, err := s.inventory.NodeCapacity(ctx, node)
	if err != nil {
		return Owner{}, fmt.Errorf("envdproxy: resolve node %q: %w", node, err)
	}
	if address == "" {
		return Owner{}, fmt.Errorf("envdproxy: node %q publishes no address", node)
	}
	return Owner{ClaimID: sb.Annotations[scale.ClaimIDAnnotation], Address: address}, nil
}
