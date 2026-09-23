package envdproxy

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"sync"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/flowcontrol"

	"github.com/cocoonstack/sandbox-operator/pkg/e2bcompat"
	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

const (
	probeTimeout     = 500 * time.Millisecond
	probeConcurrency = 16
	// probeQPS and probeBurst bound what unauthenticated ids can make this proxy ask each node.
	probeQPS   = 200
	probeBurst = 400
	// recentOwnerTTL outlives the owning node's next inventory publish.
	recentOwnerTTL = time.Minute
	recentOwnerMax = 4096
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

// Resolver locates the sandboxd that owns a published sandbox id; token finds one the inventory does not list yet.
type Resolver interface {
	Owner(ctx context.Context, sandboxID, token string) (Owner, error)
}

// storeResolver answers from the same cache-fed node inventories the aggregated
// apiserver reads, so a lookup costs no round trip to the kube-apiserver.
type storeResolver struct {
	claims    scale.ClaimIDResolver
	inventory scale.InventorySource
	namespace string

	hc         *http.Client
	probeLimit flowcontrol.RateLimiter
	recent     *recentOwners
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
	return &storeResolver{
		claims:     claims,
		inventory:  inventory,
		namespace:  namespace,
		hc:         scale.NewSandboxdHTTPClient(),
		probeLimit: flowcontrol.NewTokenBucketRateLimiter(probeQPS, probeBurst),
		recent:     &recentOwners{m: map[string]recentOwner{}},
	}, nil
}

func (s *storeResolver) Owner(ctx context.Context, sandboxID, token string) (Owner, error) {
	if strings.TrimSpace(sandboxID) == "" {
		return Owner{}, ErrSandboxNotFound
	}
	if o, ok := s.recent.get(sandboxID, time.Now()); ok {
		return o, nil
	}
	claimID := e2bcompat.ClaimID(sandboxID)
	sb, err := s.claims.GetByClaimID(ctx, s.namespace, claimID, func(id string) bool {
		return e2bcompat.MatchesID(id, sandboxID)
	})
	if k8serrors.IsNotFound(err) {
		return s.probe(ctx, sandboxID, claimID, token)
	}
	if err != nil {
		return Owner{}, err
	}
	node := sb.Status.NodeName
	if node == "" {
		return Owner{}, ErrSandboxNotFound
	}
	address, err := s.nodeAddress(ctx, node)
	if err != nil {
		return Owner{}, err
	}
	return Owner{ClaimID: sb.Annotations[scale.ClaimIDAnnotation], Address: address}, nil
}

// probe asks every node whether it holds claimID under token; only the owner answers, and a hit is kept past its next publish.
func (s *storeResolver) probe(ctx context.Context, sandboxID, claimID, token string) (Owner, error) {
	if !s.probeLimit.TryAccept() {
		return Owner{}, ErrSandboxNotFound
	}
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	found, err := scale.FirstHit(ctx, s.inventory, probeConcurrency, func(ctx context.Context, node string) string {
		address, err := s.nodeAddress(ctx, node)
		if err != nil {
			return ""
		}
		if owns, err := sandboxd.New(scale.SandboxdBaseURL(address), "", sandboxd.WithHTTPClient(s.hc)).IsOwner(ctx, claimID, token); err != nil || !owns {
			return ""
		}
		return address
	})
	if err != nil {
		return Owner{}, err
	}
	if found == "" {
		return Owner{}, ErrSandboxNotFound
	}
	o := Owner{ClaimID: claimID, Address: found}
	s.recent.put(sandboxID, o, time.Now())
	return o, nil
}

func (s *storeResolver) nodeAddress(ctx context.Context, node string) (string, error) {
	address, _, err := s.inventory.NodeCapacity(ctx, node)
	if err != nil {
		return "", fmt.Errorf("envdproxy: resolve node %q: %w", node, err)
	}
	if address == "" {
		return "", fmt.Errorf("envdproxy: node %q publishes no address", node)
	}
	return address, nil
}

type recentOwner struct {
	owner Owner
	until time.Time
}

type recentOwners struct {
	mu sync.Mutex
	m  map[string]recentOwner
}

func (r *recentOwners) get(sandboxID string, now time.Time) (Owner, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.m[sandboxID]
	if !ok || now.After(e.until) {
		return Owner{}, false
	}
	return e.owner, true
}

func (r *recentOwners) put(sandboxID string, o Owner, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.m) >= recentOwnerMax {
		maps.DeleteFunc(r.m, func(_ string, e recentOwner) bool { return now.After(e.until) })
		if len(r.m) >= recentOwnerMax {
			clear(r.m)
		}
	}
	r.m[sandboxID] = recentOwner{owner: o, until: now.Add(recentOwnerTTL)}
}
