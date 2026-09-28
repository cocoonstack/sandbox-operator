package envdproxy

import (
	"context"
	"crypto/hmac"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/cocoonstack/sandbox-operator/pkg/e2bcompat"
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

var (
	// ErrSandboxNotFound is what a Resolver returns for an id it cannot place. The
	// proxy answers it like any other failure — a caller must not be able to probe
	// which sandbox ids exist — but only this one is expected, so the rest are logged.
	ErrSandboxNotFound = errors.New("envdproxy: sandbox not found")
	// ErrAccessDenied is a placed sandbox whose envd access token the caller got wrong.
	ErrAccessDenied = errors.New("envdproxy: access token does not match the sandbox")
)

// Owner is where a sandbox's data plane lives: the node-local claim id, its
// sandboxd's internal address and the claim token that opens its relay. The
// address is the node's advertise_addr, never client_advertise — the edge
// reaches nodes over the private network, and a client must not learn either.
type Owner struct {
	ClaimID string
	Address string
	Token   string
}

// Resolver locates the sandboxd that owns a sandbox id and admits accessToken only when it derives from that claim's token.
type Resolver interface {
	Owner(ctx context.Context, sandboxID, accessToken string) (Owner, error)
	// Locate places a sandbox without admitting a caller, for a signed file URL whose signature envd verifies itself.
	Locate(ctx context.Context, sandboxID string) (Owner, error)
}

type admission func(recentOwner) (Owner, error)

// storeResolver answers from the inventory source the apiserver reads and the owning node's own record.
type storeResolver struct {
	claims    scale.ClaimIDResolver
	lifecycle scale.SandboxLifecycle
	inventory scale.InventorySource
	namespace string
	secret    []byte

	probeLimit *rate.Limiter
	recent     *recentOwners
}

// NewResolver builds the Resolver; its claim lookups read inventory only, so an unknown id reaches the nodes
// only through the rate-limited probe, and an empty namespace matches every namespace.
func NewResolver(routed scale.SandboxLifecycle, inventory scale.InventorySource, namespace string, secret []byte) (Resolver, error) {
	if routed == nil || inventory == nil {
		return nil, errors.New("envdproxy: a routed store and an inventory source are required")
	}
	if len(secret) == 0 {
		return nil, errors.New("envdproxy: the envd secret is required to verify access tokens")
	}
	return &storeResolver{
		claims:     scale.NewScatterGatherStore(inventory).(scale.ClaimIDResolver),
		lifecycle:  routed,
		inventory:  inventory,
		namespace:  namespace,
		secret:     secret,
		probeLimit: rate.NewLimiter(probeQPS, probeBurst),
		recent:     &recentOwners{m: map[string]recentOwner{}},
	}, nil
}

func (s *storeResolver) Owner(ctx context.Context, sandboxID, accessToken string) (Owner, error) {
	return s.find(ctx, sandboxID, func(e recentOwner) (Owner, error) { return e.admit(accessToken) })
}

func (s *storeResolver) Locate(ctx context.Context, sandboxID string) (Owner, error) {
	return s.find(ctx, sandboxID, func(e recentOwner) (Owner, error) { return e.owner, nil })
}

func (s *storeResolver) find(ctx context.Context, sandboxID string, admit admission) (Owner, error) {
	if strings.TrimSpace(sandboxID) == "" {
		return Owner{}, ErrSandboxNotFound
	}
	if e, ok := s.recent.get(sandboxID, time.Now()); ok {
		return admit(e)
	}
	claimID := e2bcompat.ClaimID(sandboxID)
	sb, err := s.claims.GetByClaimID(ctx, s.namespace, claimID, func(id string) bool {
		return e2bcompat.MatchesID(id, sandboxID)
	})
	if k8serrors.IsNotFound(err) {
		return s.probe(ctx, sandboxID, claimID, admit)
	}
	if err != nil {
		return Owner{}, err
	}
	if sb.Status.NodeName == "" {
		return Owner{}, ErrSandboxNotFound
	}
	return s.readOwner(ctx, sandboxID, sb.Status.NodeName, sb.Annotations[scale.ClaimIDAnnotation], admit)
}

// probe asks every node for claimID, which its inventory does not list yet, and keeps a hit past its
// next publish; a refused admit reads as not found, so an unpublished id stays unprovable.
func (s *storeResolver) probe(ctx context.Context, sandboxID, claimID string, admit admission) (Owner, error) {
	if !s.probeLimit.Allow() {
		return Owner{}, ErrSandboxNotFound
	}
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	found, err := scale.FirstHit(ctx, s.inventory, probeConcurrency, func(ctx context.Context, node string) Owner {
		o, _ := s.ownerOn(ctx, node, claimID)
		return o
	})
	if err != nil {
		return Owner{}, err
	}
	if found == (Owner{}) {
		return Owner{}, ErrSandboxNotFound
	}
	e := s.entry(found)
	o, err := admit(e)
	if err != nil {
		return Owner{}, ErrSandboxNotFound
	}
	s.recent.put(sandboxID, e, time.Now())
	return o, nil
}

// readOwner reads the claim token on its node and caches the owner before admit judges the caller.
func (s *storeResolver) readOwner(ctx context.Context, sandboxID, node, claimID string, admit admission) (Owner, error) {
	o, err := s.ownerOn(ctx, node, claimID)
	if k8serrors.IsNotFound(err) {
		return Owner{}, ErrSandboxNotFound
	}
	if err != nil {
		return Owner{}, err
	}
	e := s.entry(o)
	s.recent.put(sandboxID, e, time.Now())
	return admit(e)
}

func (s *storeResolver) ownerOn(ctx context.Context, node, claimID string) (Owner, error) {
	address, err := s.nodeAddress(ctx, node)
	if err != nil {
		return Owner{}, err
	}
	rec, err := s.lifecycle.Read(ctx, node, claimID)
	if err != nil {
		return Owner{}, err
	}
	return Owner{ClaimID: claimID, Address: address, Token: rec.Token}, nil
}

func (s *storeResolver) entry(o Owner) recentOwner {
	return recentOwner{owner: o, access: e2bcompat.AccessToken(s.secret, o.Token)}
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
	owner  Owner
	access string
	until  time.Time
}

// admit hands out the owner only to the access token derived from its claim token.
func (e recentOwner) admit(accessToken string) (Owner, error) {
	if !hmac.Equal([]byte(e.access), []byte(accessToken)) {
		return Owner{}, ErrAccessDenied
	}
	return e.owner, nil
}

type recentOwners struct {
	mu sync.Mutex
	m  map[string]recentOwner
}

func (r *recentOwners) get(sandboxID string, now time.Time) (recentOwner, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.m[sandboxID]
	if !ok || now.After(e.until) {
		return recentOwner{}, false
	}
	return e, true
}

func (r *recentOwners) put(sandboxID string, e recentOwner, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.m) >= recentOwnerMax {
		maps.DeleteFunc(r.m, func(_ string, e recentOwner) bool { return now.After(e.until) })
		if len(r.m) >= recentOwnerMax {
			clear(r.m)
		}
	}
	e.until = now.Add(recentOwnerTTL)
	r.m[sandboxID] = e
}
