package scale

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
)

func TestStoreClaim_RoutesToAWarmNode(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("n2", "10.0.0.2:7777", PoolCapacity{Template: "img", Warm: 4, Target: 5}))
	deadline := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	f := &recordingFactory{claimResult: sandboxd.ClaimResult{ID: "sb-abc", Token: "sbtok", OwnerAddr: "10.0.0.2:9000", Deadline: deadline}}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("uniform-token", f.factory()))

	a, err := store.Claim(t.Context(), "ns", "s1", PoolKey{Template: "img"}, 600)
	require.NoError(t, err)
	assert.Equal(t, "n2", a.Node)
	assert.Equal(t, "sb-abc", a.SandboxName)
	assert.Equal(t, "10.0.0.2:9000", a.Address)

	assert.Equal(t, "10.0.0.2:7777", f.builtAddr)
	assert.Equal(t, "uniform-token", f.builtToken)
	assert.Equal(t, "img", f.claimSpec.Template)
	assert.Equal(t, 600, f.claimSpec.TTLSeconds, "the caller's TTL must reach sandboxd")
	assert.Equal(t, deadline, a.Deadline, "the node-granted deadline must ride the assignment back")

	assert.Equal(t, "ns/s1", f.claimSpec.ClaimRef)
	assert.Equal(t, 1, f.claimCalls)
}

func TestStoreClaim_NoWarmCapacityIsRetryable(t *testing.T) {
	src := NewStaticInventorySource()

	src.Put(poolInv("n1", "10.0.0.1:7777", PoolCapacity{Template: "img", Warm: 0, Target: 5}))
	f := &recordingFactory{}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("t", f.factory()))

	_, err := store.Claim(t.Context(), "ns", "s1", PoolKey{Template: "img"}, 0)
	require.Error(t, err)
	assert.True(t, IsNoWarmCapacity(err), "want ErrNoWarmCapacity, got %v", err)
	assert.Equal(t, 0, f.claimCalls, "must not call sandboxd when no node is warm")
}

func TestStoreClaim_PoolKeyMatchingNormalizesDefaults(t *testing.T) {
	src := NewStaticInventorySource()

	src.Put(poolInv("n1", "10.0.0.1:7777", PoolCapacity{Template: "img", Warm: 2, Target: 2}))
	f := &recordingFactory{claimResult: sandboxd.ClaimResult{ID: "sb-1"}}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("t", f.factory()))

	_, err := store.Claim(t.Context(), "ns", "s1", PoolKey{Template: "img", Net: "none", Size: "small"}, 0)
	require.NoError(t, err)

	_, err = store.Claim(t.Context(), "ns", "s2", PoolKey{Template: "img", Net: "egress"}, 0)
	require.Error(t, err)
	assert.True(t, IsNoWarmCapacity(err), "net mismatch must be no-capacity, got %v", err)
}

func TestStoreClaim_SandboxdCapacityRaceIsRetryable(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("n1", "10.0.0.1:7777", PoolCapacity{Template: "img", Warm: 1, Target: 5}))

	f := &recordingFactory{claimErr: sandboxd.ErrNodeAtCapacity}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("t", f.factory()))

	_, err := store.Claim(t.Context(), "ns", "s1", PoolKey{Template: "img"}, 0)
	require.Error(t, err)
	assert.True(t, IsNoWarmCapacity(err), "sandboxd 429 must map to no-capacity, got %v", err)
}

func TestStoreRelease_RoutesToNodeAddressWithUniformToken(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("n2", "10.0.0.2:7777", PoolCapacity{Template: "img", Warm: 3, Target: 5}))
	f := &recordingFactory{}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("uniform-token", f.factory()))

	err := store.Release(t.Context(), "n2", "sb-abc")
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.2:7777", f.builtAddr, "release must route to the node's advertise address")
	assert.Equal(t, "sb-abc", f.releaseID)
	assert.Equal(t, "uniform-token", f.releaseToken, "release authenticates with the uniform token")
	assert.Equal(t, 1, f.releaseCalls)
}

func TestStoreClaimRelease_FailClosedWithoutRouting(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("n1", "10.0.0.1:7777", PoolCapacity{Template: "img", Warm: 1, Target: 1}))
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()))

	_, err := store.Claim(t.Context(), "ns", "s1", PoolKey{Template: "img"}, 0)
	require.Error(t, err)
	assert.False(t, IsNoWarmCapacity(err), "unconfigured routing is a config error, not no-capacity")

	require.Error(t, store.Release(t.Context(), "n1", "sb-1"))
}

func TestGetKeepsTheClaimTimeHintThroughThePublishLag(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("n1", "n1:7777", PoolCapacity{Template: "img", Warm: 2, Target: 2}))
	src.Put(poolInv("n2", "n2:7777", PoolCapacity{Template: "img", Warm: 2, Target: 2}))
	f := &recordingFactory{claimResult: sandboxd.ClaimResult{ID: "sb_1", Token: "tok", OwnerAddr: "10.0.0.1:7777"}}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("t", f.factory()))

	a, err := store.Claim(t.Context(), "ns", "s1", PoolKey{Template: "img"}, 0)
	require.NoError(t, err)

	_, err = store.Get(t.Context(), "ns", "s1")
	require.True(t, k8serrors.IsNotFound(err), "a claim neither published nor listed by its node is not readable: %v", err)

	node, hinted := store.index.lookup(nameKey("ns", "s1"))
	require.True(t, hinted, "the claim-time hint must survive a Get during the publish lag")
	assert.Equal(t, a.Node, node)

	src.Put(&NodeInventory{
		Name: a.Node, Node: a.Node, Address: a.Node + ":7777",
		Entries: []InventoryEntry{{Name: "ns/s1", ID: "sb_1", Phase: "Running"}},
	})
	src.Partition(otherNode(a.Node))
	got, err := store.Get(t.Context(), "ns", "s1")
	require.NoError(t, err)
	assert.Equal(t, a.Node, got.Status.NodeName)
}

func TestPickWarmNodeSpreadsAcrossTheFleet(t *testing.T) {
	src := NewStaticInventorySource()
	for _, n := range []struct {
		node string
		warm int
	}{{"n1", 4}, {"n2", 4}, {"n3", 4}, {"n4", 4}} {
		src.Put(poolInv(n.node, n.node+":7777", PoolCapacity{Template: "img", Warm: n.warm, Target: 5}))
	}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()))

	picked := map[string]int{}
	for range 200 {
		candidates, err := store.warmCandidates(t.Context(), PoolKey{Template: "img"})
		require.NoError(t, err)
		best, _ := pickPowerOfTwo(candidates)
		picked[best.node]++
	}

	assert.Len(t, picked, 4, "burst funneled onto a subset: %v", picked)
}

func TestPickWarmNodePrefersTheWarmerSample(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("cold", "cold:7777", PoolCapacity{Template: "img", Warm: 1, Target: 5}))
	src.Put(poolInv("warm", "warm:7777", PoolCapacity{Template: "img", Warm: 100, Target: 200}))
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()))

	warmPicks := 0
	for range 200 {
		candidates, err := store.warmCandidates(t.Context(), PoolKey{Template: "img"})
		require.NoError(t, err)
		best, _ := pickPowerOfTwo(candidates)
		if best.node == "warm" {
			warmPicks++
		}
	}

	assert.Greater(t, warmPicks, 130, "sampling lost its bias toward warm capacity")
}

func TestStoreClaimFallsBackWhenTheSampledNodeRacedToZero(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("stale", "stale:7777", PoolCapacity{Template: "img", Warm: 1, Target: 5}))
	src.Put(poolInv("warm", "warm:7777", PoolCapacity{Template: "img", Warm: 100, Target: 200}))

	f := &raceFactory{emptyAddr: "stale:7777", result: sandboxd.ClaimResult{ID: "sb-ok", Token: "tok"}}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("t", f.factory()))

	for range 40 {
		a, err := store.Claim(t.Context(), "ns", "s", PoolKey{Template: "img"}, 0)
		require.NoError(t, err, "a warm node was available but the claim reported no capacity")
		assert.Equal(t, "warm", a.Node)
	}
}

func TestStoreClaimSkipsANodeThatDeliveredNothing(t *testing.T) {
	dial := &url.Error{Op: "Post", Err: &net.OpError{Op: "dial", Err: errors.New("connection refused")}}
	for _, tt := range []struct {
		name string
		err  error
	}{
		{"sandboxd down", fmt.Errorf("claim: %w", dial)},
		{"sandboxd 500", fmt.Errorf("claim: %w", &sandboxd.HTTPError{StatusCode: 500, Message: "provisioning failed"})},
	} {
		t.Run(tt.name, func(t *testing.T) {
			src := NewStaticInventorySource()
			src.Put(poolInv("dead", "dead:7777", PoolCapacity{Template: "img", Warm: 100, Target: 100}))
			src.Put(poolInv("live", "live:7777", PoolCapacity{Template: "img", Warm: 1, Target: 5}))
			f := &raceFactory{deadAddr: "dead:7777", deadErr: tt.err, result: sandboxd.ClaimResult{ID: "sb-ok", Token: "tok"}}
			store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("t", f.factory()))

			for range 20 {
				a, err := store.Claim(t.Context(), "ns", "s", PoolKey{Template: "img"}, 0)
				require.NoError(t, err, "a live warm node was in the candidate set the whole time")
				assert.Equal(t, "live", a.Node)
			}
		})
	}
}

func TestStoreClaimDoesNotRetryElsewhereAfterATimeout(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("slow", "slow:7777", PoolCapacity{Template: "img", Warm: 100, Target: 100}))
	f := &raceFactory{deadAddr: "slow:7777", deadErr: fmt.Errorf("claim: %w", context.DeadlineExceeded)}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("t", f.factory()))

	_, err := store.Claim(t.Context(), "ns", "s", PoolKey{Template: "img"}, 0)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.False(t, IsNoWarmCapacity(err), "a lost reply may have delivered a microVM; it must not read as no capacity")
	assert.Equal(t, 1, f.calls, "the claim must not be re-issued")
}

func TestStoreClaimIsRetryableWhenEveryNodeIsDown(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("n1", "n1:7777", PoolCapacity{Template: "img", Warm: 3, Target: 5}))
	f := &raceFactory{deadAddr: "n1:7777", deadErr: fmt.Errorf("claim: %w", &sandboxd.HTTPError{StatusCode: 503})}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("t", f.factory()))

	_, err := store.Claim(t.Context(), "ns", "s", PoolKey{Template: "img"}, 0)
	require.Error(t, err)
	assert.True(t, IsNoWarmCapacity(err), "no node delivered, so the caller gets the retryable signal")
}

func TestStoreClaimReportsNoCapacityOnlyWhenEveryNodeRaced(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("n1", "n1:7777", PoolCapacity{Template: "img", Warm: 3, Target: 5}))
	src.Put(poolInv("n2", "n2:7777", PoolCapacity{Template: "img", Warm: 3, Target: 5}))

	f := &raceFactory{emptyAll: true}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("t", f.factory()))

	_, err := store.Claim(t.Context(), "ns", "s", PoolKey{Template: "img"}, 0)
	require.Error(t, err)
	assert.True(t, IsNoWarmCapacity(err), "exhausting every node must stay the retryable no-capacity signal")
	assert.Equal(t, 2, f.calls, "each node must be tried exactly once")
}

type raceFactory struct {
	emptyAddr string
	emptyAll  bool
	deadAddr  string
	deadErr   error
	result    sandboxd.ClaimResult
	calls     int
}

func (r *raceFactory) factory() SandboxdClientFactory {
	return func(addr, _ string) SandboxdClient {
		return &raceClient{recordingClient: &recordingClient{}, f: r, addr: addr}
	}
}

type raceClient struct {
	*recordingClient
	f    *raceFactory
	addr string
}

func (c *raceClient) Claim(context.Context, sandboxd.ClaimSpec) (sandboxd.ClaimResult, error) {
	c.f.calls++
	if c.f.emptyAll || c.addr == c.f.emptyAddr {
		return sandboxd.ClaimResult{}, sandboxd.ErrNodeAtCapacity
	}
	if c.addr == c.f.deadAddr {
		return sandboxd.ClaimResult{}, c.f.deadErr
	}
	return c.f.result, nil
}

func otherNode(node string) string {
	if node == "n1" {
		return "n2"
	}
	return "n1"
}

func poolInv(node, addr string, pools ...PoolCapacity) *NodeInventory {
	return &NodeInventory{
		Name:    node,
		Node:    node,
		Address: addr,
		Pools:   pools,
	}
}

type recordingFactory struct {
	mu           sync.Mutex
	builtAddr    string
	builtToken   string
	claimSpec    sandboxd.ClaimSpec
	claimCalls   int
	releaseID    string
	releaseToken string
	releaseCalls int

	claimResult sandboxd.ClaimResult
	claimErr    error
	releaseErr  error
	verbErr     error

	rows     map[string][]sandboxd.SandboxSummary
	rowReads []string
	silent   string
}

func (f *recordingFactory) factory() SandboxdClientFactory {
	return func(addr, token string) SandboxdClient {
		f.mu.Lock()
		f.builtAddr, f.builtToken = addr, token
		f.mu.Unlock()
		return &recordingClient{f: f, addr: addr}
	}
}

var _ SandboxdClient = (*recordingClient)(nil)

type recordingClient struct {
	f    *recordingFactory
	addr string
}

func (c *recordingClient) Claim(_ context.Context, spec sandboxd.ClaimSpec) (sandboxd.ClaimResult, error) {
	c.f.claimCalls++
	c.f.claimSpec = spec
	if c.f.claimErr != nil {
		return sandboxd.ClaimResult{}, c.f.claimErr
	}
	return c.f.claimResult, nil
}

func (c *recordingClient) Release(_ context.Context, id, token string) error {
	c.f.releaseCalls++
	c.f.releaseID, c.f.releaseToken = id, token
	return c.f.releaseErr
}

func (c *recordingClient) Hibernate(context.Context, string) error { return c.f.verbErr }

func (c *recordingClient) Wake(context.Context, string) error { return c.f.verbErr }

func (c *recordingClient) Renew(context.Context, string, sandboxd.RenewSpec) (time.Time, error) {
	return time.Time{}, c.f.verbErr
}

func (c *recordingClient) Fork(context.Context, string, sandboxd.ForkSpec) (sandboxd.ForkResult, error) {
	return sandboxd.ForkResult{}, c.f.verbErr
}

func (c *recordingClient) Checkpoint(context.Context, string, sandboxd.CheckpointSpec) (sandboxd.Checkpoint, error) {
	return sandboxd.Checkpoint{}, c.f.verbErr
}

func (c *recordingClient) Checkpoints(context.Context) ([]sandboxd.Checkpoint, error) {
	return nil, nil
}

func (c *recordingClient) DeleteCheckpoint(context.Context, string) error { return nil }

func (c *recordingClient) Stats(context.Context, string) (sandboxd.SandboxStats, error) {
	return sandboxd.SandboxStats{}, c.f.verbErr
}

func (c *recordingClient) Sandbox(ctx context.Context, id string) (sandboxd.SandboxSummary, error) {
	if c.addr == c.f.silent {
		<-ctx.Done()
		return sandboxd.SandboxSummary{}, ctx.Err()
	}
	c.f.mu.Lock()
	defer c.f.mu.Unlock()
	c.f.rowReads = append(c.f.rowReads, c.addr)
	for _, row := range c.f.rows[c.addr] {
		if row.ID == id {
			return row, nil
		}
	}
	return sandboxd.SandboxSummary{}, &sandboxd.HTTPError{StatusCode: http.StatusNotFound}
}

func (c *recordingClient) Sandboxes(context.Context) ([]sandboxd.SandboxSummary, error) {
	c.f.mu.Lock()
	defer c.f.mu.Unlock()
	c.f.rowReads = append(c.f.rowReads, c.addr)
	return c.f.rows[c.addr], nil
}
