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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
)

func TestStoreClaim_RoutesToAWarmNode(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("n2", "10.0.0.2:7777", PoolCapacity{Template: "img", Warm: 4, Target: 5}))
	deadline := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	f := &recordingFactory{claimResult: sandboxd.ClaimResult{ID: "sb-abc", Token: "sbtok", OwnerAddr: "10.0.0.2:9000", Deadline: deadline, NetRoute: "relay"}}
	store := NewScatterGatherStore(src, WithClaimRouting("uniform-token", f.factory()))

	a, err := store.Claim(t.Context(), "ns", "s1", PoolKey{Template: "img"}, ClaimOptions{TTLSeconds: 600})
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
	assert.Equal(t, "relay", a.NetRoute)
	assert.Nil(t, f.claimSpec.Egress, "a claim that did not opt out keeps the pool's policy")

	_, err = store.Claim(t.Context(), "ns", "s2", PoolKey{Template: "img"}, ClaimOptions{NoEgress: true})
	require.NoError(t, err)
	require.NotNil(t, f.claimSpec.Egress)
	assert.False(t, *f.claimSpec.Egress)
}

func TestStoreClaim_ANodeAdvertisingThePromotedTemplateTakesAnUnpooledClaim(t *testing.T) {
	tpl := PromotedTemplate{Template: "ns/app", Net: "none", Size: "small", ContentDigest: "sha256:aa"}
	src := NewStaticInventorySource()
	src.Put(poolInv("n1", "10.0.0.1:7777", PoolCapacity{Template: "img", Warm: 3}))
	advertiser := poolInv("n2", "10.0.0.2:7777")
	advertiser.Templates = []PromotedTemplate{tpl}
	src.Put(advertiser)
	f := &recordingFactory{claimResult: sandboxd.ClaimResult{ID: "sb_t", Token: "tok"}}
	store := NewScatterGatherStore(src, WithClaimRouting("t", f.factory()))

	a, err := store.Claim(t.Context(), "ns", "s1", PoolKey{Template: "ns/app"}, ClaimOptions{})
	require.NoError(t, err)
	assert.Equal(t, "n2", a.Node)
	assert.True(t, f.claimSpec.RequirePromoted, "a claim routed by a promoted template asks the node for no cold boot")

	_, err = store.Claim(t.Context(), "ns", "s2", PoolKey{Template: "img"}, ClaimOptions{})
	require.NoError(t, err)
	assert.False(t, f.claimSpec.RequirePromoted, "a warm pool claim is unchanged")
	assert.Equal(t, []string{"10.0.0.2:7777", "10.0.0.1:7777"}, f.claimAddrs)

	_, err = store.Claim(t.Context(), "ns", "s3", PoolKey{Template: "ns/app", Size: "medium"}, ClaimOptions{})
	assert.True(t, IsNoWarmCapacity(err), "another size of the template is not advertised: %v", err)
}

func TestStoreClaim_AnAdvertiserThatLostTheTemplateSendsTheClaimOn(t *testing.T) {
	tpl := PromotedTemplate{Template: "ns/app", ContentDigest: "sha256:aa"}
	src := NewStaticInventorySource()
	for _, n := range []string{"n1", "n2"} {
		inv := poolInv(n, "10.0.0."+n[1:]+":7777")
		inv.Templates = []PromotedTemplate{tpl}
		src.Put(inv)
	}
	f := &recordingFactory{claimResult: sandboxd.ClaimResult{ID: "sb_t"}, claimErrAt: map[string]error{
		"10.0.0.1:7777": &sandboxd.HTTPError{StatusCode: http.StatusNotFound, Message: "unknown template"},
	}}
	store := NewScatterGatherStore(src, WithClaimRouting("t", f.factory()))

	for range 4 {
		a, err := store.Claim(t.Context(), "ns", "s1", PoolKey{Template: "ns/app"}, ClaimOptions{})
		require.NoError(t, err)
		assert.Equal(t, "n2", a.Node)
	}

	f.claimErrAt["10.0.0.2:7777"] = f.claimErrAt["10.0.0.1:7777"]
	_, err := store.Claim(t.Context(), "ns", "s1", PoolKey{Template: "ns/app"}, ClaimOptions{})
	assert.True(t, IsNoWarmCapacity(err), "every advertiser lost it: %v", err)
}

func TestStoreClaim_NoWarmCapacityIsRetryable(t *testing.T) {
	src := NewStaticInventorySource()

	src.Put(poolInv("n1", "10.0.0.1:7777", PoolCapacity{Template: "img", Warm: 0, Target: 5}))
	f := &recordingFactory{}
	store := NewScatterGatherStore(src, WithClaimRouting("t", f.factory()))

	_, err := store.Claim(t.Context(), "ns", "s1", PoolKey{Template: "img"}, ClaimOptions{})
	require.Error(t, err)
	assert.True(t, IsNoWarmCapacity(err), "want ErrNoWarmCapacity, got %v", err)
	assert.Equal(t, 0, f.claimCalls, "must not call sandboxd when no node is warm")
}

func TestStoreClaim_PoolKeyMatchingNormalizesDefaults(t *testing.T) {
	src := NewStaticInventorySource()

	src.Put(poolInv("n1", "10.0.0.1:7777", PoolCapacity{Template: "img", Warm: 2, Target: 2}))
	f := &recordingFactory{claimResult: sandboxd.ClaimResult{ID: "sb-1"}}
	store := NewScatterGatherStore(src, WithClaimRouting("t", f.factory()))

	_, err := store.Claim(t.Context(), "ns", "s1", PoolKey{Template: "img", Net: "none", Size: "small"}, ClaimOptions{})
	require.NoError(t, err)

	_, err = store.Claim(t.Context(), "ns", "s2", PoolKey{Template: "img", Net: "egress"}, ClaimOptions{})
	require.Error(t, err)
	assert.True(t, IsNoWarmCapacity(err), "net mismatch must be no-capacity, got %v", err)
}

func TestStoreClaim_SandboxdCapacityRaceIsRetryable(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("n1", "10.0.0.1:7777", PoolCapacity{Template: "img", Warm: 1, Target: 5}))

	f := &recordingFactory{claimErr: sandboxd.ErrNodeAtCapacity}
	store := NewScatterGatherStore(src, WithClaimRouting("t", f.factory()))

	_, err := store.Claim(t.Context(), "ns", "s1", PoolKey{Template: "img"}, ClaimOptions{})
	require.Error(t, err)
	assert.True(t, IsNoWarmCapacity(err), "sandboxd 429 must map to no-capacity, got %v", err)
}

func TestStoreRelease_RoutesToNodeAddressWithUniformToken(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("n2", "10.0.0.2:7777", PoolCapacity{Template: "img", Warm: 3, Target: 5}))
	f := &recordingFactory{}
	store := NewScatterGatherStore(src, WithClaimRouting("uniform-token", f.factory()))

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
	store := NewScatterGatherStore(src)

	_, err := store.Claim(t.Context(), "ns", "s1", PoolKey{Template: "img"}, ClaimOptions{})
	require.Error(t, err)
	assert.False(t, IsNoWarmCapacity(err), "unconfigured routing is a config error, not no-capacity")

	require.Error(t, store.Release(t.Context(), "n1", "sb-1"))
}

func TestGetKeepsTheClaimTimeHintThroughThePublishLag(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("n1", "n1:7777", PoolCapacity{Template: "img", Warm: 2, Target: 2}))
	src.Put(poolInv("n2", "n2:7777", PoolCapacity{Template: "img", Warm: 2, Target: 2}))
	f := &recordingFactory{claimResult: sandboxd.ClaimResult{ID: "sb_1", Token: "tok", OwnerAddr: "10.0.0.1:7777"}}
	store := NewScatterGatherStore(src, WithClaimRouting("t", f.factory())).(*scatterGatherStore)

	a, err := store.Claim(t.Context(), "ns", "s1", PoolKey{Template: "img"}, ClaimOptions{})
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
	store := NewScatterGatherStore(src).(*scatterGatherStore)

	picked := map[string]int{}
	for range 200 {
		candidates, err := WarmCandidates(t.Context(), store, PoolKey{Template: "img"})
		require.NoError(t, err)
		best := pickPowerOfTwo(candidates)
		picked[best.node]++
	}

	assert.Len(t, picked, 4, "burst funneled onto a subset: %v", picked)
}

func TestPickWarmNodePrefersTheWarmerSample(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("cold", "cold:7777", PoolCapacity{Template: "img", Warm: 1, Target: 5}))
	src.Put(poolInv("warm", "warm:7777", PoolCapacity{Template: "img", Warm: 100, Target: 200}))
	store := NewScatterGatherStore(src).(*scatterGatherStore)

	warmPicks := 0
	for range 200 {
		candidates, err := WarmCandidates(t.Context(), store, PoolKey{Template: "img"})
		require.NoError(t, err)
		best := pickPowerOfTwo(candidates)
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

	f := &raceFactory{answers: map[string]error{"stale:7777": sandboxd.ErrNodeAtCapacity}, result: sandboxd.ClaimResult{ID: "sb-ok", Token: "tok"}}
	store := NewScatterGatherStore(src, WithClaimRouting("t", f.factory()))

	for range 40 {
		a, err := store.Claim(t.Context(), "ns", "s", PoolKey{Template: "img"}, ClaimOptions{})
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
		{"sandboxd unreachable", fmt.Errorf("claim: %w", &url.Error{Op: "Post", Err: &net.OpError{Op: "dial", Err: context.DeadlineExceeded}})},
		{"sandboxd 500", fmt.Errorf("claim: %w", &sandboxd.HTTPError{StatusCode: 500, Message: "provisioning failed"})},
	} {
		t.Run(tt.name, func(t *testing.T) {
			src := NewStaticInventorySource()
			src.Put(poolInv("dead", "dead:7777", PoolCapacity{Template: "img", Warm: 100, Target: 100}))
			src.Put(poolInv("live", "live:7777", PoolCapacity{Template: "img", Warm: 1, Target: 5}))
			f := &raceFactory{answers: map[string]error{"dead:7777": tt.err}, result: sandboxd.ClaimResult{ID: "sb-ok", Token: "tok"}}
			store := NewScatterGatherStore(src, WithClaimRouting("t", f.factory()))

			for range 20 {
				a, err := store.Claim(t.Context(), "ns", "s", PoolKey{Template: "img"}, ClaimOptions{})
				require.NoError(t, err, "a live warm node was in the candidate set the whole time")
				assert.Equal(t, "live", a.Node)
			}
		})
	}
}

func TestStoreClaimDoesNotRetryElsewhereAfterATimeout(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("slow", "slow:7777", PoolCapacity{Template: "img", Warm: 100, Target: 100}))
	f := &raceFactory{answers: map[string]error{"slow:7777": fmt.Errorf("claim: %w", context.DeadlineExceeded)}}
	store := NewScatterGatherStore(src, WithClaimRouting("t", f.factory()))

	_, err := store.Claim(t.Context(), "ns", "s", PoolKey{Template: "img"}, ClaimOptions{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.False(t, IsNoWarmCapacity(err), "a lost reply may have delivered a microVM; it must not read as no capacity")
	assert.Len(t, f.calls, 1, "the claim must not be re-issued")
}

func TestStoreClaimIsRetryableWhenEveryNodeIsDown(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("n1", "n1:7777", PoolCapacity{Template: "img", Warm: 3, Target: 5}))
	f := &raceFactory{answers: map[string]error{"n1:7777": fmt.Errorf("claim: %w", &sandboxd.HTTPError{StatusCode: 503})}}
	store := NewScatterGatherStore(src, WithClaimRouting("t", f.factory()))

	_, err := store.Claim(t.Context(), "ns", "s", PoolKey{Template: "img"}, ClaimOptions{})
	require.Error(t, err)
	assert.True(t, IsNoWarmCapacity(err), "no node delivered, so the caller gets the retryable signal")
}

func TestStoreClaimReportsNoCapacityOnlyWhenEveryNodeRaced(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("n1", "n1:7777", PoolCapacity{Template: "img", Warm: 3, Target: 5}))
	src.Put(poolInv("n2", "n2:7777", PoolCapacity{Template: "img", Warm: 3, Target: 5}))

	f := &raceFactory{answers: map[string]error{"n1:7777": sandboxd.ErrNodeAtCapacity, "n2:7777": sandboxd.ErrNodeAtCapacity}}
	store := NewScatterGatherStore(src, WithClaimRouting("t", f.factory()))

	_, err := store.Claim(t.Context(), "ns", "s", PoolKey{Template: "img"}, ClaimOptions{})
	require.Error(t, err)
	assert.True(t, IsNoWarmCapacity(err), "exhausting every node must stay the retryable no-capacity signal")
	assert.Len(t, f.calls, 2, "each node must be tried exactly once")
}

func TestStoreClaimFollowsARedirectToTheNodeThatDelivers(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("a", "a:7777", PoolCapacity{Template: "img", Warm: 3, Target: 5}))
	src.Put(poolInv("b", "b:7777", PoolCapacity{Template: "img", Warm: 0, Target: 5}))
	f := &raceFactory{answers: map[string]error{"a:7777": &sandboxd.RedirectError{Targets: []string{"b:7777"}}}, result: sandboxd.ClaimResult{ID: "sb_b", Token: "tok"}}
	store := NewScatterGatherStore(src, WithClaimRouting("t", f.factory())).(*scatterGatherStore)

	a, err := store.Claim(t.Context(), "ns", "s1", PoolKey{Template: "img"}, ClaimOptions{})
	require.NoError(t, err)
	assert.Equal(t, "b", a.Node)
	assert.Equal(t, "sb_b", a.SandboxName)
	assert.Equal(t, []claimCall{{"a:7777", false}, {"b:7777", true}}, f.calls)
	for _, key := range []string{nameKey("ns", "s1"), claimKey("ns", "sb_b")} {
		node, ok := store.index.lookup(key)
		require.True(t, ok, key)
		assert.Equal(t, "b", node, key)
	}
}

func TestStoreClaimSkipsARedirectItCannotFollow(t *testing.T) {
	for name, target := range map[string]string{"the target redirects again": "b:7777", "the target is no known node": "unknown:7777"} {
		t.Run(name, func(t *testing.T) {
			for range 20 {
				src := NewStaticInventorySource()
				src.Put(poolInv("a", "a:7777", PoolCapacity{Template: "img", Warm: 3, Target: 5}))
				src.Put(poolInv("b", "b:7777", PoolCapacity{Template: "img", Warm: 0, Target: 5}))
				src.Put(poolInv("c", "c:7777", PoolCapacity{Template: "img", Warm: 3, Target: 5}))
				f := &raceFactory{answers: map[string]error{
					"a:7777": &sandboxd.RedirectError{Targets: []string{target}},
					"b:7777": &sandboxd.RedirectError{Targets: []string{"a:7777"}},
				}, result: sandboxd.ClaimResult{ID: "sb_c", Token: "tok"}}
				store := NewScatterGatherStore(src, WithClaimRouting("t", f.factory()))

				a, err := store.Claim(t.Context(), "ns", "s", PoolKey{Template: "img"}, ClaimOptions{})
				require.NoError(t, err)
				assert.Equal(t, "c", a.Node)
				assert.NotContains(t, f.calls, claimCall{"a:7777", true})
				assert.NotContains(t, f.calls, claimCall{"unknown:7777", true})
			}
		})
	}
}

func TestStoreClaimDropsARedirectTargetThatDeliveredNothing(t *testing.T) {
	for range 20 {
		src := NewStaticInventorySource()
		src.Put(poolInv("a", "a:7777", PoolCapacity{Template: "img", Warm: 100, Target: 100}))
		src.Put(poolInv("b", "b:7777", PoolCapacity{Template: "img", Warm: 1, Target: 5}))
		f := &raceFactory{answers: map[string]error{
			"a:7777": &sandboxd.RedirectError{Targets: []string{"b:7777"}},
			"b:7777": sandboxd.ErrNodeAtCapacity,
		}}
		store := NewScatterGatherStore(src, WithClaimRouting("t", f.factory()))

		_, err := store.Claim(t.Context(), "ns", "s", PoolKey{Template: "img"}, ClaimOptions{})
		require.True(t, IsNoWarmCapacity(err), "want ErrNoWarmCapacity, got %v", err)
		if f.calls[0].addr == "a:7777" {
			assert.Equal(t, []claimCall{{"a:7777", false}, {"b:7777", true}}, f.calls, "b answered the redirect, so it is not sampled again")
		}
	}
}

func TestStoreClaimDoesNotRetryElsewhereAfterARedirectTargetTimesOut(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("a", "a:7777", PoolCapacity{Template: "img", Warm: 3, Target: 5}))
	src.Put(poolInv("b", "b:7777", PoolCapacity{Template: "img", Warm: 0, Target: 5}))
	src.Put(poolInv("c", "c:7777", PoolCapacity{Template: "img", Warm: 0, Target: 5}))
	f := &raceFactory{answers: map[string]error{
		"a:7777": &sandboxd.RedirectError{Targets: []string{"b:7777", "c:7777"}},
		"b:7777": fmt.Errorf("claim: %w", context.DeadlineExceeded),
	}}
	store := NewScatterGatherStore(src, WithClaimRouting("t", f.factory()))

	_, err := store.Claim(t.Context(), "ns", "s", PoolKey{Template: "img"}, ClaimOptions{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.ErrorContains(t, err, `on node "b"`)
	assert.Equal(t, []claimCall{{"a:7777", false}, {"b:7777", true}}, f.calls, "a lost reply may have delivered a microVM, so no other target is asked")
}

func TestStoreClaimReportsNoCapacityWhenEveryRedirectTargetIsFull(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("a", "a:7777", PoolCapacity{Template: "img", Warm: 3, Target: 5}))
	src.Put(poolInv("b", "b:7777", PoolCapacity{Template: "img", Warm: 0, Target: 5}))
	src.Put(poolInv("c", "c:7777", PoolCapacity{Template: "img", Warm: 0, Target: 5}))
	f := &raceFactory{answers: map[string]error{
		"a:7777": &sandboxd.RedirectError{Targets: []string{"b:7777", "c:7777"}},
		"b:7777": sandboxd.ErrNodeAtCapacity,
		"c:7777": sandboxd.ErrNodeAtCapacity,
	}}
	store := NewScatterGatherStore(src, WithClaimRouting("t", f.factory()))

	_, err := store.Claim(t.Context(), "ns", "s", PoolKey{Template: "img"}, ClaimOptions{})
	assert.True(t, IsNoWarmCapacity(err), "want ErrNoWarmCapacity, got %v", err)
	assert.Equal(t, []claimCall{{"a:7777", false}, {"b:7777", true}, {"c:7777", true}}, f.calls)
}

func TestWarmCandidatesMatchThePerNodeFanOut(t *testing.T) {
	ctx := t.Context()
	pool := PoolKey{Template: "rt", Net: NetDefault, Size: SizeClassSmall}
	src := NewStaticInventorySource()
	src.Put(poolInv("a", "10.0.0.1:7777", PoolCapacity{Template: "rt", Warm: 2}, PoolCapacity{Template: "other", Warm: 9}))
	src.Put(poolInv("b", "10.0.0.2:7777", PoolCapacity{Template: "rt", Warm: 0}))
	src.Put(poolInv("c", "", PoolCapacity{Template: "rt", Warm: 3}))
	src.Put(poolInv("d", "10.0.0.4:7777", PoolCapacity{Template: "rt", Net: NetDefault, Size: SizeClassSmall, Warm: 1}, PoolCapacity{Template: "rt", Warm: 4}))
	src.Put(poolInv("e", "10.0.0.5:7777", PoolCapacity{Template: "rt", Warm: 5}))
	src.Partition("e")
	store := NewScatterGatherStore(src).(*scatterGatherStore)

	var want []warmCandidate
	nodes, err := src.ListNodes(ctx)
	require.NoError(t, err)
	for _, n := range nodes {
		addr, pools, err := src.NodeCapacity(ctx, n)
		if err != nil || addr == "" {
			continue
		}
		for _, pc := range pools {
			if pc.Warm > 0 && poolCapacityMatches(pc, pool) {
				want = append(want, warmCandidate{node: n, addr: addr, warm: pc.Warm})
			}
		}
	}
	caps, err := store.src.NodeCapacities(ctx)
	require.NoError(t, err)
	got := warmCandidates(caps, pool)
	assert.Equal(t, want, got)
	assert.Len(t, got, 3, "a's rt pool and both of d's; b is cold, c has no address, e is partitioned")
	assert.Equal(t, "d", nodeForAddress(caps, "10.0.0.4:7777"), "a node address resolves to its node")
	assert.Empty(t, nodeForAddress(caps, "10.0.0.5:7777"), "a partitioned node's address does not resolve")
}

type claimCall struct {
	addr       string
	noRedirect bool
}

type raceFactory struct {
	answers map[string]error
	result  sandboxd.ClaimResult
	calls   []claimCall
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

func (c *raceClient) Claim(_ context.Context, spec sandboxd.ClaimSpec) (sandboxd.ClaimResult, error) {
	c.f.calls = append(c.f.calls, claimCall{c.addr, spec.NoRedirect})
	if err := c.f.answers[c.addr]; err != nil {
		return sandboxd.ClaimResult{}, err
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
	forkSpec     sandboxd.ForkSpec
	claimCalls   int
	releaseID    string
	releaseToken string
	releaseCalls int

	claimResult sandboxd.ClaimResult
	forkResult  sandboxd.ForkResult
	claimErr    error
	claimErrAt  map[string]error
	claimAddrs  []string
	releaseErr  error
	verbErr     error

	deletedTemplates []sandboxd.PoolKey

	rows         map[string][]sandboxd.SandboxSummary
	rowReads     []string
	dialErr      error
	dialErrs     map[string]error
	dialPorts    []uint16
	metadataDocs []string
	silent       string
	ignoreRef    bool
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
	c.f.claimAddrs = append(c.f.claimAddrs, c.addr)
	if err := c.f.claimErrAt[c.addr]; err != nil {
		return sandboxd.ClaimResult{}, err
	}
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

func (c *recordingClient) Fork(_ context.Context, _ string, spec sandboxd.ForkSpec) (sandboxd.ForkResult, error) {
	c.f.forkSpec = spec
	return c.f.forkResult, c.f.verbErr
}

func (c *recordingClient) Checkpoint(context.Context, string, sandboxd.CheckpointSpec) (sandboxd.Checkpoint, error) {
	return sandboxd.Checkpoint{}, c.f.verbErr
}

func (c *recordingClient) Checkpoints(context.Context) ([]sandboxd.Checkpoint, error) {
	return nil, nil
}

func (c *recordingClient) DeleteCheckpoint(context.Context, string) error { return nil }

func (c *recordingClient) DeleteTemplate(_ context.Context, key sandboxd.PoolKey, _ string) error {
	c.f.mu.Lock()
	defer c.f.mu.Unlock()
	c.f.deletedTemplates = append(c.f.deletedTemplates, key)
	return c.f.verbErr
}

func (c *recordingClient) Promote(_ context.Context, id, template string) (sandboxd.PoolKey, string, error) {
	return sandboxd.PoolKey{Template: template}, "sha256:" + id, c.f.verbErr
}

func (c *recordingClient) SetTemplateLabels(context.Context, sandboxd.PoolKey, map[string]string, string) error {
	return c.f.verbErr
}

func (c *recordingClient) Info(context.Context) (*sandboxd.NodeInfo, error) {
	return &sandboxd.NodeInfo{}, c.f.verbErr
}

func (c *recordingClient) SetInstanceMetadata(_ context.Context, id string, doc []byte) error {
	c.f.mu.Lock()
	defer c.f.mu.Unlock()
	c.f.metadataDocs = append(c.f.metadataDocs, id+" "+string(doc))
	return c.f.verbErr
}

func (c *recordingClient) DialPort(_ context.Context, id, _ string, port uint16) (net.Conn, error) {
	c.f.mu.Lock()
	defer c.f.mu.Unlock()
	c.f.dialPorts = append(c.f.dialPorts, port)
	if err, ok := c.f.dialErrs[id]; ok {
		return nil, err
	}
	if c.f.dialErr != nil {
		return nil, c.f.dialErr
	}
	conn, peer := net.Pipe()
	_ = peer.Close()
	return conn, nil
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

func (c *recordingClient) SandboxesByClaimRef(ctx context.Context, ref string) ([]sandboxd.SandboxSummary, error) {
	if c.addr == c.f.silent {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	c.f.mu.Lock()
	defer c.f.mu.Unlock()
	c.f.rowReads = append(c.f.rowReads, c.addr)
	var out []sandboxd.SandboxSummary
	for _, row := range c.f.rows[c.addr] {
		if c.f.ignoreRef || row.ClaimRef == ref {
			out = append(out, row)
		}
	}
	return out, nil
}
