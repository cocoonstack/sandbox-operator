package scale

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
)

func TestGetByClaimIDReadsTheOwningNodeBeforeItPublishes(t *testing.T) {
	claimed := time.Date(2026, 9, 23, 2, 47, 45, 0, time.UTC)
	f := &recordingFactory{rows: map[string][]sandboxd.SandboxSummary{
		"n2:7777": {{ID: "sb_1", ClaimRef: "ns/s1", Key: sandboxd.PoolKey{Template: "img"}, Deadline: claimed.Add(5 * time.Minute), ClaimedAt: claimed}},
	}}
	store, _ := unpublishedStore(f)

	got, err := store.GetByClaimID(t.Context(), "ns", "sb_1", func(id string) bool { return id == "sb_1" })
	require.NoError(t, err)
	assert.Equal(t, "n2", got.Status.NodeName)
	assert.Equal(t, "sb_1", got.Annotations[ClaimIDAnnotation])
	assert.Equal(t, metav1.NewTime(claimed), got.CreationTimestamp)
	assert.Equal(t, "img", got.Labels[TemplateLabel])

	_, err = store.GetByClaimID(t.Context(), "other", "sb_1", func(id string) bool { return id == "sb_1" })
	assert.True(t, k8serrors.IsNotFound(err), "a live row in another namespace must stay hidden: %v", err)
}

func TestASilentNodeBoundsAMiss(t *testing.T) {
	f := &recordingFactory{silent: "n2:7777"}
	store, _ := unpublishedStore(f)

	start := time.Now()
	_, err := store.GetByClaimID(t.Context(), "ns", "sb_gone", func(id string) bool { return id == "sb_gone" })
	require.True(t, k8serrors.IsNotFound(err), "a node that never answers is a miss: %v", err)
	_, err = store.Get(t.Context(), "ns", "gone")
	require.True(t, k8serrors.IsNotFound(err), "a node that never answers is a miss by name too: %v", err)
	assert.Less(t, time.Since(start), 2*liveLookupTimeout+time.Second, "a silent node must not hold a miss past the per-node bound")
}

func TestGetAsksTheClaimingNodeFirst(t *testing.T) {
	f := &recordingFactory{claimResult: sandboxd.ClaimResult{ID: "sb_1", Token: "tok"}}
	store, src := unpublishedStore(f)
	src.Put(poolInv("n1", "n1:7777", PoolCapacity{Template: "img", Warm: 1, Target: 1}))

	_, err := store.Claim(t.Context(), "ns", "s1", PoolKey{Template: "img"}, 0)
	require.NoError(t, err)
	f.rows = map[string][]sandboxd.SandboxSummary{
		"n1:7777": {{ID: "sb_1", ClaimRef: "ns/s1"}},
		"n2:7777": {{ID: "sb_2", ClaimRef: "ns/s2"}},
	}
	src.lists.Store(0)

	got, err := store.Get(t.Context(), "ns", "s1")
	require.NoError(t, err)
	assert.Equal(t, "n1", got.Status.NodeName)
	assert.Equal(t, []string{"n1:7777"}, f.rowReads, "only the node that served the claim is asked")
	assert.Zero(t, src.lists.Load(), "a hit on the claiming node must not sweep the fleet's inventories")
}

func TestGetFindsWhatAnotherReplicaClaimed(t *testing.T) {
	claimed := time.Date(2026, 9, 23, 2, 47, 45, 0, time.UTC)
	f := &recordingFactory{rows: map[string][]sandboxd.SandboxSummary{
		"n2:7777": {{ID: "sb_2", ClaimRef: "ns/s2", Key: sandboxd.PoolKey{Template: "img"}, ClaimedAt: claimed}},
	}}
	store, _ := unpublishedStore(f)

	got, err := store.Get(t.Context(), "ns", "s2")
	require.NoError(t, err)
	assert.Equal(t, "n2", got.Status.NodeName)
	assert.Equal(t, "sb_2", got.Annotations[ClaimIDAnnotation])
	assert.Equal(t, metav1.NewTime(claimed), got.CreationTimestamp)

	f.rowReads = nil
	_, err = store.Get(t.Context(), "ns", "s2")
	require.NoError(t, err)
	assert.Equal(t, []string{"n2:7777"}, f.rowReads, "a resolved name is asked of its node alone")

	f.rowReads = nil
	_, err = store.Get(t.Context(), "other", "s2")
	require.True(t, k8serrors.IsNotFound(err), "the same name in another namespace is another claim ref: %v", err)
	assert.ElementsMatch(t, []string{"n1:7777", "n2:7777"}, f.rowReads, "a miss asks every node once")
}

func TestGetMatchesTheNameWhenANodeIgnoresTheFilter(t *testing.T) {
	f := &recordingFactory{ignoreRef: true, rows: map[string][]sandboxd.SandboxSummary{
		"n1:7777": {{ID: "sb_1", ClaimRef: "ns/s1"}, {ID: "sb_2", ClaimRef: "ns/s2"}},
	}}
	store, _ := unpublishedStore(f)

	got, err := store.Get(t.Context(), "ns", "s2")
	require.NoError(t, err)
	assert.Equal(t, "sb_2", got.Annotations[ClaimIDAnnotation], "a node that returns its whole index must not hand back another claim")

	_, err = store.Get(t.Context(), "other", "s2")
	require.True(t, k8serrors.IsNotFound(err), "the namespace is part of the name: %v", err)
}

func TestLiveReadsNeedClaimRouting(t *testing.T) {
	src := &countingSource{StaticInventorySource: NewStaticInventorySource()}
	src.Put(poolInv("n1", "n1:7777"))
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()))

	_, err := store.GetByClaimID(t.Context(), "ns", "sb_1", func(id string) bool { return id == "sb_1" })
	assert.True(t, k8serrors.IsNotFound(err), "without a fleet token the store only reads published inventory: %v", err)
	_, err = store.Get(t.Context(), "ns", "s1")
	assert.True(t, k8serrors.IsNotFound(err), "without a fleet token a name is read from published inventory only: %v", err)
	assert.Equal(t, int32(2), src.lists.Load(), "without a fleet token no second, node-asking sweep runs")
}

func TestEntryFromSummaryIsWhatANodePublishes(t *testing.T) {
	deadline := time.Date(2026, 9, 23, 3, 0, 0, 0, time.UTC)
	running := EntryFromSummary(sandboxd.SandboxSummary{ID: "sb_1", ClaimRef: "ns/s1", Key: sandboxd.PoolKey{Template: "img"}, Deadline: deadline})
	assert.Equal(t, InventoryEntry{Name: "ns/s1", ID: "sb_1", Phase: PhaseRunning, ClaimRef: "ns/s1", Template: "img", Deadline: new(metav1.NewTime(deadline))}, running)

	unnamed := EntryFromSummary(sandboxd.SandboxSummary{ID: "sb_2", Hibernated: true})
	assert.Equal(t, InventoryEntry{Name: "sb_2", ID: "sb_2", Phase: PhaseHibernated, ClaimRef: "sb_2"}, unnamed)
}

func TestFirstHitReturnsTheFirstNonZeroAnswer(t *testing.T) {
	src := NewStaticInventorySource()
	for _, n := range []string{"n1", "n2", "n3"} {
		src.Put(poolInv(n, n+":7777"))
	}
	got, err := FirstHit(t.Context(), src, 2, func(_ context.Context, node string) string {
		if node == "n2" {
			return node
		}
		return ""
	})
	require.NoError(t, err)
	assert.Equal(t, "n2", got)

	none, err := FirstHit(t.Context(), src, 2, func(context.Context, string) string { return "" })
	require.NoError(t, err)
	assert.Empty(t, none)
}

func unpublishedStore(f *recordingFactory) (*scatterGatherStore, *countingSource) {
	src := &countingSource{StaticInventorySource: NewStaticInventorySource()}
	src.Put(poolInv("n1", "n1:7777"))
	src.Put(poolInv("n2", "n2:7777"))
	return NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("t", f.factory())), src
}

type countingSource struct {
	*StaticInventorySource
	lists atomic.Int32
}

func (c *countingSource) ListNodes(ctx context.Context) ([]string, error) {
	c.lists.Add(1)
	return c.StaticInventorySource.ListNodes(ctx)
}
