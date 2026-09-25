package scale

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"

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
	synctest.Test(t, func(t *testing.T) {
		f := &recordingFactory{silent: "n2:7777"}
		store, _ := unpublishedStore(f)

		start := time.Now()
		_, err := store.GetByClaimID(t.Context(), "ns", "sb_gone", func(id string) bool { return id == "sb_gone" })
		require.True(t, k8serrors.IsNotFound(err), "a node that never answers is a miss: %v", err)
		_, err = store.Get(t.Context(), "ns", "gone")
		require.True(t, k8serrors.IsNotFound(err), "a node that never answers is a miss by name too: %v", err)
		assert.Less(t, time.Since(start), 2*liveLookupTimeout+time.Second, "a silent node must not hold a miss past the per-node bound")
	})
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
	store := NewScatterGatherStore(src).(*scatterGatherStore)

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

	archived := EntryFromSummary(sandboxd.SandboxSummary{ID: "sb_3", Archived: true})
	assert.Equal(t, PhaseHibernated, archived.Phase, "an archived claim has no VM on the node; it is paused, not running")
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

func TestANamePinnedListFindsAClaimBeforeItsNodePublishes(t *testing.T) {
	for name, tc := range map[string]struct {
		opts ListOptions
		want []string
	}{
		"the claimed name":          {opts: ListOptions{Namespace: "ns", FieldSelector: "metadata.name=s2"}, want: []string{"n2"}},
		"a label the claim lacks":   {opts: ListOptions{Namespace: "ns", FieldSelector: "metadata.name=s2", LabelSelector: PhaseLabel + "=" + PhaseHibernated}},
		"an unclaimed name":         {opts: ListOptions{Namespace: "ns", FieldSelector: "metadata.name=gone"}},
		"the fleet view":            {opts: ListOptions{Namespace: "ns"}},
		"a name in every namespace": {opts: ListOptions{FieldSelector: "metadata.name=s2"}},
	} {
		t.Run(name, func(t *testing.T) {
			store, _ := unpublishedStore(liveClaimFactory())

			list, err := store.List(t.Context(), tc.opts)
			require.NoError(t, err)
			var nodes []string
			for i := range list.Items {
				nodes = append(nodes, list.Items[i].Status.NodeName)
			}
			assert.Equal(t, tc.want, nodes)
		})
	}
}

func TestANamePinnedWatchKeepsAClaimOnlyWhileItsNodeHoldsIt(t *testing.T) {
	for name, tc := range map[string]struct {
		labels string
		change func(f *recordingFactory)
	}{
		"the node releases it":   {change: func(f *recordingFactory) { delete(f.rows, "n2:7777") }},
		"it leaves the selector": {labels: PhaseLabel + "=" + PhaseRunning, change: func(f *recordingFactory) { f.rows["n2:7777"][0].Hibernated = true }},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := liveClaimFactory()
				store, _ := unpublishedStore(f)
				w, err := store.Watch(t.Context(), ListOptions{Namespace: "ns", FieldSelector: "metadata.name=s2", LabelSelector: tc.labels, WatchList: true})
				require.NoError(t, err)
				defer w.Stop()

				assert.Equal(t, []watch.EventType{watch.Added, watch.Bookmark}, drainEvents(w), "a live claim must be in the initial events")
				time.Sleep(5 * time.Second)
				assert.Empty(t, drainEvents(w), "a claim its node still holds must not read as deleted")

				f.mu.Lock()
				tc.change(f)
				f.mu.Unlock()
				time.Sleep(2 * time.Second)
				assert.Equal(t, []watch.EventType{watch.Deleted}, drainEvents(w))
			})
		})
	}
}

func TestANamePinnedWatchAsksNoNodeForAnAbsentName(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := &recordingFactory{}
		store, _ := unpublishedStore(f)
		w, err := store.Watch(t.Context(), ListOptions{Namespace: "ns", FieldSelector: "metadata.name=gone", WatchList: true})
		require.NoError(t, err)
		defer w.Stop()

		assert.Equal(t, []watch.EventType{watch.Bookmark}, drainEvents(w))
		f.mu.Lock()
		f.rowReads = nil
		f.mu.Unlock()
		time.Sleep(10 * time.Second)
		synctest.Wait()
		f.mu.Lock()
		defer f.mu.Unlock()
		assert.Empty(t, f.rowReads, "polling an absent name must not ask the nodes")
	})
}

func TestANamePinnedWatchPollsOnlyTheNodeThatHoldsItsEntry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := &countingSource{StaticInventorySource: NewStaticInventorySource()}
		src.Put(inventoryWith("n1"))
		src.Put(inventoryWith("n2", InventoryEntry{Name: "ns/s2", ID: "sb_2", Phase: "Running"}))
		store := NewScatterGatherStore(src).(*scatterGatherStore)
		w, err := store.Watch(t.Context(), ListOptions{Namespace: "ns", FieldSelector: "metadata.name=s2"})
		require.NoError(t, err)
		defer w.Stop()

		assert.Equal(t, []watch.EventType{watch.Added}, drainEvents(w))
		opened := src.lists.Load()
		time.Sleep(5 * time.Second)
		assert.Empty(t, drainEvents(w))
		assert.Equal(t, opened, src.lists.Load(), "a watch that holds its entry must not enumerate the fleet on a tick")

		src.Put(inventoryWith("n2", InventoryEntry{Name: "ns/s2", ID: "sb_2", Phase: "Paused"}))
		time.Sleep(2 * time.Second)
		assert.Equal(t, []watch.EventType{watch.Modified}, drainEvents(w))
		assert.Equal(t, opened, src.lists.Load(), "a change on the entry's node must not enumerate the fleet either")

		src.Put(inventoryWith("n2"))
		src.Put(inventoryWith("n1", InventoryEntry{Name: "ns/s2", ID: "sb_3", Phase: "Running"}))
		time.Sleep(3 * time.Second)
		assert.Equal(t, []watch.EventType{watch.Deleted, watch.Added}, drainEvents(w), "a name claimed again on another node is a delete, then an add")
	})
}

type countingSource struct {
	*StaticInventorySource
	lists atomic.Int32
}

func (c *countingSource) ListNodes(ctx context.Context) ([]string, error) {
	c.lists.Add(1)
	return c.StaticInventorySource.ListNodes(ctx)
}

func unpublishedStore(f *recordingFactory) (*scatterGatherStore, *countingSource) {
	src := &countingSource{StaticInventorySource: NewStaticInventorySource()}
	src.Put(poolInv("n1", "n1:7777"))
	src.Put(poolInv("n2", "n2:7777"))
	return NewScatterGatherStore(src, WithClaimRouting("t", f.factory())).(*scatterGatherStore), src
}

func inventoryWith(node string, entries ...InventoryEntry) *NodeInventory {
	inv := poolInv(node, node+":7777")
	inv.Entries = entries
	return inv
}

func drainEvents(w watch.Interface) []watch.EventType {
	synctest.Wait()
	var got []watch.EventType
	for len(w.ResultChan()) > 0 {
		got = append(got, (<-w.ResultChan()).Type)
	}
	return got
}

func liveClaimFactory() *recordingFactory {
	return &recordingFactory{rows: map[string][]sandboxd.SandboxSummary{"n2:7777": {{ID: "sb_2", ClaimRef: "ns/s2"}}}}
}
