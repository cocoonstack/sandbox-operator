package scale

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cocoonv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
)

func TestScatterGatherList_FlattensAllNodes(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(inv("n1", entry("ns-a/s1", "Running"), entry("ns-b/s2", "Pending")))
	src.Put(inv("n2", entry("ns-a/s3", "Running")))
	store := NewScatterGatherStore(src)

	list, err := store.List(t.Context(), ListOptions{})
	require.NoError(t, err)
	require.Len(t, list.Items, 3)

	assert.Equal(t, "ns-a/s1", list.Items[0].Namespace+"/"+list.Items[0].Name)
	assert.Equal(t, "ns-a/s3", list.Items[1].Namespace+"/"+list.Items[1].Name)
	assert.Equal(t, "ns-b/s2", list.Items[2].Namespace+"/"+list.Items[2].Name)

	byName := map[string]sandboxStatusView{}
	for _, it := range list.Items {
		byName[it.Name] = sandboxStatusView{node: it.Status.NodeName, label: it.Labels[NodeLabel]}
	}
	assert.Equal(t, "n1", byName["s1"].node)
	assert.Equal(t, "n1", byName["s1"].label)
	assert.Equal(t, "n2", byName["s3"].node)
}

func TestScatterGatherList_HonorsNamespaceFilter(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(inv("n1", entry("team-a/s1", "Running"), entry("team-b/s2", "Running")))
	src.Put(inv("n2", entry("team-a/s3", "Running")))
	store := NewScatterGatherStore(src)

	list, err := store.List(t.Context(), ListOptions{Namespace: "team-a"})
	require.NoError(t, err)
	require.Len(t, list.Items, 2)
	for _, it := range list.Items {
		assert.Equal(t, "team-a", it.Namespace)
	}
}

func TestScatterGatherList_HonorsLabelSelector(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(inv("n1", entry("ns/running", "Running")))
	src.Put(inv("n2", entry("ns/pending", "Pending")))
	store := NewScatterGatherStore(src)

	list, err := store.List(t.Context(), ListOptions{LabelSelector: PhaseLabel + "=Running"})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, "running", list.Items[0].Name)

	byNode, err := store.List(t.Context(), ListOptions{LabelSelector: NodeLabel + "=n2"})
	require.NoError(t, err)
	require.Len(t, byNode.Items, 1)
	assert.Equal(t, "pending", byNode.Items[0].Name)
}

func TestScatterGatherList_HonorsFieldSelector(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(inv("n1", entry("ns/a", "Running"), entry("ns/b", "Running")))
	src.Put(inv("n2", entry("ns/c", "Running")))
	store := NewScatterGatherStore(src)

	byName, err := store.List(t.Context(), ListOptions{FieldSelector: "metadata.name=b"})
	require.NoError(t, err)
	require.Len(t, byName.Items, 1)
	assert.Equal(t, "b", byName.Items[0].Name)

	byNode, err := store.List(t.Context(), ListOptions{FieldSelector: "status.nodeName=n2"})
	require.NoError(t, err)
	require.Len(t, byNode.Items, 1)
	assert.Equal(t, "c", byNode.Items[0].Name)
}

func TestScatterGatherList_RejectsAnUnsupportedFieldSelector(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(inv("n1", entry("ns/a", "Running")))
	store := NewScatterGatherStore(src)

	for _, sel := range []string{"spec.nonsense=x", "metadata.uid=abc"} {
		_, err := store.List(t.Context(), ListOptions{FieldSelector: sel})
		require.Error(t, err, sel)
		assert.True(t, k8serrors.IsBadRequest(err), "%s: an unindexed field must be a 400, not an empty list: %v", sel, err)

		_, err = store.Watch(t.Context(), ListOptions{FieldSelector: sel})
		require.Error(t, err, sel)
		assert.True(t, k8serrors.IsBadRequest(err), "%s: watch must reject it the same way: %v", sel, err)
	}
}

func TestScatterGatherList_RejectsAMalformedSelectorAsBadRequest(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(inv("n1", entry("ns/a", "Running")))
	store := NewScatterGatherStore(src)

	for name, opts := range map[string]ListOptions{
		"field": {FieldSelector: "a==b==c"},
		"label": {LabelSelector: "!!!"},
	} {
		_, err := store.List(t.Context(), opts)
		require.Error(t, err, name)
		assert.True(t, k8serrors.IsBadRequest(err), "%s: a selector that does not parse is a 400, not a 500: %v", name, err)

		_, err = store.Watch(t.Context(), opts)
		require.Error(t, err, name)
		assert.True(t, k8serrors.IsBadRequest(err), "%s: watch must reject it the same way: %v", name, err)
	}
}

func TestScatterGatherList_ToleratesPartitionedNode(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(inv("n1", entry("ns/s1", "Running")))
	src.Put(inv("n2", entry("ns/s2", "Running")))
	src.Partition("n2")
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()))

	list, err := store.List(t.Context(), ListOptions{})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, "s1", list.Items[0].Name)
}

func TestScatterGatherGet_RoutesToOwningNode(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(inv("n1", entry("ns/s1", "Running")))
	src.Put(inv("n2", entry("ns/s2", "Pending")))
	store := NewScatterGatherStore(src)

	got, err := store.Get(t.Context(), "ns", "s2")
	require.NoError(t, err)
	assert.Equal(t, "s2", got.Name)
	assert.Equal(t, "ns", got.Namespace)
	assert.Equal(t, "n2", got.Status.NodeName)

	_, err = store.Get(t.Context(), "ns", "missing")
	require.Error(t, err)
	assert.True(t, k8serrors.IsNotFound(err), "expected NotFound, got %v", err)
}

func TestScatterGatherGet_SynthesizesStatus(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(inv("n1", InventoryEntry{Name: "ns/s1", ID: "sb_abc123", Phase: "Running", ClaimRef: "ns/claim-1", Address: "10.1.2.3:7777"}))
	store := NewScatterGatherStore(src)

	got, err := store.Get(t.Context(), "ns", "s1")
	require.NoError(t, err)
	assert.Equal(t, []string{"10.1.2.3"}, got.Status.PodIPs)
	assert.Equal(t, "claim-1", got.Labels[ClaimLabel])

	assert.Equal(t, "sb_abc123", got.Annotations[ClaimIDAnnotation])
	require.Len(t, got.Status.Conditions, 1)
	assert.Equal(t, metav1.ConditionTrue, got.Status.Conditions[0].Status)
	assert.Equal(t, "Running", got.Status.Conditions[0].Reason)
}

func TestEntryToSandbox_StampsClaimIDAnnotation(t *testing.T) {
	withID := entryToSandbox("n1", InventoryEntry{Name: "ns/s1", ID: "sb_abc", Phase: "Running"})
	assert.Equal(t, "sb_abc", withID.Annotations[ClaimIDAnnotation])

	noID := entryToSandbox("n1", InventoryEntry{Name: "ns/s1", Phase: "Running"})
	_, ok := noID.Annotations[ClaimIDAnnotation]
	assert.False(t, ok, "expected no claim-id annotation when the entry has no id")
}

func TestEntryToSandbox_StampsDeadlineAnnotation(t *testing.T) {
	deadline := metav1.NewTime(time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC))
	with := entryToSandbox("n1", InventoryEntry{Name: "ns/s1", ID: "sb_abc", Phase: "Running", Deadline: &deadline})
	assert.Equal(t, "2026-08-17T12:00:00Z", with.Annotations[DeadlineAnnotation])
	assert.NotEqual(t, entryToSandbox("n1", InventoryEntry{Name: "ns/s1", ID: "sb_abc", Phase: "Running"}).ResourceVersion,
		with.ResourceVersion, "a refreshed deadline must surface as a Modified entry")

	_, ok := entryToSandbox("n1", InventoryEntry{Name: "ns/s1", Phase: "Running"}).Annotations[DeadlineAnnotation]
	assert.False(t, ok, "expected no deadline annotation when the node published none")
}

func TestScatterGatherWatch_EmitsAddModifyDelete(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(inv("n1", entry("ns/s1", "Pending")))
	store := NewScatterGatherStore(src, WithWatchPollInterval(10*time.Millisecond))

	ctx := t.Context()
	w, err := store.Watch(ctx, ListOptions{})
	require.NoError(t, err)
	defer w.Stop()

	added := waitForType(t, w, watch.Added, time.Second)
	assert.Equal(t, "s1", added.Object.(*sandboxv1beta1.Sandbox).Name)

	src.Put(inv("n1", entry("ns/s1", "Running")))
	waitForType(t, w, watch.Modified, 2*time.Second)

	src.Put(inv("n1"))
	waitForType(t, w, watch.Deleted, 2*time.Second)
}

func TestScatterGather_ObjectCountIsPoolsPlusNodes(t *testing.T) {
	const nodes, perNode = 4, 250
	ctx := t.Context()
	src := NewStaticInventorySource()

	for k := range nodes {
		node := fmt.Sprintf("n%d", k)
		entries := make(sliceLive, 0, perNode)
		for i := range perNode {
			entries = append(entries, entry(fmt.Sprintf("ns/s-%d-%d", k, i), "Running"))
		}
		n, err := publish(ctx, node, entries, src)
		require.NoError(t, err)
		require.Equal(t, perNode, n)
	}

	store := NewScatterGatherStore(src)
	list, err := store.List(ctx, ListOptions{})
	require.NoError(t, err)

	require.Len(t, list.Items, nodes*perNode)

	assert.Equal(t, nodes, src.ObjectCount())
	assert.Equal(t, nodes, src.ApplyCount())

	assert.Less(t, src.ObjectCount(), len(list.Items))
}

func TestPublisher_RebuildsFromLiveAfterLoss(t *testing.T) {
	ctx := t.Context()
	live := &mutableLive{entries: []InventoryEntry{entry("ns/a", "Running")}}
	src := NewStaticInventorySource()
	_, err := publish(ctx, "n1", live, src)
	require.NoError(t, err)
	require.Equal(t, 1, src.ObjectCount())

	src.Remove("n1")
	require.Equal(t, 0, src.ObjectCount())

	live.entries = []InventoryEntry{entry("ns/a", "Running"), entry("ns/b", "Running")}
	n, err := publish(ctx, "n1", live, src)
	require.NoError(t, err)
	require.Equal(t, 2, n)

	got, err := src.NodeInventory(ctx, "n1")
	require.NoError(t, err)
	require.Len(t, got.Entries, 2)
}

func TestNodeInventory_DeepCopyIsIndependent(t *testing.T) {
	orig := inv("n1", entry("ns/a", "Running"))
	cp := orig.DeepCopy()
	cp.Entries[0].Phase = "Pending"
	cp.Node = "n2"
	assert.Equal(t, "Running", orig.Entries[0].Phase, "deep copy must not alias entries")
	assert.Equal(t, "n1", orig.Node)

	obj := orig.DeepCopyObject()
	require.NotNil(t, obj)
	clone, ok := obj.(*NodeInventory)
	require.True(t, ok)
	assert.Equal(t, "n1", clone.Node)
}

func TestWatchSeesAShortLivedSandbox(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(inv("n1", entry("sb-1", "Running")))
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithWatchPollInterval(10*time.Millisecond))

	w, err := store.Watch(t.Context(), ListOptions{})
	require.NoError(t, err)
	defer w.Stop()
	require.Equal(t, watch.Added, (<-w.ResultChan()).Type)

	src.Put(inv("n1", entry("sb-1", "Running"), entry("sb-2", "Running")))

	deadline := time.After(3 * time.Second)
	for {
		select {
		case ev := <-w.ResultChan():
			if ev.Type == watch.Added {
				return
			}
		case <-deadline:
			t.Fatal("a short-lived sandbox produced no event")
		}
	}
}

func TestSSAApplier_UpsertsOneObjectPerNode(t *testing.T) {
	ctx := t.Context()
	cli := fake.NewClientBuilder().WithScheme(newScaleScheme(t)).Build()
	_, err := publish(ctx, "n1", sliceLive{entry("ns/a", "Running")}, NewSSAInventoryApplier(cli, "vk-test"))
	require.NoError(t, err)

	got := &cocoonv1beta1.NodeInventory{}
	require.NoError(t, cli.Get(ctx, client.ObjectKey{Name: "n1"}, got))
	require.Len(t, got.Entries, 1)

	_, err = publish(ctx, "n1", sliceLive{entry("ns/a", "Running"), entry("ns/b", "Running")},
		NewSSAInventoryApplier(cli, "vk-test"))
	require.NoError(t, err)

	list := &cocoonv1beta1.NodeInventoryList{}
	require.NoError(t, cli.List(ctx, list))
	require.Len(t, list.Items, 1)
	assert.Len(t, list.Items[0].Entries, 2)
}

func publish(ctx context.Context, node string, live NodeLiveSource, applier InventoryApplier) (int, error) {
	entries, err := live.LiveSandboxes(ctx)
	if err != nil {
		return 0, err
	}
	return len(entries), applier.Apply(ctx, &NodeInventory{
		Kind:       NodeInventoryGVK.Kind,
		APIVersion: NodeInventoryGVK.GroupVersion().String(),
		Name:       node,
		Node:       node,
		Entries:    entries,
	})
}

func inv(node string, entries ...InventoryEntry) *NodeInventory {
	return &NodeInventory{
		Name:    node,
		Node:    node,
		Entries: entries,
	}
}

func entry(name, phase string) InventoryEntry { return InventoryEntry{Name: name, Phase: phase} }

type sliceLive []InventoryEntry

func (s sliceLive) LiveSandboxes(context.Context) ([]InventoryEntry, error) {
	return []InventoryEntry(s), nil
}

type mutableLive struct{ entries []InventoryEntry }

func (m *mutableLive) LiveSandboxes(context.Context) ([]InventoryEntry, error) {
	return m.entries, nil
}

type sandboxStatusView struct{ node, label string }

func waitForType(t *testing.T, w watch.Interface, want watch.EventType, timeout time.Duration) watch.Event {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case ev, ok := <-w.ResultChan():
			require.True(t, ok, "watch channel closed before %s event", want)
			if ev.Type == want {
				return ev
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s event", want)
			return watch.Event{}
		}
	}
}
