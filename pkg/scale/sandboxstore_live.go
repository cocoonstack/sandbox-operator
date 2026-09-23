package scale

import (
	"cmp"
	"context"
	"errors"
	"net/http"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"

	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
)

// liveLookupTimeout bounds asking one node, whose own index answers in milliseconds; a silent node is a miss.
const liveLookupTimeout = 500 * time.Millisecond

type nodeRows func(ctx context.Context, cl SandboxdClient) ([]sandboxd.SandboxSummary, error)

// liveOnNode asks one node's sandboxd for a match its published inventory does not hold yet.
func (s *scatterGatherStore) liveOnNode(ctx context.Context, op, node string, rows nodeRows, match inventoryMatch) *sandboxv1beta1.Sandbox {
	cl, err := s.nodeClient(ctx, node, op, "")
	if err != nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, liveLookupTimeout)
	defer cancel()
	got, err := rows(ctx, cl)
	if err != nil {
		if ctx.Err() == nil {
			s.log.V(1).Info("node did not answer a live "+op, "node", node, "err", err.Error())
		}
		return nil
	}
	inv := &NodeInventory{Node: node, Entries: make([]InventoryEntry, len(got))}
	for i := range got {
		inv.Entries[i] = EntryFromSummary(got[i])
	}
	for i := range inv.Entries {
		if match(inv, i) {
			return entryToSandbox(node, inv.Entries[i])
		}
	}
	return nil
}

// EntryFromSummary is the NodeInventory entry a node publishes for one row of its sandboxd index.
func EntryFromSummary(row sandboxd.SandboxSummary) InventoryEntry {
	name := cmp.Or(row.ClaimRef, row.ID)
	phase := PhaseRunning
	if isPaused(row) {
		phase = PhaseHibernated
	}
	return InventoryEntry{
		Name:      name,
		ID:        row.ID,
		Phase:     phase,
		ClaimRef:  name,
		Template:  row.Key.Template,
		Deadline:  optionalTime(row.Deadline),
		ClaimedAt: optionalTime(row.ClaimedAt),
	}
}

func isPaused(row sandboxd.SandboxSummary) bool { return row.Hibernated || row.Archived }

func rowsByClaimRef(ref string) nodeRows {
	return func(ctx context.Context, cl SandboxdClient) ([]sandboxd.SandboxSummary, error) {
		return cl.SandboxesByClaimRef(ctx, ref)
	}
}

func rowByID(id string) nodeRows {
	return func(ctx context.Context, cl SandboxdClient) ([]sandboxd.SandboxSummary, error) {
		row, err := cl.Sandbox(ctx, id)
		if he, ok := errors.AsType[*sandboxd.HTTPError](err); ok && he.StatusCode == http.StatusNotFound {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return []sandboxd.SandboxSummary{row}, nil
	}
}

func optionalTime(t time.Time) *metav1.Time {
	if t.IsZero() {
		return nil
	}
	return new(metav1.NewTime(t))
}
