package scale

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"

	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
)

// maxCheckpointName is sandboxd's name budget (types.NameRe).
const maxCheckpointName = 63

func (s *scatterGatherStore) Pause(ctx context.Context, node, id string) error {
	cl, err := s.nodeClient(ctx, node, "pause", id)
	if err != nil {
		return err
	}
	if err := cl.Hibernate(ctx, id); err != nil {
		return nodeVerbError(err, "pause", id, node)
	}
	return nil
}

func (s *scatterGatherStore) Renew(ctx context.Context, node, id string, ttlSeconds int) (time.Time, error) {
	cl, err := s.nodeClient(ctx, node, "renew", id)
	if err != nil {
		return time.Time{}, err
	}
	deadline, err := cl.Renew(ctx, id, sandboxd.RenewSpec{TTLSeconds: ttlSeconds})
	if err != nil {
		return time.Time{}, nodeVerbError(err, "renew", id, node)
	}
	return deadline, nil
}

func (s *scatterGatherStore) Resume(ctx context.Context, node, id string) error {
	cl, err := s.nodeClient(ctx, node, "resume", id)
	if err != nil {
		return err
	}
	if err := cl.Wake(ctx, id); err != nil {
		return nodeVerbError(err, "resume", id, node)
	}
	return nil
}

func (s *scatterGatherStore) Fork(ctx context.Context, node, id string, count, ttlSeconds int) ([]Assignment, error) {
	if count < 1 {
		return nil, fmt.Errorf("scale: fork count must be >= 1, got %d", count)
	}
	cl, err := s.nodeClient(ctx, node, "fork", id)
	if err != nil {
		return nil, err
	}
	res, err := cl.Fork(ctx, id, sandboxd.ForkSpec{Count: count, TTLSeconds: ttlSeconds})
	if err != nil {
		return nil, nodeVerbError(err, "fork", id, node)
	}
	out := make([]Assignment, 0, len(res.Children))
	for _, c := range res.Children {
		out = append(out, Assignment{
			SandboxName: c.ID,
			Node:        node,
			Address:     c.OwnerAddr,
			Token:       c.Token,
			Deadline:    c.Deadline,
		})
	}
	return out, nil
}

func (s *scatterGatherStore) Snapshot(ctx context.Context, node, id, name string) (Snapshot, error) {
	cl, err := s.nodeClient(ctx, node, "snapshot", id)
	if err != nil {
		return Snapshot{}, err
	}
	ck, err := cl.Checkpoint(ctx, id, sandboxd.CheckpointSpec{Name: name})
	if err != nil {
		return Snapshot{}, nodeVerbError(err, "snapshot", id, node)
	}
	return snapshotFrom(ck, node), nil
}

func (s *scatterGatherStore) Snapshots(ctx context.Context, node string) ([]Snapshot, error) {
	cl, err := s.nodeClient(ctx, node, "list snapshots", "")
	if err != nil {
		return nil, err
	}
	cks, err := cl.Checkpoints(ctx)
	if err != nil {
		return nil, fmt.Errorf("scale: sandboxd list snapshots on node %q: %w", node, err)
	}
	out := make([]Snapshot, 0, len(cks))
	for _, ck := range cks {
		out = append(out, snapshotFrom(ck, node))
	}
	return out, nil
}

func (s *scatterGatherStore) DeleteSnapshot(ctx context.Context, node, snapshotID string) error {
	cl, err := s.nodeClient(ctx, node, "delete snapshot", snapshotID)
	if err != nil {
		return err
	}
	if err := cl.DeleteCheckpoint(ctx, snapshotID); err != nil {
		return fmt.Errorf("scale: sandboxd delete snapshot %q on node %q: %w", snapshotID, node, err)
	}
	return nil
}

func (s *scatterGatherStore) Stats(ctx context.Context, node, id string) (SandboxStats, error) {
	cl, err := s.nodeClient(ctx, node, "stats", id)
	if err != nil {
		return SandboxStats{}, err
	}
	st, err := cl.Stats(ctx, id)
	if err != nil {
		return SandboxStats{}, nodeVerbError(err, "stats", id, node)
	}
	return SandboxStats{
		CPUCount:        st.CPUCount,
		MemTotalBytes:   st.MemTotalBytes,
		MemUsedBytes:    st.MemUsedBytes,
		MemUsedMeasured: st.MemUsedMeasured,
		Paused:          st.Hibernated,
		MeasuredAt:      st.MeasuredAt,
	}, nil
}

func (s *scatterGatherStore) Read(ctx context.Context, node, id string) (SandboxRecord, error) {
	cl, err := s.nodeClient(ctx, node, "read", id)
	if err != nil {
		return SandboxRecord{}, err
	}
	row, err := cl.Sandbox(ctx, id)
	if err != nil {
		return SandboxRecord{}, nodeVerbError(err, "read", id, node)
	}
	return SandboxRecord{Token: row.Token, Paused: row.Hibernated || row.Archived, Deadline: row.Deadline}, nil
}

// nodeClient resolves a node's advertised sandboxd address and returns a client
// for it, failing closed when claim routing was never configured.
func (s *scatterGatherStore) nodeClient(ctx context.Context, node, verb, id string) (SandboxdClient, error) {
	if s.sandboxdFactory == nil {
		return nil, fmt.Errorf("scale: claim routing not configured (call WithClaimRouting)")
	}
	if node == "" {
		return nil, fmt.Errorf("scale: %s requires an owning node", verb)
	}
	addr, _, err := s.src.NodeCapacity(ctx, node)
	if err != nil {
		return nil, fmt.Errorf("scale: resolve node %q for %s of %q: %w", node, verb, id, err)
	}
	if addr == "" {
		return nil, fmt.Errorf("scale: node %q advertises no sandboxd address for %s of %q", node, verb, id)
	}
	return s.sandboxdFactory(addr, s.sandboxdToken), nil
}

// CheckpointName stamps name with its namespace, the only per-checkpoint field a node keeps, within the node's name budget.
func CheckpointName(namespace, name string) (string, error) {
	stamped := namespace + "/" + name
	if len(stamped) > maxCheckpointName {
		return "", k8serrors.NewBadRequest(fmt.Sprintf("snapshot name %q: at most %d characters in namespace %q", name, maxCheckpointName-len(namespace)-1, namespace))
	}
	return stamped, nil
}

// CheckpointNameIn strips the namespace stamp, reporting whether the checkpoint belongs to namespace.
func CheckpointNameIn(namespace, stamped string) (string, bool) {
	return strings.CutPrefix(stamped, namespace+"/")
}

func nodeVerbError(err error, verb, id, node string) error {
	he, ok := errors.AsType[*sandboxd.HTTPError](err)
	switch {
	case ok && he.StatusCode == http.StatusNotFound:
		return k8serrors.NewNotFound(sandboxv1beta1.Resource("sandboxes"), id)
	case ok && he.StatusCode >= http.StatusBadRequest && he.StatusCode < http.StatusInternalServerError:
		se := k8serrors.NewGenericServerResponse(he.StatusCode, verb, sandboxv1beta1.Resource("sandboxes"), id, he.Message, 0, false)
		if he.Message != "" {
			se.ErrStatus.Message = fmt.Sprintf("sandboxd %s of %q on node %q: %s", verb, id, node, he.Message)
		}
		return se
	}
	return fmt.Errorf("scale: sandboxd %s of %q on node %q: %w", verb, id, node, err)
}

// snapshotFrom converts a node's checkpoint record to the store's shape.
func snapshotFrom(ck sandboxd.Checkpoint, node string) Snapshot {
	return Snapshot{
		ID:        ck.ID,
		Name:      ck.Name,
		SandboxID: ck.SandboxID,
		Pool:      PoolKey{Template: ck.Key.Template, Net: ck.Key.Net, Size: ck.Key.Size},
		CreatedAt: ck.CreatedAt,
		Node:      node,
	}
}
