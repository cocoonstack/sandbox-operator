package e2bcompat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"time"

	"github.com/projecteru2/core/log"
	"golang.org/x/sync/errgroup"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

// maxNodeConcurrency bounds a fleet-wide fan-out so a handful of wedged nodes
// cannot serialize a handler into the minutes.
const maxNodeConcurrency = 16

// pauseSandbox hibernates the sandbox: its memory is written out and the VM
// stops, so the cost is proportional to guest RAM. e2b's contract is specific
// about the already-paused case — the SDK reads 409 as "already paused" and
// returns false rather than raising — so that state is reported, not retried.
func (s *Server) pauseSandbox(w http.ResponseWriter, r *http.Request) {
	var req SandboxPauseRequest
	if !decodeOptionalBody(w, r, &req) {
		return
	}
	// memory=false asks for a filesystem-only snapshot whose resume cold-boots.
	// The node's hibernate always captures memory, so honoring it would mean
	// silently giving back a different sandbox than asked for.
	if req.Memory != nil && !*req.Memory {
		writeError(w, http.StatusBadRequest,
			"filesystem-only pause (memory=false) is not supported; this backend always snapshots memory")
		return
	}
	id := r.PathValue("sandboxID")
	sb, err := s.lookup(r, id)
	if err != nil {
		s.writeLookupError(w, r, err, "pause")
		return
	}
	paused, err := s.isPaused(r.Context(), sb)
	if err != nil {
		s.writeLookupError(w, r, err, "pause")
		return
	}
	if paused {
		writeError(w, http.StatusConflict, fmt.Sprintf("sandbox %q is already paused", id))
		return
	}
	if err := s.store.Pause(r.Context(), sb.Status.NodeName, claimIDOf(sb)); err != nil {
		s.writeVerbError(w, r, err, "pause", "failed to pause the sandbox")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// connectSandbox is the SDK's resume: it returns the sandbox's connection
// details, restoring it first when paused. 200 means it was already running,
// 201 that it was paused and got resumed — the SDK accepts either, and the
// distinction is what tells an operator whether a restore actually happened.
func (s *Server) connectSandbox(w http.ResponseWriter, r *http.Request) {
	var req ConnectSandbox
	if !decodeOptionalBody(w, r, &req) {
		return
	}
	if req.Memory != nil && !*req.Memory {
		writeError(w, http.StatusBadRequest, "memory=false is not supported; a paused sandbox resumes from its memory snapshot")
		return
	}
	id := r.PathValue("sandboxID")
	sb, err := s.lookup(r, id)
	if err != nil {
		s.writeLookupError(w, r, err, "connect")
		return
	}
	node, claimID := sb.Status.NodeName, claimIDOf(sb)
	rec, err := s.store.Read(r.Context(), node, claimID)
	if err != nil {
		s.writeVerbError(w, r, err, "connect: read", "failed to connect the sandbox")
		return
	}
	status := http.StatusOK
	if rec.Paused {
		if err = s.store.Resume(r.Context(), node, claimID); err != nil {
			s.writeVerbError(w, r, err, "connect: resume", "failed to resume the sandbox")
			return
		}
		status = http.StatusCreated
		if rec, err = s.store.Read(r.Context(), node, claimID); err != nil {
			s.writeVerbError(w, r, err, "connect: read", "failed to connect the sandbox")
			return
		}
	}
	if ttl := s.timeoutSeconds(req.Timeout); time.Now().Add(time.Duration(ttl) * time.Second).After(rec.Deadline) {
		if _, err := s.store.Renew(r.Context(), node, claimID, ttl); err != nil {
			s.writeVerbError(w, r, err, "connect: renew", "failed to connect the sandbox")
			return
		}
	}
	writeJSON(w, status, Sandbox{
		TemplateID:      templateOf(sb),
		SandboxID:       PublicID(claimID),
		ClientID:        node,
		EnvdVersion:     s.opts.EnvdVersion,
		EnvdAccessToken: rec.Token,
		Domain:          s.opts.Domain,
	})
}

// forkSandbox branches the sandbox into count children. The parent is
// checkpointed in place and keeps running; every child is a fresh sandbox with
// its own id and lease. Per e2b's contract a partial failure is still a 201
// carrying per-child detail — a non-201 means nothing was attempted — so the
// node's all-or-nothing fork is reported as a whole-request failure only when
// it rejects the request outright.
func (s *Server) forkSandbox(w http.ResponseWriter, r *http.Request) {
	var req SandboxForkRequest
	if !decodeOptionalBody(w, r, &req) {
		return
	}
	count := int32(1)
	if req.Count != nil {
		count = *req.Count
	}
	if count < 1 {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("count must be >= 1, got %d", count))
		return
	}
	if req.Timeout != nil && *req.Timeout < 0 {
		writeError(w, http.StatusBadRequest, "timeout must be >= 0")
		return
	}
	id := r.PathValue("sandboxID")
	sb, err := s.lookup(r, id)
	if err != nil {
		s.writeLookupError(w, r, err, "fork")
		return
	}
	paused, err := s.isPaused(r.Context(), sb)
	if err != nil {
		s.writeLookupError(w, r, err, "fork")
		return
	}
	if paused {
		writeError(w, http.StatusConflict,
			fmt.Sprintf("sandbox %q is paused and cannot be forked; resume it first", id))
		return
	}
	children, err := s.store.Fork(r.Context(), s.namespace(r), sb.Status.NodeName, claimIDOf(sb), int(count), s.timeoutSeconds(req.Timeout))
	if err != nil {
		s.writeVerbError(w, r, err, "fork", "failed to fork the sandbox")
		return
	}
	template := templateOf(sb)
	out := make([]SandboxForkResult, 0, len(children))
	for _, child := range children {
		out = append(out, SandboxForkResult{Sandbox: &Sandbox{
			TemplateID:      template,
			SandboxID:       PublicID(child.SandboxName),
			ClientID:        child.Node,
			EnvdVersion:     s.opts.EnvdVersion,
			EnvdAccessToken: child.Token,
			Domain:          s.opts.Domain,
		}})
	}
	writeJSON(w, http.StatusCreated, out)
}

// createSnapshot captures the sandbox's state as a checkpoint later sandboxes
// can branch from. The source keeps running.
func (s *Server) createSnapshot(w http.ResponseWriter, r *http.Request) {
	var req SandboxSnapshotRequest
	if !decodeOptionalBody(w, r, &req) {
		return
	}
	id := r.PathValue("sandboxID")
	sb, err := s.lookup(r, id)
	if err != nil {
		s.writeLookupError(w, r, err, "snapshot")
		return
	}
	name, err := scale.CheckpointName(s.namespace(r), req.Name)
	if err != nil {
		s.writeVerbError(w, r, err, "snapshot", "failed to snapshot the sandbox")
		return
	}
	snap, err := s.store.Snapshot(r.Context(), sb.Status.NodeName, claimIDOf(sb), name)
	if err != nil {
		s.writeVerbError(w, r, err, "snapshot", "failed to snapshot the sandbox")
		return
	}
	snap.Name = req.Name
	writeJSON(w, http.StatusCreated, snapshotInfo(snap))
}

// listSnapshots reports the caller's checkpoints, narrowed by the sandboxID and name filters.
func (s *Server) listSnapshots(w http.ResponseWriter, r *http.Request) {
	snaps, _, err := s.snapshotsOf(r)
	if err != nil {
		log.WithFunc("e2bcompat.listSnapshots").Error(r.Context(), err, "e2b list snapshots failed")
		writeError(w, http.StatusInternalServerError, "failed to list snapshots")
		return
	}
	sandboxID, name := r.URL.Query().Get("sandboxID"), r.URL.Query().Get("name")
	out := make([]SnapshotInfo, 0, len(snaps))
	for _, snap := range snaps {
		if (sandboxID != "" && !MatchesID(snap.SandboxID, sandboxID)) || (name != "" && snap.Name != name) {
			continue
		}
		out = append(out, snapshotInfo(snap))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) deleteSnapshot(w http.ResponseWriter, r *http.Request) {
	snapshotID := r.PathValue("snapshotID")
	snaps, complete, err := s.snapshotsOf(r)
	if err != nil {
		log.WithFunc("e2bcompat.deleteSnapshot").Errorf(r.Context(), err, "e2b delete snapshot: listing failed snapshotID=%s", snapshotID)
		writeError(w, http.StatusInternalServerError, "failed to delete the snapshot")
		return
	}
	i := slices.IndexFunc(snaps, func(snap scale.Snapshot) bool { return snap.ID == snapshotID })
	switch {
	case i < 0 && !complete:
		writeError(w, http.StatusInternalServerError, "failed to delete the snapshot: a node did not answer")
		return
	case i < 0:
		writeError(w, http.StatusNotFound, "snapshot not found")
		return
	}
	if err := s.store.DeleteSnapshot(r.Context(), snaps[i].Node, snapshotID); err != nil {
		log.WithFunc("e2bcompat.deleteSnapshot").Errorf(r.Context(), err, "e2b delete snapshot failed node=%s snapshotID=%s", snaps[i].Node, snapshotID)
		writeError(w, http.StatusInternalServerError, "failed to delete the snapshot")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// snapshotsOf lists the caller's checkpoints across the nodes with the
// namespace stamp stripped; complete is false when a node did not answer.
func (s *Server) snapshotsOf(r *http.Request) (snaps []scale.Snapshot, complete bool, err error) {
	nodes, err := s.nodesWithSandboxes(r)
	if err != nil {
		return nil, false, err
	}
	ns := s.namespace(r)
	perNode := make([][]scale.Snapshot, len(nodes))
	answered := make([]bool, len(nodes))
	var g errgroup.Group
	g.SetLimit(maxNodeConcurrency)
	for i, node := range nodes {
		g.Go(func() error {
			snaps, err := s.store.Snapshots(r.Context(), node)
			if err != nil {
				log.WithFunc("e2bcompat.snapshotsOf").Errorf(r.Context(), err, "e2b snapshots: node failed node=%s", node)
				return nil
			}
			answered[i] = true
			for _, snap := range snaps {
				if name, ok := scale.CheckpointNameIn(ns, snap.Name); ok {
					snap.Name = name
					perNode[i] = append(perNode[i], snap)
				}
			}
			return nil
		})
	}
	_ = g.Wait()
	return slices.Concat(perNode...), !slices.Contains(answered, false), nil
}

// sandboxMetrics reports one sandbox's resource usage. e2b's schema requires
// every field, so all are emitted; the ones this backend cannot measure are
// reported as zero rather than invented (see SandboxStats).
func (s *Server) sandboxMetrics(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("sandboxID")
	sb, err := s.lookup(r, id)
	if err != nil {
		s.writeLookupError(w, r, err, "metrics")
		return
	}
	st, err := s.store.Stats(r.Context(), sb.Status.NodeName, claimIDOf(sb))
	if err != nil {
		s.writeVerbError(w, r, err, "metrics", "failed to read sandbox metrics")
		return
	}
	at := st.MeasuredAt
	if at.IsZero() {
		at = time.Now()
	}
	writeJSON(w, http.StatusOK, []SandboxMetric{{
		Timestamp:     at.UTC().Format(time.RFC3339),
		TimestampUnix: at.Unix(),
		CPUCount:      int32(st.CPUCount),
		MemUsed:       st.MemUsedBytes,
		MemTotal:      st.MemTotalBytes,
	}})
}

// listTemplates reports the pools this fleet can serve claims from. e2b's
// templates are build artifacts with their own lifecycle; the equivalent here
// is the set of warm-pool keys nodes advertise, which is what a caller can
// actually pass as templateID on create.
func (s *Server) listTemplates(w http.ResponseWriter, r *http.Request) {
	nodes, err := s.inventories(r)
	if err != nil {
		log.WithFunc("e2bcompat.listTemplates").Error(r.Context(), err, "e2b list templates failed")
		writeError(w, http.StatusInternalServerError, "failed to list templates")
		return
	}
	seen := map[string]struct{}{}
	out := []Template{}
	for _, inv := range nodes {
		for _, pc := range inv.Pools {
			if pc.Template == "" {
				continue
			}
			if _, dup := seen[pc.Template]; dup {
				continue
			}
			seen[pc.Template] = struct{}{}
			out = append(out, Template{
				TemplateID:  pc.Template,
				BuildID:     pc.Template,
				Public:      true,
				Aliases:     []string{},
				Names:       []string{pc.Template},
				EnvdVersion: s.opts.EnvdVersion,
			})
		}
	}
	writeJSON(w, http.StatusOK, out)
}

// inventories returns every node's published inventory, the fleet view the
// pool-derived surfaces (templates, snapshot listing) are assembled from.
func (s *Server) inventories(r *http.Request) ([]*scale.NodeInventory, error) {
	if s.opts.Inventory == nil {
		return nil, errors.New("e2bcompat: no inventory source configured")
	}
	nodes, err := s.opts.Inventory.ListNodes(r.Context())
	if err != nil {
		return nil, err
	}
	out := make([]*scale.NodeInventory, 0, len(nodes))
	for _, node := range nodes {
		inv, err := s.opts.Inventory.NodeInventory(r.Context(), node)
		if err != nil {
			// A partitioned node is skipped, not fatal — the same rule the
			// aggregated read path applies.
			log.WithFunc("e2bcompat.inventories").Errorf(r.Context(), err, "e2b: node inventory unavailable node=%s", node)
			continue
		}
		out = append(out, inv)
	}
	return out, nil
}

// nodesWithSandboxes lists the nodes a checkpoint could live on.
func (s *Server) nodesWithSandboxes(r *http.Request) ([]string, error) {
	if s.opts.Inventory == nil {
		return nil, errors.New("e2bcompat: no inventory source configured")
	}
	return s.opts.Inventory.ListNodes(r.Context())
}

// isPaused asks the owning node: the listed phase comes from NodeInventory,
// which lags a pause by up to its publish cadence, and a stale Running would
// turn e2b's 409 "already paused" into a second 204. An unreachable node falls
// back to the cached label.
func (s *Server) isPaused(ctx context.Context, sb *sandboxv1beta1.Sandbox) (bool, error) {
	if node, id := sb.Status.NodeName, claimIDOf(sb); node != "" && id != "" {
		rec, err := s.store.Read(ctx, node, id)
		switch {
		case err == nil:
			return rec.Paused, nil
		case k8serrors.IsNotFound(err):
			return false, errSandboxNotFound
		}
	}
	return sb.Labels[scale.PhaseLabel] == scale.PhaseHibernated, nil
}

func (s *Server) writeVerbError(w http.ResponseWriter, r *http.Request, err error, op, msg string) {
	id := r.PathValue("sandboxID")
	if k8serrors.IsNotFound(err) || errors.Is(err, errSandboxNotFound) {
		writeError(w, http.StatusNotFound, fmt.Sprintf("sandbox %q not found", id))
		return
	}
	if se, ok := errors.AsType[*k8serrors.StatusError](err); ok && se.ErrStatus.Code >= http.StatusBadRequest && se.ErrStatus.Code < http.StatusInternalServerError {
		writeError(w, int(se.ErrStatus.Code), se.ErrStatus.Message)
		return
	}
	log.WithFunc("e2bcompat.writeVerbError").Errorf(r.Context(), err, "e2b %s failed sandboxID=%s", op, id)
	writeError(w, http.StatusInternalServerError, msg)
}

// timeoutSeconds resolves an optional TTL to the configured default.
func (s *Server) timeoutSeconds(v *int32) int {
	if v == nil {
		return s.opts.DefaultTimeoutSeconds
	}
	return int(*v)
}

func decodeBody(w http.ResponseWriter, r *http.Request, out any) bool {
	return reportBadBody(w, json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(out))
}

func decodeOptionalBody(w http.ResponseWriter, r *http.Request, out any) bool {
	err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(out)
	if errors.Is(err, io.EOF) {
		return true
	}
	return reportBadBody(w, err)
}

func reportBadBody(w http.ResponseWriter, err error) bool {
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return false
	}
	return true
}

// claimIDOf reports the node-local claim id the store's verbs address.
func claimIDOf(sb *sandboxv1beta1.Sandbox) string {
	return sb.Annotations[scale.ClaimIDAnnotation]
}

// snapshotInfo renders a checkpoint in e2b's snapshot shape.
func snapshotInfo(snap scale.Snapshot) SnapshotInfo {
	names := []string{}
	if snap.Name != "" {
		names = append(names, snap.Name)
	}
	return SnapshotInfo{SnapshotID: snap.ID, Names: names}
}
