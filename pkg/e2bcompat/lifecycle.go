package e2bcompat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/projecteru2/core/log"
	"golang.org/x/sync/errgroup"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"

	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

// maxNodeConcurrency bounds a fleet-wide fan-out so a handful of wedged nodes
// cannot serialize a handler into the minutes.
const (
	maxNodeConcurrency = 16
	maxMetricsIDs      = 100
)

// pauseSandbox answers 409 when already paused because the e2b SDK reads 409 as "already paused".
func (s *Server) pauseSandbox(w http.ResponseWriter, r *http.Request) {
	var req SandboxPauseRequest
	if !decodeOptionalBody(w, r, &req) {
		return
	}
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

// connectSandbox is the e2b SDK's resume; the SDK accepts both 200 and 201.
func (s *Server) connectSandbox(w http.ResponseWriter, r *http.Request) {
	var req ConnectSandbox
	if !decodeOptionalBody(w, r, &req) {
		return
	}
	s.connect(w, r, req, false, nil)
}

func (s *Server) resumeSandbox(w http.ResponseWriter, r *http.Request) {
	var req ResumedSandbox
	if !decodeOptionalBody(w, r, &req) {
		return
	}
	s.connect(w, r, req.ConnectSandbox, true, req.AutoPause)
}

func (s *Server) connect(w http.ResponseWriter, r *http.Request, req ConnectSandbox, refuseRunning bool, autoPause *bool) {
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
		s.writeVerbError(w, r, err, "connect: read", connectFailure)
		return
	}
	if !rec.Paused && refuseRunning {
		writeError(w, http.StatusConflict, fmt.Sprintf("sandbox %q is already running", id))
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
			s.writeVerbError(w, r, err, "connect: read", connectFailure)
			return
		}
	}
	ttl, left := s.timeoutSeconds(req.Timeout), int(math.Ceil(time.Until(rec.Deadline).Seconds()))
	if onExpire := expireFor(autoPause); ttl > left || onExpire != "" {
		if _, err := s.store.Renew(r.Context(), node, claimID, max(ttl, left), onExpire); err != nil {
			s.writeVerbError(w, r, err, "connect: renew", connectFailure)
			return
		}
	}
	writeJSON(w, status, Sandbox{
		TemplateID:      templateOf(sb),
		Alias:           s.aliasOf(templateOf(sb)),
		SandboxID:       PublicID(claimID),
		ClientID:        node,
		EnvdVersion:     s.opts.EnvdVersion,
		EnvdAccessToken: AccessToken(s.opts.EnvdSecret, rec.Token),
		Domain:          s.opts.Domain,
	})
}

// forkSandbox sets no per-child error because the node fork is all-or-nothing.
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
	alias := s.aliasOf(template)
	out := make([]SandboxForkResult, len(children))
	var g errgroup.Group
	g.SetLimit(maxNodeConcurrency)
	for i, child := range children {
		token := AccessToken(s.opts.EnvdSecret, child.Token)
		out[i] = SandboxForkResult{Sandbox: &Sandbox{
			TemplateID:      template,
			Alias:           alias,
			SandboxID:       PublicID(child.SandboxName),
			ClientID:        child.Node,
			EnvdVersion:     s.opts.EnvdVersion,
			EnvdAccessToken: token,
			Domain:          s.opts.Domain,
		}}
		g.Go(func() error { return s.handOver(r.Context(), child, token) })
	}
	if err := g.Wait(); err != nil {
		s.releaseAll(r.Context(), children)
		s.writeVerbError(w, r, err, "fork: envd init", "failed to fork the sandbox")
		return
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
	logger := log.WithFunc("e2bcompat.deleteSnapshot")
	snapshotID := r.PathValue("templateID")
	snaps, complete, err := s.snapshotsOf(r)
	if err != nil {
		logger.Errorf(r.Context(), err, "e2b delete snapshot: listing failed snapshotID=%s", snapshotID)
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
		logger.Errorf(r.Context(), err, "e2b delete snapshot failed node=%s snapshotID=%s", snaps[i].Node, snapshotID)
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
				log.WithFunc("e2bcompat.snapshotsOf").Warnf(r.Context(), "e2b snapshots: node failed node=%s err=%v", node, err)
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

func (s *Server) sandboxMetrics(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("sandboxID")
	sb, err := s.lookup(r, id)
	if err != nil {
		s.writeLookupError(w, r, err, "metrics")
		return
	}
	m, live, err := s.readEnvdMetrics(r.Context(), sb.Status.NodeName, claimIDOf(sb))
	if err != nil {
		s.writeVerbError(w, r, err, "metrics", "failed to read sandbox metrics")
		return
	}
	out := []SandboxMetric{}
	if live {
		out = append(out, metricOf(m))
	}
	writeJSON(w, http.StatusOK, out)
}

// sandboxesMetrics answers for the running sandboxes among the ids; one the key cannot see, a paused one, or a failed read is left out.
func (s *Server) sandboxesMetrics(w http.ResponseWriter, r *http.Request) {
	ids, err := sandboxIDsOf(r.URL.Query().Get("sandbox_ids"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var mu sync.Mutex
	out := make(map[string]SandboxMetric, len(ids))
	var g errgroup.Group
	g.SetLimit(maxNodeConcurrency)
	for _, id := range ids {
		g.Go(func() error {
			sb, err := s.lookup(r, id)
			if err != nil {
				return nil
			}
			m, live, err := s.readEnvdMetrics(r.Context(), sb.Status.NodeName, claimIDOf(sb))
			if err != nil {
				log.WithFunc("e2bcompat.sandboxesMetrics").Warnf(r.Context(), "metrics read failed sandboxID=%s err=%v", id, err)
				return nil
			}
			if live {
				mu.Lock()
				out[id] = metricOf(m)
				mu.Unlock()
			}
			return nil
		})
	}
	_ = g.Wait()
	writeJSON(w, http.StatusOK, SandboxesWithMetrics{Sandboxes: out})
}

func (s *Server) sandboxLogs(reply any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, err := s.lookup(r, r.PathValue("sandboxID")); err != nil {
			s.writeLookupError(w, r, err, "logs")
			return
		}
		writeJSON(w, http.StatusOK, reply)
	}
}

// inventories is the fleet's claimable pools and templates, the view the template surfaces are assembled from.
func (s *Server) inventories(ctx context.Context) ([]scale.NodePools, error) {
	if s.opts.Inventory == nil {
		return nil, errors.New("e2bcompat: no inventory source configured")
	}
	return s.opts.Inventory.NodeCapacities(ctx)
}

// nodesWithSandboxes lists the nodes a checkpoint could live on.
func (s *Server) nodesWithSandboxes(r *http.Request) ([]string, error) {
	if s.opts.Inventory == nil {
		return nil, errors.New("e2bcompat: no inventory source configured")
	}
	return s.opts.Inventory.ListNodes(r.Context())
}

// isPaused asks the owning node because the NodeInventory phase lags a pause by up to one publish.
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

// expireFor maps a resume's autoPause onto the claim's lease-end action; nil keeps the current one.
func expireFor(autoPause *bool) sandboxd.ExpireAction {
	switch {
	case autoPause == nil:
		return ""
	case *autoPause:
		return sandboxd.ExpireArchive
	}
	return sandboxd.ExpireDestroy
}

func metricOf(m envdMetrics) SandboxMetric {
	at := time.Unix(m.Timestamp, 0)
	if m.Timestamp == 0 {
		at = time.Now()
	}
	return SandboxMetric{
		Timestamp:     at.UTC().Format(time.RFC3339),
		TimestampUnix: at.Unix(),
		CPUCount:      m.CPUCount,
		CPUUsedPct:    m.CPUUsedPct,
		MemUsed:       m.MemUsed,
		MemTotal:      m.MemTotal,
		MemCache:      m.MemCache,
		DiskUsed:      m.DiskUsed,
		DiskTotal:     m.DiskTotal,
	}
}

// sandboxIDsOf parses sandbox_ids, the spec's comma-separated list of up to 100 distinct ids.
func sandboxIDsOf(raw string) ([]string, error) {
	var ids []string
	for id := range strings.SplitSeq(raw, ",") {
		if id = strings.TrimSpace(id); id == "" {
			return nil, errors.New("sandbox_ids must list comma-separated sandbox ids")
		}
		if len(ids) == maxMetricsIDs {
			return nil, fmt.Errorf("sandbox_ids must list at most %d ids", maxMetricsIDs)
		}
		if slices.Contains(ids, id) {
			return nil, fmt.Errorf("sandbox_ids repeats %q", id)
		}
		ids = append(ids, id)
	}
	return ids, nil
}
