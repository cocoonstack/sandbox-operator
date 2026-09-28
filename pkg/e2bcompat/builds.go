package e2bcompat

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apiserver/pkg/storage/names"

	"github.com/cocoonstack/sandbox-operator/pkg/e2bbuild"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

var logLevels = []string{"debug", "info", "warn", "error"}

// requestBuild opens a waiting build of name; a template it replaces keeps serving until the build is ready.
func (s *Server) requestBuild(w http.ResponseWriter, r *http.Request) {
	var req TemplateBuildRequestV3
	if !decodeBody(w, r, &req) {
		return
	}
	name, tag, _ := strings.Cut(cmp.Or(req.Name, req.Alias), ":")
	if name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if _, err := scale.StampedName("template", templatePrefix, s.namespace(r), name); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	tags := slices.Clone(req.Tags)
	if tag != "" && !slices.Contains(tags, tag) {
		tags = append(tags, tag)
	}
	buildID := uuid.NewString()
	s.builds.Register(s.buildKey(r, name, buildID), e2bbuild.Request{Size: sizeFor(req.CPUCount, req.MemoryMB), Tags: tags})
	writeJSON(w, http.StatusAccepted, TemplateRequestResponseV3{
		TemplateID: name, BuildID: buildID, Public: true, Names: taggedNames(name, tags), Tags: append([]string{}, tags...), Aliases: []string{name},
	})
}

// startBuild runs a registered build from its image and steps; a retry of a started build answers 202 again.
func (s *Server) startBuild(w http.ResponseWriter, r *http.Request) {
	var req TemplateBuildStartV2
	if !decodeBody(w, r, &req) {
		return
	}
	if msg, bad := unsupportedBuildOption(req, s.opts.Builds.Uploads != nil); bad {
		writeError(w, http.StatusBadRequest, msg)
		return
	}
	name, buildID := r.PathValue("templateID"), r.PathValue("buildID")
	id := s.buildKey(r, name, buildID)
	pending, ok := s.builds.Request(id)
	if !ok {
		writeError(w, http.StatusNotFound, "build not found on this replica")
		return
	}
	scope := s.templateScope(r)
	pool := s.poolKey(req.FromImage)
	pool.Net = scale.NetDefault
	pool.Size = cmp.Or(pending.Size, pool.Size)
	err := s.builds.Start(r.Context(), id, e2bbuild.Spec{
		Namespace:  s.namespace(r),
		ClaimName:  names.SimpleNameGenerator.GenerateName(namePrefix + "build-"),
		Pool:       pool,
		Template:   scope + name,
		TTLSeconds: int(s.opts.Builds.Timeout / time.Second),
		Steps:      req.Steps,
		StartCmd:   req.StartCmd,
		ReadyCmd:   req.ReadyCmd,
		RelayEnvs:  relayEnvs,
		Archive:    s.archive(s.namespace(r), name),
		Publish:    s.publishBuild(scope, name, pending.Tags),
	})
	switch {
	case errors.Is(err, e2bbuild.ErrBusy):
		writeError(w, http.StatusTooManyRequests, "every build slot is busy; retry the start")
	case errors.Is(err, e2bbuild.ErrUnknownBuild):
		writeError(w, http.StatusNotFound, "build not found on this replica")
	default:
		w.WriteHeader(http.StatusAccepted)
	}
}

func (s *Server) buildStatus(w http.ResponseWriter, r *http.Request) {
	name, buildID := r.PathValue("templateID"), r.PathValue("buildID")
	info, ok := s.builds.Status(s.buildKey(r, name, buildID))
	if !ok {
		writeError(w, http.StatusNotFound, "build not found on this replica")
		return
	}
	q := r.URL.Query()
	offset, err := queryInt(q.Get("logsOffset"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "logsOffset must be a non-negative integer")
		return
	}
	limit, err := queryInt(q.Get("limit"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "limit must be a non-negative integer")
		return
	}
	floor := max(slices.Index(logLevels, q.Get("level")), 0)
	var entries []BuildLogEntry
	for _, l := range info.Logs {
		if slices.Index(logLevels, l.Level) >= floor {
			entries = append(entries, logEntryOf(l))
		}
	}
	entries = entries[min(offset, len(entries)):]
	if limit > 0 {
		entries = entries[:min(limit, len(entries))]
	}
	out := TemplateBuildInfo{TemplateID: name, BuildID: buildID, Status: info.Status, Logs: []string{}, LogEntries: append([]BuildLogEntry{}, entries...)}
	if info.Status == e2bbuild.StatusError {
		out.Reason = &BuildStatusReason{Message: info.Failure, Step: sdkStep(info.FailedPhase), LogEntries: []BuildLogEntry{}}
		if n := len(info.Logs); n > 0 {
			out.Reason.LogEntries = append(out.Reason.LogEntries, logEntryOf(info.Logs[n-1]))
		}
	}
	writeJSON(w, http.StatusOK, out)
}

// publishBuild deletes the holders of name older than this build's promote, so concurrent builds converge on the newest, then tags it.
// It asks each node itself: an inventory a tick behind would miss a build that finished moments ago.
func (s *Server) publishBuild(scope, name string, tags []string) func(context.Context, string, scale.PoolKey, string) error {
	return func(ctx context.Context, node string, key scale.PoolKey, digest string) error {
		holders, err := s.liveHolders(ctx, scope+name)
		if err != nil {
			return fmt.Errorf("list the previous builds: %w", err)
		}
		i := slices.IndexFunc(holders, func(h templateHolder) bool { return h.node == node && h.key == key })
		if i < 0 {
			return fmt.Errorf("the build's template is no longer on %s", node)
		}
		own := holders[i]
		stale := slices.DeleteFunc(holders, func(h templateHolder) bool { return !h.created.Before(own.created) })
		if err := forEachHolder(stale, func(h templateHolder) error { return s.store.DeleteTemplate(ctx, h.node, h.key) }); err != nil {
			return fmt.Errorf("delete the previous build: %w", err)
		}
		if len(tags) == 0 {
			return nil
		}
		labels := make(map[string]string, len(tags))
		for _, t := range tags {
			labels[t] = digest
		}
		return s.store.SetTemplateLabels(ctx, node, key, labels)
	}
}

// liveHolders reads every node's operator-owned copies of template from the node itself; one node that does not answer fails the read.
func (s *Server) liveHolders(ctx context.Context, template string) ([]templateHolder, error) {
	nodes, err := s.opts.Inventory.ListNodes(ctx)
	if err != nil {
		return nil, err
	}
	var (
		mu      sync.Mutex
		holders []templateHolder
		g       errgroup.Group
	)
	g.SetLimit(maxNodeConcurrency)
	for _, n := range nodes {
		g.Go(func() error {
			held, err := s.store.NodeTemplates(ctx, n)
			if err != nil {
				return fmt.Errorf("node %s: %w", n, err)
			}
			mu.Lock()
			defer mu.Unlock()
			for _, t := range held {
				if t.Template == template && t.Tenant == "" {
					h := templateHolder{node: n, key: scale.PoolKey{Template: t.Template, Net: t.Net, Size: t.Size}}
					if t.CreatedAt != nil {
						h.created = t.CreatedAt.Time
					}
					holders = append(holders, h)
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return holders, nil
}

func (s *Server) buildKey(r *http.Request, name, buildID string) string {
	return s.namespace(r) + "/" + name + "/" + buildID
}

// unsupportedBuildOption refuses what a build cannot honor, a malformed step included.
func unsupportedBuildOption(req TemplateBuildStartV2, canCopy bool) (string, bool) {
	switch {
	case req.FromTemplate != "":
		return "fromTemplate is not supported yet; build from an image", true
	case len(req.FromImageRegistry) > 0 && string(req.FromImageRegistry) != "null":
		return "fromImageRegistry is not supported; a pool pulls its own image", true
	case req.FromImage == "":
		return "fromImage is required", true
	case strings.HasPrefix(req.FromImage, templatePrefix):
		return "fromImage names a built template; build from an image", true
	}
	msg := e2bbuild.Invalid(req.Steps, canCopy)
	return msg, msg != ""
}

// sizeFor maps a build's cpuCount and memoryMB onto the size class a pod's requests would get, empty when neither is set.
func sizeFor(cpuCount, memoryMB int32) string {
	if cpuCount <= 0 && memoryMB <= 0 {
		return ""
	}
	requests := corev1.ResourceList{}
	if cpuCount > 0 {
		requests[corev1.ResourceCPU] = *resource.NewQuantity(int64(cpuCount), resource.DecimalSI)
	}
	if memoryMB > 0 {
		requests[corev1.ResourceMemory] = *resource.NewQuantity(int64(memoryMB)<<20, resource.BinarySI)
	}
	return scale.SizeClassForContainers([]corev1.Container{{Resources: corev1.ResourceRequirements{Requests: requests}}})
}

func logEntryOf(l e2bbuild.LogEntry) BuildLogEntry {
	return BuildLogEntry{Timestamp: l.Time.UTC().Format(time.RFC3339Nano), Message: l.Message, Level: l.Level, Step: sdkStep(l.Phase)}
}

// sdkStep names a build phase as the SDK maps steps onto its stack traces: base for the image, a step by its number, finalize for the rest.
func sdkStep(phase string) string {
	switch phase {
	case e2bbuild.PhaseClaim:
		return "base"
	case e2bbuild.PhaseFinalize, e2bbuild.PhasePromote, e2bbuild.PhasePublish:
		return "finalize"
	}
	return phase
}

func queryInt(v string) (int, error) {
	if v == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0, errors.New("negative or malformed")
	}
	return n, nil
}
