package e2bcompat

import (
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/projecteru2/core/log"
	"golang.org/x/sync/errgroup"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

const (
	// templatePrefix scopes the sandboxd templates this surface owns: e2b/<namespace>/<name>.
	templatePrefix = "e2b/"
	buildReady     = "ready"
	defaultTag     = "default"
)

// buildNamespace keys the UUIDv5 the read surfaces derive from a content digest, the same on every replica.
var buildNamespace = uuid.NewSHA1(uuid.NameSpaceURL, []byte("https://github.com/cocoonstack/sandbox-operator/e2b"))

type templateHolder struct {
	node    string
	key     scale.PoolKey
	digest  string
	labels  map[string]string
	created time.Time
}

type builtTemplate struct {
	name     string
	holders  []templateHolder
	digests  map[string]int
	created  time.Time
	cpuCount int32
	memoryMB int32
}

// digest is the one most holders report; a re-promote mid-publish can leave nodes briefly apart.
func (b *builtTemplate) digest() string {
	best := ""
	for _, d := range slices.Sorted(maps.Keys(b.digests)) {
		if best == "" || b.digests[d] > b.digests[best] {
			best = d
		}
	}
	return best
}

// current is a holder of the current build, whose labels are the template's tags.
func (b *builtTemplate) current() templateHolder {
	d := b.digest()
	return b.holders[slices.IndexFunc(b.holders, func(h templateHolder) bool { return h.digest == d })]
}

func (b *builtTemplate) names() []string {
	return taggedNames(b.name, slices.Sorted(maps.Keys(b.liveOnly(b.current().labels))))
}

// liveOnly keeps the tags whose digest a holder still reports; a tag on a replaced build is gone.
func (b *builtTemplate) liveOnly(labels map[string]string) map[string]string {
	out := map[string]string{}
	for tag, d := range labels {
		if b.digests[d] > 0 {
			out[tag] = d
		}
	}
	return out
}

// listTemplates reports the advertised warm-pool keys and the namespace's built templates, the values create accepts as templateID.
func (s *Server) listTemplates(w http.ResponseWriter, r *http.Request) {
	nodes, err := s.inventories(r.Context())
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
				Aliases:     append([]string{}, s.imageAliases[pc.Template]...),
				Names:       []string{pc.Template},
				EnvdVersion: s.opts.EnvdVersion,
				BuildStatus: buildReady,
			})
		}
	}
	built := builtTemplates(nodes, s.templateScope(r), "")
	for _, name := range slices.Sorted(maps.Keys(built)) {
		b := built[name]
		created := b.created.UTC().Format(time.RFC3339)
		out = append(out, Template{
			TemplateID:  name,
			BuildID:     buildUUID(b.digest()),
			CPUCount:    b.cpuCount,
			MemoryMB:    b.memoryMB,
			Public:      true,
			Aliases:     []string{name},
			Names:       b.names(),
			CreatedAt:   created,
			UpdatedAt:   created,
			BuildCount:  int32(len(b.digests)),
			EnvdVersion: s.opts.EnvdVersion,
			BuildStatus: buildReady,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// templateSub serves GET /templates/{templateID}/{sub}, the one pattern that coexists with the alias route; only tags lives there.
func (s *Server) templateSub(w http.ResponseWriter, r *http.Request) {
	if r.PathValue("sub") != "tags" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	b, ok := s.builtTemplate(w, r, r.PathValue("templateID"))
	if !ok {
		return
	}
	tags, err := s.liveTags(r, b)
	if err != nil {
		s.writeTagError(w, r, err, b.name)
		return
	}
	created := b.created.UTC().Format(time.RFC3339)
	out := make([]TemplateTag, 0, len(tags))
	for _, tag := range slices.Sorted(maps.Keys(tags)) {
		out = append(out, TemplateTag{Tag: tag, BuildID: buildUUID(tags[tag]), CreatedAt: created})
	}
	writeJSON(w, http.StatusOK, out)
}

// templateAlias resolves the alias table, then a built template of the namespace, then an advertised pool image.
func (s *Server) templateAlias(w http.ResponseWriter, r *http.Request) {
	alias := r.PathValue("alias")
	if key, ok := s.aliases[alias]; ok {
		writeJSON(w, http.StatusOK, TemplateAliasResponse{TemplateID: key.Template, Public: true})
		return
	}
	if b, pool, err := s.resolveTemplate(r, alias); err == nil && (b != nil || pool) {
		writeJSON(w, http.StatusOK, TemplateAliasResponse{TemplateID: alias, Public: true})
		return
	}
	writeError(w, http.StatusNotFound, fmt.Sprintf("template alias %q not found", alias))
}

func (s *Server) getTemplate(w http.ResponseWriter, r *http.Request) {
	b, ok := s.builtTemplate(w, r, r.PathValue("templateID"))
	if !ok {
		return
	}
	created := b.created.UTC().Format(time.RFC3339)
	builds := make([]TemplateBuild, 0, len(b.digests))
	for _, d := range slices.Sorted(maps.Keys(b.digests)) {
		builds = append(builds, TemplateBuild{
			BuildID: buildUUID(d), Status: buildReady, CreatedAt: created, UpdatedAt: created, FinishedAt: created,
			CPUCount: b.cpuCount, MemoryMB: b.memoryMB, EnvdVersion: s.opts.EnvdVersion,
		})
	}
	writeJSON(w, http.StatusOK, TemplateWithBuilds{
		TemplateID: b.name, Public: true, Aliases: []string{b.name}, Names: b.names(),
		CreatedAt: created, UpdatedAt: created, Builds: builds,
	})
}

// patchTemplate accepts public and changes nothing: every template here is visible to every key of the namespace.
func (s *Server) patchTemplate(w http.ResponseWriter, r *http.Request) {
	var req TemplateUpdateRequest
	if !decodeOptionalBody(w, r, &req) {
		return
	}
	if _, ok := s.builtTemplate(w, r, r.PathValue("templateID")); ok {
		w.WriteHeader(http.StatusOK)
	}
}

func (s *Server) assignTemplateTags(w http.ResponseWriter, r *http.Request) {
	var req AssignTemplateTagsRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if len(req.Tags) == 0 || slices.Contains(req.Tags, "") {
		writeError(w, http.StatusBadRequest, "tags must name at least one non-empty tag")
		return
	}
	name, tag, _ := strings.Cut(req.Target, ":")
	b, ok := s.builtTemplate(w, r, name)
	if !ok {
		return
	}
	tags, err := s.liveTags(r, b)
	if err != nil {
		s.writeTagError(w, r, err, name)
		return
	}
	digest := b.digest()
	if tag != "" && tag != defaultTag {
		d, found := tags[tag]
		if !found {
			writeError(w, http.StatusNotFound, fmt.Sprintf("template %q has no tag %q", name, tag))
			return
		}
		digest = d
	}
	for _, t := range req.Tags {
		tags[t] = digest
	}
	if err := s.writeTags(r, b, tags); err != nil {
		s.writeTagError(w, r, err, name)
		return
	}
	writeJSON(w, http.StatusCreated, AssignedTemplateTags{Tags: req.Tags, BuildID: buildUUID(digest)})
}

func (s *Server) deleteTemplateTags(w http.ResponseWriter, r *http.Request) {
	var req DeleteTemplateTagsRequest
	if !decodeBody(w, r, &req) {
		return
	}
	name, _, _ := strings.Cut(req.Name, ":")
	b, ok := s.builtTemplate(w, r, name)
	if !ok {
		return
	}
	tags, err := s.liveTags(r, b)
	if err != nil {
		s.writeTagError(w, r, err, name)
		return
	}
	for _, t := range req.Tags {
		delete(tags, t)
	}
	if err := s.writeTags(r, b, tags); err != nil {
		s.writeTagError(w, r, err, name)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// deleteTemplate removes a built template on every node that lists it, else deletes the snapshot the id names.
func (s *Server) deleteTemplate(w http.ResponseWriter, r *http.Request) {
	logger := log.WithFunc("e2bcompat.deleteTemplate")
	name := r.PathValue("templateID")
	b, _, err := s.resolveTemplate(r, name)
	if err != nil {
		logger.Error(r.Context(), err, "e2b delete template: listing failed")
		writeError(w, http.StatusInternalServerError, "failed to delete the template")
		return
	}
	if b == nil {
		s.deleteSnapshot(w, r)
		return
	}
	if err := forEachHolder(b.holders, func(h templateHolder) error { return s.store.DeleteTemplate(r.Context(), h.node, h.key) }); err != nil {
		logger.Errorf(r.Context(), err, "e2b delete template failed template=%s", name)
		writeError(w, http.StatusInternalServerError, "failed to delete the template")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// builtTemplate writes the refusal and reports false unless name is a built template of the caller's namespace.
func (s *Server) builtTemplate(w http.ResponseWriter, r *http.Request, name string) (*builtTemplate, bool) {
	if _, err := scale.StampedName("template", templatePrefix, s.namespace(r), name); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return nil, false
	}
	b, pool, err := s.resolveTemplate(r, name)
	switch {
	case err != nil:
		log.WithFunc("e2bcompat.builtTemplate").Errorf(r.Context(), err, "e2b template lookup failed template=%s", name)
		writeError(w, http.StatusInternalServerError, "failed to look up the template")
	case b != nil:
		return b, true
	case pool:
		writeError(w, http.StatusNotFound, fmt.Sprintf("template %q is a pool image, not a built template", name))
	default:
		writeError(w, http.StatusNotFound, fmt.Sprintf("template %q not found", name))
	}
	return nil, false
}

// resolveTemplate reports name's built template in the caller's namespace, and whether an alias or an advertised pool image names it.
func (s *Server) resolveTemplate(r *http.Request, name string) (*builtTemplate, bool, error) {
	_, aliased := s.aliases[name]
	nodes, err := s.inventories(r.Context())
	if err != nil {
		return nil, aliased, err
	}
	return builtTemplates(nodes, s.templateScope(r), name)[name], aliased || advertisedIn(nodes, name), nil
}

func (s *Server) templateScope(r *http.Request) string {
	return templatePrefix + s.namespace(r) + "/"
}

// liveTags reads the tags from the current build's holder itself, so a write never merges onto a stale inventory.
func (s *Server) liveTags(r *http.Request, b *builtTemplate) (map[string]string, error) {
	h := b.current()
	held, err := s.store.NodeTemplates(r.Context(), h.node)
	if err != nil {
		return nil, err
	}
	i := slices.IndexFunc(held, func(t scale.PromotedTemplate) bool {
		return t.Template == h.key.Template && t.Net == h.key.Net && t.Size == h.key.Size
	})
	if i < 0 {
		return map[string]string{}, nil
	}
	return b.liveOnly(held[i].Labels), nil
}

func (s *Server) writeTags(r *http.Request, b *builtTemplate, tags map[string]string) error {
	return forEachHolder(b.holders, func(h templateHolder) error { return s.store.SetTemplateLabels(r.Context(), h.node, h.key, tags) })
}

func (s *Server) writeTagError(w http.ResponseWriter, r *http.Request, err error, name string) {
	if se, ok := errors.AsType[*k8serrors.StatusError](err); ok && se.ErrStatus.Code >= http.StatusBadRequest && se.ErrStatus.Code < http.StatusInternalServerError {
		writeError(w, int(se.ErrStatus.Code), se.ErrStatus.Message)
		return
	}
	log.WithFunc("e2bcompat.writeTagError").Errorf(r.Context(), err, "e2b template tags failed template=%s", name)
	writeError(w, http.StatusInternalServerError, "failed to update the template's tags")
}

// builtTemplates groups the operator-owned templates under scope by their bare name, or only the one named only.
func builtTemplates(nodes []scale.NodePools, scope, only string) map[string]*builtTemplate {
	out := map[string]*builtTemplate{}
	for _, inv := range nodes {
		for _, t := range inv.Templates {
			name, ok := strings.CutPrefix(t.Template, scope)
			if !ok || t.Tenant != "" || only != "" && name != only {
				continue
			}
			b := out[name]
			if b == nil {
				b = &builtTemplate{name: name, digests: map[string]int{}, cpuCount: t.CPUCount, memoryMB: int32(t.MemoryBytes >> 20)}
				out[name] = b
			}
			b.holders = append(b.holders, templateHolder{
				node: inv.Node, key: scale.PoolKey{Template: t.Template, Net: t.Net, Size: t.Size}, digest: t.ContentDigest, labels: t.Labels,
			})
			b.digests[t.ContentDigest]++
			if t.CreatedAt != nil && t.CreatedAt.After(b.created) {
				b.created = t.CreatedAt.Time
			}
		}
	}
	return out
}

// forEachHolder runs fn on every holder at the fleet fan-out bound; a node's failure fails the whole, which a retry converges.
func forEachHolder(holders []templateHolder, fn func(templateHolder) error) error {
	var g errgroup.Group
	g.SetLimit(maxNodeConcurrency)
	for _, h := range holders {
		g.Go(func() error {
			if err := fn(h); err != nil {
				return fmt.Errorf("node %s: %w", h.node, err)
			}
			return nil
		})
	}
	return g.Wait()
}

// taggedNames is how e2b spells a template's names: the bare name, then name:tag per tag.
func taggedNames(name string, tags []string) []string {
	names := []string{name}
	for _, tag := range tags {
		names = append(names, name+":"+tag)
	}
	return names
}

func buildUUID(digest string) string {
	return uuid.NewSHA1(buildNamespace, []byte(digest)).String()
}

func advertisedIn(nodes []scale.NodePools, image string) bool {
	return slices.ContainsFunc(nodes, func(inv scale.NodePools) bool {
		return slices.ContainsFunc(inv.Pools, func(pc scale.PoolCapacity) bool { return pc.Template == image })
	})
}
