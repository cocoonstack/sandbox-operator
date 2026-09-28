// Package e2bcompat serves an e2b-compatible REST surface in front of the L3
// sandbox store, so an unmodified e2b SDK (JS or Python) can drive cocoon warm
// pools by pointing E2B_API_URL at this server.
//
// It is a translation layer, not a second control plane: every request lands on
// the same scale.SandboxStore the aggregated apiserver uses, so an e2b Create is
// the identical node-local claim a `kubectl create sandbox` performs, and the
// sandbox it returns is visible to `kubectl get sandboxes`. Nothing is stored
// here; public identity is a DNS-safe rendering of the node's sandboxd claim id.
//
// Mapping to the e2b contract (e2b-dev/E2B spec/openapi.yml):
//
//	POST/GET/DELETE /sandboxes                    -> claim, list, get, release
//	POST /sandboxes/{id}/pause|connect|resume|fork -> pause, resume, fork
//	POST /sandboxes/{id}/snapshots                -> create checkpoint
//	GET /snapshots, DELETE /templates/{id}        -> list or delete checkpoints and built templates
//	GET/PATCH /templates[/{id}], aliases/{a}, tags -> warm-pool keys, built templates, alias lookup, tags
//	GET /sandboxes/{id}/metrics|logs              -> node statistics, an empty log page
//	POST timeout|refreshes, GET /health           -> lease renewal, liveness
package e2bcompat

import (
	"cmp"
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/projecteru2/core/log"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apiserver/pkg/storage/names"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"

	"github.com/cocoonstack/sandbox-operator/pkg/e2bbuild"
	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

const (
	// DefaultEnvdVersion is the envd the e2b flavors ship, which the SDK version-compares.
	DefaultEnvdVersion = "0.8.0"
	// DefaultTimeoutSeconds is the node's default lease; the SDK's own 15s reaps a cold client's sandbox.
	DefaultTimeoutSeconds = 300
	apiKeyHeader          = "X-API-KEY"

	connectFailure  = "failed to connect the sandbox"
	maxListLimit    = 100
	nextTokenHeader = "X-Next-Token"

	readHeaderTimeout = 10 * time.Second
	shutdownTimeout   = 10 * time.Second
)

var (
	errSandboxNotFound = errors.New("sandbox not found")
	errNoOwningNode    = errors.New("sandbox inventory entry names no owning node")

	sizeClasses = []string{scale.SizeClassSmall, scale.SizeClassMedium, scale.SizeClassLarge}
)

// Options configures the compat server.
type Options struct {
	// Namespace is where anonymous claims and claims by a key that names no namespace land.
	Namespace string
	// Domain is the required base domain the SDK derives the envd host from, as "{port}-{sandboxID}.{domain}".
	Domain string
	// EnvdVersion overrides DefaultEnvdVersion and must name the envd installed in the pool's image.
	EnvdVersion string
	// DefaultTimeoutSeconds overrides DefaultTimeoutSeconds for a create without a timeout and for a refresh.
	DefaultTimeoutSeconds int
	// APIKeys holds the accepted X-API-KEY values, each "key" or "key namespace"; empty requires AllowAnonymous.
	APIKeys []string
	// AllowAnonymous permits serving with no API key (local development).
	AllowAnonymous bool
	// Inventory enumerates the fleet's nodes; without it template and snapshot listing fail rather than report none.
	Inventory scale.InventorySource
	// TemplateAliases maps a templateID to a pool image and size, each entry "alias image [size]"; the SDK's default "base" is one.
	TemplateAliases []string
	// EnvdSecret keys every sandbox's envd access token, AccessToken(EnvdSecret, claim token); the edge shares it.
	EnvdSecret []byte
	// Builds serves the template build API when Parallel is set; builds run in this process.
	Builds BuildOptions
}

// BuildOptions bounds the in-process template builds; Uploads, when set, keeps the archives their COPY steps read.
type BuildOptions struct {
	Parallel int
	Timeout  time.Duration
	LogLines int
	Uploads  uploadStore
}

type namespaceKey struct{}

// Server translates e2b REST calls onto a scale.SandboxStore.
type Server struct {
	store    scale.SandboxStore
	resolver scale.ClaimIDResolver
	opts     Options
	keys     map[string]string

	aliases      map[string]scale.PoolKey
	imageAliases map[string][]string
	builds       *e2bbuild.Executor
}

// NewServer builds a compat server. It fails when no API key is configured and
// anonymous access was not explicitly allowed, or when store cannot resolve a
// sandbox by claim id — every by-id verb would otherwise degrade to a
// cluster-wide List scan.
func NewServer(store scale.SandboxStore, opts Options) (*Server, error) {
	if store == nil {
		return nil, errors.New("e2bcompat: store is required")
	}
	resolver, ok := store.(scale.ClaimIDResolver)
	if !ok {
		return nil, errors.New("e2bcompat: store does not implement scale.ClaimIDResolver")
	}
	opts.Namespace = cmp.Or(opts.Namespace, "default")
	opts.EnvdVersion = cmp.Or(opts.EnvdVersion, DefaultEnvdVersion)
	opts.DefaultTimeoutSeconds = cmp.Or(opts.DefaultTimeoutSeconds, DefaultTimeoutSeconds)
	keys := make(map[string]string, len(opts.APIKeys))
	for _, entry := range opts.APIKeys {
		switch fields := strings.Fields(entry); len(fields) {
		case 0:
		case 1:
			keys[fields[0]] = opts.Namespace
		case 2:
			keys[fields[0]] = fields[1]
		default:
			return nil, fmt.Errorf("e2bcompat: api key entry %q: want \"key\" or \"key namespace\"", entry)
		}
	}
	if len(keys) == 0 && !opts.AllowAnonymous {
		return nil, errors.New("e2bcompat: no API key configured; set one or enable anonymous access explicitly")
	}
	if strings.TrimSpace(opts.Domain) == "" {
		return nil, errors.New("e2bcompat: no domain configured; the SDK cannot reach a sandbox without one")
	}
	if len(opts.EnvdSecret) == 0 {
		return nil, errors.New("e2bcompat: an envd secret is required: every sandbox's access token derives from it")
	}
	aliases, imageAliases := map[string]scale.PoolKey{}, map[string][]string{}
	for _, entry := range opts.TemplateAliases {
		fields := strings.Fields(entry)
		if len(fields) < 2 || len(fields) > 3 {
			return nil, fmt.Errorf("e2bcompat: template alias entry %q: want \"alias image [size]\"", entry)
		}
		if _, dup := aliases[fields[0]]; dup {
			return nil, fmt.Errorf("e2bcompat: template alias %q is named twice", fields[0])
		}
		key := scale.PoolKey{Template: fields[1], Size: scale.SizeClassSmall}
		if len(fields) == 3 {
			if !slices.Contains(sizeClasses, fields[2]) {
				return nil, fmt.Errorf("e2bcompat: template alias %q: size %q is not one of %v", fields[0], fields[2], sizeClasses)
			}
			key.Size = fields[2]
		}
		aliases[fields[0]] = key
		imageAliases[fields[1]] = append(imageAliases[fields[1]], fields[0])
	}
	for _, aliasNames := range imageAliases {
		slices.Sort(aliasNames)
	}
	s := &Server{store: store, resolver: resolver, opts: opts, keys: keys, aliases: aliases, imageAliases: imageAliases}
	if b := opts.Builds; b.Parallel > 0 {
		s.builds = e2bbuild.New(store, envdGuest{s: s}, b.Parallel, b.Timeout, b.LogLines)
	}
	return s, nil
}

// Handler returns the routed, authenticated HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	// /health is unauthenticated so probes work without a key.
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	mux.Handle("POST /sandboxes", s.auth(http.HandlerFunc(s.createSandbox)))
	mux.Handle("POST /v2/sandboxes", s.auth(http.HandlerFunc(s.createSandbox)))
	mux.Handle("GET /sandboxes", s.auth(http.HandlerFunc(s.listSandboxes)))
	mux.Handle("GET /v2/sandboxes", s.auth(http.HandlerFunc(s.listSandboxesV2)))
	mux.Handle("GET /sandboxes/{sandboxID}", s.auth(http.HandlerFunc(s.getSandbox)))
	mux.Handle("DELETE /sandboxes/{sandboxID}", s.auth(http.HandlerFunc(s.deleteSandbox)))
	mux.Handle("POST /sandboxes/{sandboxID}/timeout", s.auth(http.HandlerFunc(s.setTimeout)))
	mux.Handle("POST /sandboxes/{sandboxID}/refreshes", s.auth(http.HandlerFunc(s.refresh)))

	mux.Handle("POST /sandboxes/{sandboxID}/pause", s.auth(http.HandlerFunc(s.pauseSandbox)))
	mux.Handle("POST /sandboxes/{sandboxID}/connect", s.auth(http.HandlerFunc(s.connectSandbox)))
	mux.Handle("POST /v2/sandboxes/{sandboxID}/connect", s.auth(http.HandlerFunc(s.connectSandbox)))
	mux.Handle("POST /sandboxes/{sandboxID}/resume", s.auth(http.HandlerFunc(s.resumeSandbox)))
	mux.Handle("POST /sandboxes/{sandboxID}/fork", s.auth(http.HandlerFunc(s.forkSandbox)))
	mux.Handle("POST /sandboxes/{sandboxID}/snapshots", s.auth(http.HandlerFunc(s.createSnapshot)))
	mux.Handle("GET /snapshots", s.auth(http.HandlerFunc(s.listSnapshots)))
	mux.Handle("GET /sandboxes/{sandboxID}/metrics", s.auth(http.HandlerFunc(s.sandboxMetrics)))
	mux.Handle("GET /sandboxes/metrics", s.auth(http.HandlerFunc(s.sandboxesMetrics)))
	mux.Handle("GET /sandboxes/{sandboxID}/logs", s.auth(s.sandboxLogs(SandboxLogs{Logs: []struct{}{}, LogEntries: []struct{}{}})))
	mux.Handle("GET /v2/sandboxes/{sandboxID}/logs", s.auth(s.sandboxLogs(SandboxLogsV2{Logs: []struct{}{}})))
	mux.Handle("GET /templates", s.auth(http.HandlerFunc(s.listTemplates)))
	mux.Handle("GET /v2/templates", s.auth(http.HandlerFunc(s.listTemplates)))
	mux.Handle("GET /templates/{templateID}", s.auth(http.HandlerFunc(s.getTemplate)))
	mux.Handle("PATCH /templates/{templateID}", s.auth(http.HandlerFunc(s.patchTemplate)))
	mux.Handle("GET /templates/aliases/{alias}", s.auth(http.HandlerFunc(s.templateAlias)))
	mux.Handle("GET /templates/{templateID}/{sub}", s.auth(http.HandlerFunc(s.templateSub)))
	mux.Handle("POST /templates/tags", s.auth(http.HandlerFunc(s.assignTemplateTags)))
	mux.Handle("DELETE /templates/tags", s.auth(http.HandlerFunc(s.deleteTemplateTags)))
	// e2b addresses a snapshot as a template on delete.
	mux.Handle("DELETE /templates/{templateID}", s.auth(http.HandlerFunc(s.deleteTemplate)))
	if s.builds != nil {
		mux.Handle("POST /v3/templates", s.auth(http.HandlerFunc(s.requestBuild)))
		mux.Handle("POST /v2/templates/{templateID}/builds/{buildID}", s.auth(http.HandlerFunc(s.startBuild)))
		mux.Handle("GET /templates/{templateID}/builds/{buildID}/status", s.auth(http.HandlerFunc(s.buildStatus)))
		if s.opts.Builds.Uploads != nil {
			mux.Handle("GET /templates/{templateID}/files/{hash}", s.auth(http.HandlerFunc(s.fileUploadLink)))
		}
		if d, ok := s.opts.Builds.Uploads.(*dirUploads); ok {
			mux.Handle("PUT /templates/{templateID}/files/{hash}", d)
		}
	}
	return mux
}

// Serve listens on addr and serves the surface in the background; the returned stop drains it.
func (s *Server) Serve(ctx context.Context, addr string) (func(), error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on e2b address %q: %w", addr, err)
	}
	httpSrv := &http.Server{Addr: addr, Handler: s.Handler(), ReadHeaderTimeout: readHeaderTimeout}
	logger := log.WithFunc("e2bcompat.Serve")
	logger.Infof(ctx, "serving e2b-compatible API address=%s namespace=%s authenticated=%t", addr, s.opts.Namespace, len(s.keys) > 0)
	go func() {
		if err := httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error(ctx, err, "e2b-compatible API server exited")
		}
	}()
	e2bCtx, stop := context.WithCancel(ctx)
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		<-e2bCtx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		defer cancel()
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			logger.Error(ctx, err, "e2b-compatible API server shutdown")
		}
	}()
	return func() { stop(); <-drained }, nil
}

// auth enforces the X-API-KEY header unless anonymous access is allowed and
// scopes the request to the key's namespace.
func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(s.keys) > 0 {
			ns, ok := s.namespaceForKey(r.Header.Get(apiKeyHeader))
			if !ok {
				writeError(w, http.StatusUnauthorized, "invalid API key")
				return
			}
			r = r.WithContext(context.WithValue(r.Context(), namespaceKey{}, ns))
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) namespaceForKey(presented string) (string, bool) {
	if presented == "" {
		return "", false
	}
	ns, ok := "", false
	for k, keyNS := range s.keys {
		if subtle.ConstantTimeCompare([]byte(k), []byte(presented)) == 1 {
			ns, ok = keyNS, true
		}
	}
	return ns, ok
}

// namespace is the caller's scope: the key's namespace, or the configured one without keys.
func (s *Server) namespace(r *http.Request) string {
	if ns, ok := r.Context().Value(namespaceKey{}).(string); ok {
		return ns
	}
	return s.opts.Namespace
}

// createSandbox claims a warm microVM for the requested template. It is the
// same node-local claim the aggregated apiserver's Create performs.
func (s *Server) createSandbox(w http.ResponseWriter, r *http.Request) {
	var req NewSandbox
	if !decodeBody(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.TemplateID) == "" {
		writeError(w, http.StatusBadRequest, "templateID is required")
		return
	}
	if strings.HasPrefix(req.TemplateID, templatePrefix) {
		writeError(w, http.StatusNotFound, fmt.Sprintf("template %q not found", req.TemplateID))
		return
	}
	if req.Timeout != nil && *req.Timeout < 0 {
		writeError(w, http.StatusBadRequest, "timeout must be >= 0")
		return
	}
	if msg, ok := unsupportedCreateOption(req); ok {
		writeError(w, http.StatusBadRequest, msg)
		return
	}

	logger := log.WithFunc("e2bcompat.createSandbox")
	name := names.SimpleNameGenerator.GenerateName(namePrefix)
	pool := s.poolKey(req.TemplateID)
	pool.Net = netFor(req.AllowInternetAccess)
	opts := scale.ClaimOptions{TTLSeconds: s.timeoutSeconds(req.Timeout), Metadata: req.Metadata, NoEgress: req.AllowInternetAccess != nil && !*req.AllowInternetAccess}
	if req.AutoPause != nil && *req.AutoPause {
		opts.OnExpire = sandboxd.ExpireArchive
	}
	assignment, err := s.store.Claim(r.Context(), s.namespace(r), name, pool, opts)
	known, built := true, false
	if scale.IsNoWarmCapacity(err) {
		b, pooled, lookupErr := s.resolveTemplate(r, req.TemplateID)
		known = lookupErr != nil || b != nil || pooled
		if lookupErr == nil && b != nil && !pooled {
			built = true
			assignment, err = s.store.Claim(r.Context(), s.namespace(r), name, b.current().key, opts)
		}
	}
	if err != nil {
		if scale.IsNoWarmCapacity(err) {
			if !known {
				writeError(w, http.StatusNotFound, fmt.Sprintf("template %q not found", req.TemplateID))
				return
			}
			writeError(w, http.StatusServiceUnavailable, fmt.Sprintf(
				"no warm sandbox available for template %q; retry as warm capacity refills", req.TemplateID))
			return
		}
		if he, ok := errors.AsType[*sandboxd.HTTPError](err); ok && (he.StatusCode == http.StatusBadRequest || he.StatusCode == http.StatusConflict) {
			writeError(w, he.StatusCode, he.Message)
			return
		}
		logger.Errorf(r.Context(), err, "e2b create: claim failed template=%s name=%s", req.TemplateID, name)
		writeError(w, http.StatusInternalServerError, "failed to claim a sandbox")
		return
	}

	token := AccessToken(s.opts.EnvdSecret, assignment.Token)
	init := envdInit{AccessToken: token}
	if built {
		init.EnvVars, err = s.templateEnvs(r.Context(), assignment, req.EnvVars)
	} else {
		init.EnvVars, init.DefaultUser, init.DefaultWorkdir = withRelay(assignment.NetRoute, req.EnvVars), envdDefaultUser, envdDefaultWorkdir
	}
	if err == nil {
		err = s.initEnvd(r.Context(), assignment.Node, assignment.SandboxName, "", init)
	}
	if err != nil {
		logger.Errorf(r.Context(), err, "e2b create: envd init failed sandboxID=%s node=%s", assignment.SandboxName, assignment.Node)
		s.releaseAll(r.Context(), []scale.Assignment{assignment})
		writeError(w, http.StatusInternalServerError, "failed to start the sandbox")
		return
	}
	writeJSON(w, http.StatusCreated, Sandbox{
		TemplateID:      pool.Template,
		Alias:           s.aliasOf(pool.Template),
		SandboxID:       PublicID(assignment.SandboxName),
		ClientID:        assignment.Node,
		EnvdVersion:     s.opts.EnvdVersion,
		EnvdAccessToken: token,
		Domain:          s.opts.Domain,
	})
}

// listSandboxes reports the live sandboxes in the caller's namespace.
func (s *Server) listSandboxes(w http.ResponseWriter, r *http.Request) {
	if out, ok := s.listed(w, r); ok {
		writeJSON(w, http.StatusOK, out)
	}
}

func (s *Server) listSandboxesV2(w http.ResponseWriter, r *http.Request) {
	page, err := listPageOf(r.URL.Query())
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, ok := s.listed(w, r)
	if !ok {
		return
	}
	out, next := page.cut(out)
	if next != "" {
		w.Header().Set(nextTokenHeader, next)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) listed(w http.ResponseWriter, r *http.Request) ([]SandboxDetail, bool) {
	filter, err := listFilterOf(r.URL.Query())
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return nil, false
	}
	filter.template = s.poolKey(filter.template).Template
	list, err := s.store.List(r.Context(), scale.ListOptions{Namespace: s.namespace(r)})
	if err != nil {
		log.WithFunc("e2bcompat.listed").Error(r.Context(), err, "e2b list: store list failed")
		writeError(w, http.StatusInternalServerError, "failed to list sandboxes")
		return nil, false
	}
	out := make([]SandboxDetail, 0, len(list.Items))
	for i := range list.Items {
		if d := s.detailFor(&list.Items[i]); filter.keeps(d) && filter.matchesMetadata(&list.Items[i]) {
			out = append(out, d)
		}
	}
	return out, true
}

// getSandbox resolves one sandbox by its e2b sandboxID (the sandboxd claim id); state and endAt come from the owning node, which a published inventory lags.
func (s *Server) getSandbox(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("sandboxID")
	sb, err := s.lookup(r, id)
	if err != nil {
		s.writeLookupError(w, r, err, "get")
		return
	}
	rec, err := s.store.Read(r.Context(), sb.Status.NodeName, claimIDOf(sb))
	if err != nil {
		s.writeVerbError(w, r, err, "get: read", "failed to read the sandbox")
		return
	}
	d := s.detailFor(sb)
	d.State = StateRunning
	if rec.Paused {
		d.State = StatePaused
	}
	if !rec.Deadline.IsZero() {
		d.EndAt = rec.Deadline.UTC().Format(time.RFC3339)
	}
	writeJSON(w, http.StatusOK, d)
}

// deleteSandbox releases the claim, which destroys its microVM on the owning node.
func (s *Server) deleteSandbox(w http.ResponseWriter, r *http.Request) {
	logger := log.WithFunc("e2bcompat.deleteSandbox")
	id := r.PathValue("sandboxID")
	sb, err := s.lookup(r, id)
	if err != nil {
		s.writeLookupError(w, r, err, "delete")
		return
	}
	node := sb.Status.NodeName
	if node == "" {
		logger.Errorf(r.Context(), errNoOwningNode, "e2b delete: release failed sandboxID=%s", id)
		writeError(w, http.StatusInternalServerError, "failed to release the sandbox")
		return
	}
	claimID := claimIDOf(sb)
	if err := s.store.Release(r.Context(), node, claimID); err != nil {
		logger.Errorf(r.Context(), err, "e2b delete: release failed sandboxID=%s claimID=%s node=%s", id, claimID, node)
		writeError(w, http.StatusInternalServerError, "failed to release the sandbox")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// setTimeout moves the sandbox's deadline to timeout seconds from now. The
// node's grant is authoritative — it defaults a zero and clamps an oversized
// request — so the call reports success only once the owning node has renewed.
func (s *Server) setTimeout(w http.ResponseWriter, r *http.Request) {
	var req SandboxTimeoutRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if req.Timeout < 0 {
		writeError(w, http.StatusBadRequest, "timeout must be >= 0")
		return
	}
	s.renew(w, r, "timeout", int(req.Timeout))
}

// refresh is the SDK keepalive: it renews the lease for the configured default
// rather than only confirming the sandbox is alive, so a client that keeps
// calling it keeps its sandbox.
func (s *Server) refresh(w http.ResponseWriter, r *http.Request) {
	var req SandboxRefreshRequest
	if !decodeOptionalBody(w, r, &req) {
		return
	}
	ttl := s.opts.DefaultTimeoutSeconds
	if req.Duration != nil && *req.Duration > 0 {
		ttl = int(*req.Duration)
	}
	s.renew(w, r, "refresh", ttl)
}

// renew resolves the sandbox and extends its node-owned lease.
func (s *Server) renew(w http.ResponseWriter, r *http.Request, op string, ttlSeconds int) {
	id := r.PathValue("sandboxID")
	sb, err := s.lookup(r, id)
	if err != nil {
		s.writeLookupError(w, r, err, op)
		return
	}
	if _, err := s.store.Renew(r.Context(), sb.Status.NodeName, claimIDOf(sb), ttlSeconds, ""); err != nil {
		s.writeVerbError(w, r, err, op, "failed to extend the sandbox lease")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// lookup finds the sandbox whose sandboxd claim id matches id. The id is the
// node-assigned claim id, which the store stamps on each synthesized Sandbox.
func (s *Server) lookup(r *http.Request, id string) (*sandboxv1beta1.Sandbox, error) {
	if strings.TrimSpace(id) == "" {
		return nil, errSandboxNotFound
	}
	sb, err := s.resolver.GetByClaimID(r.Context(), s.namespace(r), ClaimID(id), func(claimID string) bool {
		return MatchesID(claimID, id)
	})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil, errSandboxNotFound
		}
		return nil, err
	}
	return sb, nil
}

func (s *Server) writeLookupError(w http.ResponseWriter, r *http.Request, err error, op string) {
	s.writeVerbError(w, r, err, op+": lookup", "failed to resolve the sandbox")
}

func (s *Server) aliasOf(image string) string {
	if aliasNames := s.imageAliases[image]; len(aliasNames) > 0 {
		return aliasNames[0]
	}
	return ""
}

// poolKey is the pool a create of templateID claims from: an alias's image and size, else that image at small.
func (s *Server) poolKey(templateID string) scale.PoolKey {
	if key, ok := s.aliases[templateID]; ok {
		return key
	}
	return scale.PoolKey{Template: templateID, Size: scale.SizeClassSmall}
}

// detailFor renders a live Sandbox as the e2b detail shape. Fields e2b requires
// but cocoon does not track per sandbox (disk size) are reported as zero values
// rather than omitted, so the SDK's decoder stays happy.
func (s *Server) detailFor(sb *sandboxv1beta1.Sandbox) SandboxDetail {
	started := sb.CreationTimestamp.Time
	if started.IsZero() {
		started = time.Now()
	}
	state := StateRunning
	if sb.Labels[scale.PhaseLabel] == scale.PhaseHibernated {
		state = StatePaused
	}
	endAt := started.Add(time.Duration(s.opts.DefaultTimeoutSeconds) * time.Second)
	if deadline, err := time.Parse(time.RFC3339, sb.Annotations[scale.DeadlineAnnotation]); err == nil {
		endAt = deadline
	}
	return SandboxDetail{
		TemplateID:   templateOf(sb),
		Alias:        s.aliasOf(templateOf(sb)),
		SandboxID:    PublicID(claimIDOf(sb)),
		ClientID:     sb.Status.NodeName,
		StartedAt:    started.UTC().Format(time.RFC3339),
		EndAt:        endAt.UTC().Format(time.RFC3339),
		State:        state,
		EnvdVersion:  s.opts.EnvdVersion,
		CPUCount:     int32(annotationInt(sb, scale.CPUCountAnnotation)),
		MemoryMB:     int32(annotationInt(sb, scale.MemoryBytesAnnotation) >> 20),
		Metadata:     json.RawMessage(sb.Annotations[scale.MetadataAnnotation]),
		Domain:       s.opts.Domain,
		startedAtKey: sb.CreationTimestamp.UTC().Format(time.RFC3339),
	}
}

// listFilter is the GET /v2/sandboxes query the read view can answer.
type listFilter struct {
	states       []string
	template     string
	startedAfter time.Time
	metadata     map[string]string
}

func listFilterOf(q url.Values) (listFilter, error) {
	f := listFilter{template: q.Get("template")}
	if v := q.Get("metadata"); v != "" {
		md, err := metadataFilterOf(v)
		if err != nil {
			return listFilter{}, err
		}
		f.metadata = md
	}
	for _, v := range q["state"] {
		for state := range strings.SplitSeq(v, ",") {
			if state != StateRunning && state != StatePaused {
				return listFilter{}, fmt.Errorf("unknown state %q", state)
			}
			f.states = append(f.states, state)
		}
	}
	if v := q.Get("startedAfter"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return listFilter{}, fmt.Errorf("startedAfter: %w", err)
		}
		f.startedAfter = t
	}
	return f, nil
}

func (f listFilter) keeps(d SandboxDetail) bool {
	if len(f.states) > 0 && !slices.Contains(f.states, d.State) {
		return false
	}
	if f.template != "" && d.TemplateID != f.template {
		return false
	}
	if !f.startedAfter.IsZero() {
		started, err := time.Parse(time.RFC3339, d.StartedAt)
		if err != nil || started.Before(f.startedAfter.Truncate(time.Second)) {
			return false
		}
	}
	return true
}

func (f listFilter) matchesMetadata(sb *sandboxv1beta1.Sandbox) bool {
	if len(f.metadata) == 0 {
		return true
	}
	md := scale.MetadataOf(sb)
	for k, v := range f.metadata {
		if got, ok := md[k]; !ok || got != v {
			return false
		}
	}
	return true
}

type pageKey struct{ startedAt, sandboxID string }

type listPage struct {
	limit int
	desc  bool
	after pageKey
}

func listPageOf(q url.Values) (listPage, error) {
	p := listPage{limit: maxListLimit, desc: true}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxListLimit {
			return listPage{}, fmt.Errorf("limit must be between 1 and %d, got %q", maxListLimit, v)
		}
		p.limit = n
	}
	switch q.Get("order") {
	case "", "desc":
	case "asc":
		p.desc = false
	default:
		return listPage{}, fmt.Errorf("order must be asc or desc, got %q", q.Get("order"))
	}
	if v := q.Get("nextToken"); v != "" {
		raw, err := base64.RawURLEncoding.DecodeString(v)
		started, id, ok := strings.Cut(string(raw), " ")
		if err != nil || !ok {
			return listPage{}, fmt.Errorf("nextToken %q was not issued by this server", v)
		}
		p.after = pageKey{started, id}
	}
	return p, nil
}

func (p listPage) cut(items []SandboxDetail) ([]SandboxDetail, string) {
	if p.after != (pageKey{}) {
		items = slices.DeleteFunc(items, func(d SandboxDetail) bool { return p.compare(keyOf(d), p.after) <= 0 })
	}
	slices.SortFunc(items, func(a, b SandboxDetail) int { return p.compare(keyOf(a), keyOf(b)) })
	if len(items) <= p.limit {
		return items, ""
	}
	last := keyOf(items[p.limit-1])
	return items[:p.limit], base64.RawURLEncoding.EncodeToString([]byte(last.startedAt + " " + last.sandboxID))
}

func (p listPage) compare(a, b pageKey) int {
	c := cmp.Or(strings.Compare(a.startedAt, b.startedAt), strings.Compare(a.sandboxID, b.sandboxID))
	if p.desc {
		return -c
	}
	return c
}

func keyOf(d SandboxDetail) pageKey { return pageKey{d.startedAtKey, d.SandboxID} }

// templateOf is the template a sandbox was claimed from, a built template by its bare name.
func templateOf(sb *sandboxv1beta1.Sandbox) string {
	t := sb.Labels[scale.TemplateLabel]
	if name, ok := strings.CutPrefix(t, templatePrefix+sb.Namespace+"/"); ok {
		return name
	}
	return t
}

// netFor maps e2b's allow_internet_access onto the pool's network axis.
func netFor(allowInternet *bool) string {
	if allowInternet != nil && *allowInternet {
		return scale.NetEgress
	}
	return scale.NetDefault
}

// unsupportedCreateOption names the first requested option this backend cannot honor.
func unsupportedCreateOption(req NewSandbox) (string, bool) {
	switch {
	case req.Secure != nil && !*req.Secure:
		return "secure=false is not supported; every sandbox this backend hands out is reachable only with its own access token", true
	case req.AutoPause != nil && *req.AutoPause && req.AllowInternetAccess != nil && *req.AllowInternetAccess:
		return "autoPause is not supported with allow_internet_access: the internet lane cannot pause", true
	case len(req.Network) > 0:
		return "network rules are not supported; allow_internet_access picks the pool's lane and nothing else is enforced", true
	case len(req.VolumeMounts) > 0:
		return "volumeMounts are not supported", true
	case req.AutoPauseMemory != nil:
		return "autoPauseMemory is not supported; a pause always keeps memory", true
	case req.AutoResume != nil && req.AutoResume.Enabled:
		return "autoResume is not supported; resume explicitly with connect", true
	case len(req.MCP) > 0:
		return "mcp is not supported", true
	case len(req.IAM) > 0:
		return "iam is not supported", true
	}
	return "", false
}

// metadataFilterOf parses e2b's metadata query: key=value pairs joined by &, each part URL-encoded twice as the SDKs send it.
func metadataFilterOf(query string) (map[string]string, error) {
	md := map[string]string{}
	for pair := range strings.SplitSeq(query, "&") {
		rawKey, rawValue, ok := strings.Cut(pair, "=")
		key, keyErr := unescapeTwice(rawKey)
		value, valueErr := unescapeTwice(rawValue)
		if !ok || key == "" || keyErr != nil || valueErr != nil {
			return nil, fmt.Errorf("metadata filter %q must be URL-encoded key=value pairs joined by &", query)
		}
		if _, dup := md[key]; dup {
			return nil, fmt.Errorf("metadata filter repeats key %q", key)
		}
		md[key] = value
	}
	return md, nil
}

func unescapeTwice(s string) (string, error) {
	once, err := url.QueryUnescape(s)
	if err != nil {
		return "", err
	}
	return url.PathUnescape(once)
}

func annotationInt(sb *sandboxv1beta1.Sandbox, key string) int64 {
	raw := sb.Annotations[key]
	if raw == "" {
		return 0
	}
	v, _ := strconv.ParseInt(raw, 10, 64)
	return v
}
