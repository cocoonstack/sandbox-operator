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
//	POST/GET/DELETE /sandboxes                 -> claim, list, get, release
//	POST /sandboxes/{id}/pause|connect|fork    -> pause, resume, fork
//	POST /sandboxes/{id}/snapshots             -> create checkpoint
//	GET /snapshots, DELETE /templates/{id}     -> list or delete checkpoints
//	GET /templates, /v2/templates              -> advertised warm-pool keys
//	GET /sandboxes/{id}/metrics                -> node resource statistics
//	POST timeout|refreshes, GET /health         -> lease renewal, liveness
package e2bcompat

import (
	"cmp"
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/go-logr/logr"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apiserver/pkg/storage/names"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

const (
	// DefaultEnvdVersion is reported to the SDK when no version is configured.
	// The SDK version-compares this before choosing the envd auth style, so it
	// must be a real semver at or above the modern-auth cutoff (0.4.0).
	DefaultEnvdVersion = "0.4.0"
	// DefaultTimeoutSeconds matches the node's own default lease. The e2b SDK
	// defaults to 15s, which reaps a sandbox before a first exec on a cold
	// client, so an omitted timeout takes the node's default instead.
	DefaultTimeoutSeconds = 300
	// apiKeyHeader is the header the e2b SDKs authenticate with.
	apiKeyHeader = "X-API-KEY"
)

var (
	errSandboxNotFound = errors.New("sandbox not found")
	errNoOwningNode    = errors.New("sandbox inventory entry names no owning node")
)

// Options configures the compat server.
type Options struct {
	// Namespace is where a key that names no namespace claims, and where
	// anonymous claims land.
	Namespace string
	// Domain is echoed as the sandbox `domain`, from which the SDK derives the
	// envd host as "{port}-{sandboxID}.{domain}". It is required: a sandbox
	// handed out without one has no address its client can reach.
	//
	// The sandbox ids published here are DNS-label safe (see sandboxid.go), so
	// that host form is valid; wildcard DNS and a proxy must still route the host
	// or the E2b-Sandbox-Id / E2b-Sandbox-Port headers the SDK sends.
	Domain string
	// EnvdVersion overrides DefaultEnvdVersion. It must name the envd actually
	// installed in the pool's image: the SDK version-compares it and kills the
	// sandbox when it cannot parse one.
	EnvdVersion string
	// DefaultTimeoutSeconds overrides DefaultTimeoutSeconds for a create that
	// names no timeout, and is the lease a refresh grants.
	DefaultTimeoutSeconds int
	// APIKeys, when non-empty, is the set of accepted X-API-KEY values, each
	// "key" or "key namespace" (Namespace when none is given); a key sees
	// nothing outside its namespace. Empty is refused unless AllowAnonymous.
	APIKeys []string //nolint:gosec // the field holds API keys by design
	// AllowAnonymous permits serving with no API key (local development).
	AllowAnonymous bool
	// SizeClass pins the warm-pool size axis for compat claims (default
	// "small"); e2b's NewSandbox carries no size selector.
	SizeClass string
	// Inventory enumerates the fleet's nodes and their advertised pools. It is
	// required by the surfaces that are fleet-wide rather than sandbox-scoped
	// (template listing, snapshot listing); without it those report an error
	// instead of an empty list, so a missing dependency cannot read as "none".
	Inventory scale.InventorySource
	// Log receives request-level errors.
	Log logr.Logger
}

type namespaceKey struct{}

// Server translates e2b REST calls onto a scale.SandboxStore.
type Server struct {
	store    scale.SandboxStore
	resolver scale.ClaimIDResolver
	opts     Options
	keys     map[string]string
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
	opts.SizeClass = cmp.Or(opts.SizeClass, scale.SizeClassSmall)
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
	// The SDK derives the envd host from the domain, so an empty one hands out
	// sandboxes whose data plane the client cannot address at all.
	if strings.TrimSpace(opts.Domain) == "" {
		return nil, errors.New("e2bcompat: no domain configured; the SDK cannot reach a sandbox without one")
	}
	return &Server{store: store, resolver: resolver, opts: opts, keys: keys}, nil
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
	mux.Handle("GET /v2/sandboxes", s.auth(http.HandlerFunc(s.listSandboxes)))
	mux.Handle("GET /sandboxes/{sandboxID}", s.auth(http.HandlerFunc(s.getSandbox)))
	mux.Handle("DELETE /sandboxes/{sandboxID}", s.auth(http.HandlerFunc(s.deleteSandbox)))
	mux.Handle("POST /sandboxes/{sandboxID}/timeout", s.auth(http.HandlerFunc(s.setTimeout)))
	mux.Handle("POST /sandboxes/{sandboxID}/refreshes", s.auth(http.HandlerFunc(s.refresh)))

	mux.Handle("POST /sandboxes/{sandboxID}/pause", s.auth(http.HandlerFunc(s.pauseSandbox)))
	mux.Handle("POST /sandboxes/{sandboxID}/connect", s.auth(http.HandlerFunc(s.connectSandbox)))
	mux.Handle("POST /v2/sandboxes/{sandboxID}/connect", s.auth(http.HandlerFunc(s.connectSandbox)))
	mux.Handle("POST /sandboxes/{sandboxID}/fork", s.auth(http.HandlerFunc(s.forkSandbox)))
	mux.Handle("POST /sandboxes/{sandboxID}/snapshots", s.auth(http.HandlerFunc(s.createSnapshot)))
	mux.Handle("GET /snapshots", s.auth(http.HandlerFunc(s.listSnapshots)))
	mux.Handle("GET /sandboxes/{sandboxID}/metrics", s.auth(http.HandlerFunc(s.sandboxMetrics)))
	mux.Handle("GET /templates", s.auth(http.HandlerFunc(s.listTemplates)))
	mux.Handle("GET /v2/templates", s.auth(http.HandlerFunc(s.listTemplates)))
	// e2b addresses a snapshot as a template on delete.
	mux.Handle("DELETE /templates/{snapshotID}", s.auth(http.HandlerFunc(s.deleteSnapshot)))
	return mux
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
	if req.Timeout != nil && *req.Timeout < 0 {
		writeError(w, http.StatusBadRequest, "timeout must be >= 0")
		return
	}
	if msg, ok := unsupportedCreateOption(req); ok {
		writeError(w, http.StatusBadRequest, msg)
		return
	}

	name := names.SimpleNameGenerator.GenerateName(namePrefix)
	pool := scale.PoolKey{
		Template: req.TemplateID,
		Net:      netFor(req.AllowInternetAccess),
		Size:     s.opts.SizeClass,
	}
	assignment, err := s.store.Claim(r.Context(), s.namespace(r), name, pool, s.timeoutSeconds(req.Timeout))
	if err != nil {
		if scale.IsNoWarmCapacity(err) {
			writeError(w, http.StatusServiceUnavailable, fmt.Sprintf(
				"no warm sandbox available for template %q; retry as warm capacity refills", req.TemplateID))
			return
		}
		s.opts.Log.Error(err, "e2b create: claim failed", "template", req.TemplateID, "name", name)
		writeError(w, http.StatusInternalServerError, "failed to claim a sandbox")
		return
	}

	writeJSON(w, http.StatusCreated, Sandbox{
		TemplateID:      req.TemplateID,
		SandboxID:       PublicID(assignment.SandboxName),
		ClientID:        assignment.Node,
		EnvdVersion:     s.opts.EnvdVersion,
		EnvdAccessToken: assignment.Token,
		Domain:          s.opts.Domain,
	})
}

// listSandboxes reports the live sandboxes in the caller's namespace.
func (s *Server) listSandboxes(w http.ResponseWriter, r *http.Request) {
	filter, err := listFilterOf(r.URL.Query())
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	list, err := s.store.List(r.Context(), scale.ListOptions{Namespace: s.namespace(r)})
	if err != nil {
		s.opts.Log.Error(err, "e2b list: store list failed")
		writeError(w, http.StatusInternalServerError, "failed to list sandboxes")
		return
	}
	out := make([]SandboxDetail, 0, len(list.Items))
	for i := range list.Items {
		if d := s.detailFor(&list.Items[i]); filter.keeps(d) {
			out = append(out, d)
		}
	}
	writeJSON(w, http.StatusOK, out)
}

// getSandbox resolves one sandbox by its e2b sandboxID (the sandboxd claim id).
func (s *Server) getSandbox(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("sandboxID")
	sb, err := s.lookup(r, id)
	if err != nil {
		s.writeLookupError(w, err, id, "get")
		return
	}
	writeJSON(w, http.StatusOK, s.detailFor(sb))
}

// deleteSandbox releases the claim back to its owning node's warm pool.
func (s *Server) deleteSandbox(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("sandboxID")
	sb, err := s.lookup(r, id)
	if err != nil {
		s.writeLookupError(w, err, id, "delete")
		return
	}
	node := sb.Status.NodeName
	if node == "" {
		s.opts.Log.Error(errNoOwningNode, "e2b delete: release failed", "sandboxID", id)
		writeError(w, http.StatusInternalServerError, "failed to release the sandbox")
		return
	}
	claimID := claimIDOf(sb)
	if err := s.store.Release(r.Context(), node, claimID); err != nil {
		s.opts.Log.Error(err, "e2b delete: release failed", "sandboxID", id, "claimID", claimID, "node", node)
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
		s.writeLookupError(w, err, id, op)
		return
	}
	if _, err := s.store.Renew(r.Context(), sb.Status.NodeName, claimIDOf(sb), ttlSeconds); err != nil {
		s.writeVerbError(w, err, id, op, "failed to extend the sandbox lease")
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

func (s *Server) writeLookupError(w http.ResponseWriter, err error, id, op string) {
	s.writeVerbError(w, err, id, op+": lookup", "failed to resolve the sandbox")
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
		TemplateID:  templateOf(sb),
		SandboxID:   PublicID(claimIDOf(sb)),
		ClientID:    sb.Status.NodeName,
		StartedAt:   started.UTC().Format(time.RFC3339),
		EndAt:       endAt.UTC().Format(time.RFC3339),
		State:       state,
		EnvdVersion: s.opts.EnvdVersion,
		Domain:      s.opts.Domain,
	}
}

// listFilter is the GET /v2/sandboxes query the read view can answer; metadata is not stored, so it is refused.
type listFilter struct {
	states       []string
	template     string
	startedAfter time.Time
}

func listFilterOf(q url.Values) (listFilter, error) {
	if q.Get("metadata") != "" {
		return listFilter{}, errors.New("metadata filters are not supported: metadata is not stored")
	}
	f := listFilter{template: q.Get("template")}
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

// templateOf reports the pool template a sandbox was claimed from: the label the
// store stamps, which is the only place it survives (a synthesized Sandbox holds
// no pod spec).
func templateOf(sb *sandboxv1beta1.Sandbox) string {
	return sb.Labels[scale.TemplateLabel]
}

// netFor maps e2b's allow_internet_access onto the pool's network axis.
func netFor(allowInternet *bool) string {
	if allowInternet != nil && *allowInternet {
		return scale.NetEgress
	}
	return scale.NetDefault
}

// unsupportedCreateOption names the first requested option this backend cannot
// honor. Honoring it silently would hand back a different sandbox than asked
// for, which is how an SDK ends up trusting a guarantee that does not hold.
func unsupportedCreateOption(req NewSandbox) (string, bool) {
	switch {
	case req.Secure != nil && !*req.Secure:
		return "secure=false is not supported; every sandbox this backend hands out is reachable only with its own access token", true
	case len(req.EnvVars) > 0:
		return "envVars is not supported; set the environment inside the sandbox after it starts", true
	case req.AutoPause != nil && *req.AutoPause:
		return "autoPause is not supported; pause explicitly, or let the lease expire", true
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
