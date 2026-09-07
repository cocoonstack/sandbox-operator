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
//	POST timeout|refreshes, GET /health         -> existence or liveness checks
package e2bcompat

import (
	"cmp"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-logr/logr"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

const (
	// DefaultEnvdVersion is reported to the SDK when no version is configured.
	// The SDK version-compares this before choosing the envd auth style, so it
	// must be a real semver at or above the modern-auth cutoff (0.4.0).
	DefaultEnvdVersion = "0.4.0"
	// DefaultTimeoutSeconds matches the e2b SDK's own default TTL.
	DefaultTimeoutSeconds = 15
	// apiKeyHeader is the header the e2b SDKs authenticate with.
	apiKeyHeader = "X-API-KEY"
	// phaseHibernated is the phase label value a node publishes for a paused
	// sandbox (vk-sandbox inventory publisher).
	phaseHibernated = "Hibernated"
)

var (
	errSandboxNotFound = errors.New("sandbox not found")
	errNoOwningNode    = errors.New("sandbox inventory entry names no owning node")
)

// Options configures the compat server.
type Options struct {
	// Namespace is the Kubernetes namespace claims are made in. e2b has no
	// namespace concept, so every compat claim lands in this one.
	Namespace string
	// Domain is echoed as the sandbox `domain`, from which the SDK derives the
	// envd host as "{port}-{sandboxID}.{domain}". Empty leaves it unset, which
	// makes the SDK fall back to its configured E2B_DOMAIN/E2B_SANDBOX_URL.
	//
	// The sandbox ids published here are DNS-label safe (see sandboxid.go), so
	// that host form is valid; wildcard DNS and a proxy must still route the host
	// or the E2b-Sandbox-Id / E2b-Sandbox-Port headers the SDK sends.
	Domain string
	// EnvdVersion overrides DefaultEnvdVersion.
	EnvdVersion string
	// APIKeys, when non-empty, is the set of accepted X-API-KEY values. Empty
	// disables authentication and is refused unless AllowAnonymous is set, so a
	// misconfigured deployment cannot silently serve an open claim endpoint.
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

// Server translates e2b REST calls onto a scale.SandboxStore.
type Server struct {
	store    scale.SandboxStore
	resolver scale.ClaimIDResolver
	opts     Options
	keys     map[string]struct{}
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
	opts.SizeClass = cmp.Or(opts.SizeClass, scale.SizeClassSmall)
	keys := make(map[string]struct{}, len(opts.APIKeys))
	for _, k := range opts.APIKeys {
		if k = strings.TrimSpace(k); k != "" {
			keys[k] = struct{}{}
		}
	}
	if len(keys) == 0 && !opts.AllowAnonymous {
		return nil, errors.New("e2bcompat: no API key configured; set one or enable anonymous access explicitly")
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
	mux.Handle("GET /sandboxes", s.auth(http.HandlerFunc(s.listSandboxes)))
	mux.Handle("GET /v2/sandboxes", s.auth(http.HandlerFunc(s.listSandboxes)))
	mux.Handle("GET /sandboxes/{sandboxID}", s.auth(http.HandlerFunc(s.getSandbox)))
	mux.Handle("DELETE /sandboxes/{sandboxID}", s.auth(http.HandlerFunc(s.deleteSandbox)))
	mux.Handle("POST /sandboxes/{sandboxID}/timeout", s.auth(http.HandlerFunc(s.setTimeout)))
	mux.Handle("POST /sandboxes/{sandboxID}/refreshes", s.auth(http.HandlerFunc(s.refresh)))

	// Lifecycle verbs.
	mux.Handle("POST /sandboxes/{sandboxID}/pause", s.auth(http.HandlerFunc(s.pauseSandbox)))
	mux.Handle("POST /sandboxes/{sandboxID}/connect", s.auth(http.HandlerFunc(s.connectSandbox)))
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

// auth enforces the X-API-KEY header unless anonymous access is allowed. The
// comparison is constant-time so a valid key cannot be recovered by timing.
func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(s.keys) > 0 {
			presented := r.Header.Get(apiKeyHeader)
			if !s.validKey(presented) {
				writeError(w, http.StatusUnauthorized, "invalid API key")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) validKey(presented string) bool {
	if presented == "" {
		return false
	}
	ok := false
	for k := range s.keys {
		if subtle.ConstantTimeCompare([]byte(k), []byte(presented)) == 1 {
			ok = true
		}
	}
	return ok
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

	name := generateName()
	pool := scale.PoolKey{
		Template: req.TemplateID,
		Net:      netFor(req.AllowInternetAccess),
		Size:     s.opts.SizeClass,
	}
	assignment, err := s.store.Claim(r.Context(), s.opts.Namespace, name, pool, timeoutSeconds(req.Timeout))
	if err != nil {
		if scale.IsNoWarmCapacity(err) {
			// Retryable: warm capacity refills asynchronously on the node.
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
		SandboxID:       publicID(assignment.SandboxName),
		ClientID:        assignment.Node,
		EnvdVersion:     s.opts.EnvdVersion,
		EnvdAccessToken: assignment.Token,
		Domain:          s.opts.Domain,
	})
}

// listSandboxes reports the live sandboxes in the compat namespace.
func (s *Server) listSandboxes(w http.ResponseWriter, r *http.Request) {
	list, err := s.store.List(r.Context(), scale.ListOptions{Namespace: s.opts.Namespace})
	if err != nil {
		s.opts.Log.Error(err, "e2b list: store list failed")
		writeError(w, http.StatusInternalServerError, "failed to list sandboxes")
		return
	}
	out := make([]SandboxDetail, 0, len(list.Items))
	for i := range list.Items {
		out = append(out, s.detailFor(&list.Items[i]))
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
	// Release against the raw node-local claim id, never the id as the client
	// spelled it: the published id is a DNS-safe rendering, and sandboxd knows
	// only the original.
	claimID := sb.Annotations[scale.ClaimIDAnnotation]
	if err := s.store.Release(r.Context(), node, claimID); err != nil {
		s.opts.Log.Error(err, "e2b delete: release failed", "sandboxID", id, "claimID", claimID, "node", node)
		writeError(w, http.StatusInternalServerError, "failed to release the sandbox")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// setTimeout accepts the SDK's TTL update. The node fixes a claim's TTL when it
// hands the microVM over, so this verifies the sandbox exists and acknowledges;
// it does not silently claim to have extended a deadline it cannot move.
func (s *Server) setTimeout(w http.ResponseWriter, r *http.Request) {
	var req SandboxTimeoutRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if req.Timeout < 0 {
		writeError(w, http.StatusBadRequest, "timeout must be >= 0")
		return
	}
	id := r.PathValue("sandboxID")
	if _, err := s.lookup(r, id); err != nil {
		s.writeLookupError(w, err, id, "timeout")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// refresh is the SDK keepalive. Liveness is node-owned, so this confirms the
// sandbox is still live and acknowledges.
func (s *Server) refresh(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("sandboxID")
	if _, err := s.lookup(r, id); err != nil {
		s.writeLookupError(w, err, id, "refresh")
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
	sb, err := s.resolver.GetByClaimID(r.Context(), s.opts.Namespace, id, func(claimID string) bool {
		return matchesID(claimID, id)
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
	if errors.Is(err, errSandboxNotFound) {
		writeError(w, http.StatusNotFound, fmt.Sprintf("sandbox %q not found", id))
		return
	}
	s.opts.Log.Error(err, "e2b "+op+": lookup failed", "sandboxID", id)
	writeError(w, http.StatusInternalServerError, "failed to resolve the sandbox")
}

// detailFor renders a live Sandbox as the e2b detail shape. Fields e2b requires
// but cocoon does not track per sandbox (disk size) are reported as zero values
// rather than omitted, so the SDK's decoder stays happy. envdAccessToken is one
// of them on this path: the token is handed out once at claim time and node
// inventory deliberately carries no per-sandbox secret.
func (s *Server) detailFor(sb *sandboxv1beta1.Sandbox) SandboxDetail {
	started := sb.CreationTimestamp.Time
	if started.IsZero() {
		started = time.Now()
	}
	state := StateRunning
	if sb.Labels[scale.PhaseLabel] == phaseHibernated {
		state = StatePaused
	}
	endAt := started.Add(DefaultTimeoutSeconds * time.Second)
	if deadline, err := time.Parse(time.RFC3339, sb.Annotations[scale.DeadlineAnnotation]); err == nil {
		endAt = deadline
	}
	return SandboxDetail{
		TemplateID:  templateOf(sb),
		SandboxID:   publicID(sb.Annotations[scale.ClaimIDAnnotation]),
		ClientID:    sb.Status.NodeName,
		StartedAt:   started.UTC().Format(time.RFC3339),
		EndAt:       endAt.UTC().Format(time.RFC3339),
		State:       state,
		EnvdVersion: s.opts.EnvdVersion,
		Domain:      s.opts.Domain,
	}
}

// templateOf reports the pool template a sandbox was claimed from. A sandbox
// synthesized from node inventory carries it as a label; only an object that
// still holds its own pod spec can be read for the container image.
func templateOf(sb *sandboxv1beta1.Sandbox) string {
	if t := sb.Labels[scale.TemplateLabel]; t != "" {
		return t
	}
	if c := sb.Spec.PodTemplate.Spec.Containers; len(c) > 0 {
		return c[0].Image
	}
	return ""
}

// netFor maps e2b's allow_internet_access onto the pool's network axis.
func netFor(allowInternet *bool) string {
	if allowInternet != nil && *allowInternet {
		return scale.NetEgress
	}
	return scale.NetDefault
}
