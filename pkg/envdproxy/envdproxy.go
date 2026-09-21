// Package envdproxy is the edge data plane for the e2b-compatible surface: one
// public entry point that carries an unmodified e2b SDK's files, commands and
// pty traffic into the right sandbox.
//
// It translates nothing. The SDK speaks envd's own contract end to end; this
// only decides which sandbox a request belongs to, authorizes it with that
// sandbox's token, and hands the bytes to the owning node's guest-port relay
// (GET /v1/sandboxes/{id}/ports/{port}), which reaches 127.0.0.1 inside the
// guest over vsock. A sandbox on the hardened `none` lane has no NIC, so that
// relay is the only path in, and no client ever learns a node address.
//
// It is deliberately a separate deployment from the compute nodes: the e2b
// shape is a proxy pool in front of them, and sandboxd is already the per-node
// half.
package envdproxy

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
)

const (
	// DefaultDialTimeout bounds a node dial. The relay it opens is unbounded by
	// design: a pty or a watch stream lives as long as the client keeps it.
	DefaultDialTimeout = 10 * time.Second
	// flushInterval streams a proxied response as it arrives. ConnectRPC
	// streaming and pty output are useless buffered.
	flushInterval = -1
)

// Options configures the proxy.
type Options struct {
	// Domain is the base the SDK derives a sandbox host from, as
	// "{port}-{sandboxID}.{domain}". It must match the apiserver's
	// --e2b-domain or the two disagree about what a sandbox is called.
	Domain string
	// DialTimeout overrides DefaultDialTimeout.
	DialTimeout time.Duration
	// Log receives request-level failures.
	Log logr.Logger
}

// Server routes one public host onto many sandboxes' guest ports.
type Server struct {
	resolver  Resolver
	transport *guestTransport
	opts      Options
}

// NewServer builds the proxy. It fails without a domain: the SDK derives the
// sandbox host from it, so a proxy that does not know it cannot route at all.
func NewServer(resolver Resolver, opts Options) (*Server, error) {
	if resolver == nil {
		return nil, errors.New("envdproxy: resolver is required")
	}
	if strings.TrimSpace(opts.Domain) == "" {
		return nil, errors.New("envdproxy: no domain configured; the SDK's sandbox host is derived from it")
	}
	if opts.DialTimeout <= 0 {
		opts.DialTimeout = DefaultDialTimeout
	}
	dialer := &net.Dialer{Timeout: opts.DialTimeout}
	return &Server{resolver: resolver, transport: newGuestTransport(dialGuest(dialer)), opts: opts}, nil
}

// Handler returns the routed handler. Serve it with Protocols(): a ConnectRPC
// stream needs HTTP/2, which must survive an edge that terminated TLS in front
// of this process.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/", s.serve)
	return mux
}

// serve resolves the sandbox a request names and relays it into the guest.
func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	rt, ok := routeOf(r, s.opts.Domain)
	if !ok {
		writeError(w, http.StatusBadRequest, "no sandbox in the request host or headers")
		return
	}
	if internalPath(r.URL.Path) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	token := strings.TrimSpace(r.Header.Get(accessTokenHeader))
	if token == "" {
		writeError(w, http.StatusUnauthorized, "missing "+accessTokenHeader)
		return
	}
	owner, err := s.resolver.Owner(r.Context(), rt.sandboxID)
	if err != nil {
		// A caller must not learn from this whether the id exists, which node
		// holds it, or whether the fleet is reachable.
		if !errors.Is(err, errSandboxNotFound) {
			s.opts.Log.Error(err, "envd-proxy: resolve sandbox", "sandboxID", rt.sandboxID)
		}
		writeError(w, http.StatusBadGateway, "sandbox unavailable")
		return
	}
	s.proxy(rt).ServeHTTP(w, r.WithContext(withTarget(r.Context(), target{
		owner: owner,
		port:  rt.port,
		token: token,
		h2:    r.ProtoMajor == 2,
	})))
}

// proxy forwards one request into the guest untouched apart from the host-side
// credentials, which are stripped: the sandbox token authorizes this hop only,
// and a guest that learned it could drive its own sandbox's control plane.
func (s *Server) proxy(rt route) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		FlushInterval: flushInterval,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetXForwarded()
			pr.Out.URL.Scheme = "http"
			// the derived host is unique per sandbox and port, which is what
			// keeps the HTTP/2 connection pool from crossing sandboxes
			pr.Out.URL.Host = s.sandboxHost(rt)
			pr.Out.Host = pr.Out.URL.Host
			pr.Out.Header.Del(accessTokenHeader)
			pr.Out.Header.Del(apiKeyHeader)
		},
		Transport: s.transport,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			s.writeUpstreamError(w, r, err)
		},
	}
}

// sandboxHost is the SDK's own spelling of a sandbox's envd host.
func (s *Server) sandboxHost(rt route) string {
	return strconv.FormatUint(uint64(rt.port), 10) + "-" + rt.sandboxID + "." + s.opts.Domain
}

// writeUpstreamError maps a node's refusal without naming the node. sandboxd
// answers 404 for both an unknown id and a wrong token; the id was just
// resolved from inventory, so the token is what the caller can still fix.
func (s *Server) writeUpstreamError(w http.ResponseWriter, r *http.Request, err error) {
	if status, ok := errors.AsType[nodeStatusError](err); ok {
		switch status.status {
		case http.StatusNotFound, http.StatusUnauthorized:
			writeError(w, http.StatusUnauthorized, "invalid sandbox access token")
			return
		case http.StatusBadRequest:
			writeError(w, http.StatusBadRequest, "unsupported sandbox port")
			return
		}
	}
	s.opts.Log.Error(err, "envd-proxy: relay failed", "host", r.Host, "path", r.URL.Path)
	writeError(w, http.StatusBadGateway, "sandbox unreachable")
}

// Protocols is what an http.Server fronting Handler must offer: HTTP/1.1 plus
// HTTP/2 both encrypted and in the clear, since the proxy may sit behind an
// edge that already terminated TLS.
func Protocols() *http.Protocols {
	p := &http.Protocols{}
	p.SetHTTP1(true)
	p.SetHTTP2(true)
	p.SetUnencryptedHTTP2(true)
	return p
}

// writeError answers in e2b's envelope, which the SDK surfaces as the failure.
func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, "{\"code\":%d,\"message\":%q}\n", status, msg)
}
