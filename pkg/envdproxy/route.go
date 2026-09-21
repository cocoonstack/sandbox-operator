package envdproxy

import (
	"net"
	"net/http"
	"strconv"
	"strings"
)

const (
	// sandboxIDHeader and sandboxPortHeader are what the e2b SDK sends when one
	// shared host serves every sandbox instead of a per-sandbox wildcard name.
	sandboxIDHeader   = "E2b-Sandbox-Id"
	sandboxPortHeader = "E2b-Sandbox-Port"
	// accessTokenHeader is the per-sandbox data-plane credential.
	accessTokenHeader = "X-Access-Token" //nolint:gosec // a header name, not a credential
	apiKeyHeader      = "X-API-KEY"
)

// internalPaths are envd's own control surface (x-internal in its spec). They
// reconfigure or freeze the guest and are never part of the SDK's data plane,
// so the edge refuses them rather than relaying a client into them.
var internalPaths = []string{"/init", "/freeze", "/unfreeze", "/fsfreeze", "/fsthaw", "/collapse"}

// route is one request's resolved destination.
type route struct {
	sandboxID string
	port      uint16
}

// routeOf reads the destination from the two forms the SDK sends: the derived
// host "{port}-{sandboxID}.{domain}", and the header pair used when every
// sandbox shares one host. Headers win, because a client that sends them has
// already decided the host does not carry the address.
func routeOf(r *http.Request, domain string) (route, bool) {
	if id := strings.TrimSpace(r.Header.Get(sandboxIDHeader)); id != "" {
		port, ok := parsePort(r.Header.Get(sandboxPortHeader))
		if !ok {
			return route{}, false
		}
		return route{sandboxID: id, port: port}, true
	}
	return routeOfHost(r.Host, domain)
}

// routeOfHost splits "{port}-{sandboxID}.{domain}". A sandbox id carries
// hyphens of its own, so only the first one separates the port.
func routeOfHost(host, domain string) (route, bool) {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	label, ok := strings.CutSuffix(strings.ToLower(host), "."+strings.ToLower(domain))
	if !ok || label == "" || strings.Contains(label, ".") {
		return route{}, false
	}
	portStr, id, ok := strings.Cut(label, "-")
	if !ok || id == "" {
		return route{}, false
	}
	port, ok := parsePort(portStr)
	if !ok {
		return route{}, false
	}
	return route{sandboxID: id, port: port}, true
}

// parsePort accepts only a bare 1-65535: ParseUint rejects a sign, so "+22"
// cannot smuggle a second spelling of a port past a policy that lists one.
func parsePort(s string) (uint16, bool) {
	port, err := strconv.ParseUint(strings.TrimSpace(s), 10, 16)
	if err != nil || port == 0 {
		return 0, false
	}
	return uint16(port), true
}

// internalPath reports whether the request addresses envd's own control surface.
func internalPath(path string) bool {
	clean := "/" + strings.Trim(path, "/")
	for _, p := range internalPaths {
		if strings.EqualFold(clean, p) {
			return true
		}
	}
	return false
}
