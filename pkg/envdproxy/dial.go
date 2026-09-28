package envdproxy

import (
	"context"
	"net"
	"net/http"

	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
)

// target carries one request's destination through the transport, which only
// ever sees the outbound request and its context.
type target struct {
	owner Owner
	port  uint16
	h2    bool
}

// targetKey addresses target in a request context.
type targetKey struct{}

// guestTransport carries a request to the guest daemon over the protocol that
// daemon serves, which is not the one the client used: the edge may answer
// HTTP/2 while the guest speaks only HTTP/1.1.
type guestTransport struct {
	h1 *http.Transport
	h2 *http.Transport
}

// newGuestTransport builds both halves over sandboxd's guest-port relay. Neither pools: a reused
// connection would outlive the sandboxd relay carrying it, and reusing one
// across sandboxes would cross a tenancy boundary.
func newGuestTransport(dialer *net.Dialer) *guestTransport {
	dialContext := func(ctx context.Context, _, _ string) (net.Conn, error) {
		t, _ := targetFrom(ctx)
		return sandboxd.DialPort(ctx, dialer, t.owner.Address, t.owner.ClaimID, t.owner.Token, t.port)
	}
	// With HTTP/1 also set, a plaintext transport cannot negotiate HTTP/2 and picks HTTP/1.
	var h2 http.Protocols
	h2.SetUnencryptedHTTP2(true)
	return &guestTransport{
		h1: &http.Transport{DisableKeepAlives: true, DialContext: dialContext},
		h2: &http.Transport{DisableKeepAlives: true, DialContext: dialContext, Protocols: &h2},
	}
}

func (t *guestTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if tgt, ok := targetFrom(r.Context()); ok && tgt.h2 {
		return t.h2.RoundTrip(r)
	}
	return t.h1.RoundTrip(r)
}

// withTarget attaches the destination the transport dials for this request.
func withTarget(ctx context.Context, t target) context.Context {
	return context.WithValue(ctx, targetKey{}, t)
}

func targetFrom(ctx context.Context) (target, bool) {
	t, ok := ctx.Value(targetKey{}).(target)
	return t, ok
}
