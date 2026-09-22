package envdproxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
)

const (
	// upgradeProto names sandboxd's guest-port passthrough.
	upgradeProto = "tcp"
	// handshakeReplyMax caps the 101 reply, so a wedged node cannot stream a
	// header block into this process.
	handshakeReplyMax = 16 << 10
)

var errNoTarget = errors.New("envdproxy: request carries no target")

// target carries one request's destination through the transport, which only
// ever sees the outbound request and its context.
type target struct {
	owner Owner
	port  uint16
	token string
	h2    bool
}

// targetKey addresses target in a request context.
type targetKey struct{}

// withTarget attaches the destination the transport dials for this request.
func withTarget(ctx context.Context, t target) context.Context {
	return context.WithValue(ctx, targetKey{}, t)
}

func targetFrom(ctx context.Context) (target, bool) {
	t, ok := ctx.Value(targetKey{}).(target)
	return t, ok
}

// guestDialer opens the connection a request's target names.
type guestDialer func(ctx context.Context, t target) (net.Conn, error)

// dialGuest opens sandboxd's guest-port relay and hands back the upgraded
// connection as a plain net.Conn.
func dialGuest(dialer *net.Dialer) guestDialer {
	return func(ctx context.Context, t target) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, "tcp", t.owner.Address)
		if err != nil {
			return nil, fmt.Errorf("dial node: %w", err)
		}
		stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
		defer stop()

		req, reqErr := upgradeRequest(ctx, t)
		if reqErr != nil {
			_ = conn.Close()
			return nil, reqErr
		}
		if writeErr := req.Write(conn); writeErr != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("write upgrade: %w", writeErr)
		}
		br := bufio.NewReader(io.LimitReader(conn, handshakeReplyMax))
		resp, readErr := http.ReadResponse(br, req)
		if readErr != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("read upgrade reply: %w", readErr)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusSwitchingProtocols {
			_ = conn.Close()
			return nil, nodeStatusError{status: resp.StatusCode}
		}
		// the handshake reader may already hold bytes the guest sent
		return &bufConn{Conn: conn, r: io.MultiReader(io.LimitReader(br, int64(br.Buffered())), conn)}, nil
	}
}

func upgradeRequest(ctx context.Context, t target) (*http.Request, error) {
	path := "/v1/sandboxes/" + url.PathEscape(t.owner.ClaimID) + "/ports/" + strconv.FormatUint(uint64(t.port), 10)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+t.owner.Address+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+t.token)
	req.Header.Set("Upgrade", upgradeProto)
	req.Header.Set("Connection", "Upgrade")
	return req, nil
}

// nodeStatusError is a non-101 answer from the owning node.
type nodeStatusError struct {
	status int
}

func (e nodeStatusError) Error() string {
	return fmt.Sprintf("node refused the guest-port relay: %d", e.status)
}

// guestTransport carries a request to the guest daemon over the protocol that
// daemon serves, which is not the one the client used: the edge may answer
// HTTP/2 while the guest speaks only HTTP/1.1.
type guestTransport struct {
	h1 *http.Transport
	h2 *http.Transport
}

// newGuestTransport builds both halves over dial. Neither pools: a reused
// connection would outlive the sandboxd relay carrying it, and reusing one
// across sandboxes would cross a tenancy boundary.
func newGuestTransport(dial guestDialer) *guestTransport {
	dialContext := func(ctx context.Context, _, _ string) (net.Conn, error) {
		t, ok := targetFrom(ctx)
		if !ok {
			return nil, errNoTarget
		}
		return dial(ctx, t)
	}
	// envd serves h2c, so the HTTP/2 half must offer unencrypted HTTP/2 alone:
	// with HTTP/1 also set a plaintext transport cannot negotiate and picks it.
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

// bufConn replays what the upgrade handshake's reader buffered past the 101.
type bufConn struct {
	net.Conn
	r io.Reader
}

func (c *bufConn) Read(p []byte) (int, error) { return c.r.Read(p) }
