package sandboxd

import (
	"bufio"
	"cmp"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const (
	portUpgradeProto = "tcp"
	// portReplyMax caps the 101 reply, so a wedged node cannot stream a header block into this process.
	portReplyMax = 16 << 10
)

// DialPort opens a guest port of sandbox id through this node's relay; an empty token presents the node api_token, which the node keeps passive (a paused sandbox answers 409 unwoken), the sandbox's own token wakes it and stamps activity.
func (c *Client) DialPort(ctx context.Context, id, token string, port uint16) (net.Conn, error) {
	var d net.Dialer
	return DialPort(ctx, &d, c.baseURL, id, cmp.Or(token, c.token), port)
}

// DialPort opens the node's GET /v1/sandboxes/{id}/ports/{port} relay at base, a host:port or an http(s) origin; with the sandbox's own token the node wakes a paused sandbox for it.
func DialPort(ctx context.Context, d *net.Dialer, base, id, token string, port uint16) (net.Conn, error) {
	scheme, host := "http", base
	if u, err := url.Parse(base); err == nil && strings.Contains(base, "://") {
		scheme, host = u.Scheme, u.Host
	}
	if _, _, err := net.SplitHostPort(host); err != nil {
		host = net.JoinHostPort(host, map[bool]string{true: "443", false: "80"}[scheme == "https"])
	}
	conn, err := d.DialContext(ctx, "tcp", host)
	if err != nil {
		return nil, fmt.Errorf("dial node: %w", err)
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if scheme == "https" {
		hostname, _, _ := net.SplitHostPort(host)
		tc := tls.Client(conn, &tls.Config{ServerName: hostname, MinVersion: tls.VersionTLS12})
		if err = tc.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("tls to node: %w", err)
		}
		conn = tc
	}

	path := "/v1/sandboxes/" + url.PathEscape(id) + "/ports/" + strconv.FormatUint(uint64(port), 10)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, scheme+"://"+host+path, nil)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Upgrade", portUpgradeProto)
	req.Header.Set("Connection", "Upgrade")
	if err = req.Write(conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write upgrade: %w", err)
	}
	br := bufio.NewReader(io.LimitReader(conn, portReplyMax))
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("read upgrade reply: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		refused := statusError(resp)
		_ = conn.Close()
		return nil, refused
	}
	// The handshake reader may already hold bytes the guest sent.
	return &bufConn{Conn: conn, r: io.MultiReader(io.LimitReader(br, int64(br.Buffered())), conn)}, nil
}

// bufConn replays what the upgrade handshake's reader buffered past the 101.
type bufConn struct {
	net.Conn
	r io.Reader
}

func (c *bufConn) Read(p []byte) (int, error) { return c.r.Read(p) }
