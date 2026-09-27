package sandboxd

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
)

const (
	// EnvdPort is the port envd serves inside an e2b guest.
	EnvdPort = 49983

	portUpgradeProto = "tcp"
	// portReplyMax caps the 101 reply, so a wedged node cannot stream a header block into this process.
	portReplyMax  = 16 << 10
	envdReplyMax  = 64 << 10
	envdHostAlias = "envd"
)

// EnvdMetrics is envd's GET /metrics reply: the guest's own view of its CPU, memory and root disk.
type EnvdMetrics struct {
	Timestamp  int64   `json:"ts"`
	CPUCount   int32   `json:"cpu_count"`
	CPUUsedPct float32 `json:"cpu_used_pct"`
	MemTotal   int64   `json:"mem_total"`
	MemUsed    int64   `json:"mem_used"`
	MemCache   int64   `json:"mem_cache"`
	DiskUsed   int64   `json:"disk_used"`
	DiskTotal  int64   `json:"disk_total"`
}

// bufConn replays what the upgrade handshake's reader buffered past the 101.
type bufConn struct {
	net.Conn
	r io.Reader
}

func (c *bufConn) Read(p []byte) (int, error) { return c.r.Read(p) }

// EnvdMetrics reads envd's GET /metrics inside sandbox id over one relay opened with the node api_token,
// which the node keeps passive: a paused sandbox answers 409 unwoken and the read stamps no activity.
func (c *Client) EnvdMetrics(ctx context.Context, id string) (EnvdMetrics, error) {
	u, _ := url.Parse(c.baseURL)
	var d net.Dialer
	conn, err := DialPort(ctx, &d, u.Host, id, c.token, EnvdPort)
	if err != nil {
		return EnvdMetrics{}, fmt.Errorf("envd metrics of %s: %w", id, err)
	}
	defer func() { _ = conn.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+envdHostAlias+"/metrics", nil)
	if err != nil {
		return EnvdMetrics{}, err
	}
	if err = req.Write(conn); err != nil {
		return EnvdMetrics{}, fmt.Errorf("envd metrics of %s: %w", id, err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		return EnvdMetrics{}, fmt.Errorf("envd metrics of %s: %w", id, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return EnvdMetrics{}, fmt.Errorf("envd metrics of %s: envd answered %d", id, resp.StatusCode)
	}
	var out EnvdMetrics
	if err := json.NewDecoder(io.LimitReader(resp.Body, envdReplyMax)).Decode(&out); err != nil {
		return EnvdMetrics{}, fmt.Errorf("decode envd metrics of %s: %w", id, err)
	}
	return out, nil
}

// DialPort opens the node's GET /v1/sandboxes/{id}/ports/{port} relay; with the sandbox's own token the node wakes a paused sandbox for it.
func DialPort(ctx context.Context, d *net.Dialer, addr, id, token string, port uint16) (net.Conn, error) {
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial node: %w", err)
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	path := "/v1/sandboxes/" + url.PathEscape(id) + "/ports/" + strconv.FormatUint(uint64(port), 10)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+path, nil)
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
