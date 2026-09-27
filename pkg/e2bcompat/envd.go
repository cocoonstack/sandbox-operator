package e2bcompat

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
)

const (
	// envdPort is the port envd serves inside an e2b guest.
	envdPort       = 49983
	envdReplyMax   = 64 << 10
	envdHostAlias  = "envd"
	metricsTimeout = 2 * time.Second
)

// envdMetrics is envd's GET /metrics reply: the guest's own view of its CPU, memory and root disk.
type envdMetrics struct {
	Timestamp  int64   `json:"ts"`
	CPUCount   int32   `json:"cpu_count"`
	CPUUsedPct float32 `json:"cpu_used_pct"`
	MemTotal   int64   `json:"mem_total"`
	MemUsed    int64   `json:"mem_used"`
	MemCache   int64   `json:"mem_cache"`
	DiskUsed   int64   `json:"disk_used"`
	DiskTotal  int64   `json:"disk_total"`
}

// readEnvdMetrics reads envd's GET /metrics inside sandbox id through its node's passive relay; live is false for a paused one, which is never woken.
func (s *Server) readEnvdMetrics(ctx context.Context, node, id string) (envdMetrics, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, metricsTimeout)
	defer cancel()
	conn, err := s.store.DialGuestPort(ctx, node, id, envdPort)
	if k8serrors.IsConflict(err) {
		return envdMetrics{}, false, nil
	}
	if err != nil {
		return envdMetrics{}, false, err
	}
	defer func() { _ = conn.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+envdHostAlias+"/metrics", nil)
	if err != nil {
		return envdMetrics{}, false, err
	}
	if err = req.Write(conn); err != nil {
		return envdMetrics{}, false, fmt.Errorf("envd metrics of %s: %w", id, err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		return envdMetrics{}, false, fmt.Errorf("envd metrics of %s: %w", id, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return envdMetrics{}, false, fmt.Errorf("envd metrics of %s: envd answered %d", id, resp.StatusCode)
	}
	var out envdMetrics
	if err := json.NewDecoder(io.LimitReader(resp.Body, envdReplyMax)).Decode(&out); err != nil {
		return envdMetrics{}, false, fmt.Errorf("decode envd metrics of %s: %w", id, err)
	}
	return out, true, nil
}
