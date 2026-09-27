package e2bcompat

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/projecteru2/core/log"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

const (
	// envdPort is the port envd serves inside an e2b guest.
	envdPort       = 49983
	envdReplyMax   = 64 << 10
	envdHostAlias  = "envd"
	metricsTimeout = 2 * time.Second
	initTimeout    = 5 * time.Second

	envdDefaultUser    = "user"
	envdDefaultWorkdir = "/home/user"
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

// envdInit is envd's POST /init body.
type envdInit struct {
	AccessToken    string            `json:"accessToken"`
	EnvVars        map[string]string `json:"envVars,omitempty"`
	DefaultUser    string            `json:"defaultUser,omitempty"`
	DefaultWorkdir string            `json:"defaultWorkdir,omitempty"`
	Timestamp      time.Time         `json:"timestamp"`
}

// AccessToken derives a sandbox's envd access token from its claim token, so the edge verifies one without storing it.
func AccessToken(secret []byte, claimToken string) string {
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(claimToken))
	return hex.EncodeToString(m.Sum(nil))
}

// readEnvdMetrics reads envd's GET /metrics inside sandbox id through its node's passive relay; live is false for a paused one, which is never woken.
func (s *Server) readEnvdMetrics(ctx context.Context, node, id string) (envdMetrics, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, metricsTimeout)
	defer cancel()
	rec, err := s.store.Read(ctx, node, id)
	if err != nil {
		return envdMetrics{}, false, err
	}
	if rec.Paused {
		return envdMetrics{}, false, nil
	}
	status, body, err := s.envdCall(ctx, node, id, http.MethodGet, "/metrics", AccessToken(s.opts.EnvdSecret, rec.Token), nil)
	if k8serrors.IsConflict(err) {
		return envdMetrics{}, false, nil
	}
	if err != nil {
		return envdMetrics{}, false, fmt.Errorf("envd metrics of %s: %w", id, err)
	}
	if status != http.StatusOK {
		return envdMetrics{}, false, fmt.Errorf("envd metrics of %s: envd answered %d", id, status)
	}
	var out envdMetrics
	if err := json.Unmarshal(body, &out); err != nil {
		return envdMetrics{}, false, fmt.Errorf("decode envd metrics of %s: %w", id, err)
	}
	return out, true, nil
}

// initEnvd sets envd's access token, defaults and env inside sandbox id, and stamps the guest clock.
func (s *Server) initEnvd(ctx context.Context, node, id string, req envdInit) error {
	ctx, cancel := context.WithTimeout(ctx, initTimeout)
	defer cancel()
	req.Timestamp = time.Now()
	payload, err := json.Marshal(req) //nolint:gosec // /init is how envd receives its access token
	if err != nil {
		return err
	}
	status, _, err := s.envdCall(ctx, node, id, http.MethodPost, "/init", "", payload)
	if err != nil {
		return fmt.Errorf("envd init of %s: %w", id, err)
	}
	if status != http.StatusNoContent {
		return fmt.Errorf("envd init of %s: envd answered %d", id, status)
	}
	return nil
}

// handOver moves a forked child's envd onto token: the child starts with its parent's, which only the metadata hash overrides.
func (s *Server) handOver(ctx context.Context, child scale.Assignment, token string) error {
	sum := sha512.Sum512([]byte(token))
	doc := []byte(`{"accessTokenHash":"` + hex.EncodeToString(sum[:]) + `"}`)
	if err := s.store.SetInstanceMetadata(ctx, child.Node, child.SandboxName, doc); err != nil {
		return err
	}
	return s.initEnvd(ctx, child.Node, child.SandboxName, envdInit{AccessToken: token})
}

// releaseAll gives back claims whose envd could not be initialized; a failed release is left to the lease.
func (s *Server) releaseAll(ctx context.Context, claims []scale.Assignment) {
	ctx = context.WithoutCancel(ctx)
	for _, a := range claims {
		if err := s.store.Release(ctx, a.Node, a.SandboxName); err != nil {
			log.WithFunc("e2bcompat.releaseAll").Warnf(ctx, "release after a failed envd init sandboxID=%s node=%s: %v", a.SandboxName, a.Node, err)
		}
	}
}

// envdCall sends one request to envd inside sandbox id over its node's passive relay and reads the bounded reply; token authenticates it to envd.
func (s *Server) envdCall(ctx context.Context, node, id, method, path, token string, body []byte) (int, []byte, error) {
	conn, err := s.store.DialGuestPort(ctx, node, id, envdPort)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = conn.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	req, err := http.NewRequestWithContext(ctx, method, "http://"+envdHostAlias+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("X-Access-Token", token)
	}
	if err = req.Write(conn); err != nil {
		return 0, nil, err
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	reply, err := io.ReadAll(io.LimitReader(resp.Body, envdReplyMax))
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, reply, nil
}
