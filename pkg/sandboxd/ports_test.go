package sandboxd

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnvdMetricsSpeaksHTTPToEnvdThroughTheRelay(t *testing.T) {
	want := EnvdMetrics{Timestamp: 1790517471, CPUCount: 1, CPUUsedPct: 0.26, MemTotal: 490504192, MemUsed: 209805312, MemCache: 125820928, DiskUsed: 27705344, DiskTotal: 10464022528}
	var gotPath, gotAuth, gotUpgrade, gotEnvdPath string
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotAuth, gotUpgrade = r.URL.Path, r.Header.Get("Authorization"), r.Header.Get("Upgrade")
		conn, rw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: tcp\r\nConnection: Upgrade\r\n\r\n")
		_ = rw.Flush()
		req, err := http.ReadRequest(bufio.NewReader(rw))
		if err != nil {
			return
		}
		gotEnvdPath = req.URL.Path
		body, _ := json.Marshal(map[string]any{
			"ts": want.Timestamp, "cpu_count": want.CPUCount, "cpu_used_pct": want.CPUUsedPct, "mem_total_mib": 467, "mem_used_mib": 200,
			"mem_total": want.MemTotal, "mem_used": want.MemUsed, "mem_cache": want.MemCache, "disk_used": want.DiskUsed, "disk_total": want.DiskTotal,
		})
		resp := &http.Response{
			StatusCode: http.StatusOK, ProtoMajor: 1, ProtoMinor: 1, Header: http.Header{"Content-Type": {"application/json"}},
			ContentLength: int64(len(body)), Body: io.NopCloser(strings.NewReader(string(body))),
		}
		_ = resp.Write(conn)
	}))
	defer node.Close()

	got, err := New(node.URL, "root-token").EnvdMetrics(t.Context(), "sb_1")
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Equal(t, "/v1/sandboxes/sb_1/ports/49983", gotPath)
	assert.Equal(t, "Bearer root-token", gotAuth, "the node api_token makes the relay passive")
	assert.Equal(t, "tcp", gotUpgrade)
	assert.Equal(t, "/metrics", gotEnvdPath)
}

func TestDialPortReportsTheNodesRefusalAsAnHTTPError(t *testing.T) {
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "unknown sandbox"})
	}))
	defer node.Close()

	_, err := DialPort(t.Context(), &net.Dialer{}, strings.TrimPrefix(node.URL, "http://"), "sb_gone", "tok", EnvdPort)
	he, ok := errors.AsType[*HTTPError](err)
	require.True(t, ok, "want *HTTPError, got %v", err)
	assert.Equal(t, http.StatusNotFound, he.StatusCode)
	assert.Equal(t, "unknown sandbox", he.Message)
}

func TestEnvdMetricsReportsAnEnvdFailureAsNoNodeStatus(t *testing.T) {
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conn, rw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: tcp\r\nConnection: Upgrade\r\n\r\n")
		_ = rw.Flush()
		if _, err := http.ReadRequest(bufio.NewReader(rw)); err == nil {
			_, _ = conn.Write([]byte("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n"))
		}
	}))
	defer node.Close()

	_, err := New(node.URL, "root-token").EnvdMetrics(t.Context(), "sb_1")
	require.Error(t, err)
	_, isNode := errors.AsType[*HTTPError](err)
	assert.False(t, isNode, "an envd 404 must not read as the node not knowing the sandbox: %v", err)
}
