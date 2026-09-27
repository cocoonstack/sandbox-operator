package e2bcompat

import (
	"bufio"
	"net"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
)

func TestReadEnvdMetricsAsksEnvdOnItsGuestPort(t *testing.T) {
	want := envdMetrics{Timestamp: 1790517471, CPUCount: 1, CPUUsedPct: 0.26, MemTotal: 490504192, MemUsed: 209805312, MemCache: 125820928, DiskUsed: 27705344, DiskTotal: 10464022528}
	store := &lifecycleStore{metrics: map[string]envdMetrics{"sb_1": want}}
	s := &Server{store: store}

	got, live, err := s.readEnvdMetrics(t.Context(), "node-a", "sb_1")
	require.NoError(t, err)
	assert.True(t, live)
	assert.Equal(t, want, got)
	assert.Equal(t, uint32(envdPort), store.dialPort.Load())

	_, live, err = s.readEnvdMetrics(t.Context(), "node-a", "sb_paused")
	require.NoError(t, err)
	assert.False(t, live, "a paused sandbox's Conflict is no live sample, not an error")
}

func TestReadEnvdMetricsTakesAnEnvdFailureForAnError(t *testing.T) {
	store := &lifecycleStore{envdDown: map[string]bool{"sb_1": true}}
	s := &Server{store: store}

	_, live, err := s.readEnvdMetrics(t.Context(), "node-a", "sb_1")
	require.Error(t, err)
	assert.False(t, live)
	assert.False(t, k8serrors.IsNotFound(err) || k8serrors.IsConflict(err), "an envd 404 must not read as the node's answer: %v", err)
}

func envdPipe(status int, body string) net.Conn {
	conn, envd := net.Pipe()
	go func() {
		defer func() { _ = envd.Close() }()
		if _, err := http.ReadRequest(bufio.NewReader(envd)); err != nil {
			return
		}
		_, _ = envd.Write([]byte("HTTP/1.1 " + strconv.Itoa(status) + " " + http.StatusText(status) + "\r\nContent-Length: " + strconv.Itoa(len(body)) + "\r\n\r\n" + body))
	}()
	return conn
}
