package sandboxd

import (
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

func TestClientDialPortOpensThePassiveRelayWithTheNodeToken(t *testing.T) {
	var gotPath, gotAuth, gotUpgrade string
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotAuth, gotUpgrade = r.URL.Path, r.Header.Get("Authorization"), r.Header.Get("Upgrade")
		conn, rw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: tcp\r\nConnection: Upgrade\r\n\r\nearly ")
		_ = rw.Flush()
		line, _ := rw.ReadString('\n')
		_, _ = conn.Write([]byte("echo " + line))
	}))
	defer node.Close()

	conn, err := New(node.URL, "root-token").DialPort(t.Context(), "sb_1", 8080)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	_, err = conn.Write([]byte("ping\n"))
	require.NoError(t, err)
	got, err := io.ReadAll(conn)
	require.NoError(t, err)
	assert.Equal(t, "early echo ping\n", string(got))
	assert.Equal(t, "/v1/sandboxes/sb_1/ports/8080", gotPath)
	assert.Equal(t, "Bearer root-token", gotAuth, "the node api_token makes the relay passive")
	assert.Equal(t, "tcp", gotUpgrade)
}

func TestDialPortReportsTheNodesRefusalAsAnHTTPError(t *testing.T) {
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "unknown sandbox"})
	}))
	defer node.Close()

	_, err := DialPort(t.Context(), &net.Dialer{}, strings.TrimPrefix(node.URL, "http://"), "sb_gone", "tok", 8080)
	he, ok := errors.AsType[*HTTPError](err)
	require.True(t, ok, "want *HTTPError, got %v", err)
	assert.Equal(t, http.StatusNotFound, he.StatusCode)
	assert.Equal(t, "unknown sandbox", he.Message)
}
