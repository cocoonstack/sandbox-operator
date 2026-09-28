package e2bcompat

import (
	"bufio"
	"context"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"

	"github.com/cocoonstack/sandbox-operator/pkg/e2bbuild"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

func TestReadEnvdMetricsAsksEnvdOnItsGuestPort(t *testing.T) {
	want := envdMetrics{Timestamp: 1790517471, CPUCount: 1, CPUUsedPct: 0.26, MemTotal: 490504192, MemUsed: 209805312, MemCache: 125820928, DiskUsed: 27705344, DiskTotal: 10464022528}
	store := &lifecycleStore{metrics: map[string]envdMetrics{"sb_1": want}, pausedIDs: map[string]bool{"sb_paused": true}}
	s := &Server{store: store, opts: Options{EnvdSecret: []byte(testEnvdSecret)}}

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
	s := &Server{store: store, opts: Options{EnvdSecret: []byte(testEnvdSecret)}}

	_, live, err := s.readEnvdMetrics(t.Context(), "node-a", "sb_1")
	require.Error(t, err)
	assert.False(t, live)
	assert.False(t, k8serrors.IsNotFound(err) || k8serrors.IsConflict(err), "an envd 404 must not read as the node's answer: %v", err)
}

func TestCreateInitsEnvdWithTheDerivedToken(t *testing.T) {
	store := &fakeStore{assign: scale.Assignment{SandboxName: "sb_new", Node: "node-a", Token: "claim-tok"}}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes", `{"templateID":"img","envVars":{"A":"1"}}`, testKey)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	var got Sandbox
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	token := AccessToken([]byte(testEnvdSecret), "claim-tok")
	assert.Equal(t, token, got.EnvdAccessToken)
	assert.NotEqual(t, "claim-tok", got.EnvdAccessToken, "the claim token never reaches the client")

	require.Len(t, store.envdCalls, 1)
	id, rest, _ := strings.Cut(store.envdCalls[0], " ")
	method, rest, _ := strings.Cut(rest, " ")
	path, body, _ := strings.Cut(rest, " ")
	assert.Equal(t, "sb_new POST /init", id+" "+method+" "+path)
	var init envdInit
	require.NoError(t, json.Unmarshal([]byte(body), &init))
	assert.Equal(t, token, init.AccessToken)
	assert.Equal(t, map[string]string{"A": "1"}, init.EnvVars)
	assert.Equal(t, "user /home/user", init.DefaultUser+" "+init.DefaultWorkdir)
	assert.WithinDuration(t, time.Now(), init.Timestamp, time.Minute)
	assert.Empty(t, store.metadataDocs, "create relies on envd's first-time setup, not a metadata hash")
}

func TestARelayedCreateHandsEnvdTheProxyUnderTheRequestEnvs(t *testing.T) {
	for route, want := range map[string]map[string]string{
		"relay":  {"http_proxy": "http://127.0.0.1:3128", "https_proxy": "http://127.0.0.1:3128", "no_proxy": "mine", "A": "1"},
		"none":   {"no_proxy": "mine", "A": "1"},
		"direct": {"no_proxy": "mine", "A": "1"},
	} {
		store := &fakeStore{assign: scale.Assignment{SandboxName: "sb_new", Node: "node-a", Token: "claim-tok", NetRoute: route}}
		h := newTestServer(t, store)
		w := do(t, h, http.MethodPost, "/sandboxes", `{"templateID":"img","envVars":{"A":"1","no_proxy":"mine"}}`, testKey)
		require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
		require.Len(t, store.envdCalls, 1)
		_, body, _ := strings.Cut(store.envdCalls[0], " POST /init ")
		var init envdInit
		require.NoError(t, json.Unmarshal([]byte(body), &init))
		assert.Equal(t, want, init.EnvVars, route)
		assert.False(t, store.claimOpts.NoEgress, "an unset allow_internet_access keeps the pool's policy")
	}

	store := &fakeStore{assign: scale.Assignment{SandboxName: "sb_new", Node: "node-a"}}
	w := do(t, newTestServer(t, store), http.MethodPost, "/sandboxes", `{"templateID":"img","allow_internet_access":false}`, testKey)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	assert.Equal(t, [2]any{"none", true}, [2]any{store.claimPool.Net, store.claimOpts.NoEgress}, "false asks for no egress on a pool image too")
}

func TestCreateReleasesTheClaimWhenEnvdInitFails(t *testing.T) {
	store := &fakeStore{assign: scale.Assignment{SandboxName: "sb_new", Node: "node-a", Token: "claim-tok"}, initStatus: http.StatusUnauthorized}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes", `{"templateID":"img"}`, testKey)
	assert.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
	assert.Equal(t, "node-a sb_new", store.releasedNode+" "+store.releasedID)
}

func TestForkHandsEachChildItsOwnToken(t *testing.T) {
	store := &lifecycleStore{forkChildren: []scale.Assignment{
		{SandboxName: "sb_c1", Node: "node-a", Token: "t1"},
		{SandboxName: "sb_c2", Node: "node-a", Token: "t2"},
	}}
	store.items = []sandboxv1beta1.Sandbox{liveSandbox("s1", "sb_abc", "node-a", "img")}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/fork", `{"count":2}`, testKey)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	var got []SandboxForkResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got, 2)
	slices.Sort(store.metadataDocs)
	slices.Sort(store.envdCalls)
	for i, claim := range []string{"t1", "t2"} {
		token := AccessToken([]byte(testEnvdSecret), claim)
		sum := sha512.Sum512([]byte(token))
		child := store.forkChildren[i].SandboxName
		assert.Equal(t, token, got[i].Sandbox.EnvdAccessToken)
		assert.Equal(t, child+` {"accessTokenHash":"`+hex.EncodeToString(sum[:])+`"}`, store.metadataDocs[i])
		assert.Contains(t, store.envdCalls[i], child+" POST /init ")
		assert.Contains(t, store.envdCalls[i], `"accessToken":"`+token+`"`)
	}
}

func TestAStartedCommandReturnsWhileItRuns(t *testing.T) {
	g := envdGuest{s: &Server{store: runningEnvd{}}}
	done := make(chan error, 1)
	go func() {
		done <- g.Start(t.Context(), scale.Assignment{SandboxName: "sb_1"}, e2bbuild.Command{Line: "serve", User: "user"})
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Start waited for the process to end")
	}
}

func processReply(stdout string, exit int) string {
	out := base64.StdEncoding.EncodeToString([]byte(stdout))
	return string(slices.Concat(connectFrame(0, []byte(`{"event":{"start":{"pid":7}}}`)), connectFrame(0, []byte(`{"event":{"data":{"stdout":"`+out+`"}}}`)),
		connectFrame(0, []byte(`{"event":{"end":{"exitCode":`+strconv.Itoa(exit)+`,"exited":true}}}`)), connectFrame(connectEndStream, []byte(`{}`))))
}

func envdPipe(status int, body string) net.Conn {
	return fakeEnvd(func(*http.Request, string) (int, string) { return status, body })
}

// fakeEnvd serves one request on a pipe with the answer serve picks for it and its body.
func fakeEnvd(serve func(r *http.Request, body string) (int, string)) net.Conn {
	conn, envd := net.Pipe()
	go func() {
		defer func() { _ = envd.Close() }()
		r, err := http.ReadRequest(bufio.NewReader(envd))
		if err != nil {
			return
		}
		b, _ := io.ReadAll(r.Body)
		status, body := serve(r, string(b))
		_, _ = envd.Write([]byte("HTTP/1.1 " + strconv.Itoa(status) + " " + http.StatusText(status) + "\r\nContent-Length: " + strconv.Itoa(len(body)) + "\r\n\r\n" + body))
	}()
	return conn
}

// runningEnvd answers a process start with its start event and holds the stream open, as envd does while the process runs.
type runningEnvd struct {
	scale.SandboxStore
}

func (runningEnvd) DialGuestPort(context.Context, string, string, uint16) (net.Conn, error) {
	conn, envd := net.Pipe()
	go func() {
		r, err := http.ReadRequest(bufio.NewReader(envd))
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		start := connectFrame(0, []byte(`{"event":{"start":{"pid":7}}}`))
		_, _ = fmt.Fprintf(envd, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n%x\r\n%s\r\n", len(start), start)
		_, _ = io.Copy(io.Discard, envd)
	}()
	return conn, nil
}
