package envdproxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"golang.org/x/net/http2"
)

const testDomain = "sandbox.example.com"

func TestProxyReachesTheGuestPort(t *testing.T) {
	node := newFakeNode(t, guestEcho)
	h := newTestProxy(t, node.resolver())

	resp := request(t, h, "49983-sb-abc."+testDomain, "/files?path=/etc/hosts", "tok")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if got := string(body); !strings.Contains(got, "GET /files?path=/etc/hosts") {
		t.Errorf("guest saw %q, want the untouched request line", got)
	}
	if node.lastPath != "/v1/sandboxes/sb_abc/ports/49983" {
		t.Errorf("node path = %q, want the raw claim id and the requested port", node.lastPath)
	}
	if node.lastAuth != "Bearer tok" {
		t.Errorf("node auth = %q, want the presented sandbox token", node.lastAuth)
	}
}

func TestProxyRoutesByHeadersOnASharedHost(t *testing.T) {
	node := newFakeNode(t, guestEcho)
	h := newTestProxy(t, node.resolver())

	r := httptest.NewRequest(http.MethodGet, "/files", nil)
	r.Host = "sandbox." + testDomain
	r.Header.Set(sandboxIDHeader, "sb-abc")
	r.Header.Set(sandboxPortHeader, "8080")
	r.Header.Set(accessTokenHeader, "tok")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if node.lastPath != "/v1/sandboxes/sb_abc/ports/8080" {
		t.Errorf("node path = %q, want the header-named port", node.lastPath)
	}
}

func TestProxyStripsHostCredentialsFromTheGuest(t *testing.T) {
	node := newFakeNode(t, guestEcho)
	h := newTestProxy(t, node.resolver())

	r := httptest.NewRequest(http.MethodGet, "/files", nil)
	r.Host = "49983-sb-abc." + testDomain
	r.Header.Set(accessTokenHeader, "tok")
	r.Header.Set(apiKeyHeader, "e2b_key")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	seen := w.Body.String()
	for _, header := range []string{accessTokenHeader, apiKeyHeader, "tok", "e2b_key"} {
		if strings.Contains(seen, header) {
			t.Errorf("the guest saw %q; a host-side credential must not cross into the sandbox", header)
		}
	}
}

func TestProxyRequiresTheAccessToken(t *testing.T) {
	node := newFakeNode(t, guestEcho)
	h := newTestProxy(t, node.resolver())

	resp := request(t, h, "49983-sb-abc."+testDomain, "/files", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
	if node.lastPath != "" {
		t.Error("an unauthenticated request reached the node")
	}
}

func TestProxyRefusesEnvdInternalPaths(t *testing.T) {
	node := newFakeNode(t, guestEcho)
	h := newTestProxy(t, node.resolver())

	for _, path := range []string{"/init", "/freeze", "/unfreeze", "/fsfreeze", "/collapse", "/init/", "/INIT"} {
		t.Run(path, func(t *testing.T) {
			resp := request(t, h, "49983-sb-abc."+testDomain, path, "tok")
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("status = %d, want 404", resp.StatusCode)
			}
			if node.lastPath != "" {
				t.Error("an internal path reached the guest")
			}
		})
	}
}

func TestProxyRejectsAnUnroutableHost(t *testing.T) {
	node := newFakeNode(t, guestEcho)
	h := newTestProxy(t, node.resolver())

	for _, host := range []string{"example.com", "sb-abc." + testDomain, "-sb-abc." + testDomain, "0-sb-abc." + testDomain, "x-sb-abc." + testDomain, "a.b-sb." + testDomain} {
		t.Run(host, func(t *testing.T) {
			resp := request(t, h, host, "/files", "tok")
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
		})
	}
}

func TestProxyHidesAnUnknownSandbox(t *testing.T) {
	h := newTestProxy(t, resolverFunc(func(context.Context, string) (Owner, error) {
		return Owner{}, ErrSandboxNotFound
	}))

	resp := request(t, h, "49983-sb-gone."+testDomain, "/files", "tok")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), "sb-gone") {
		t.Errorf("the reply echoed the requested id: %s", body)
	}
}

func TestProxyMapsNodeRefusals(t *testing.T) {
	tests := []struct {
		name       string
		nodeStatus int
		want       int
	}{
		{"wrong token", http.StatusNotFound, http.StatusUnauthorized},
		{"bad port", http.StatusBadRequest, http.StatusBadRequest},
		{"no guest listener", http.StatusBadGateway, http.StatusBadGateway},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := newFakeNode(t, nil)
			node.refuse = tt.nodeStatus
			h := newTestProxy(t, node.resolver())

			resp := request(t, h, "49983-sb-abc."+testDomain, "/files", "tok")
			defer resp.Body.Close()
			if resp.StatusCode != tt.want {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tt.want)
			}
			body, _ := io.ReadAll(resp.Body)
			if strings.Contains(string(body), node.addr) {
				t.Errorf("the reply leaked the node address: %s", body)
			}
		})
	}
}

func TestProxyReportsAnUnreachableNode(t *testing.T) {
	h := newTestProxy(t, resolverFunc(func(context.Context, string) (Owner, error) {
		return Owner{ClaimID: "sb_abc", Address: "127.0.0.1:1"}, nil
	}))

	resp := request(t, h, "49983-sb-abc."+testDomain, "/files", "tok")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
}

func TestProxyCarriesHTTP2ToTheGuest(t *testing.T) {
	node := newFakeNode(t, func(c net.Conn) {
		srv := &http2.Server{}
		srv.ServeConn(c, &http2.ServeConnOpts{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "proto=%d authority=%s", r.ProtoMajor, r.Host)
		})})
	})
	h := newTestProxy(t, node.resolver())

	ts := httptest.NewUnstartedServer(h)
	ts.EnableHTTP2 = true
	ts.StartTLS()
	t.Cleanup(ts.Close)

	client := ts.Client()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/filesystem.Filesystem/Stat", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Host = "49983-sb-abc." + testDomain
	req.Header.Set(accessTokenHeader, "tok")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "proto=2") {
		t.Errorf("guest saw %q, want an HTTP/2 request", body)
	}
	if !strings.Contains(string(body), "authority=49983-sb-abc."+testDomain) {
		t.Errorf("guest saw %q, want the per-sandbox authority", body)
	}
}

func TestProxyServesCleartextHTTP2(t *testing.T) {
	node := newFakeNode(t, func(c net.Conn) {
		srv := &http.Server{Protocols: Protocols(), Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "proto=%d", r.ProtoMajor)
		})}
		_ = srv.Serve(&oneConnListener{c: c})
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	edge := &http.Server{Protocols: Protocols(), Handler: newTestProxy(t, node.resolver())}
	go func() { _ = edge.Serve(ln) }()
	t.Cleanup(func() { _ = edge.Close() })

	var h2 http.Protocols
	h2.SetUnencryptedHTTP2(true)
	client := &http.Client{Transport: &http.Transport{Protocols: &h2}}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://"+ln.Addr().String()+"/process.Process/Start", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Host = "49983-sb-abc." + testDomain
	req.Header.Set(accessTokenHeader, "tok")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 2 {
		t.Errorf("edge answered over HTTP/%d, want cleartext HTTP/2", resp.ProtoMajor)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "proto=2") {
		t.Errorf("guest saw %q, want HTTP/2 carried through", body)
	}
}

func TestNewServerRequiresADomainAndAResolver(t *testing.T) {
	if _, err := NewServer(nil, Options{Domain: testDomain}); err == nil {
		t.Error("NewServer accepted a nil resolver")
	}
	if _, err := NewServer(resolverFunc(nil), Options{}); err == nil {
		t.Error("NewServer accepted an empty domain; every sandbox host is derived from it")
	}
}

func newTestProxy(t *testing.T, r Resolver) http.Handler {
	t.Helper()
	s, err := NewServer(r, Options{Domain: testDomain, Log: logr.Discard()})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return s.Handler()
}

func request(t *testing.T, h http.Handler, host, path, token string) *http.Response {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, path, nil)
	r.Host = host
	if token != "" {
		r.Header.Set(accessTokenHeader, token)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w.Result()
}

// guestEcho answers one request with the bytes it received, so a test can
// assert on exactly what crossed into the sandbox.
func guestEcho(c net.Conn) {
	defer c.Close()
	br := bufio.NewReader(c)
	var seen strings.Builder
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return
		}
		seen.WriteString(line)
		if line == "\r\n" {
			break
		}
	}
	body := seen.String()
	fmt.Fprintf(c, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", len(body), body)
}

// oneConnListener serves a single already-accepted connection, so a guest can
// be an http.Server on the far side of the relay.
type oneConnListener struct {
	c    net.Conn
	done bool
}

func (l oneConnListener) Addr() net.Addr { return l.c.LocalAddr() }

func (l oneConnListener) Close() error { return nil }

func (l *oneConnListener) Accept() (net.Conn, error) {
	if l.done {
		return nil, io.EOF
	}
	l.done = true
	return l.c, nil
}

// fakeNode is a sandboxd stand-in: it answers the guest-port upgrade and then
// hands the connection to a guest handler.
type fakeNode struct {
	addr     string
	refuse   int
	lastPath string
	lastAuth string
}

func newFakeNode(t *testing.T, guest func(net.Conn)) *fakeNode {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	n := &fakeNode{addr: ln.Addr().String()}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go n.handle(conn, guest)
		}
	}()
	return n
}

func (n *fakeNode) handle(conn net.Conn, guest func(net.Conn)) {
	br := bufio.NewReader(conn)
	req, err := http.ReadRequest(br)
	if err != nil {
		_ = conn.Close()
		return
	}
	n.lastPath, n.lastAuth = req.URL.Path, req.Header.Get("Authorization")
	if n.refuse != 0 {
		fmt.Fprintf(conn, "HTTP/1.1 %d %s\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", n.refuse, http.StatusText(n.refuse))
		_ = conn.Close()
		return
	}
	if _, err := io.WriteString(conn, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: tcp\r\nConnection: Upgrade\r\n\r\n"); err != nil {
		_ = conn.Close()
		return
	}
	guest(conn)
}

func (n *fakeNode) resolver() Resolver {
	return resolverFunc(func(_ context.Context, id string) (Owner, error) {
		if id != "sb-abc" {
			return Owner{}, ErrSandboxNotFound
		}
		return Owner{ClaimID: "sb_abc", Address: n.addr}, nil
	})
}

// resolverFunc adapts a func to Resolver.
type resolverFunc func(ctx context.Context, sandboxID string) (Owner, error)

func (f resolverFunc) Owner(ctx context.Context, sandboxID string) (Owner, error) {
	if f == nil {
		return Owner{}, errors.New("no resolver")
	}
	return f(ctx, sandboxID)
}
