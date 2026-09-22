//go:build envdproxysmoke

// envdproxysmoke drives the real envd-proxy against a real sandbox: it serves
// the proxy in-process and reaches an HTTP listener inside a live microVM
// through the owning node's guest-port relay. It is the hardware half of
// pkg/envdproxy's tests, which stop at a fake node.
//
// The sandbox is set up by the sandbox repo's portsmoke -hold, which claims it,
// uploads guestserver and prints the claim:
//
//	go run -tags envdproxysmoke ./test/envdproxysmoke \
//	  -node 127.0.0.1:7990 -sandbox sb_abc -token <token> -port 49983
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-logr/logr"

	"github.com/cocoonstack/sandbox-operator/pkg/e2bcompat"
	"github.com/cocoonstack/sandbox-operator/pkg/envdproxy"
)

const domain = "sandbox.smoke.invalid"

func main() {
	node := flag.String("node", "", "owning node's sandboxd address")
	sandboxID := flag.String("sandbox", "", "node-local claim id")
	token := flag.String("token", "", "per-sandbox access token")
	port := flag.Uint("port", 49983, "guest port the listener is on")
	guestHTTP2 := flag.Bool("guest-http2", false, "forward to the guest over cleartext HTTP/2")
	mode := flag.String("guest", "echo", "what listens in the guest: echo (guestserver) or envd")
	flag.Parse()

	if *node == "" || *sandboxID == "" || *token == "" {
		fmt.Fprintln(os.Stderr, "envdproxysmoke: -node, -sandbox and -token are required")
		os.Exit(1)
	}
	if err := run(*node, *sandboxID, *token, uint16(*port), *guestHTTP2, *mode); err != nil {
		fmt.Fprintln(os.Stderr, "envdproxysmoke:", err)
		os.Exit(1)
	}
	fmt.Println("ENVDPROXYSMOKE PASS")
}

func run(node, sandboxID, token string, port uint16, guestHTTP2 bool, mode string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// The published id is the DNS-safe rendering, exactly as the compat API
	// hands it to the SDK; the proxy must resolve it back to the claim id.
	publicID := e2bcompat.PublicID(sandboxID)
	srv, err := envdproxy.NewServer(staticResolver{claimID: sandboxID, address: node, publicID: publicID},
		envdproxy.Options{Domain: domain, GuestHTTP2: guestHTTP2, Log: logr.Discard()})
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	edge := &http.Server{Handler: srv.Handler(), Protocols: envdproxy.Protocols(), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = edge.Serve(ln) }()
	defer func() { _ = edge.Close() }()

	c := &client{base: "http://" + ln.Addr().String(), host: fmt.Sprintf("%d-%s.%s", port, publicID, domain), token: token}
	fmt.Printf("proxying %s -> %s/%s:%d\n", c.host, node, sandboxID, port)

	steps := []struct {
		name string
		run  func(context.Context, *client) error
	}{
		{"missing token is 401", stepNoToken},
		{"wrong token is 401", stepWrongToken},
		{"envd internal path is 404", stepInternalPath},
		{"unknown sandbox is 502", stepUnknownSandbox},
	}
	switch mode {
	case "envd":
		steps = append([]struct {
			name string
			run  func(context.Context, *client) error
		}{
			{"http/1.1 reaches envd", stepEnvdHealth},
			{"http/2 client reaches envd", stepEnvdHealthH2},
			{"connect unary through the proxy", stepEnvdConnect},
			{"header routing", stepEnvdHeaderRouting},
		}, steps...)
	case "echo":
		steps = append([]struct {
			name string
			run  func(context.Context, *client) error
		}{
			{"http/1.1 reaches the guest", stepHTTP1},
			{"http/2 client reaches the guest", stepH2Client},
			{"host credentials are stripped", stepStripped},
			{"header routing", stepHeaderRouting},
		}, steps...)
	default:
		return fmt.Errorf("unknown -guest %q", mode)
	}
	for _, step := range steps {
		t0 := time.Now()
		if err := step.run(ctx, c); err != nil {
			return fmt.Errorf("%s: %w", step.name, err)
		}
		fmt.Printf("  ok  %-32s %5.1fms\n", step.name, float64(time.Since(t0).Microseconds())/1000)
	}
	return nil
}

func stepHTTP1(ctx context.Context, c *client) error {
	body, err := c.get(ctx, false, "/echo?probe=1", c.host, c.token, nil)
	if err != nil {
		return err
	}
	return wantReport(body, "proto=1", "path=/echo?probe=1")
}

// stepH2Client proves an HTTP/2 client is served end to end: ConnectRPC uses
// it, and the guest leg's own protocol must not leak back into that.
func stepH2Client(ctx context.Context, c *client) error {
	_, err := c.get(ctx, true, "/process.Process/Start", c.host, c.token, nil)
	return err
}

// stepStripped proves a host-side credential never crosses into the guest: a
// sandbox that learned its own token could drive its own control plane.
func stepStripped(ctx context.Context, c *client) error {
	extra := http.Header{"X-Api-Key": []string{"e2b_secret"}, "X-Probe": []string{"kept"}}
	body, err := c.get(ctx, false, "/echo", c.host, c.token, extra)
	if err != nil {
		return err
	}
	if err := wantReport(body, "header[X-Access-Token]=\n", "header[X-API-KEY]=\n"); err != nil {
		return err
	}
	if strings.Contains(body, c.token) || strings.Contains(body, "e2b_secret") {
		return fmt.Errorf("guest saw a host credential:\n%s", body)
	}
	return wantReport(body, "header[X-Probe]=kept")
}

// stepHeaderRouting proves the shared-host form works: one name for the whole
// fleet, with the sandbox named by headers.
func stepHeaderRouting(ctx context.Context, c *client) error {
	label, _, _ := strings.Cut(c.host, ".")
	port, sandboxID, _ := strings.Cut(label, "-")
	extra := http.Header{
		"E2b-Sandbox-Id":   []string{sandboxID},
		"E2b-Sandbox-Port": []string{port},
	}
	body, err := c.get(ctx, false, "/echo", "sandbox."+domain, c.token, extra)
	if err != nil {
		return err
	}
	return wantReport(body, "proto=1", "path=/echo")
}

// stepEnvdHealth is the first thing an e2b SDK does after create.
func stepEnvdHealth(ctx context.Context, c *client) error {
	return c.wantStatus(ctx, "/health", c.host, c.token, http.StatusNoContent)
}

func stepEnvdHealthH2(ctx context.Context, c *client) error {
	resp, err := c.send(ctx, true, http.MethodGet, "/health", c.host, c.token, nil, "")
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.ProtoMajor != 2 {
		return fmt.Errorf("edge answered over HTTP/%d, want HTTP/2 to the client", resp.ProtoMajor)
	}
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("status %s, want 204", resp.Status)
	}
	return nil
}

// stepEnvdConnect drives the RPC surface every SDK file and process call rides.
func stepEnvdConnect(ctx context.Context, c *client) error {
	extra := http.Header{
		"Content-Type":             []string{"application/json"},
		"Connect-Protocol-Version": []string{"1"},
		"X-User":                   []string{"root"},
	}
	body, err := c.post(ctx, "/filesystem.Filesystem/Stat", c.host, c.token, extra, `{"path":"/etc/envd-version"}`)
	if err != nil {
		return err
	}
	return wantReport(body, "entry")
}

func stepEnvdHeaderRouting(ctx context.Context, c *client) error {
	label, _, _ := strings.Cut(c.host, ".")
	port, sandboxID, _ := strings.Cut(label, "-")
	extra := http.Header{
		"E2b-Sandbox-Id":   []string{sandboxID},
		"E2b-Sandbox-Port": []string{port},
	}
	resp, err := c.send(ctx, false, http.MethodGet, "/health", "sandbox."+domain, c.token, extra, "")
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("status %s, want 204", resp.Status)
	}
	return nil
}

func stepNoToken(ctx context.Context, c *client) error {
	return c.wantStatus(ctx, "/echo", c.host, "", http.StatusUnauthorized)
}

func stepWrongToken(ctx context.Context, c *client) error {
	return c.wantStatus(ctx, "/echo", c.host, "not-the-token", http.StatusUnauthorized)
}

func stepInternalPath(ctx context.Context, c *client) error {
	for _, path := range []string{"/init", "/freeze", "/fsfreeze"} {
		if err := c.wantStatus(ctx, path, c.host, c.token, http.StatusNotFound); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
	}
	return nil
}

func stepUnknownSandbox(ctx context.Context, c *client) error {
	return c.wantStatus(ctx, "/echo", "49983-sb-nosuchsandbox."+domain, c.token, http.StatusBadGateway)
}

// wantReport asserts guestserver reported each expected line.
func wantReport(body string, want ...string) error {
	for _, line := range want {
		if !strings.Contains(body, line) {
			return fmt.Errorf("guest report missing %q:\n%s", line, body)
		}
	}
	return nil
}

// client drives the proxy the way an unmodified e2b SDK would.
type client struct {
	base  string
	host  string
	token string
}

func (c *client) get(ctx context.Context, h2 bool, path, host, token string, extra http.Header) (string, error) {
	resp, err := c.send(ctx, h2, http.MethodPost, path, host, token, extra, "{}")
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %s: %s", resp.Status, body)
	}
	return string(body), nil
}

func (c *client) post(ctx context.Context, path, host, token string, extra http.Header, body string) (string, error) {
	resp, err := c.send(ctx, false, http.MethodPost, path, host, token, extra, body)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %s: %s", resp.Status, out)
	}
	return string(out), nil
}

func (c *client) wantStatus(ctx context.Context, path, host, token string, want int) error {
	resp, err := c.send(ctx, false, http.MethodGet, path, host, token, nil, "")
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != want {
		return fmt.Errorf("status %d, want %d", resp.StatusCode, want)
	}
	return nil
}

func (c *client) send(ctx context.Context, h2 bool, method, path, host, token string, extra http.Header, body string) (*http.Response, error) {
	tr := &http.Transport{}
	if h2 {
		var protocols http.Protocols
		protocols.SetUnencryptedHTTP2(true)
		tr.Protocols = &protocols
	}
	var payload io.Reader
	if body != "" {
		payload = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, payload)
	if err != nil {
		return nil, err
	}
	req.Host = host
	if token != "" {
		req.Header.Set("X-Access-Token", token)
	}
	for k, v := range extra {
		req.Header[k] = v
	}
	return (&http.Client{Transport: tr}).Do(req)
}

// staticResolver stands in for the inventory lookup: the hardware harness knows
// the owning node already, and the k8s read path has its own tests.
type staticResolver struct {
	claimID  string
	address  string
	publicID string
}

func (r staticResolver) Owner(_ context.Context, sandboxID string) (envdproxy.Owner, error) {
	if sandboxID != r.publicID && sandboxID != r.claimID {
		return envdproxy.Owner{}, envdproxy.ErrSandboxNotFound
	}
	return envdproxy.Owner{ClaimID: r.claimID, Address: r.address}, nil
}
