//go:build envdsmoke

// envdsmoke proves the e2b flavor image on real hardware from the operator's side: it claims a
// sandbox from an envd-carrying pool with pkg/sandboxd and drives the real envd through the
// node's guest-port relay, the path the e2b surface and the envd proxy take. It asserts what the
// e2b SDK depends on (the daemon is up, its version is the one the compat API reports, files and
// the ConnectRPC surface answer, the default user has sudo) and that envd serves HTTP/1.1 only.
package main

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
)

const (
	envdPort        = 49983
	interpreterPort = 49999
	readyWait       = 60 * time.Second
	claimTTL        = 30 * time.Minute
	runTimeout      = 300 * time.Second

	uploadPath   = "/tmp/envdsmoke-probe.txt"
	uploadProbe  = "envd-wrote-this"
	processProbe = "envd-process-ok"
	contentType  = "Content-Type"
)

type relay struct {
	client *sandboxd.Client
	id     string
	token  string
}

func (r relay) Dial(ctx context.Context, port uint16) (net.Conn, error) {
	return r.client.DialPort(ctx, r.id, r.token, port)
}

func (r relay) Do(ctx context.Context, port uint16, method, path string, headers http.Header, body io.Reader) (*http.Response, error) {
	client := &http.Client{Transport: &http.Transport{
		DisableKeepAlives: true,
		DialContext:       func(ctx context.Context, _, _ string) (net.Conn, error) { return r.Dial(ctx, port) },
	}}
	req, err := http.NewRequestWithContext(ctx, method, "http://guest"+path, body)
	if err != nil {
		return nil, err
	}
	maps.Copy(req.Header, headers)
	return client.Do(req)
}

type smokeStep struct {
	name string
	run  func(context.Context, relay) error
}

func main() {
	addr := flag.String("addr", "127.0.0.1:7777", "sandboxd address, a host:port or an http(s) origin")
	token := flag.String("token", "", "node api token")
	template := flag.String("template", "", "e2b flavor template ref")
	wantVersion := flag.String("envd-version", "", "version envd must report; empty only prints it")
	hold := flag.Duration("hold", 0, "after the steps pass, print the claim and keep it alive for this long")
	interpreter := flag.Bool("code-interpreter", false, "also drive the code-interpreter API on 49999 (the e2b-ci flavor)")
	size := flag.String("size", "small", "size of the pool to claim from")
	flag.Parse()

	if err := run(*addr, *token, *template, *wantVersion, *size, *hold, *interpreter); err != nil {
		fmt.Fprintln(os.Stderr, "envdsmoke:", err)
		os.Exit(1)
	}
	fmt.Println("ENVDSMOKE PASS")
}

func run(addr, token, template, wantVersion, size string, hold time.Duration, interpreter bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), runTimeout+hold)
	defer cancel()

	base := addr
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	client := sandboxd.New(base, token)
	res, err := client.Claim(ctx, sandboxd.ClaimSpec{Template: template, Net: "none", Size: size, TTLSeconds: int(claimTTL / time.Second)})
	if err != nil {
		return fmt.Errorf("claim: %w", err)
	}
	defer func() { _ = client.Release(context.WithoutCancel(ctx), res.ID, res.Token) }()
	fmt.Printf("claimed %s on %s\n", res.ID, res.OwnerAddr)
	rt := relay{client: client, id: res.ID, token: res.Token}

	if err = waitReady(ctx, rt); err != nil {
		return err
	}
	version, err := envdVersion(ctx, rt)
	if err != nil {
		return err
	}
	fmt.Printf("  envd %s answering through the relay\n", version)
	if wantVersion != "" && version != wantVersion {
		return fmt.Errorf("envd reports %q, want %q: the compat API's envdVersion would be a lie", version, wantVersion)
	}

	steps := []smokeStep{
		{"GET /health", stepHealth},
		{"POST /files then GET /files", stepUploadDownload},
		{"connect unary over http/1.1", stepConnectH1},
		{"process start under -no-cgroups", stepProcessStart},
		{"envd serves http/1.1 only", stepProtocol},
		{"user account with sudo", stepUserAccount},
	}
	if interpreter {
		steps = append(steps, smokeStep{"interpreter runs 1+1", stepRunCode})
	}
	for _, step := range steps {
		t0 := time.Now()
		if err := step.run(ctx, rt); err != nil {
			return fmt.Errorf("%s: %w", step.name, err)
		}
		fmt.Printf("  ok  %-30s %5.1fms\n", step.name, float64(time.Since(t0).Microseconds())/1000)
	}
	if hold > 0 {
		fmt.Printf("SANDBOX %s %s %s %d\n", res.ID, res.Token, res.OwnerAddr, envdPort)
		select {
		case <-time.After(hold):
		case <-ctx.Done():
		}
	}
	return nil
}

func waitReady(ctx context.Context, rt relay) error {
	deadline := time.Now().Add(readyWait)
	var last error
	for time.Now().Before(deadline) {
		resp, err := rt.Do(ctx, envdPort, http.MethodGet, "/health", nil, nil)
		if err == nil {
			_ = resp.Body.Close()
			return nil
		}
		last = err
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("envd never answered /health: %w", last)
}

func envdVersion(ctx context.Context, rt relay) (string, error) {
	body, err := download(ctx, rt, "/etc/envd-version")
	if err != nil {
		return "", fmt.Errorf("GET /files /etc/envd-version: %w", err)
	}
	version := strings.TrimSpace(body)
	if version == "" {
		return "", fmt.Errorf("empty /etc/envd-version")
	}
	return version, nil
}

func download(ctx context.Context, rt relay, path string) (string, error) {
	resp, err := rt.Do(ctx, envdPort, http.MethodGet, "/files?path="+path+"&username=root", nil, nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %s: %s", resp.Status, body)
	}
	return string(body), nil
}

func stepHealth(ctx context.Context, rt relay) error {
	resp, err := rt.Do(ctx, envdPort, http.MethodGet, "/health", nil, nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("status %s, want 204", resp.Status)
	}
	return nil
}

func stepUploadDownload(ctx context.Context, rt relay) error {
	const boundary = "envdsmoke"
	body := fmt.Sprintf("--%s\r\nContent-Disposition: form-data; name=\"file\"; filename=\"probe.txt\"\r\n"+
		"Content-Type: text/plain\r\n\r\n%s\r\n--%s--\r\n", boundary, uploadProbe, boundary)
	headers := http.Header{contentType: []string{"multipart/form-data; boundary=" + boundary}}
	resp, err := rt.Do(ctx, envdPort, http.MethodPost, "/files?path="+uploadPath+"&username=root", headers, strings.NewReader(body))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("status %s: %s", resp.Status, out)
	}
	seen, err := download(ctx, rt, uploadPath)
	if err != nil {
		return fmt.Errorf("read back: %w", err)
	}
	if strings.TrimSpace(seen) != uploadProbe {
		return fmt.Errorf("envd reads %q at %s, wrote %q", seen, uploadPath, uploadProbe)
	}
	return nil
}

func stepConnectH1(ctx context.Context, rt relay) error {
	headers := http.Header{
		contentType:                []string{"application/json"},
		"Connect-Protocol-Version": []string{"1"},
		"X-User":                   []string{"root"},
	}
	resp, err := rt.Do(ctx, envdPort, http.MethodPost, "/filesystem.Filesystem/Stat", headers, strings.NewReader(`{"path":"/etc/envd-version"}`))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %s: %s", resp.Status, body)
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		return fmt.Errorf("decode %q: %w", body, err)
	}
	if _, ok := out["entry"]; !ok {
		return fmt.Errorf("stat reply carries no entry: %s", body)
	}
	return nil
}

func stepProcessStart(ctx context.Context, rt relay) error {
	events, err := startProcess(ctx, rt, "root", "/bin/echo", processProbe)
	if err != nil {
		return err
	}
	joined := strings.Join(events, "\n")
	if !strings.Contains(joined, `"start"`) {
		return fmt.Errorf("no start event in the stream:\n%s", joined)
	}
	if !strings.Contains(joined, base64.StdEncoding.EncodeToString([]byte(processProbe+"\n"))) {
		return fmt.Errorf("the command's stdout never arrived:\n%s", joined)
	}
	return nil
}

func stepProtocol(ctx context.Context, rt relay) error {
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	client := &http.Client{Transport: &http.Transport{
		Protocols:   &protocols,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) { return rt.Dial(ctx, envdPort) },
	}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://guest/health", nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err == nil {
		defer func() { _ = resp.Body.Close() }()
		return fmt.Errorf("envd answered h2c with %s", resp.Status)
	}
	return stepHealth(ctx, rt)
}

func stepUserAccount(ctx context.Context, rt relay) error {
	events, err := startProcess(ctx, rt, "user", "/bin/sh", "-c", `id -un; echo "$HOME"; sudo -n true && echo sudo`)
	if err != nil {
		return err
	}
	out := stdoutOf(events)
	if got := strings.Fields(out); !slices.Equal(got, []string{"user", "/home/user", "sudo"}) {
		return fmt.Errorf("the user account reports %q, want user, /home/user and passwordless sudo", out)
	}
	return nil
}

func stepRunCode(ctx context.Context, rt relay) error {
	headers := http.Header{contentType: []string{"application/json"}}
	resp, err := rt.Do(ctx, interpreterPort, http.MethodPost, "/execute", headers, strings.NewReader(`{"code":"1+1"}`))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %s: %s", resp.Status, body)
	}
	for line := range strings.Lines(string(body)) {
		var out struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if json.Unmarshal([]byte(line), &out) == nil && out.Type == "result" && out.Text == "2" {
			return nil
		}
	}
	return fmt.Errorf("no result line with text 2 in:\n%s", body)
}

func startProcess(ctx context.Context, rt relay, user, cmd string, args ...string) ([]string, error) {
	headers := http.Header{
		contentType:                []string{"application/connect+json"},
		"Connect-Protocol-Version": []string{"1"},
		"X-User":                   []string{user},
	}
	req, _ := json.Marshal(map[string]any{"process": map[string]any{"cmd": cmd, "args": args}})
	resp, err := rt.Do(ctx, envdPort, http.MethodPost, "/process.Process/Start", headers, strings.NewReader(envelope(string(req))))
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status %s: %s", resp.Status, body)
	}
	return readEnvelopes(resp.Body)
}

func stdoutOf(events []string) string {
	var out strings.Builder
	for _, e := range events {
		var ev struct {
			Event struct {
				Data struct {
					Stdout string `json:"stdout"`
				} `json:"data"`
			} `json:"event"`
		}
		if json.Unmarshal([]byte(e), &ev) != nil || ev.Event.Data.Stdout == "" {
			continue
		}
		if b, err := base64.StdEncoding.DecodeString(ev.Event.Data.Stdout); err == nil {
			out.Write(b)
		}
	}
	return out.String()
}

func envelope(payload string) string {
	head := make([]byte, 5)
	binary.BigEndian.PutUint32(head[1:], uint32(len(payload)))
	return string(head) + payload
}

func readEnvelopes(r io.Reader) ([]string, error) {
	var out []string
	head := make([]byte, 5)
	for {
		if _, err := io.ReadFull(r, head); err != nil {
			return out, fmt.Errorf("stream ended before its end-of-stream envelope: %w", err)
		}
		body := make([]byte, binary.BigEndian.Uint32(head[1:]))
		if _, err := io.ReadFull(r, body); err != nil {
			return out, err
		}
		if head[0]&0x02 == 0 {
			out = append(out, string(body))
			continue
		}
		var end struct {
			Error json.RawMessage `json:"error"`
		}
		if err := json.Unmarshal(body, &end); err != nil {
			return out, fmt.Errorf("parse end-of-stream envelope %q: %w", body, err)
		}
		if len(end.Error) > 0 && string(end.Error) != "null" {
			return out, fmt.Errorf("rpc failed: %s", end.Error)
		}
		return out, nil
	}
}
