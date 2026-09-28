package e2bcompat

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/projecteru2/core/log"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/cocoonstack/sandbox-operator/pkg/e2bbuild"
	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
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

	envdProcessStart = "/process.Process/Start"
	connectEndStream = 0x02
	connectFrameMax  = 1 << 20
	logLineMax       = 64 << 10
)

// relayEnvs point a guest process at the node's egress proxy, as silkd's unit points its own.
var relayEnvs = map[string]string{
	"http_proxy":  "http://127.0.0.1:3128",
	"https_proxy": "http://127.0.0.1:3128",
	"no_proxy":    "localhost,127.0.0.1,::1,169.254.169.254",
}

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
	AccessToken    string            `json:"accessToken,omitempty"`
	EnvVars        map[string]string `json:"envVars,omitempty"`
	DefaultUser    string            `json:"defaultUser,omitempty"`
	DefaultWorkdir string            `json:"defaultWorkdir,omitempty"`
	Timestamp      time.Time         `json:"timestamp"`
}

// envdStart is envd's process.Process/Start request.
type envdStart struct {
	Process envdProcess `json:"process"`
	Stdin   bool        `json:"stdin"`
}

type envdProcess struct {
	Cmd  string            `json:"cmd"`
	Args []string          `json:"args"`
	Envs map[string]string `json:"envs,omitempty"`
	Cwd  string            `json:"cwd,omitempty"`
}

// envdEvent is one process.Process/Start stream message, or the stream's end frame.
type envdEvent struct {
	Event struct {
		Start *struct{} `json:"start"`
		Data  *struct {
			Stdout []byte `json:"stdout"`
			Stderr []byte `json:"stderr"`
		} `json:"data"`
		End *struct {
			ExitCode int `json:"exitCode"`
		} `json:"end"`
	} `json:"event"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// envdGuest runs build commands through envd's process API inside a claimed sandbox.
type envdGuest struct {
	s *Server
}

func (g envdGuest) Run(ctx context.Context, a scale.Assignment, cmd e2bbuild.Command, stdout, stderr func(string)) (int, error) {
	outLines, errLines := &lineSplitter{out: stdout}, &lineSplitter{out: stderr}
	code, _, err := g.s.envdProcess(ctx, a, cmd, func(o, e []byte) {
		outLines.write(o)
		errLines.write(e)
	})
	outLines.flush()
	errLines.flush()
	return code, err
}

// Start returns once envd reports cmd running; envd keeps a process after its stream closes.
func (g envdGuest) Start(ctx context.Context, a scale.Assignment, cmd e2bbuild.Command) error {
	_, running, err := g.s.envdProcess(ctx, a, cmd, nil)
	if err == nil && !running {
		return errors.New("the start command exited at once")
	}
	return err
}

// Init sets the defaults with no access token, which envd takes as first-time setup; like every build call it rides the claim's own relay, which keeps the sandbox awake.
func (g envdGuest) Init(ctx context.Context, a scale.Assignment, defaults e2bbuild.Command) error {
	return g.s.initEnvd(ctx, a.Node, a.SandboxName, a.Token, envdInit{EnvVars: defaults.Envs, DefaultUser: defaults.User, DefaultWorkdir: defaults.Workdir})
}

func (g envdGuest) Write(ctx context.Context, a scale.Assignment, path string, r io.Reader) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+envdHostAlias+"/files?"+url.Values{"path": {path}, "username": {"root"}}.Encode(), r)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := g.s.envdRoundTrip(ctx, a.Node, a.SandboxName, a.Token, req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("envd file write in %s: envd answered %d", a.SandboxName, resp.StatusCode)
	}
	return nil
}

// lineSplitter hands out complete lines, and a partial one once it outgrows logLineMax.
type lineSplitter struct {
	buf []byte
	out func(string)
}

func (l *lineSplitter) write(p []byte) {
	l.buf = append(l.buf, p...)
	for {
		i := bytes.IndexByte(l.buf, '\n')
		if i < 0 {
			break
		}
		l.out(string(l.buf[:i]))
		l.buf = l.buf[i+1:]
	}
	if len(l.buf) > logLineMax {
		l.flush()
	}
}

func (l *lineSplitter) flush() {
	if len(l.buf) > 0 {
		l.out(string(l.buf))
		l.buf = nil
	}
}

// connBody is a reply body that owns its connection; Close closes the connection and never drains a stream that has not ended.
type connBody struct {
	io.Reader
	conn net.Conn
	stop func() bool
}

func (b *connBody) Close() error {
	b.stop()
	return b.conn.Close()
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
func (s *Server) initEnvd(ctx context.Context, node, id, relay string, req envdInit) error {
	ctx, cancel := context.WithTimeout(ctx, initTimeout)
	defer cancel()
	req.Timestamp = time.Now()
	payload, err := json.Marshal(req) //nolint:gosec // /init is how envd receives its access token
	if err != nil {
		return err
	}
	status, _, err := s.envdCallOver(ctx, node, id, relay, http.MethodPost, "/init", "", payload)
	if err != nil {
		return fmt.Errorf("envd init of %s: %w", id, err)
	}
	if status != http.StatusNoContent {
		return fmt.Errorf("envd init of %s: envd answered %d", id, status)
	}
	return nil
}

// templateEnvs is what a built template's create sends as envVars: nil keeps the template's, and envd replaces them with any map, so a request's own come on top of the template's.
func (s *Server) templateEnvs(ctx context.Context, a scale.Assignment, envs map[string]string) (map[string]string, error) {
	if len(envs) == 0 {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, initTimeout)
	defer cancel()
	status, body, err := s.envdCall(ctx, a.Node, a.SandboxName, http.MethodGet, "/envs", "", nil)
	if err != nil {
		return nil, fmt.Errorf("envd envs of %s: %w", a.SandboxName, err)
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("envd envs of %s: envd answered %d", a.SandboxName, status)
	}
	var out map[string]string
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode envd envs of %s: %w", a.SandboxName, err)
	}
	if out == nil {
		out = make(map[string]string, len(envs))
	}
	maps.Copy(out, envs)
	return out, nil
}

// handOver moves a forked child's envd onto token: the child starts with its parent's, which only the metadata hash overrides.
func (s *Server) handOver(ctx context.Context, child scale.Assignment, token string) error {
	sum := sha512.Sum512([]byte(token))
	doc := []byte(`{"accessTokenHash":"` + hex.EncodeToString(sum[:]) + `"}`)
	if err := s.store.SetInstanceMetadata(ctx, child.Node, child.SandboxName, doc); err != nil {
		return err
	}
	return s.initEnvd(ctx, child.Node, child.SandboxName, "", envdInit{AccessToken: token})
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

// envdProcess starts cmd through envd's process API in a's sandbox and passes its output to data until it ends with its exit code; with no data it returns running once envd starts it.
func (s *Server) envdProcess(ctx context.Context, a scale.Assignment, cmd e2bbuild.Command, data func(stdout, stderr []byte)) (int, bool, error) {
	msg, err := json.Marshal(envdStart{Process: envdProcess{Cmd: "/bin/bash", Args: []string{"-l", "-c", cmd.Line}, Envs: cmd.Envs, Cwd: cmd.Workdir}})
	if err != nil {
		return 0, false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+envdHostAlias+envdProcessStart, bytes.NewReader(connectFrame(0, msg)))
	if err != nil {
		return 0, false, err
	}
	req.Header.Set("Content-Type", "application/connect+json")
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(cmd.User+":")))
	resp, err := s.envdRoundTrip(ctx, a.Node, a.SandboxName, a.Token, req)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 0, false, fmt.Errorf("envd process start in %s: envd answered %d", a.SandboxName, resp.StatusCode)
	}
	for {
		var head [5]byte
		if _, err := io.ReadFull(resp.Body, head[:]); err != nil {
			return 0, false, fmt.Errorf("envd process stream in %s: %w", a.SandboxName, err)
		}
		n := binary.BigEndian.Uint32(head[1:])
		if n > connectFrameMax {
			return 0, false, fmt.Errorf("envd process stream in %s: a %d-byte frame", a.SandboxName, n)
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(resp.Body, payload); err != nil {
			return 0, false, fmt.Errorf("envd process stream in %s: %w", a.SandboxName, err)
		}
		var ev envdEvent
		if err := json.Unmarshal(payload, &ev); err != nil {
			return 0, false, fmt.Errorf("decode envd process stream in %s: %w", a.SandboxName, err)
		}
		if head[0]&connectEndStream != 0 {
			if ev.Error != nil {
				return 0, false, fmt.Errorf("envd process in %s: %s", a.SandboxName, ev.Error.Message)
			}
			return 0, false, fmt.Errorf("envd process stream in %s ended before the process", a.SandboxName)
		}
		switch e := ev.Event; {
		case e.End != nil:
			return e.End.ExitCode, false, nil
		case e.Start != nil && data == nil:
			return 0, true, nil
		case e.Data != nil && data != nil:
			data(e.Data.Stdout, e.Data.Stderr)
		}
	}
}

// envdCall sends one request to envd inside sandbox id over its node's passive relay and reads the bounded reply; token authenticates it to envd.
func (s *Server) envdCall(ctx context.Context, node, id, method, path, token string, body []byte) (int, []byte, error) {
	return s.envdCallOver(ctx, node, id, "", method, path, token, body)
}

func (s *Server) envdCallOver(ctx context.Context, node, id, relay, method, path, token string, body []byte) (int, []byte, error) {
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
	resp, err := s.envdRoundTrip(ctx, node, id, relay, req)
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

// envdRoundTrip writes req to envd inside sandbox id over its node's relay, passive with an empty relay token; closing the reply's body closes the connection.
func (s *Server) envdRoundTrip(ctx context.Context, node, id, relay string, req *http.Request) (*http.Response, error) {
	conn, err := s.store.DialGuestPort(ctx, node, id, relay, envdPort)
	if err != nil {
		return nil, err
	}
	body := &connBody{conn: conn, stop: context.AfterFunc(ctx, func() { _ = conn.Close() })}
	if err = req.Write(conn); err != nil {
		_ = body.Close()
		return nil, err
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		_ = body.Close()
		return nil, err
	}
	body.Reader = resp.Body
	resp.Body = body
	return resp, nil
}

func connectFrame(flags byte, msg []byte) []byte {
	return append(binary.BigEndian.AppendUint32([]byte{flags}, uint32(len(msg))), msg...)
}

// withRelay sets the proxy variables under envs when the claim relays, so a request's own value wins.
func withRelay(route string, envs map[string]string) map[string]string {
	if route != sandboxd.NetRouteRelay {
		return envs
	}
	out := maps.Clone(relayEnvs)
	maps.Copy(out, envs)
	return out
}
