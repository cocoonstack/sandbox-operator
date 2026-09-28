// Package sandboxd is a small HTTP client for the node-local sandboxd warm-pool
// daemon (the sandbox repo's docs/sandboxd-api.md). It uses only the standard
// library. The aggregated store fronts one of these clients per node: Claim
// transfers ownership of an already-running microVM in sub-millisecond time, and
// Release destroys a sandbox's VM on owner-authorized teardown.
package sandboxd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const (
	// ExpireDestroy destroys the claim at lease end; it is the node default.
	ExpireDestroy ExpireAction = "destroy"
	// ExpireArchive hibernates and archives the claim at lease end, so a later wake resumes it.
	ExpireArchive ExpireAction = "archive"

	// NetRouteRelay is a claim whose guest reaches the network through the node's egress proxy on 127.0.0.1:3128.
	NetRouteRelay = "relay"

	captureTimeout = 10 * time.Minute
)

var (
	// ErrNodeAtCapacity is returned by Claim when sandboxd answers 429 (the node is
	// at max_claims, the calling tenant is at its own max_claims, or the node is
	// draining) or a 200 that delivers no sandbox. In every case this node handed
	// over no VM, so the store tries another node or reports no warm capacity.
	ErrNodeAtCapacity = errors.New("sandboxd: node at capacity or draining")

	// RelayEnv points a guest process at the node's egress proxy on a NetRouteRelay claim, as silkd's unit points its own.
	RelayEnv = map[string]string{
		"http_proxy":  "http://127.0.0.1:3128",
		"https_proxy": "http://127.0.0.1:3128",
		"no_proxy":    "localhost,127.0.0.1,::1,169.254.169.254",
	}
)

// ExpireAction is what the node does with a claim whose lease ends; empty keeps the claim's current action, destroy on a new claim.
type ExpireAction string

// HTTPError carries a non-2xx sandboxd status that is not otherwise typed (e.g.
// 400 bad body, 401 bad api token, 409 egress mismatch, 500 provisioning failed).
type HTTPError struct {
	StatusCode int
	// Message is the decoded {"error": "..."} body when present.
	Message string
}

func (e *HTTPError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("sandboxd: http %d: %s", e.StatusCode, e.Message)
	}
	return fmt.Sprintf("sandboxd: http %d", e.StatusCode)
}

// RedirectError is a 200 claim answer naming warm peers instead of a delivered sandbox; it matches ErrNodeAtCapacity.
type RedirectError struct {
	Targets []string
}

func (e *RedirectError) Error() string {
	return "sandboxd: claim redirected to " + strings.Join(e.Targets, ", ")
}

func (e *RedirectError) Is(target error) bool { return target == ErrNodeAtCapacity }

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient overrides the default *http.Client (for custom transports or
// timeouts). The default client has no timeout: sandboxd keeps read/write
// timeouts at zero because cold claims legitimately block, so callers bound the
// call with the context instead.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) { c.hc = hc }
}

// ClaimSpec is the POST /v1/claim body. Net defaults to "none" and Size to
// "small" server-side; TTLSeconds 0 means the server default.
type ClaimSpec struct {
	Template   string `json:"template"`
	Net        string `json:"net,omitempty"`
	Size       string `json:"size,omitempty"`
	TTLSeconds int    `json:"ttl_seconds,omitzero"`
	// ClaimRef is the k8s "<namespace>/<name>" of the Sandbox this claim is
	// created for. sandboxd records it on the claim and echoes it in its
	// operator index, so the aggregated read path can map a listed sandbox back
	// to the name it was claimed under. Empty for claims with no k8s identity.
	ClaimRef   string            `json:"claim_ref,omitempty"`
	NoRedirect bool              `json:"no_redirect,omitzero"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	OnExpire   ExpireAction      `json:"on_expire,omitempty"`
	// RequirePromoted asks the node to provision only from a promoted template, never a cold image boot.
	RequirePromoted bool `json:"require_promoted,omitzero"`
	// Egress false claims with no egress policy, whatever the pool's; nil keeps the policy.
	Egress *bool `json:"egress,omitzero"`
}

// ClaimResult is the POST /v1/claim success body.
type ClaimResult struct {
	ID        string    `json:"id"`
	Token     string    `json:"token"`
	Deadline  time.Time `json:"deadline"`
	OwnerAddr string    `json:"owner_addr"`
	// FromCheckpoint is the lineage edge when the claim branched from a checkpoint.
	FromCheckpoint string `json:"from_checkpoint,omitempty"`
	// NetRoute is how the guest reaches the network: relay, direct or none.
	NetRoute string `json:"net_route,omitempty"`
	// Redirect, when non-empty on a 200, names warm peers to retry at instead of a
	// delivered sandbox. Claim returns it as a *RedirectError.
	Redirect []string `json:"redirect,omitempty"`
}

// PoolSpec is one entry of the PUT /v1/pools body: the desired warm watermark
// for a single (template, net, size) pool on this node. It mirrors the claim
// key so a SandboxWarmPool's target lands on the exact pool a Create claims from.
type PoolSpec struct {
	Template string `json:"template"`
	Net      string `json:"net,omitempty"`
	Size     string `json:"size,omitempty"`
	Warm     int    `json:"warm"`
}

// NodePool is one pool's live state in a NodeInfo.
type NodePool struct {
	Key       PoolKey `json:"key"`
	Warm      int     `json:"warm"`
	Refilling int     `json:"refilling"`
	Target    int     `json:"target"`
	Golden    bool    `json:"golden"`
}

// NodeTemplate is one promoted template in a NodeInfo; an empty Tenant means the operator.
type NodeTemplate struct {
	Key           PoolKey           `json:"key"`
	ContentDigest string            `json:"content_digest"`
	Tenant        string            `json:"tenant,omitempty"`
	CreatedAt     time.Time         `json:"created_at"`
	CPUCount      int               `json:"cpu_count,omitzero"`
	MemTotalBytes int64             `json:"mem_total_bytes,omitzero"`
	Labels        map[string]string `json:"labels,omitempty"`
}

// NodeInfo is the PUT /v1/pools (and GET /v1/info) response: the node's live
// per-pool warm state, its promoted templates, and its lifecycle counters.
type NodeInfo struct {
	Pools         []NodePool     `json:"pools"`
	Templates     []NodeTemplate `json:"templates"`
	Claimed       int            `json:"claimed"`
	Hibernated    int            `json:"hibernated"`
	Archived      int            `json:"archived"`
	AdvertiseAddr string         `json:"advertise_addr,omitempty"`
	Peers         []string       `json:"peers,omitempty"`
}

// Client talks to a single sandboxd instance. It is safe for concurrent use.
type Client struct {
	baseURL string
	// token is the node api_token every verb presents, except Release, which takes one per call.
	token   string
	hc      *http.Client
	capture *http.Client
}

// New returns a Client for the sandboxd at baseURL; token is the node api_token, empty when sandboxd runs without auth.
func New(baseURL, token string, opts ...Option) *Client {
	c := &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		hc:      &http.Client{},
	}
	for _, o := range opts {
		o(c)
	}
	capture := *c.hc
	capture.Timeout = captureTimeout
	c.capture = &capture
	return c
}

// Claim performs POST /v1/claim; a 429 or an empty 200 yields ErrNodeAtCapacity and a redirect-only 200 a *RedirectError.
func (c *Client) Claim(ctx context.Context, spec ClaimSpec) (ClaimResult, error) {
	body, err := json.Marshal(spec)
	if err != nil {
		return ClaimResult{}, fmt.Errorf("sandboxd: encode claim: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/claim", bytes.NewReader(body))
	if err != nil {
		return ClaimResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	c.authenticate(req, c.token)

	resp, err := c.hc.Do(req)
	if err != nil {
		return ClaimResult{}, fmt.Errorf("sandboxd: claim: %w", err)
	}
	defer drainAndClose(resp)

	switch resp.StatusCode {
	case http.StatusOK:
		var r ClaimResult
		if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
			return ClaimResult{}, fmt.Errorf("sandboxd: decode claim response: %w", err)
		}
		if r.ID == "" && len(r.Redirect) > 0 {
			return ClaimResult{}, &RedirectError{Targets: r.Redirect}
		}
		if r.ID == "" {
			return ClaimResult{}, ErrNodeAtCapacity
		}
		return r, nil
	case http.StatusTooManyRequests:
		return ClaimResult{}, ErrNodeAtCapacity
	default:
		return ClaimResult{}, statusError(resp)
	}
}

// SetPools performs PUT /v1/pools, replacing this node's desired warm targets
// with the supplied set (an omitted pool is drained). It authenticates with the
// node api_token. The whole set is sent in one request because sandboxd replaces
// its pool config wholesale — a partial list silently drains the rest.
func (c *Client) SetPools(ctx context.Context, pools []PoolSpec) (*NodeInfo, error) {
	if pools == nil {
		pools = []PoolSpec{}
	}
	var info NodeInfo
	if err := c.sendJSON(ctx, http.MethodPut, "/v1/pools", struct {
		Pools []PoolSpec `json:"pools"`
	}{Pools: pools}, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

// Info performs GET /v1/info: live per-pool warm state and lifecycle counters, without touching pool config.
func (c *Client) Info(ctx context.Context) (*NodeInfo, error) {
	var info NodeInfo
	if err := c.getJSON(ctx, "/v1/info", &info); err != nil {
		return nil, err
	}
	return &info, nil
}

// Release performs POST /v1/sandboxes/{id}/release, which DESTROYS the VM,
// authenticated with the sandbox's own token or the node api_token. A 404
// (unknown id or already gone) is success, matching the SDK. Callers must only
// reach this on owner-authorized teardown — see the SandboxStore.Release contract.
func (c *Client) Release(ctx context.Context, id, token string) error {
	if id == "" {
		return fmt.Errorf("sandboxd: release requires a sandbox id")
	}
	return c.send(ctx, http.MethodPost, "/v1/sandboxes/"+url.PathEscape(id)+"/release", token, "release", nil, http.StatusNoContent, http.StatusNotFound)
}

func (c *Client) send(ctx context.Context, method, path, token, op string, body []byte, ok ...int) error {
	return c.sendWith(ctx, c.hc, method, path, token, op, body, ok...)
}

func (c *Client) sendWith(ctx context.Context, hc *http.Client, method, path, token, op string, body []byte, ok ...int) error {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, r)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	c.authenticate(req, token)

	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("sandboxd: %s: %w", op, err)
	}
	defer drainAndClose(resp)

	if !slices.Contains(ok, resp.StatusCode) {
		return statusError(resp)
	}
	return nil
}

func (c *Client) authenticate(req *http.Request, token string) {
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
}

// TokenFrom returns the api_token read from file (a Secret mount) when file is set, else literal.
func TokenFrom(literal, file string) (string, error) {
	if file == "" {
		return literal, nil
	}
	b, err := os.ReadFile(filepath.Clean(file))
	if err != nil {
		return "", fmt.Errorf("sandboxd: read token file %q: %w", file, err)
	}
	return strings.TrimSpace(string(b)), nil
}

func statusError(resp *http.Response) error {
	var payload struct {
		Error string `json:"error"`
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	_ = json.Unmarshal(b, &payload)
	return &HTTPError{StatusCode: resp.StatusCode, Message: payload.Error}
}

// drainAndClose reads the body out so the keep-alive connection is reused, which sub-millisecond claims depend on.
func drainAndClose(resp *http.Response) {
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}
