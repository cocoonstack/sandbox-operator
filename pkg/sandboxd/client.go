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
	"slices"
	"strings"
	"time"
)

// ErrNodeAtCapacity is returned by Claim when sandboxd answers 429 (the node is
// at max_claims, the calling tenant is at its own max_claims, or the node is
// draining), or when a 200 carries only a peer redirect rather than a delivered
// sandbox. In every case this node handed over no VM, so the store tries another
// node or reports no warm capacity.
var ErrNodeAtCapacity = errors.New("sandboxd: node at capacity or draining")

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
	TTLSeconds int    `json:"ttl_seconds,omitempty"`
	// NoRedirect is set by an SDK retrying at a redirect target; this client does
	// not chase redirects (a redirect-only reply is a capacity miss), so it stays false.
	NoRedirect bool `json:"no_redirect,omitempty"`
	// ClaimRef is the k8s "<namespace>/<name>" of the Sandbox this claim is
	// created for. sandboxd records it on the claim and echoes it in its
	// operator index, so the aggregated read path can map a listed sandbox back
	// to the name it was claimed under. Empty for claims with no k8s identity.
	ClaimRef string `json:"claim_ref,omitempty"`
}

// ClaimResult is the POST /v1/claim success body.
type ClaimResult struct {
	ID        string    `json:"id"`
	Token     string    `json:"token"`
	Deadline  time.Time `json:"deadline"`
	OwnerAddr string    `json:"owner_addr"`
	// FromCheckpoint is the lineage edge when the claim branched from a checkpoint.
	FromCheckpoint string `json:"from_checkpoint,omitempty"`
	// Redirect, when non-empty on a 200, names warm peers to retry at instead of a
	// delivered sandbox. Claim treats this as a capacity miss (see ErrNodeAtCapacity).
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

// NodeInfo is the PUT /v1/pools (and GET /v1/info) response: the node's live
// per-pool warm state plus its lifecycle counters.
type NodeInfo struct {
	Pools      []NodePool `json:"pools"`
	Claimed    int        `json:"claimed"`
	Hibernated int        `json:"hibernated"`
	Archived   int        `json:"archived"`
}

// Client talks to a single sandboxd instance. It is safe for concurrent use.
type Client struct {
	baseURL string
	// token is the node api_token (root or tenant) every other verb presents;
	// Release and IsOwner authenticate with a token passed per call.
	token string
	hc    *http.Client
}

// New returns a Client for the sandboxd at baseURL, authenticating resource verbs
// with the node api_token (may be empty when sandboxd runs without auth).
func New(baseURL, token string, opts ...Option) *Client {
	c := &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		hc:      &http.Client{},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Claim performs POST /v1/claim, returning the delivered sandbox on success.
// A 429, or a 200 that carries only a peer redirect, yields ErrNodeAtCapacity.
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
		if r.ID == "" {
			// A redirect-only body (warm miss, or node at max_claims with a warm
			// peer) or an empty body: this node delivered no sandbox.
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
	if err := c.putJSON(ctx, "/v1/pools", struct {
		Pools []PoolSpec `json:"pools"`
	}{Pools: pools}, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

// Info performs GET /v1/info: the node's live per-pool warm state and lifecycle
// counters, read without touching its pool config.
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
	return c.sendNoBody(ctx, http.MethodPost, "/v1/sandboxes/"+url.PathEscape(id)+"/release", token, "release", http.StatusNoContent, http.StatusNotFound)
}

// IsOwner performs GET /v1/sandboxes/{id}/owner with the sandbox's own token; the node's 404 is false.
func (c *Client) IsOwner(ctx context.Context, id, token string) (bool, error) {
	if id == "" {
		return false, fmt.Errorf("sandboxd: owner requires a sandbox id")
	}
	err := c.sendNoBody(ctx, http.MethodGet, "/v1/sandboxes/"+url.PathEscape(id)+"/owner", token, "owner", http.StatusOK)
	if he, ok := errors.AsType[*HTTPError](err); ok && he.StatusCode == http.StatusNotFound {
		return false, nil
	}
	return err == nil, err
}

// sendNoBody performs a body-less request authenticated with token, accepting the statuses in ok.
func (c *Client) sendNoBody(ctx context.Context, method, path, token, op string, ok ...int) error {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	c.authenticate(req, token)

	resp, err := c.hc.Do(req)
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

// statusError reads the {"error": "..."} body and returns a typed *HTTPError.
func statusError(resp *http.Response) error {
	var payload struct {
		Error string `json:"error"`
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	_ = json.Unmarshal(b, &payload)
	return &HTTPError{StatusCode: resp.StatusCode, Message: payload.Error}
}

// drainAndClose fully reads and closes the body so the keep-alive connection can
// be reused — essential for stable sub-millisecond claim latency under load.
func drainAndClose(resp *http.Response) {
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}
