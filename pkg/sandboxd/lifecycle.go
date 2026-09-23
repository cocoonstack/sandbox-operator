package sandboxd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// maxReplyBytes caps a sandboxd reply body. At the measured 213 bytes per
// listed sandbox it clears 78k sandboxes on one node, well past the 2000 the
// design projects.
const maxReplyBytes = 16 << 20

// ForkSpec is the POST /v1/sandboxes/{id}/fork body. Token stays empty on the
// operator path; Count must be within the node's max_fork_count.
type ForkSpec struct {
	Token      string `json:"token,omitempty"`
	Count      int    `json:"count"`
	TTLSeconds int    `json:"ttl_seconds,omitempty"`
	// ClaimRefPrefix records each child under prefix + its id.
	ClaimRefPrefix string `json:"claim_ref_prefix,omitempty"`
}

// ForkResult carries one claim per child; children are fresh sandboxes with
// their own ids and leases, and the parent keeps running.
type ForkResult struct {
	Children []ClaimResult `json:"children"`
}

// RenewSpec is the POST /v1/sandboxes/{id}/renew body. TTLSeconds 0 asks for
// the node default.
type RenewSpec struct {
	TTLSeconds int `json:"ttl_seconds,omitempty"`
}

// RenewResult carries the lease deadline the node granted.
type RenewResult struct {
	Deadline time.Time `json:"deadline"`
}

// CheckpointSpec is the POST /v1/sandboxes/{id}/checkpoint body.
type CheckpointSpec struct {
	Token string `json:"token,omitempty"`
	Name  string `json:"name,omitempty"`
}

// Checkpoint is a captured sandbox state that branches can be claimed from.
type Checkpoint struct {
	ID        string    `json:"id"`
	Name      string    `json:"name,omitempty"`
	SandboxID string    `json:"sandbox_id"`
	Key       PoolKey   `json:"key"`
	CreatedAt time.Time `json:"created_at"`
}

// PoolKey is a checkpoint's or template's pool identity.
type PoolKey struct {
	Template string `json:"template"`
	Net      string `json:"net,omitempty"`
	Size     string `json:"size,omitempty"`
	Engine   string `json:"engine,omitempty"`
}

// SandboxSummary is one live claim as the owning node reports it.
type SandboxSummary struct {
	ID             string    `json:"id"`
	Key            PoolKey   `json:"key"`
	Deadline       time.Time `json:"deadline"`
	ClaimedAt      time.Time `json:"claimed_at"`
	Hibernated     bool      `json:"hibernated"`
	Archived       bool      `json:"archived,omitempty"`
	FromCheckpoint string    `json:"from_checkpoint,omitempty"`
	ClaimRef       string    `json:"claim_ref,omitempty"`
	// Token is the claim's own bearer token; only the by-id read reports it.
	Token string `json:"token,omitempty"`
}

// SandboxStats is one sandbox's resource usage. CPUCount and MemTotalBytes are
// the tier the VM was booted with (authoritative); MemUsedBytes is the host
// VMM's resident set and is only meaningful when MemUsedMeasured is true — a
// hibernated sandbox has no process to measure.
type SandboxStats struct {
	ID              string    `json:"id"`
	CPUCount        int       `json:"cpu_count"`
	MemTotalBytes   int64     `json:"mem_total_bytes"`
	MemUsedBytes    int64     `json:"mem_used_bytes"`
	Hibernated      bool      `json:"hibernated"`
	MeasuredAt      time.Time `json:"measured_at"`
	MemUsedMeasured bool      `json:"mem_used_measured"`
}

// Hibernate performs POST /v1/sandboxes/{id}/hibernate: it snapshots the
// sandbox and stops its VM, freeing the node's memory. This is the pause verb;
// its cost is proportional to guest RAM because the memory is written out.
func (c *Client) Hibernate(ctx context.Context, id string) error {
	return c.sandboxVerb(ctx, id, "hibernate")
}

// Wake performs POST /v1/sandboxes/{id}/wake, restoring a hibernated sandbox
// through cocoon's mmap fast path (~55 ms) and leaving it running. Idempotent
// on a sandbox that is already awake.
func (c *Client) Wake(ctx context.Context, id string) error {
	return c.sandboxVerb(ctx, id, "wake")
}

// Renew performs POST /v1/sandboxes/{id}/renew, resetting the lease to
// TTLSeconds from now. The node's grant is authoritative: 0 takes its default
// and an oversized request is clamped, so the reply carries what was granted.
func (c *Client) Renew(ctx context.Context, id string, spec RenewSpec) (time.Time, error) {
	if id == "" {
		return time.Time{}, fmt.Errorf("sandboxd: renew requires a sandbox id")
	}
	var out RenewResult
	err := c.postJSON(ctx, "/v1/sandboxes/"+url.PathEscape(id)+"/renew", spec, &out)
	return out.Deadline, err
}

// Fork performs POST /v1/sandboxes/{id}/fork, branching the sandbox into count
// children. The parent is checkpointed in place and keeps running; each child
// is a fresh claim with its own id and lease.
func (c *Client) Fork(ctx context.Context, id string, spec ForkSpec) (ForkResult, error) {
	var out ForkResult
	if id == "" {
		return out, fmt.Errorf("sandboxd: fork requires a sandbox id")
	}
	err := c.postJSON(ctx, "/v1/sandboxes/"+url.PathEscape(id)+"/fork", spec, &out)
	return out, err
}

// Checkpoint performs POST /v1/sandboxes/{id}/checkpoint, capturing the
// sandbox's state under a fresh id. The source keeps running.
func (c *Client) Checkpoint(ctx context.Context, id string, spec CheckpointSpec) (Checkpoint, error) {
	if id == "" {
		return Checkpoint{}, fmt.Errorf("sandboxd: checkpoint requires a sandbox id")
	}
	var out struct {
		Checkpoint Checkpoint `json:"checkpoint"`
	}
	err := c.postJSON(ctx, "/v1/sandboxes/"+url.PathEscape(id)+"/checkpoint", spec, &out)
	return out.Checkpoint, err
}

// Checkpoints performs GET /v1/checkpoints, newest first.
func (c *Client) Checkpoints(ctx context.Context) ([]Checkpoint, error) {
	var out struct {
		Checkpoints []Checkpoint `json:"checkpoints"`
	}
	err := c.getJSON(ctx, "/v1/checkpoints", &out)
	return out.Checkpoints, err
}

// DeleteCheckpoint performs DELETE /v1/checkpoints/{id}. A 404 is success.
func (c *Client) DeleteCheckpoint(ctx context.Context, checkpointID string) error {
	if checkpointID == "" {
		return fmt.Errorf("sandboxd: delete checkpoint requires a checkpoint id")
	}
	return c.sendNoBody(ctx, http.MethodDelete, "/v1/checkpoints/"+url.PathEscape(checkpointID), c.token, "delete checkpoint", http.StatusNoContent, http.StatusNotFound)
}

// Stats performs GET /v1/sandboxes/{id}/stats.
func (c *Client) Stats(ctx context.Context, id string) (SandboxStats, error) {
	var out SandboxStats
	if id == "" {
		return out, fmt.Errorf("sandboxd: stats requires a sandbox id")
	}
	err := c.getJSON(ctx, "/v1/sandboxes/"+url.PathEscape(id)+"/stats", &out)
	return out, err
}

// Sandbox performs GET /v1/sandboxes/{id}, one live claim in the index-row shape.
func (c *Client) Sandbox(ctx context.Context, id string) (SandboxSummary, error) {
	var out SandboxSummary
	if id == "" {
		return out, fmt.Errorf("sandboxd: sandbox read requires a sandbox id")
	}
	err := c.getJSON(ctx, "/v1/sandboxes/"+url.PathEscape(id), &out)
	return out, err
}

// Sandboxes performs GET /v1/sandboxes, this node's live claims.
func (c *Client) Sandboxes(ctx context.Context) ([]SandboxSummary, error) {
	return c.listSandboxes(ctx, "/v1/sandboxes")
}

// SandboxesByClaimRef performs GET /v1/sandboxes?claim_ref=, this node's live claims recorded under ref.
func (c *Client) SandboxesByClaimRef(ctx context.Context, ref string) ([]SandboxSummary, error) {
	return c.listSandboxes(ctx, "/v1/sandboxes?claim_ref="+url.QueryEscape(ref))
}

func (c *Client) listSandboxes(ctx context.Context, path string) ([]SandboxSummary, error) {
	var out struct {
		Sandboxes []SandboxSummary `json:"sandboxes"`
	}
	err := c.getJSON(ctx, path, &out)
	return out.Sandboxes, err
}

// sandboxVerb posts a body-less sandbox-scoped verb and expects 204.
func (c *Client) sandboxVerb(ctx context.Context, id, verb string) error {
	if id == "" {
		return fmt.Errorf("sandboxd: %s requires a sandbox id", verb)
	}
	return c.sendNoBody(ctx, http.MethodPost, "/v1/sandboxes/"+url.PathEscape(id)+"/"+verb, c.token, verb, http.StatusNoContent)
}

func (c *Client) postJSON(ctx context.Context, path string, body, out any) error {
	return c.sendJSON(ctx, http.MethodPost, path, body, out)
}

func (c *Client) putJSON(ctx context.Context, path string, body, out any) error {
	return c.sendJSON(ctx, http.MethodPut, path, body, out)
}

func (c *Client) sendJSON(ctx context.Context, method, path string, body, out any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("sandboxd: encode %s: %w", path, err)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	c.authenticate(req, c.token)

	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("sandboxd: %s: %w", path, err)
	}
	defer drainAndClose(resp)

	if resp.StatusCode/100 != 2 {
		return statusError(resp)
	}
	return decodeInto(resp, out, path)
}

func (c *Client) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	c.authenticate(req, c.token)

	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("sandboxd: %s: %w", path, err)
	}
	defer drainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		return statusError(resp)
	}
	return decodeInto(resp, out, path)
}

// decodeInto reads a bounded reply body into out. Reading one byte past the cap
// distinguishes an over-cap reply from malformed JSON: silently truncating a
// node's sandbox or checkpoint listing would surface as "unexpected end of JSON
// input" and make that node's sandboxes vanish from the aggregated view.
func decodeInto(resp *http.Response, out any, path string) error {
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxReplyBytes+1))
	if err != nil {
		return fmt.Errorf("sandboxd: read %s reply: %w", path, err)
	}
	if len(b) > maxReplyBytes {
		return fmt.Errorf("sandboxd: %s reply exceeds %d bytes", path, maxReplyBytes)
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("sandboxd: decode %s reply: %w", path, err)
	}
	return nil
}
