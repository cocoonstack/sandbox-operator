package sandboxd

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClaimSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/v1/claim", r.URL.Path)
		assert.Equal(t, "Bearer root-token", r.Header.Get("Authorization"))
		var spec ClaimSpec
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&spec))
		assert.Equal(t, "base:24.04", spec.Template)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ClaimResult{
			ID: "sb_abc", Token: "sbtok",
			Deadline:  time.Date(2026, 7, 6, 0, 5, 0, 0, time.UTC),
			OwnerAddr: "10.0.0.5:7777",
		})
	}))
	defer srv.Close()

	c := New(srv.URL, "root-token")
	res, err := c.Claim(t.Context(), ClaimSpec{Template: "base:24.04", Net: "none", Size: "small", TTLSeconds: 300})
	require.NoError(t, err)
	require.Equal(t, "sb_abc", res.ID)
	require.Equal(t, "sbtok", res.Token)
	require.Equal(t, "10.0.0.5:7777", res.OwnerAddr)
}

func TestSandboxdClaimFallbackOn429(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "node at max_claims"})
	}))
	defer srv.Close()

	c := New(srv.URL, "root-token")
	_, err := c.Claim(t.Context(), ClaimSpec{Template: "base:24.04"})
	require.ErrorIs(t, err, ErrNodeAtCapacity)
}

func TestSandboxdClaimRedirectIsCapacityMiss(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ClaimResult{Redirect: []string{"10.0.0.6:7777"}})
	}))
	defer srv.Close()

	c := New(srv.URL, "root-token")
	_, err := c.Claim(t.Context(), ClaimSpec{Template: "base:24.04"})
	require.ErrorIs(t, err, ErrNodeAtCapacity)
}

func TestClaimRedirectCarriesItsTargets(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(ClaimResult{Redirect: []string{"10.0.0.6:7777", "10.0.0.7:7777"}})
	}))
	defer srv.Close()

	_, err := New(srv.URL, "root-token").Claim(t.Context(), ClaimSpec{Template: "base:24.04"})
	re, ok := errors.AsType[*RedirectError](err)
	require.True(t, ok, "a redirect-only 200 is a *RedirectError, got %v", err)
	assert.Equal(t, []string{"10.0.0.6:7777", "10.0.0.7:7777"}, re.Targets)
}

func TestClaimEmptyBodyIsCapacityMissNotRedirect(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	_, err := New(srv.URL, "root-token").Claim(t.Context(), ClaimSpec{Template: "base:24.04"})
	require.ErrorIs(t, err, ErrNodeAtCapacity)
	_, isRedirect := errors.AsType[*RedirectError](err)
	assert.False(t, isRedirect)
}

func TestClaimSendsNoRedirectOnlyWhenSet(t *testing.T) {
	var bodies []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b map[string]any
		_ = json.NewDecoder(r.Body).Decode(&b)
		bodies = append(bodies, b)
		_ = json.NewEncoder(w).Encode(ClaimResult{ID: "sb_1", Token: "tok"})
	}))
	defer srv.Close()

	c := New(srv.URL, "root-token")
	_, err := c.Claim(t.Context(), ClaimSpec{Template: "base:24.04", NoRedirect: true})
	require.NoError(t, err)
	_, err = c.Claim(t.Context(), ClaimSpec{Template: "base:24.04"})
	require.NoError(t, err)
	require.Len(t, bodies, 2)
	assert.Equal(t, true, bodies[0]["no_redirect"])
	assert.NotContains(t, bodies[1], "no_redirect")
}

func TestClaimAndRenewCarryMetadataAndOnExpireOnlyWhenSet(t *testing.T) {
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		bodies = append(bodies, strings.TrimSpace(string(b)))
		_ = json.NewEncoder(w).Encode(ClaimResult{ID: "sb_1", Token: "tok", Deadline: time.Now()})
	}))
	defer srv.Close()

	c := New(srv.URL, "root-token")
	_, err := c.Claim(t.Context(), ClaimSpec{Template: "base:24.04"})
	require.NoError(t, err)
	_, err = c.Claim(t.Context(), ClaimSpec{Template: "base:24.04", Metadata: map[string]string{"user": "u1"}, OnExpire: ExpireArchive})
	require.NoError(t, err)
	_, err = c.Renew(t.Context(), "sb_1", RenewSpec{TTLSeconds: 60, OnExpire: ExpireDestroy})
	require.NoError(t, err)
	require.Len(t, bodies, 3)
	assert.JSONEq(t, `{"template":"base:24.04"}`, bodies[0])
	assert.JSONEq(t, `{"template":"base:24.04","metadata":{"user":"u1"},"on_expire":"archive"}`, bodies[1])
	assert.JSONEq(t, `{"ttl_seconds":60,"on_expire":"destroy"}`, bodies[2])
}

func TestClaimServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "provisioning failed"})
	}))
	defer srv.Close()

	c := New(srv.URL, "root-token")
	_, err := c.Claim(t.Context(), ClaimSpec{Template: "base:24.04"})
	require.Error(t, err)
	var he *HTTPError
	require.ErrorAs(t, err, &he)
	require.Equal(t, http.StatusInternalServerError, he.StatusCode)
	require.Equal(t, "provisioning failed", he.Message)
}

func TestReleaseSuccessAndAlreadyGone(t *testing.T) {
	var releases atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := releases.Add(1)
		assert.Equal(t, "Bearer sbtok", r.Header.Get("Authorization"), "release authenticates with the sandbox's own token")
		assert.Equal(t, "/v1/sandboxes/sb_abc/release", r.URL.Path)
		if n == 1 {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "root-token")
	require.NoError(t, c.Release(t.Context(), "sb_abc", "sbtok"))

	require.NoError(t, c.Release(t.Context(), "sb_abc", "sbtok"))
	require.Equal(t, int64(2), releases.Load())
}

func TestInfo(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/v1/info", r.URL.Path)
		assert.Equal(t, "Bearer root-token", r.Header.Get("Authorization"), "info is a root-token operator surface")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"pools":[{"key":{"template":"base:24.04","net":"none","size":"small"},"warm":3,"refilling":1,"target":4}],"claimed":2,"hibernated":1,"archived":0,"advertise_addr":"10.0.0.5:7777","peers":["10.0.0.6:7777"]}`))
	}))
	defer srv.Close()

	info, err := New(srv.URL, "root-token").Info(t.Context())
	require.NoError(t, err)
	require.Len(t, info.Pools, 1)
	assert.Equal(t, PoolKey{Template: "base:24.04", Net: "none", Size: "small"}, info.Pools[0].Key)
	assert.Equal(t, 3, info.Pools[0].Warm)
	assert.Equal(t, 4, info.Pools[0].Target)
	assert.Equal(t, 2, info.Claimed)
	assert.Equal(t, 1, info.Hibernated)
	assert.Equal(t, "10.0.0.5:7777", info.AdvertiseAddr)
	assert.Equal(t, []string{"10.0.0.6:7777"}, info.Peers)
}

func TestSandboxesDecodesTheClaimTime(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/sandboxes", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"sandboxes":[{"id":"sb_1","key":{"template":"base:24.04"},"deadline":"2030-01-02T03:14:05Z","claimed_at":"2030-01-02T03:04:05Z","metadata":{"user":"u1"},"on_expire":"archive","cpu_count":2,"mem_total_bytes":1073741824},{"id":"sb_2","key":{"template":"base:24.04"},"deadline":"2030-01-02T03:14:05Z"}]}`))
	}))
	defer srv.Close()

	rows, err := New(srv.URL, "root-token").Sandboxes(t.Context())
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC), rows[0].ClaimedAt)
	assert.True(t, rows[1].ClaimedAt.IsZero(), "a node that publishes no claimed_at leaves it zero")
	assert.Equal(t, map[string]string{"user": "u1"}, rows[0].Metadata)
	assert.Equal(t, int32(2), rows[0].CPUCount)
	assert.Equal(t, int64(1<<30), rows[0].MemTotalBytes)
}

func TestSandboxesByClaimRefAsksForOneName(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/sandboxes", r.URL.Path)
		assert.Equal(t, "claim_ref=team-a%2Fdemo", r.URL.RawQuery)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"sandboxes":[{"id":"sb_1","key":{"template":"base:24.04"},"claim_ref":"team-a/demo"}]}`))
	}))
	defer srv.Close()

	rows, err := New(srv.URL, "root-token").SandboxesByClaimRef(t.Context(), "team-a/demo")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "team-a/demo", rows[0].ClaimRef)
}

func TestSandboxReadsOneRow(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer root-token", r.Header.Get("Authorization"), "the per-id read is a root-token operator surface")
		if r.URL.Path != "/v1/sandboxes/sb_1" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"sb_1","key":{"template":"base:24.04"},"deadline":"2030-01-02T03:14:05Z","claimed_at":"2030-01-02T03:04:05Z","claim_ref":"ns/s1"}`))
	}))
	defer srv.Close()
	c := New(srv.URL, "root-token")

	row, err := c.Sandbox(t.Context(), "sb_1")
	require.NoError(t, err)
	assert.Equal(t, "ns/s1", row.ClaimRef)
	assert.Equal(t, time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC), row.ClaimedAt)

	_, err = c.Sandbox(t.Context(), "sb_2")
	he, ok := errors.AsType[*HTTPError](err)
	require.True(t, ok, "an unknown id must surface the node's status, got %v", err)
	assert.Equal(t, http.StatusNotFound, he.StatusCode)
}

func TestIsOwnerAsksWithTheSandboxsOwnToken(t *testing.T) {
	var fail atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case fail.Load():
			w.WriteHeader(http.StatusInternalServerError)
		case r.URL.Path == "/v1/sandboxes/sb_1/owner" && r.Header.Get("Authorization") == "Bearer sbtok":
			_, _ = w.Write([]byte(`{"owner_addr":"10.0.0.1:7777"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	c := New(srv.URL, "root-token")

	owns, err := c.IsOwner(t.Context(), "sb_1", "sbtok")
	require.NoError(t, err)
	assert.True(t, owns)
	owns, err = c.IsOwner(t.Context(), "sb_1", "root-token")
	require.NoError(t, err)
	assert.False(t, owns, "the node's own api token is not the sandbox's")

	fail.Store(true)
	_, err = c.IsOwner(t.Context(), "sb_1", "sbtok")
	assert.Error(t, err, "a node error is not a no")
}

func TestSetPoolsReplacesTheNodeTargetsAndDecodesTheEcho(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPut, r.Method)
		assert.Equal(t, "/v1/pools", r.URL.Path)
		assert.Equal(t, "Bearer root-token", r.Header.Get("Authorization"), "pools is a root-token operator surface")
		var body struct {
			Pools []PoolSpec `json:"pools"`
		}
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, []PoolSpec{{Template: "base:24.04", Net: "none", Size: "small", Warm: 4}}, body.Pools)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"pools":[{"key":{"template":"base:24.04","net":"none","size":"small"},"warm":1,"refilling":3,"target":4}],"claimed":0}`))
	}))
	defer srv.Close()

	info, err := New(srv.URL, "root-token").SetPools(t.Context(), []PoolSpec{{Template: "base:24.04", Net: "none", Size: "small", Warm: 4}})
	require.NoError(t, err)
	require.Len(t, info.Pools, 1)
	assert.Equal(t, 1, info.Pools[0].Warm)
	assert.Equal(t, 3, info.Pools[0].Refilling)
	assert.Equal(t, 4, info.Pools[0].Target)
}

func TestSetPoolsSendsAnEmptyListNotNull(t *testing.T) {
	var raw string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		raw = string(b)
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "duplicate pool"})
	}))
	defer srv.Close()

	_, err := New(srv.URL, "root-token").SetPools(t.Context(), nil)
	require.Error(t, err)
	var he *HTTPError
	require.ErrorAs(t, err, &he)
	assert.Equal(t, http.StatusBadRequest, he.StatusCode)
	assert.Equal(t, "duplicate pool", he.Message)
	assert.JSONEq(t, `{"pools":[]}`, raw, "a nil set must drain as [] rather than null")
}
