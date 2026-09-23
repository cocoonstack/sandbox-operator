package sandboxd

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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
		_, _ = w.Write([]byte(`{"pools":[{"key":{"template":"base:24.04","net":"none","size":"small"},"warm":3,"refilling":1,"target":4}],"claimed":2,"hibernated":1,"archived":0,"peers":["10.0.0.6:7777"]}`))
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
}

func TestSandboxesDecodesTheClaimTime(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/sandboxes", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"sandboxes":[{"id":"sb_1","key":{"template":"base:24.04"},"deadline":"2030-01-02T03:14:05Z","claimed_at":"2030-01-02T03:04:05Z"},{"id":"sb_2","key":{"template":"base:24.04"},"deadline":"2030-01-02T03:14:05Z"}]}`))
	}))
	defer srv.Close()

	rows, err := New(srv.URL, "root-token").Sandboxes(t.Context())
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC), rows[0].ClaimedAt)
	assert.True(t, rows[1].ClaimedAt.IsZero(), "a node that publishes no claimed_at leaves it zero")
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
