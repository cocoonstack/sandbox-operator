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
		_, _ = w.Write([]byte(`{"pools":[{"key":{"template":"base:24.04","net":"none","size":"small"},"warm":3,"refilling":1,"target":4}],"templates":[{"key":{"template":"app:v1","net":"none","size":"medium"},"content_digest":"sha256:aa","created_at":"2026-09-28T01:02:03Z","cpu_count":2,"mem_total_bytes":1073741824}],"claimed":2,"hibernated":1,"archived":0,"advertise_addr":"10.0.0.5:7777","peers":["10.0.0.6:7777"]}`))
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
	assert.Equal(t, []NodeTemplate{{
		Key: PoolKey{Template: "app:v1", Net: "none", Size: "medium"}, ContentDigest: "sha256:aa",
		CreatedAt: time.Date(2026, 9, 28, 1, 2, 3, 0, time.UTC), CPUCount: 2, MemTotalBytes: 1 << 30,
	}}, info.Templates)
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

func TestSetInstanceMetadataPutsTheDocumentVerbatim(t *testing.T) {
	var gotBody, gotAuth, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/sandboxes/sb_1/instance-metadata" {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":"sandbox is paused"}`))
			return
		}
		b, _ := io.ReadAll(r.Body)
		gotBody, gotAuth, gotMethod = string(b), r.Header.Get("Authorization"), r.Method
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	c := New(srv.URL, "root-token")

	require.NoError(t, c.SetInstanceMetadata(t.Context(), "sb_1", []byte(`{"accessTokenHash":"ab"}`)))
	assert.Equal(t, `{"accessTokenHash":"ab"}`, gotBody)
	assert.Equal(t, "Bearer root-token", gotAuth)
	assert.Equal(t, http.MethodPut, gotMethod)

	err := c.SetInstanceMetadata(t.Context(), "sb_2", []byte(`{}`))
	he, ok := errors.AsType[*HTTPError](err)
	require.True(t, ok, "the node's refusal must surface as its status, got %v", err)
	assert.Equal(t, http.StatusConflict, he.StatusCode)
}

func TestDeleteTemplateAsksThisNodeAloneAndTakesAMissAsGone(t *testing.T) {
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.Method+" "+r.URL.Path+"?"+r.URL.RawQuery+" "+r.Header.Get("Authorization"))
		switch r.URL.Query().Get("template") {
		case "ns/gone":
			w.WriteHeader(http.StatusNotFound)
		case "ns/pooled":
			w.WriteHeader(http.StatusConflict)
		case "ns/replaced":
			w.WriteHeader(http.StatusPreconditionFailed)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer srv.Close()
	c := New(srv.URL, "root-token")

	require.NoError(t, c.DeleteTemplate(t.Context(), PoolKey{Template: "ns/app", Net: "none", Size: "medium"}, ""))
	require.NoError(t, c.DeleteTemplate(t.Context(), PoolKey{Template: "ns/gone"}, ""))
	err := c.DeleteTemplate(t.Context(), PoolKey{Template: "ns/pooled"}, "")
	he, ok := errors.AsType[*HTTPError](err)
	require.True(t, ok, "the node's refusal must surface as its status, got %v", err)
	assert.Equal(t, http.StatusConflict, he.StatusCode)
	assert.Equal(t, "DELETE /v1/templates?net=none&no_redirect=1&size=medium&template=ns%2Fapp Bearer root-token", got[0])
	assert.Equal(t, "DELETE /v1/templates?no_redirect=1&template=ns%2Fgone Bearer root-token", got[1])

	err = c.DeleteTemplate(t.Context(), PoolKey{Template: "ns/replaced"}, "sha256:x")
	he, ok = errors.AsType[*HTTPError](err)
	require.True(t, ok, "a replaced generation must surface as 412, got %v", err)
	assert.Equal(t, http.StatusPreconditionFailed, he.StatusCode)
	assert.Equal(t, "DELETE /v1/templates?digest=sha256%3Ax&no_redirect=1&template=ns%2Freplaced Bearer root-token", got[3])
}

func TestSetTemplateLabelsPutsTheWholeMap(t *testing.T) {
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got = append(got, r.Method+" "+r.URL.Path+"?"+r.URL.RawQuery+" "+string(b))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	c := New(srv.URL, "root-token")

	require.NoError(t, c.SetTemplateLabels(t.Context(), PoolKey{Template: "ns/app", Size: "medium"}, map[string]string{"v1": "sha256:aa"}, "sha256:aa"))
	require.NoError(t, c.SetTemplateLabels(t.Context(), PoolKey{Template: "ns/app"}, nil, ""))
	assert.Equal(t, []string{
		`PUT /v1/templates/labels?digest=sha256%3Aaa&size=medium&template=ns%2Fapp {"labels":{"v1":"sha256:aa"}}`,
		`PUT /v1/templates/labels?template=ns%2Fapp {"labels":null}`,
	}, got)
}

func TestPromoteActsAsTheOperatorByID(t *testing.T) {
	var gotPath, gotAuth, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotPath, gotAuth, gotBody = r.Method+" "+r.URL.Path, r.Header.Get("Authorization"), string(b)
		_, _ = w.Write([]byte(`{"key":{"template":"ns/app","net":"none","size":"small"},"content_digest":"sha256:aa"}`))
	}))
	defer srv.Close()

	key, digest, err := New(srv.URL, "root-token").Promote(t.Context(), "sb_1", "ns/app")
	require.NoError(t, err)
	assert.Equal(t, PoolKey{Template: "ns/app", Net: "none", Size: "small"}, key)
	assert.Equal(t, "sha256:aa", digest)
	assert.Equal(t, "POST /v1/sandboxes/sb_1/promote", gotPath)
	assert.Equal(t, "Bearer root-token", gotAuth)
	assert.JSONEq(t, `{"template":"ns/app"}`, gotBody, "no body token: the root token acts on the sandbox by id")
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

func TestCaptureVerbsOutliveTheRequestTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(150 * time.Millisecond)
		switch r.URL.Path {
		case "/v1/sandboxes/sb_1/promote":
			_, _ = w.Write([]byte(`{"key":{"template":"ns/app","net":"none","size":"small"},"content_digest":"sha256:aa"}`))
		case "/v1/sandboxes/sb_1/hibernate":
			w.WriteHeader(http.StatusNoContent)
		default:
			_, _ = w.Write([]byte(`{"id":"sb_2","token":"t"}`))
		}
	}))
	defer srv.Close()
	c := New(srv.URL, "root-token", WithHTTPClient(&http.Client{Timeout: 50 * time.Millisecond}))

	_, _, err := c.Promote(t.Context(), "sb_1", "ns/app")
	require.NoError(t, err, "a promote runs as long as the guest's memory takes")
	require.NoError(t, c.Hibernate(t.Context(), "sb_1"))
	_, err = c.Claim(t.Context(), ClaimSpec{Template: "img"})
	require.Error(t, err, "a claim keeps the request timeout")
}
