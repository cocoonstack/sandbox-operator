package scale

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
)

func TestLifecycleVerbsMapANodeUnknownSandboxToNotFound(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("n1", "n1:7777"))
	f := &recordingFactory{verbErr: &sandboxd.HTTPError{StatusCode: http.StatusNotFound, Message: "unknown sandbox"}}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("t", f.factory()))
	ctx := t.Context()

	_, statsErr := store.Stats(ctx, "n1", "sb_gone")
	_, forkErr := store.Fork(ctx, "n1", "sb_gone", 1, 0)
	_, snapErr := store.Snapshot(ctx, "n1", "sb_gone", "")
	for name, err := range map[string]error{
		"pause":    store.Pause(ctx, "n1", "sb_gone"),
		"resume":   store.Resume(ctx, "n1", "sb_gone"),
		"stats":    statsErr,
		"fork":     forkErr,
		"snapshot": snapErr,
	} {
		require.Error(t, err, name)
		assert.True(t, k8serrors.IsNotFound(err), "%s: a sandboxd 404 must surface as NotFound, got %v", name, err)
	}

	f.verbErr = errors.New("connection refused")
	err := store.Pause(ctx, "n1", "sb_live")
	require.Error(t, err)
	assert.False(t, k8serrors.IsNotFound(err), "a transport failure is not NotFound: %v", err)
}

func TestReadReportsTheClaimAsItsNodeHoldsIt(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("n1", "n1:7777"))
	deadline := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	f := &recordingFactory{rows: map[string][]sandboxd.SandboxSummary{"n1:7777": {{ID: "sb_live", Token: "secret", Hibernated: true, Deadline: deadline}}}}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("t", f.factory()))

	rec, err := store.Read(t.Context(), "n1", "sb_live")
	require.NoError(t, err)
	assert.Equal(t, SandboxRecord{Token: "secret", Paused: true, Deadline: deadline}, rec)

	f.rows["n1:7777"] = []sandboxd.SandboxSummary{{ID: "sb_live", Archived: true}}
	rec, err = store.Read(t.Context(), "n1", "sb_live")
	require.NoError(t, err)
	assert.True(t, rec.Paused, "an archived claim is paused: Resume restores it")

	_, err = store.Read(t.Context(), "n1", "sb_gone")
	assert.True(t, k8serrors.IsNotFound(err), "a sandboxd 404 must surface as NotFound, got %v", err)
}

func TestCheckpointNamesCarryTheNamespaceWithinTheNodeBudget(t *testing.T) {
	stamped, err := CheckpointName("team-a", "before")
	require.NoError(t, err)
	assert.Equal(t, "team-a/before", stamped)
	name, ok := CheckpointNameIn("team-a", stamped)
	assert.True(t, ok)
	assert.Equal(t, "before", name)
	_, ok = CheckpointNameIn("team-b", stamped)
	assert.False(t, ok)

	_, err = CheckpointName("team-a", strings.Repeat("x", 57))
	assert.True(t, k8serrors.IsBadRequest(err), "a stamped name past 63 characters must be a 400, got %v", err)
}

func TestLifecycleVerbsKeepANodeRejectionsStatus(t *testing.T) {
	src := NewStaticInventorySource()
	src.Put(poolInv("n1", "n1:7777"))
	f := &recordingFactory{verbErr: &sandboxd.HTTPError{StatusCode: http.StatusBadRequest, Message: "count 9999 exceeds max_fork_count"}}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("t", f.factory()))

	_, err := store.Fork(t.Context(), "n1", "sb_live", 9999, 0)
	require.Error(t, err)
	assert.True(t, k8serrors.IsBadRequest(err), "a node 400 must stay a 400, got %v", err)
	assert.Contains(t, err.Error(), "max_fork_count", "the node's reason must reach the caller")

	f.verbErr = &sandboxd.HTTPError{StatusCode: http.StatusConflict, Message: "already paused"}
	err = store.Pause(t.Context(), "n1", "sb_live")
	require.Error(t, err)
	assert.True(t, k8serrors.IsConflict(err), "a node 409 must stay a 409, got %v", err)

	f.verbErr = &sandboxd.HTTPError{StatusCode: http.StatusInternalServerError, Message: "provisioning failed"}
	err = store.Pause(t.Context(), "n1", "sb_live")
	require.Error(t, err)
	assert.False(t, k8serrors.IsBadRequest(err) || k8serrors.IsConflict(err), "a node 5xx is not a client error: %v", err)
}

func TestReleaseOfAReapedSandboxIsASuccessThroughTheRealClient(t *testing.T) {
	var releases atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		releases.Add(1)
		assert.Equal(t, "/v1/sandboxes/sb_gone/release", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	src := NewStaticInventorySource()
	src.Put(poolInv("n1", strings.TrimPrefix(srv.URL, "http://")))
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("t", NewSandboxdClientFactory()))

	require.NoError(t, store.Release(t.Context(), "n1", "sb_gone"), "the node's 404 on release means already gone, which the client reports as success")
	require.Equal(t, int64(1), releases.Load())
}
