package scale

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

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
