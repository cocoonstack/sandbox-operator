package scale

import (
	"errors"
	"net/http"
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
	gone := &sandboxd.HTTPError{StatusCode: http.StatusNotFound, Message: "unknown sandbox"}
	f := &recordingFactory{verbErr: gone, releaseErr: gone}
	store := NewScatterGatherStore(src, WithLogger(logr.Discard()), WithClaimRouting("t", f.factory()))
	ctx := t.Context()

	_, statsErr := store.Stats(ctx, "n1", "sb_gone")
	_, forkErr := store.Fork(ctx, "n1", "sb_gone", 1, 0)
	_, snapErr := store.Snapshot(ctx, "n1", "sb_gone", "")
	for name, err := range map[string]error{
		"pause":    store.Pause(ctx, "n1", "sb_gone"),
		"resume":   store.Resume(ctx, "n1", "sb_gone"),
		"release":  store.Release(ctx, "n1", "sb_gone"),
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
