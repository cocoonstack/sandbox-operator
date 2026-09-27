package e2bbuild

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

func TestABuildClaimsPromotesPublishesAndReleases(t *testing.T) {
	store := &fakeStore{}
	e := New(store, 2, time.Minute, 100)
	var published string
	e.Register("ns/app/b1", Request{Size: "medium", Tags: []string{"v1"}})
	req, ok := e.Request("ns/app/b1")
	require.True(t, ok)
	assert.Equal(t, Request{Size: "medium", Tags: []string{"v1"}}, req)

	require.NoError(t, e.Start(t.Context(), "ns/app/b1", Spec{
		Namespace: "ns", ClaimName: "build-1", Pool: scale.PoolKey{Template: "img", Net: "none", Size: "medium"}, Template: "e2b/ns/app",
		Publish: func(_ context.Context, node string, key scale.PoolKey, digest string) error {
			published = node + " " + key.Template + " " + digest
			return nil
		},
	}))
	info := waitDone(t, e, "ns/app/b1")
	assert.Equal(t, StatusReady, info.Status)
	assert.Equal(t, "node-a e2b/ns/app sha256:sb_1", published)
	assert.Equal(t, []string{"claim ns/build-1 img medium", "promote node-a sb_1 e2b/ns/app", "release node-a sb_1"}, store.calls())
	assert.Len(t, info.Logs, 3)
	assert.Contains(t, info.Logs[2].Message, "content digest sha256:sb_1")
	assert.Equal(t, PhasePromote, info.Logs[2].Phase)
}

func TestAFailedBuildNamesItsPhase(t *testing.T) {
	for name, tc := range map[string]struct {
		store   *fakeStore
		publish error
		step    string
		calls   []string
	}{
		"claim":   {&fakeStore{claimErr: errors.New("node n7 unreachable")}, nil, PhaseClaim, []string{"claim ns/build-1 img small"}},
		"promote": {&fakeStore{promoteErr: errors.New("409 egress lane")}, nil, PhasePromote, []string{"claim ns/build-1 img small", "promote node-a sb_1 e2b/ns/app", "release node-a sb_1"}},
		"publish": {&fakeStore{}, errors.New("node-b: connection refused"), PhasePublish, []string{"claim ns/build-1 img small", "promote node-a sb_1 e2b/ns/app", "release node-a sb_1"}},
	} {
		t.Run(name, func(t *testing.T) {
			e := New(tc.store, 1, time.Minute, 100)
			e.Register("b", Request{})
			require.NoError(t, e.Start(t.Context(), "b", Spec{
				Namespace: "ns", ClaimName: "build-1", Pool: scale.PoolKey{Template: "img", Net: "none", Size: "small"}, Template: "e2b/ns/app",
				Publish: func(context.Context, string, scale.PoolKey, string) error { return tc.publish },
			}))
			info := waitDone(t, e, "b")
			assert.Equal(t, [2]string{StatusError, tc.step}, [2]string{info.Status, info.FailedPhase})
			assert.NotEmpty(t, info.Failure)
			assert.NotContains(t, info.Failure, "node", "the failure a caller sees names no node")
			assert.Equal(t, "error", info.Logs[len(info.Logs)-1].Level)
			assert.Equal(t, tc.calls, tc.store.calls())
		})
	}
}

func TestStartIsBoundedIdempotentAndKnowsItsBuilds(t *testing.T) {
	store := &fakeStore{hold: make(chan struct{})}
	e := New(store, 1, time.Minute, 100)
	spec := Spec{Namespace: "ns", ClaimName: "c", Pool: scale.PoolKey{Template: "img"}, Template: "e2b/ns/app"}
	e.Register("first", Request{})
	e.Register("second", Request{})

	require.ErrorIs(t, e.Start(t.Context(), "nope", spec), ErrUnknownBuild)
	require.NoError(t, e.Start(t.Context(), "first", spec))
	require.NoError(t, e.Start(t.Context(), "first", spec), "a retried start of a running build is a no-op")
	require.ErrorIs(t, e.Start(t.Context(), "second", spec), ErrBusy)
	info, _ := e.Status("second")
	assert.Equal(t, StatusWaiting, info.Status)

	close(store.hold)
	waitDone(t, e, "first")
	require.Eventually(t, func() bool { return e.Start(t.Context(), "second", spec) == nil }, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, StatusReady, waitDone(t, e, "second").Status)
}

func TestTheLogIsCappedAndFinishedBuildsAgeOut(t *testing.T) {
	e := New(&fakeStore{}, 1, time.Minute, 1)
	e.Register("b", Request{})
	require.NoError(t, e.Start(t.Context(), "b", Spec{Pool: scale.PoolKey{Template: "img"}, Template: "t"}))
	assert.Len(t, waitDone(t, e, "b").Logs, 1)

	e.mu.Lock()
	e.builds["b"].finished = time.Now().Add(-2 * recordTTL)
	e.mu.Unlock()
	_, ok := e.Status("b")
	assert.False(t, ok, "a build finished over an hour ago is dropped")

	e.Register("left", Request{})
	e.mu.Lock()
	e.builds["left"].registered = time.Now().Add(-2 * recordTTL)
	e.mu.Unlock()
	_, ok = e.Status("left")
	assert.False(t, ok, "a build registered over an hour ago and never started is dropped")
}

func waitDone(t *testing.T, e *Executor, id string) Info {
	t.Helper()
	var info Info
	require.Eventually(t, func() bool {
		info, _ = e.Status(id)
		return info.Status == StatusReady || info.Status == StatusError
	}, 5*time.Second, 5*time.Millisecond)
	return info
}

type fakeStore struct {
	scale.SandboxStore

	claimErr   error
	promoteErr error
	hold       chan struct{}

	mu  sync.Mutex
	log []string
}

func (f *fakeStore) Claim(_ context.Context, ns, name string, pool scale.PoolKey, _ scale.ClaimOptions) (scale.Assignment, error) {
	if f.hold != nil {
		<-f.hold
	}
	f.record("claim " + ns + "/" + name + " " + pool.Template + " " + pool.Size)
	if f.claimErr != nil {
		return scale.Assignment{}, f.claimErr
	}
	return scale.Assignment{SandboxName: "sb_1", Node: "node-a"}, nil
}

func (f *fakeStore) Promote(_ context.Context, node, id, template string) (scale.PoolKey, string, error) {
	f.record("promote " + node + " " + id + " " + template)
	return scale.PoolKey{Template: template}, "sha256:" + id, f.promoteErr
}

func (f *fakeStore) Release(_ context.Context, node, id string) error {
	f.record("release " + node + " " + id)
	return nil
}

func (f *fakeStore) record(line string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log = append(f.log, strings.TrimSpace(line))
}

func (f *fakeStore) calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.log...)
}
