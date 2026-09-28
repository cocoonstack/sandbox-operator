package e2bbuild

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	e := New(store, store, 2, time.Minute, 100)
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
	assert.Equal(t, []string{"claim ns/build-1 img medium", "init user  map[]", "promote node-a sb_1 e2b/ns/app", "release node-a sb_1"}, store.calls())
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
		"promote": {&fakeStore{promoteErr: errors.New("409 egress lane")}, nil, PhasePromote, []string{"claim ns/build-1 img small", "init user  map[]", "promote node-a sb_1 e2b/ns/app", "release node-a sb_1"}},
		"publish": {&fakeStore{}, errors.New("node-b: connection refused"), PhasePublish, []string{"claim ns/build-1 img small", "init user  map[]", "promote node-a sb_1 e2b/ns/app", "release node-a sb_1"}},
	} {
		t.Run(name, func(t *testing.T) {
			e := New(tc.store, tc.store, 1, time.Minute, 100)
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

func TestStepsCarryTheirUserWorkdirAndEnvIntoLaterCommandsAndTheDefaults(t *testing.T) {
	store := &fakeStore{exits: map[string][]int{"check": {1, 0}}}
	e := New(store, store, 1, time.Minute, 100)
	e.Register("b", Request{})
	require.NoError(t, e.Start(t.Context(), "b", Spec{
		Namespace: "ns", ClaimName: "c", Pool: scale.PoolKey{Template: "img", Size: "small"}, Template: "e2b/ns/app",
		Steps: []Step{
			{Type: stepEnv, Args: []string{"A", "$HOME/bin"}},
			{Type: stepUser, Args: []string{"bob"}},
			{Type: stepWorkdir, Args: []string{"app"}},
			{Type: stepRun, Args: []string{"make"}},
			{Type: stepRun, Args: []string{"apt-get install -y vim", "root"}},
		},
		StartCmd: "serve", ReadyCmd: "check",
	}))
	info := waitDone(t, e, "b")
	require.Equal(t, StatusReady, info.Status, info.Failure)
	assert.Equal(t, []string{
		"claim ns/c img small",
		`run root  map[] printf "%s" "$HOME/bin"`,
		"run root  map[A:$HOME/bin] id -u 'bob' >/dev/null 2>&1 || useradd --create-home --shell /bin/bash 'bob'",
		`run root  map[A:$HOME/bin] t='/app'; [ -d "$t" ] && exit 0; n=$t; while [ ! -d "$(dirname "$n")" ]; do n=$(dirname "$n"); done; mkdir -p "$t" && chown -R 'bob': "$n"`,
		"run bob /app map[A:$HOME/bin] make",
		"run root /app map[A:$HOME/bin] apt-get install -y vim",
		"init bob /app map[A:$HOME/bin]",
		"start bob /app serve",
		"run bob /app map[A:$HOME/bin] check",
		"run bob /app map[A:$HOME/bin] check",
		"promote node-a sb_1 e2b/ns/app",
		"release node-a sb_1",
	}, store.calls())
	assert.Equal(t, [2]string{"4", "out of make"}, [2]string{info.Logs[4].Phase, info.Logs[4].Message})
}

func TestARelayedBuildSeedsItsStepsWithTheProxyEnvironment(t *testing.T) {
	for route, want := range map[string]string{"relay": "map[A:1 http_proxy:p]", "none": "map[A:1]"} {
		store := &fakeStore{route: route}
		e := New(store, store, 1, time.Minute, 100)
		e.Register("b", Request{})
		require.NoError(t, e.Start(t.Context(), "b", Spec{
			Pool: scale.PoolKey{Template: "img"}, Template: "t",
			Steps:     []Step{{Type: stepEnv, Args: []string{"A", "1"}}, {Type: stepRun, Args: []string{"make"}}},
			RelayEnvs: map[string]string{"http_proxy": "p"},
		}))
		require.Equal(t, StatusReady, waitDone(t, e, "b").Status)
		calls := store.calls()
		assert.Contains(t, calls, "run user  "+want+" make", route)
		assert.Contains(t, calls, "init user  "+want, route)
	}
}

func TestAStepThatExitsNonZeroFailsTheBuildAtItsIndex(t *testing.T) {
	store := &fakeStore{exits: map[string][]int{"false": {2}}}
	e := New(store, store, 1, time.Minute, 100)
	e.Register("b", Request{})
	require.NoError(t, e.Start(t.Context(), "b", Spec{
		Namespace: "ns", ClaimName: "c", Pool: scale.PoolKey{Template: "img"}, Template: "e2b/ns/app",
		Steps: []Step{{Type: stepEnv, Args: []string{"A", "1"}}, {Type: stepRun, Args: []string{"true"}}, {Type: stepRun, Args: []string{"false"}}, {Type: stepRun, Args: []string{"never"}}},
	}))
	info := waitDone(t, e, "b")
	assert.Equal(t, [3]string{StatusError, "3", `step 3 (RUN) failed: "false" exited with code 2`}, [3]string{info.Status, info.FailedPhase, info.Failure})
	tail := info.Logs[len(info.Logs)-2:]
	assert.Equal(t, [4]string{"3", "out of false", "3", "error"}, [4]string{tail[0].Phase, tail[0].Message, tail[1].Phase, tail[1].Level})
	calls := store.calls()
	assert.Equal(t, "release node-a sb_1", calls[len(calls)-1])
	assert.NotContains(t, strings.Join(calls, "\n"), "never")
	assert.NotContains(t, strings.Join(calls, "\n"), "promote")
}

func TestAReadyCommandThatNeverPassesFailsAtFinalize(t *testing.T) {
	store := &fakeStore{exits: map[string][]int{"check": {1}}}
	e := New(store, store, 1, 50*time.Millisecond, 100)
	e.Register("b", Request{})
	require.NoError(t, e.Start(t.Context(), "b", Spec{Pool: scale.PoolKey{Template: "img"}, Template: "t", StartCmd: "serve", ReadyCmd: "check"}))
	info := waitDone(t, e, "b")
	assert.Equal(t, [3]string{StatusError, PhaseFinalize, `the ready command "check" did not exit 0 before the build timed out`}, [3]string{info.Status, info.FailedPhase, info.Failure})
	assert.NotContains(t, strings.Join(store.calls(), "\n"), "promote")
}

func TestACopyStepWritesItsUploadAndMovesItAsRoot(t *testing.T) {
	hash := strings.Repeat("ab", 32)
	store := &fakeStore{}
	e := New(store, store, 1, time.Minute, 100)
	e.Register("b", Request{})
	require.NoError(t, e.Start(t.Context(), "b", Spec{
		Pool: scale.PoolKey{Template: "img"}, Template: "t",
		Steps: []Step{{Type: stepUser, Args: []string{"bob"}}, {Type: stepCopy, Args: []string{"app/*.py", "/srv/", "", "0640"}, FilesHash: hash}},
		Archive: func(_ context.Context, h string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("tar of " + h)), nil
		},
	}))
	info := waitDone(t, e, "b")
	require.Equal(t, StatusReady, info.Status, info.Failure)
	calls := store.calls()
	assert.Equal(t, "write /tmp/"+hash+".tar tar of "+hash, calls[2])
	script := calls[3]
	assert.True(t, strings.HasPrefix(script, "run root  map[] archive='/tmp/"+hash+".tar'"), script)
	for _, want := range []string{"source='/tmp/" + hash + "/unpack/app'", "target='/srv/'", "owner='bob:bob'", "mode='0640'", "user='bob'", `tar -xf "$archive" -C "$unpack"`} {
		assert.Contains(t, script, want)
	}
}

func TestInvalidNamesTheFirstStepABuildCannotRun(t *testing.T) {
	hash := strings.Repeat("0", 64)
	copyStep := Step{Type: stepCopy, Args: []string{"a", "/a"}, FilesHash: hash}
	for _, tc := range []struct {
		steps   []Step
		canCopy bool
		want    string
	}{
		{nil, false, ""},
		{[]Step{copyStep}, true, ""},
		{[]Step{copyStep}, false, "step 1 (COPY): no upload store is configured"},
		{[]Step{{Type: stepRun, Args: []string{"true"}}, {Type: stepCopy, Args: []string{"a"}, FilesHash: hash}}, true, "step 2 (COPY): needs a source and a destination"},
		{[]Step{{Type: stepCopy, Args: []string{"a", "/a"}, FilesHash: "../x"}}, true, "step 1 (COPY): needs the filesHash of its upload"},
		{[]Step{{Type: stepEnv, Args: []string{"A"}}}, true, "step 1 (ENV): needs key and value pairs"},
		{[]Step{{Type: "ARG", Args: []string{"A", "1"}}}, true, "step 1 (ARG): not a supported step type"},
	} {
		assert.Equal(t, tc.want, Invalid(tc.steps, tc.canCopy))
	}
}

func TestGlobBaseStopsAtTheFirstGlobSegment(t *testing.T) {
	for src, want := range map[string]string{"app": "app", "app/": "app", "app/*.py": "app", "*.py": "", "a/b/**/c": "a/b", "a/[ab]": "a"} {
		assert.Equal(t, want, globBase(src), src)
	}
}

func TestStartIsBoundedIdempotentAndKnowsItsBuilds(t *testing.T) {
	store := &fakeStore{hold: make(chan struct{})}
	e := New(store, store, 1, time.Minute, 100)
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
	store := &fakeStore{}
	e := New(store, store, 1, time.Minute, 1)
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
	exits      map[string][]int
	route      string

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
	return scale.Assignment{SandboxName: "sb_1", Node: "node-a", NetRoute: f.route}, nil
}

func (f *fakeStore) Promote(_ context.Context, node, id, template string) (scale.PoolKey, string, error) {
	f.record("promote " + node + " " + id + " " + template)
	return scale.PoolKey{Template: template}, "sha256:" + id, f.promoteErr
}

func (f *fakeStore) Release(_ context.Context, node, id string) error {
	f.record("release " + node + " " + id)
	return nil
}

func (f *fakeStore) Run(_ context.Context, _ scale.Assignment, cmd Command, out func(string)) (int, error) {
	f.record(fmt.Sprintf("run %s %s %v %s", cmd.User, cmd.Workdir, cmd.Envs, cmd.Line))
	if v, ok := strings.CutPrefix(cmd.Line, `printf "%s" "`); ok {
		out(strings.TrimSuffix(v, `"`))
		return 0, nil
	}
	out("out of " + cmd.Line)
	f.mu.Lock()
	defer f.mu.Unlock()
	codes := f.exits[cmd.Line]
	if len(codes) == 0 {
		return 0, nil
	}
	if len(codes) > 1 {
		f.exits[cmd.Line] = codes[1:]
	}
	return codes[0], nil
}

func (f *fakeStore) Start(_ context.Context, _ scale.Assignment, cmd Command) error {
	f.record("start " + cmd.User + " " + cmd.Workdir + " " + cmd.Line)
	return nil
}

func (f *fakeStore) Write(_ context.Context, _ scale.Assignment, path string, r io.Reader) error {
	b, err := io.ReadAll(r)
	f.record(fmt.Sprintf("write %s %s", path, b))
	return err
}

func (f *fakeStore) Init(_ context.Context, _ scale.Assignment, defaults Command) error {
	f.record(fmt.Sprintf("init %s %s %v", defaults.User, defaults.Workdir, defaults.Envs))
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
