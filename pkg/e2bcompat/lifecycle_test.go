package e2bcompat

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

func TestPauseAlreadyPausedIs409(t *testing.T) {
	store := &lifecycleStore{}
	nodeReportsPaused(store)
	store.items = []sandboxv1beta1.Sandbox{pausedSandbox("s1", "sb_abc", "node-a", "img")}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/pause", ``, testKey)
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 (the SDK reads it as already-paused): %s", w.Code, w.Body.String())
	}
	if store.pausedID != "" {
		t.Errorf("Pause was routed to the node for an already-paused sandbox (id %q)", store.pausedID)
	}
}

func TestPauseRoutesToOwningNode(t *testing.T) {
	store := &lifecycleStore{}
	store.items = []sandboxv1beta1.Sandbox{liveSandbox("s1", "sb_abc", "node-a", "img")}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/pause", ``, testKey)
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %s", w.Code, w.Body.String())
	}
	if store.pausedNode != "node-a" || store.pausedID != "sb_abc" {
		t.Errorf("Pause(%q, %q), want (node-a, sb_abc) — the raw claim id, not the published one",
			store.pausedNode, store.pausedID)
	}
}

func TestPauseFilesystemOnlyIsRejected(t *testing.T) {
	store := &lifecycleStore{}
	store.items = []sandboxv1beta1.Sandbox{liveSandbox("s1", "sb_abc", "node-a", "img")}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/pause", `{"memory":false}`, testKey)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for an unsupported pause mode", w.Code)
	}
	if store.pausedID != "" {
		t.Error("the sandbox was paused anyway; an unsupported mode must not fall through")
	}
}

func TestPauseFilesystemOnlyIsRejectedBeforeThePausedCheck(t *testing.T) {
	store := &lifecycleStore{}
	nodeReportsPaused(store)
	store.items = []sandboxv1beta1.Sandbox{pausedSandbox("s1", "sb_abc", "node-a", "img")}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/pause", `{"memory":false}`, testKey)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 naming the unsupported option, not 409 for the paused state", w.Code)
	}
}

func TestForkKeepsTheNodesRejectionStatus(t *testing.T) {
	store := &lifecycleStore{err: k8serrors.NewBadRequest("count 9999 exceeds max_fork_count")}
	store.items = []sandboxv1beta1.Sandbox{liveSandbox("s1", "sb_abc", "node-a", "img")}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/fork", `{"count":9999}`, testKey)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want the node's 400, not a retryable 500: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "max_fork_count") {
		t.Errorf("body = %s, want the node's reason", w.Body.String())
	}
}

func TestConnectRunningIs200(t *testing.T) {
	store := &lifecycleStore{}
	store.items = []sandboxv1beta1.Sandbox{liveSandbox("s1", "sb_abc", "node-a", "img")}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/connect", `{"timeout":30}`, testKey)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 for a running sandbox: %s", w.Code, w.Body.String())
	}
	if store.resumedID != "" {
		t.Errorf("a running sandbox was resumed (id %q); connect must be a no-op then", store.resumedID)
	}
	var got Sandbox
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.SandboxID != "sb-abc" {
		t.Errorf("sandboxID = %q, want the DNS-safe published id", got.SandboxID)
	}
}

func TestConnectPausedIs201AndResumes(t *testing.T) {
	store := &lifecycleStore{}
	nodeReportsPaused(store)
	store.items = []sandboxv1beta1.Sandbox{pausedSandbox("s1", "sb_abc", "node-a", "img")}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/connect", `{"timeout":30}`, testKey)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 when a paused sandbox is resumed: %s", w.Code, w.Body.String())
	}
	if store.resumedNode != "node-a" || store.resumedID != "sb_abc" {
		t.Errorf("Resume(%q, %q), want (node-a, sb_abc)", store.resumedNode, store.resumedID)
	}
}

func TestConnectReturnsTheSandboxAccessToken(t *testing.T) {
	store := &lifecycleStore{token: "sandbox-secret"}
	store.items = []sandboxv1beta1.Sandbox{liveSandbox("s1", "sb_abc", "node-a", "img")}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/connect", `{"timeout":30}`, testKey)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var got Sandbox
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.EnvdAccessToken != "sandbox-secret" {
		t.Errorf("envdAccessToken = %q, want the token the owning node holds", got.EnvdAccessToken)
	}
	if store.tokenNode != "node-a" || store.tokenID != "sb_abc" {
		t.Errorf("AccessToken(%q, %q), want (node-a, sb_abc)", store.tokenNode, store.tokenID)
	}
}

func TestForkPausedIs409(t *testing.T) {
	store := &lifecycleStore{}
	nodeReportsPaused(store)
	store.items = []sandboxv1beta1.Sandbox{pausedSandbox("s1", "sb_abc", "node-a", "img")}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/fork", `{"count":2}`, testKey)
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 for forking a paused sandbox", w.Code)
	}
	if store.forkedID != "" {
		t.Error("fork was routed to the node for a paused source")
	}
}

func TestForkReturnsPerChildResults(t *testing.T) {
	store := &lifecycleStore{
		forkChildren: []scale.Assignment{
			{SandboxName: "sb_c1", Node: "node-a", Token: "t1"},
			{SandboxName: "sb_c2", Node: "node-a", Token: "t2"},
		},
	}
	store.items = []sandboxv1beta1.Sandbox{liveSandbox("s1", "sb_abc", "node-a", "img")}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/fork", `{"count":2}`, testKey)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	var got []SandboxForkResult
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d results, want one per child", len(got))
	}
	if store.forkCount != 2 {
		t.Errorf("fork count routed = %d, want 2", store.forkCount)
	}
	for i, want := range []string{"sb-c1", "sb-c2"} {
		if got[i].Sandbox == nil || got[i].Sandbox.SandboxID != want {
			t.Errorf("child %d = %+v, want published id %q", i, got[i].Sandbox, want)
		}
	}
}

func TestForkDefaultsToOneChild(t *testing.T) {
	store := &lifecycleStore{forkChildren: []scale.Assignment{{SandboxName: "sb_c1", Node: "node-a"}}}
	store.items = []sandboxv1beta1.Sandbox{liveSandbox("s1", "sb_abc", "node-a", "img")}
	h := newTestServer(t, store)

	if w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/fork", ``, testKey); w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 for a body-less fork: %s", w.Code, w.Body.String())
	}
	if store.forkCount != 1 {
		t.Errorf("fork count = %d, want the schema default of 1", store.forkCount)
	}
}

func TestSnapshotReturns201WithID(t *testing.T) {
	store := &lifecycleStore{snapshot: scale.Snapshot{ID: "ck_1234", Name: "before-migration", Node: "node-a"}}
	store.items = []sandboxv1beta1.Sandbox{liveSandbox("s1", "sb_abc", "node-a", "img")}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/snapshots", `{"name":"before-migration"}`, testKey)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	var got SnapshotInfo
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.SnapshotID != "ck_1234" {
		t.Errorf("snapshotID = %q, want the checkpoint id", got.SnapshotID)
	}
	if len(got.Names) != 1 || got.Names[0] != "before-migration" {
		t.Errorf("names = %v, want the requested label echoed", got.Names)
	}
	if store.snapshotName != "sandboxes/before-migration" {
		t.Errorf("name routed = %q, want it stamped with the key's namespace", store.snapshotName)
	}
}

func TestLifecycleVerbsOnUnknownSandboxAre404(t *testing.T) {
	for _, path := range []string{
		"/sandboxes/sb-missing/pause",
		"/sandboxes/sb-missing/connect",
		"/sandboxes/sb-missing/fork",
		"/sandboxes/sb-missing/snapshots",
	} {
		t.Run(path, func(t *testing.T) {
			h := newTestServer(t, &lifecycleStore{})
			if w := do(t, h, http.MethodPost, path, `{}`, testKey); w.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404: %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestPauseTrustsTheNodeNotTheStaleView(t *testing.T) {
	store := &lifecycleStore{}
	store.nodePaused = true
	sb := liveSandbox("s1", "sb_abc", "node-a", "img")
	sb.Labels = map[string]string{scale.PhaseLabel: "Running"}
	store.items = []sandboxv1beta1.Sandbox{sb}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/pause", ``, testKey)
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409: a stale Running label must not mask the node's hibernated state", w.Code)
	}
	if store.pausedID != "" {
		t.Error("pause was routed to the node for an already-hibernated sandbox")
	}
}

func TestConnectTrustsTheNodeNotTheStaleView(t *testing.T) {
	store := &lifecycleStore{}
	store.nodePaused = true
	sb := liveSandbox("s1", "sb_abc", "node-a", "img")
	sb.Labels = map[string]string{scale.PhaseLabel: "Running"}
	store.items = []sandboxv1beta1.Sandbox{sb}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/connect", `{"timeout":30}`, testKey)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: the node says paused, so connect must actually resume", w.Code)
	}
	if store.resumedID != "sb_abc" {
		t.Errorf("resumed %q, want sb_abc — a stale label must not skip the restore", store.resumedID)
	}
}

func TestLifecycleVerbsOnAReapedSandboxAre404(t *testing.T) {
	gone := k8serrors.NewNotFound(sandboxv1beta1.Resource("sandboxes"), "sb_abc")
	for _, tc := range []struct{ method, path, body string }{
		{http.MethodPost, "/sandboxes/sb-abc/pause", ``},
		{http.MethodPost, "/sandboxes/sb-abc/connect", `{"timeout":30}`},
		{http.MethodPost, "/sandboxes/sb-abc/fork", `{"count":1}`},
		{http.MethodPost, "/sandboxes/sb-abc/snapshots", `{}`},
		{http.MethodGet, "/sandboxes/sb-abc/metrics", ``},
	} {
		t.Run(tc.path, func(t *testing.T) {
			store := &lifecycleStore{err: gone, statsErr: gone}
			sb := liveSandbox("s1", "sb_abc", "node-a", "img")
			sb.Labels[scale.PhaseLabel] = "Running"
			store.items = []sandboxv1beta1.Sandbox{sb}
			h := newTestServer(t, store)

			w := do(t, h, tc.method, tc.path, tc.body, testKey)
			if w.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404: the node no longer holds the sandbox, the stale read view must not answer for it: %s", w.Code, w.Body.String())
			}
			if store.resumedID != "" {
				t.Errorf("connect resumed %q on a node that reported the sandbox gone", store.resumedID)
			}
		})
	}
}

type lifecycleStore struct {
	fakeStore

	pausedNode, pausedID   string
	resumedNode, resumedID string
	forkedID               string
	forkCount              int
	forkChildren           []scale.Assignment
	snapshotName           string
	snapshot               scale.Snapshot
	renewedID              string
	renewedTTL             int
	renewDeadline          time.Time
	token                  string
	tokenNode, tokenID     string
	err                    error
	statsErr               error

	nodePaused bool
}

func (f *lifecycleStore) AccessToken(_ context.Context, node, id string) (string, error) {
	f.tokenNode, f.tokenID = node, id
	return f.token, nil
}

func (f *lifecycleStore) Stats(context.Context, string, string) (scale.SandboxStats, error) {
	return scale.SandboxStats{Paused: f.nodePaused, CPUCount: 1, MemTotalBytes: 512 << 20}, f.statsErr
}

func (f *lifecycleStore) Pause(_ context.Context, node, id string) error {
	f.pausedNode, f.pausedID = node, id
	return f.err
}

func (f *lifecycleStore) Resume(_ context.Context, node, id string) error {
	f.resumedNode, f.resumedID = node, id
	return f.err
}

func (f *lifecycleStore) Renew(_ context.Context, _, id string, ttlSeconds int) (time.Time, error) {
	f.renewedID, f.renewedTTL = id, ttlSeconds
	return f.renewDeadline, f.err
}

func (f *lifecycleStore) Fork(_ context.Context, _, id string, count, _ int) ([]scale.Assignment, error) {
	f.forkedID, f.forkCount = id, count
	return f.forkChildren, f.err
}

func (f *lifecycleStore) Snapshot(_ context.Context, _, _, name string) (scale.Snapshot, error) {
	f.snapshotName = name
	return f.snapshot, f.err
}

func pausedSandbox(name, claimID, node, template string) sandboxv1beta1.Sandbox {
	sb := liveSandbox(name, claimID, node, template)
	sb.Labels[scale.PhaseLabel] = scale.PhaseHibernated
	return sb
}

func nodeReportsPaused(s *lifecycleStore) { s.nodePaused = true }
