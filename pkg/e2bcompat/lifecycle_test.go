package e2bcompat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
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
	if store.readNode != "node-a" || store.readID != "sb_abc" {
		t.Errorf("Read(%q, %q), want (node-a, sb_abc)", store.readNode, store.readID)
	}
}

func TestConnectRestoresAPausedClaimThenJudgesTheLeaseItsWakeGranted(t *testing.T) {
	for _, tc := range []struct {
		name         string
		wakeDeadline time.Duration
		wantRenew    bool
	}{
		{"hibernated: the lease continues, 20 s left", 0, true},
		{"archived: the wake grants an hour", time.Hour, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &lifecycleStore{nodePaused: true, deadline: time.Now().Add(20 * time.Second)}
			if tc.wakeDeadline != 0 {
				store.wakeDeadline = time.Now().Add(tc.wakeDeadline)
			}
			store.items = []sandboxv1beta1.Sandbox{pausedSandbox("s1", "sb_abc", "node-a", "img")}
			h := newTestServer(t, store)
			if w := do(t, h, http.MethodPost, "/v2/sandboxes/sb-abc/connect", `{"timeout":300}`, testKey); w.Code != http.StatusCreated {
				t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
			}
			if store.resumedID != "sb_abc" || (store.renewedID != "") != tc.wantRenew {
				t.Errorf("resumed %q, renewed %q; want a resume and renew=%v", store.resumedID, store.renewedID, tc.wantRenew)
			}
		})
	}
}

func TestConnectExtendsALeaseShorterThanItsTimeout(t *testing.T) {
	for _, tc := range []struct {
		name     string
		left     time.Duration
		body     string
		wantTTL  int
		wantCall bool
	}{
		{"20 s left, timeout 300", 20 * time.Second, `{"timeout":300}`, 300, true},
		{"20 s left, timeout omitted", 20 * time.Second, `{}`, DefaultTimeoutSeconds, true},
		{"an hour left, timeout 300", time.Hour, `{"timeout":300}`, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &lifecycleStore{deadline: time.Now().Add(tc.left)}
			store.items = []sandboxv1beta1.Sandbox{liveSandbox("s1", "sb_abc", "node-a", "img")}
			h := newTestServer(t, store)
			if w := do(t, h, http.MethodPost, "/v2/sandboxes/sb-abc/connect", tc.body, testKey); w.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
			}
			if called := store.renewedID != ""; called != tc.wantCall || store.renewedTTL != tc.wantTTL || store.renewedExpire != "" {
				t.Errorf("renew called %v for %d s, want %v for %d s", called, store.renewedTTL, tc.wantCall, tc.wantTTL)
			}
		})
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
	if store.forkCount != 2 || store.forkNamespace != "sandboxes" {
		t.Errorf("fork routed count %d in %q, want 2 in the key's namespace", store.forkCount, store.forkNamespace)
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

func TestSnapshotNameMustFitTheNodeBudgetWithItsStamp(t *testing.T) {
	store := &lifecycleStore{}
	store.items = []sandboxv1beta1.Sandbox{liveSandbox("s1", "sb_abc", "node-a", "img")}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/snapshots", `{"name":"`+strings.Repeat("x", 54)+`"}`, testKey)
	if w.Code != http.StatusBadRequest || store.snapshotName != "" {
		t.Fatalf("status = %d, routed %q; want 400 and no snapshot: %s", w.Code, store.snapshotName, w.Body.String())
	}
}

func TestLifecycleVerbsOnUnknownSandboxAre404(t *testing.T) {
	for _, path := range []string{
		"/sandboxes/sb-missing/pause",
		"/sandboxes/sb-missing/connect",
		"/sandboxes/sb-missing/resume",
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
			store := &lifecycleStore{err: gone, nodeErr: gone}
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

func TestPauseAndForkTreatAnArchivedSandboxAsPaused(t *testing.T) {
	for _, tc := range []struct{ path, body string }{
		{"/sandboxes/sb-abc/pause", ``},
		{"/sandboxes/sb-abc/fork", `{"count":1}`},
	} {
		t.Run(tc.path, func(t *testing.T) {
			store := &lifecycleStore{nodeArchived: true}
			store.items = []sandboxv1beta1.Sandbox{liveSandbox("s1", "sb_abc", "node-a", "img")}
			h := newTestServer(t, store)

			if w := do(t, h, http.MethodPost, tc.path, tc.body, testKey); w.Code != http.StatusConflict {
				t.Fatalf("status = %d, want 409: the node holds the claim archived, which is paused: %s", w.Code, w.Body.String())
			}
			if store.pausedID != "" || store.forkedID != "" {
				t.Errorf("the verb reached the node (paused %q, forked %q) for an archived sandbox", store.pausedID, store.forkedID)
			}
		})
	}
}

func TestResumeRestoresAPausedSandboxWith201(t *testing.T) {
	store := &lifecycleStore{token: "sandbox-secret"}
	nodeReportsPaused(store)
	store.items = []sandboxv1beta1.Sandbox{pausedSandbox("s1", "sb_abc", "node-a", "img")}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/resume", `{"timeout":30}`, testKey)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if store.resumedNode != "node-a" || store.resumedID != "sb_abc" {
		t.Errorf("Resume(%q, %q), want (node-a, sb_abc)", store.resumedNode, store.resumedID)
	}
	var got Sandbox
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.SandboxID != "sb-abc" || got.EnvdAccessToken != "sandbox-secret" {
		t.Errorf("sandbox = %+v, want sb-abc with the token the owning node holds", got)
	}
}

func TestResumeOfARunningSandboxIs409(t *testing.T) {
	store := &lifecycleStore{}
	store.items = []sandboxv1beta1.Sandbox{liveSandbox("s1", "sb_abc", "node-a", "img")}
	h := newTestServer(t, store)

	if w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/resume", ``, testKey); w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409: %s", w.Code, w.Body.String())
	}
	if store.resumedID != "" || store.renewedID != "" {
		t.Errorf("a running sandbox was resumed %q or renewed %q", store.resumedID, store.renewedID)
	}
}

func TestResumeRefusesWhatConnectCannotGive(t *testing.T) {
	for _, body := range []string{`{"memory":false}`} {
		t.Run(body, func(t *testing.T) {
			store := &lifecycleStore{}
			nodeReportsPaused(store)
			store.items = []sandboxv1beta1.Sandbox{pausedSandbox("s1", "sb_abc", "node-a", "img")}
			h := newTestServer(t, store)

			if w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/resume", body, testKey); w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
			}
			if store.readID != "" || store.resumedID != "" {
				t.Errorf("a refused resume reached the node (read %q, resumed %q)", store.readID, store.resumedID)
			}
		})
	}
}

func TestResumeAutoPauseSetsTheLeaseEndActionWithoutShorteningTheLease(t *testing.T) {
	for _, tc := range []struct {
		body       string
		wantExpire sandboxd.ExpireAction
	}{
		{`{"timeout":30,"autoPause":true}`, sandboxd.ExpireArchive},
		{`{"timeout":30,"autoPause":false}`, sandboxd.ExpireDestroy},
	} {
		t.Run(tc.body, func(t *testing.T) {
			store := &lifecycleStore{wakeDeadline: time.Now().Add(time.Hour)}
			nodeReportsPaused(store)
			store.items = []sandboxv1beta1.Sandbox{pausedSandbox("s1", "sb_abc", "node-a", "img")}
			h := newTestServer(t, store)
			if w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/resume", tc.body, testKey); w.Code != http.StatusCreated {
				t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
			}
			if store.renewedExpire != tc.wantExpire || store.renewedTTL < 3599 {
				t.Errorf("renew(%d s, %q), want the hour left and %q", store.renewedTTL, store.renewedExpire, tc.wantExpire)
			}
		})
	}
}

func TestMetricsReportEnvdsSampleAndNothingForAPausedSandbox(t *testing.T) {
	store := &lifecycleStore{metrics: map[string]scale.SandboxMetrics{"sb_run": {Timestamp: 1790517471, CPUCount: 1, CPUUsedPct: 97.5, MemTotal: 490504192, MemUsed: 209805312, MemCache: 125820928, DiskUsed: 27705344, DiskTotal: 10464022528}}}
	store.items = []sandboxv1beta1.Sandbox{liveSandbox("s1", "sb_run", "node-a", "img"), pausedSandbox("s2", "sb_paused", "node-a", "img")}
	h := newTestServer(t, store)

	var got []SandboxMetric
	w := do(t, h, http.MethodGet, "/sandboxes/sb-run/metrics", ``, testKey)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, []SandboxMetric{{
		Timestamp: "2026-09-27T13:57:51Z", TimestampUnix: 1790517471, CPUCount: 1, CPUUsedPct: 97.5,
		MemUsed: 209805312, MemTotal: 490504192, MemCache: 125820928, DiskUsed: 27705344, DiskTotal: 10464022528,
	}}, got)

	w = do(t, h, http.MethodGet, "/sandboxes/sb-paused/metrics", ``, testKey)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.JSONEq(t, `[]`, w.Body.String())
}

func TestBatchMetricsKeepOnlyTheRunningSandboxesItCanRead(t *testing.T) {
	store := &lifecycleStore{
		metrics:    map[string]scale.SandboxMetrics{"sb_a": {CPUCount: 1, CPUUsedPct: 10}, "sb_b": {CPUCount: 1, CPUUsedPct: 20}},
		metricsErr: map[string]error{"sb_bad": errors.New("relay reset")},
	}
	store.items = []sandboxv1beta1.Sandbox{
		liveSandbox("a", "sb_a", "node-a", "img"), liveSandbox("b", "sb_b", "node-b", "img"),
		pausedSandbox("p", "sb_paused", "node-a", "img"), liveSandbox("x", "sb_bad", "node-a", "img"),
	}
	h := newTestServer(t, store)

	var got SandboxesWithMetrics
	w := do(t, h, http.MethodGet, "/sandboxes/metrics?sandbox_ids=sb-a,sb-b,sb-paused,sb-bad,sb-unknown", ``, testKey)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, []string{"sb-a", "sb-b"}, slices.Sorted(maps.Keys(got.Sandboxes)))
	assert.InDelta(t, 20, got.Sandboxes["sb-b"].CPUUsedPct, 0.001)

	many := make([]string, maxMetricsIDs+1)
	for i := range many {
		many[i] = fmt.Sprintf("sb-%d", i)
	}
	for _, q := range []string{"", "sandbox_ids=", "sandbox_ids=sb-a,,sb-b", "sandbox_ids=sb-a,sb-a", "sandbox_ids=" + strings.Join(many, ",")} {
		if w := do(t, h, http.MethodGet, "/sandboxes/metrics?"+q, ``, testKey); w.Code != http.StatusBadRequest {
			t.Errorf("?%.40s: status %d, want 400: %s", q, w.Code, w.Body.String())
		}
	}
}

func TestLogsAnswerAnEmptyPageForAKnownSandbox(t *testing.T) {
	for path, want := range map[string]string{
		"/sandboxes/sb-abc/logs":    `{"logs":[],"logEntries":[]}`,
		"/v2/sandboxes/sb-abc/logs": `{"logs":[]}`,
	} {
		t.Run(path, func(t *testing.T) {
			store := &lifecycleStore{}
			store.items = []sandboxv1beta1.Sandbox{liveSandbox("s1", "sb_abc", "node-a", "img")}
			h := newTestServer(t, store)

			w := do(t, h, http.MethodGet, path, "", testKey)
			if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != want {
				t.Fatalf("got %d %s, want 200 %s", w.Code, w.Body.String(), want)
			}
			if w := do(t, h, http.MethodGet, strings.Replace(path, "sb-abc", "sb-missing", 1), "", testKey); w.Code != http.StatusNotFound {
				t.Fatalf("unknown sandbox status = %d, want 404: %s", w.Code, w.Body.String())
			}
		})
	}
}

type lifecycleStore struct {
	fakeStore

	pausedNode, pausedID   string
	resumedNode, resumedID string
	forkNamespace          string
	forkedID               string
	forkCount              int
	forkChildren           []scale.Assignment
	snapshotName           string
	snapshot               scale.Snapshot
	renewedID              string
	renewedTTL             int
	renewedExpire          sandboxd.ExpireAction
	renewDeadline          time.Time
	token                  string
	deadline, wakeDeadline time.Time
	readNode, readID       string
	err                    error
	nodeErr                error
	metrics                map[string]scale.SandboxMetrics
	metricsErr             map[string]error

	nodePaused   bool
	nodeArchived bool
}

func (f *lifecycleStore) Read(_ context.Context, node, id string) (scale.SandboxRecord, error) {
	f.readNode, f.readID = node, id
	return scale.SandboxRecord{Token: f.token, Paused: f.nodePaused || f.nodeArchived, Deadline: f.deadline}, f.nodeErr
}

func (f *lifecycleStore) Metrics(_ context.Context, _, id string) (scale.SandboxMetrics, bool, error) {
	if f.nodeErr != nil {
		return scale.SandboxMetrics{}, false, f.nodeErr
	}
	m, live := f.metrics[id]
	return m, live, f.metricsErr[id]
}

func (f *lifecycleStore) Pause(_ context.Context, node, id string) error {
	f.pausedNode, f.pausedID = node, id
	return f.err
}

func (f *lifecycleStore) Resume(_ context.Context, node, id string) error {
	f.resumedNode, f.resumedID = node, id
	f.nodePaused = false
	if !f.wakeDeadline.IsZero() {
		f.deadline = f.wakeDeadline
	}
	return f.err
}

func (f *lifecycleStore) Renew(_ context.Context, _, id string, ttlSeconds int, onExpire sandboxd.ExpireAction) (time.Time, error) {
	f.renewedID, f.renewedTTL, f.renewedExpire = id, ttlSeconds, onExpire
	return f.renewDeadline, f.err
}

func (f *lifecycleStore) Fork(_ context.Context, namespace, _, id string, count, _ int) ([]scale.Assignment, error) {
	f.forkNamespace, f.forkedID, f.forkCount = namespace, id, count
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
