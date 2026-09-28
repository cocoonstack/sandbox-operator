package e2bcompat

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/cocoonstack/sandbox-operator/pkg/e2bbuild"
	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

func TestABuildFromAnImagePromotesReplacesTheOldHolderAndTags(t *testing.T) {
	store := &fakeStore{assign: scale.Assignment{SandboxName: "sb_1", Node: "n"}}
	h := newTestServer(t, store, withAliases("base reg/rt:24.04"), withBuilds(), withBuildFleet(store))

	w := do(t, h, http.MethodPost, "/v3/templates", `{"name":"app:v1","cpuCount":2}`, testKey)
	require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
	var reqd TemplateRequestResponseV3
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &reqd))
	assert.Equal(t, [3]any{"app", []string{"app", "app:v1"}, []string{"v1"}}, [3]any{reqd.TemplateID, reqd.Names, reqd.Tags})

	w = do(t, h, http.MethodPost, "/v2/templates/app/builds/"+reqd.BuildID, `{"fromImage":"base","steps":[],"force":false}`, testKey)
	require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
	info := waitBuild(t, h, reqd.BuildID)
	require.Equal(t, e2bbuild.StatusReady, info.Status, info)
	assert.Equal(t, [2]string{"reg/rt:24.04", scale.SizeClassMedium}, [2]string{store.claimPool.Template, store.claimPool.Size}, "the alias names the image; cpuCount 2 picks the size")
	assert.Equal(t, []string{"n sb_1 e2b/sandboxes/app"}, store.promoted)
	assert.Equal(t, []string{"m e2b/sandboxes/app small sha256:old"}, store.deletedTemplates, "the previous build on another node goes")
	assert.Equal(t, map[string]string{"v1": "sha256:sb_1"}, fleetLabels(t, store, "n", "e2b/sandboxes/app"))
	assert.Equal(t, "n sb_1", store.releasedNode+" "+store.releasedID)
	assert.Len(t, info.LogEntries, 3)

	w = do(t, h, http.MethodGet, "/templates/app/builds/"+reqd.BuildID+"/status?logsOffset=2", "", testKey)
	var tail TemplateBuildInfo
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &tail))
	assert.Len(t, tail.LogEntries, 1)
	assert.Equal(t, http.StatusBadRequest, do(t, h, http.MethodGet, "/templates/app/builds/"+reqd.BuildID+"/status?logsOffset=-1", "", testKey).Code)
	assert.Equal(t, http.StatusNotFound, do(t, h, http.MethodGet, "/templates/app/builds/nope/status", "", testKey).Code)
	assert.Equal(t, http.StatusAccepted, do(t, h, http.MethodPost, "/v2/templates/app/builds/"+reqd.BuildID, `{"fromImage":"base"}`, testKey).Code, "a retried start is a no-op")
}

func TestABuildReplacesAPreviousBuildTheInventoryHasNotPublishedYet(t *testing.T) {
	old := scale.PromotedTemplate{Template: "e2b/sandboxes/app", Net: "none", Size: "small", ContentDigest: "sha256:old"}
	store := &fakeStore{assign: scale.Assignment{SandboxName: "sb_1", Node: "n"}, live: map[string][]scale.PromotedTemplate{"n": nil, "m": {old}}}
	inv := scale.NewStaticInventorySource()
	inv.Put(&scale.NodeInventory{Node: "n"})
	inv.Put(&scale.NodeInventory{Node: "m"})
	h := newTestServer(t, store, withBuilds(), func(o *Options) { o.Inventory = inv })

	id := requestBuild(t, h, "app")
	require.Equal(t, http.StatusAccepted, do(t, h, http.MethodPost, "/v2/templates/app/builds/"+id, `{"fromImage":"img"}`, testKey).Code)
	require.Equal(t, e2bbuild.StatusReady, waitBuild(t, h, id).Status)
	assert.Equal(t, []string{"m e2b/sandboxes/app small sha256:old"}, store.deletedTemplates, "node m's build finished after its last publish and still goes")

	delete(store.live, "m")
	id = requestBuild(t, h, "app")
	require.Equal(t, http.StatusAccepted, do(t, h, http.MethodPost, "/v2/templates/app/builds/"+id, `{"fromImage":"img"}`, testKey).Code)
	info := waitBuild(t, h, id)
	require.Equal(t, e2bbuild.StatusError, info.Status, "a node that does not answer could still hold the previous build")
	assert.Equal(t, "finalize", info.Reason.Step)
}

func TestABuildThatCannotClaimFailsAtTheBaseStep(t *testing.T) {
	store := &fakeStore{claimErr: scale.ErrNoWarmCapacity}
	h := newTestServer(t, store, withBuilds())
	id := requestBuild(t, h, "app")
	require.Equal(t, http.StatusAccepted, do(t, h, http.MethodPost, "/v2/templates/app/builds/"+id, `{"fromImage":"reg/py:3.12"}`, testKey).Code)
	info := waitBuild(t, h, id)
	require.Equal(t, e2bbuild.StatusError, info.Status)
	require.NotNil(t, info.Reason)
	assert.Equal(t, "base", info.Reason.Step)
	assert.Equal(t, "no warm sandbox of reg/py:3.12 (none, small); a pool must serve the build's image and size", info.Reason.Message)
	assert.Equal(t, scale.SizeClassSmall, store.claimPool.Size, "no cpuCount or memoryMB keeps the image's small")
}

func TestABuildRunsItsStepsThroughEnvdAndLeavesItsDefaultsInTheSandbox(t *testing.T) {
	store := &fakeStore{assign: scale.Assignment{SandboxName: "sb_1", Node: "n", Token: "claim-tok"}, processExits: map[string]int{"false": 1}}
	h := newTestServer(t, store, withBuilds(), withBuildFleet(store))

	id := requestBuild(t, h, "app")
	body := `{"fromImage":"img","steps":[{"type":"ENV","args":["A","1"]},{"type":"RUN","args":["make"]}],"startCmd":"serve","readyCmd":"check"}`
	require.Equal(t, http.StatusAccepted, do(t, h, http.MethodPost, "/v2/templates/app/builds/"+id, body, testKey).Code)
	info := waitBuild(t, h, id)
	require.Equal(t, e2bbuild.StatusReady, info.Status, info)
	require.Len(t, store.envdCalls, 5)
	init, ok := strings.CutPrefix(store.envdCalls[2], "sb_1 POST /init ")
	require.True(t, ok, store.envdCalls[2])
	assert.Equal(t, []string{`sb_1 RUN root  printf "%s" "1"`, "sb_1 RUN user  make"}, store.envdCalls[:2])
	assert.Equal(t, []string{"sb_1 RUN user  serve", "sb_1 RUN user  check"}, store.envdCalls[3:])
	assert.NotContains(t, init, "accessToken", "the build leaves envd without a token, as a warm sandbox is")
	var defaults envdInit
	require.NoError(t, json.Unmarshal([]byte(init), &defaults))
	assert.Equal(t, [2]any{"user", map[string]string{"A": "1"}}, [2]any{defaults.DefaultUser, defaults.EnvVars})
	assert.Contains(t, info.LogEntries, BuildLogEntry{Timestamp: info.LogEntries[2].Timestamp, Message: "ok", Level: "info", Step: "2"})
	assert.Equal(t, []string{"n sb_1 e2b/sandboxes/app"}, store.promoted)
	assert.Equal(t, []string{"claim-tok", "claim-tok", "claim-tok", "claim-tok", "claim-tok"}, store.envdRelays, "every build call rides the claim's own relay, which wakes and keeps the sandbox awake")

	id = requestBuild(t, h, "app")
	body = `{"fromImage":"img","steps":[{"type":"ENV","args":["A","1"]},{"type":"WORKDIR","args":["/w"]},{"type":"RUN","args":["false"]}]}`
	require.Equal(t, http.StatusAccepted, do(t, h, http.MethodPost, "/v2/templates/app/builds/"+id, body, testKey).Code)
	info = waitBuild(t, h, id)
	require.Equal(t, e2bbuild.StatusError, info.Status)
	assert.Equal(t, [2]string{"3", `step 3 (RUN) failed: "false" exited with code 1`}, [2]string{info.Reason.Step, info.Reason.Message})
	assert.Equal(t, "3", info.Reason.LogEntries[0].Step)
	assert.Len(t, store.promoted, 1, "a failed step never promotes")
}

func TestARelayedBuildLeavesTheProxyInTheTemplateDefaults(t *testing.T) {
	store := &fakeStore{assign: scale.Assignment{SandboxName: "sb_1", Node: "n", NetRoute: "relay"}}
	h := newTestServer(t, store, withBuilds(), withBuildFleet(store))
	id := requestBuild(t, h, "app")
	require.Equal(t, http.StatusAccepted, do(t, h, http.MethodPost, "/v2/templates/app/builds/"+id, `{"fromImage":"img","steps":[{"type":"RUN","args":["pip install requests"]}]}`, testKey).Code)
	require.Equal(t, e2bbuild.StatusReady, waitBuild(t, h, id).Status)
	var init envdInit
	for _, c := range store.envdCalls {
		if body, ok := strings.CutPrefix(c, "sb_1 POST /init "); ok {
			require.NoError(t, json.Unmarshal([]byte(body), &init))
		}
	}
	assert.Equal(t, sandboxd.RelayEnv, init.EnvVars)
}

func TestABuildRefusesWhatItCannotHonor(t *testing.T) {
	h := newTestServer(t, &fakeStore{}, withBuilds())
	id := requestBuild(t, h, "app")
	for _, body := range []string{
		`{"fromImage":"img","steps":[{"type":"COPY","args":["a","/a"],"filesHash":"h"}]}`,
		`{"fromImage":"img","steps":[{"type":"ENV","args":["A"]}]}`,
		`{"fromImage":"img","steps":[{"type":"RUN","args":[]}]}`,
		`{"fromImage":"img","steps":[{"type":"ARG","args":["A","1"]}]}`,
		`{"fromTemplate":"other"}`,
		`{"fromImage":"img","fromImageRegistry":{"type":"registry","username":"u","password":"p"}}`,
		`{}`,
	} {
		assert.Equal(t, http.StatusBadRequest, do(t, h, http.MethodPost, "/v2/templates/app/builds/"+id, body, testKey).Code, body)
	}
	assert.Equal(t, http.StatusNotFound, do(t, h, http.MethodPost, "/v2/templates/app/builds/nope", `{"fromImage":"img"}`, testKey).Code)
	assert.Equal(t, http.StatusBadRequest, do(t, h, http.MethodPost, "/v3/templates", `{"name":"`+strings.Repeat("x", 60)+`"}`, testKey).Code)
	assert.Equal(t, http.StatusBadRequest, do(t, h, http.MethodPost, "/v3/templates", `{"name":"my app"}`, testKey).Code, "a name outside sandboxd's grammar is refused before the build runs")
	assert.Equal(t, http.StatusBadRequest, do(t, h, http.MethodPost, "/v3/templates", `{}`, testKey).Code)
	assert.Equal(t, http.StatusNotFound, do(t, newTestServer(t, &fakeStore{}), http.MethodPost, "/v3/templates", `{"name":"app"}`, testKey).Code, "builds are off unless enabled")
}

func TestAPublishLeavesANewerConcurrentBuildAlone(t *testing.T) {
	newer := scale.PromotedTemplate{Template: "e2b/sandboxes/app", Net: "none", Size: "small", ContentDigest: "sha256:newer", CreatedAt: new(metav1.NewTime(time.Now().Add(time.Minute)))}
	older := scale.PromotedTemplate{Template: "e2b/sandboxes/app", Net: "none", Size: "small", ContentDigest: "sha256:older", CreatedAt: new(metav1.NewTime(time.Now().Add(-time.Minute)))}
	store := &fakeStore{assign: scale.Assignment{SandboxName: "sb_1", Node: "n"}, live: map[string][]scale.PromotedTemplate{"n": nil, "m": {newer}, "k": {older}}}
	inv := scale.NewStaticInventorySource()
	for _, n := range []string{"n", "m", "k"} {
		inv.Put(&scale.NodeInventory{Node: n})
	}
	h := newTestServer(t, store, withBuilds(), func(o *Options) { o.Inventory = inv })

	id := requestBuild(t, h, "app")
	require.Equal(t, http.StatusAccepted, do(t, h, http.MethodPost, "/v2/templates/app/builds/"+id, `{"fromImage":"img"}`, testKey).Code)
	require.Equal(t, e2bbuild.StatusReady, waitBuild(t, h, id).Status)
	assert.Equal(t, []string{"k e2b/sandboxes/app small sha256:older"}, store.deletedTemplates, "only the holder older than this promote goes, by the digest observed; a newer concurrent build keeps its node")
}

func requestBuild(t *testing.T, h http.Handler, name string) string {
	t.Helper()
	w := do(t, h, http.MethodPost, "/v3/templates", `{"name":"`+name+`"}`, testKey)
	require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
	var got TemplateRequestResponseV3
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	return got.BuildID
}

func waitBuild(t *testing.T, h http.Handler, buildID string) TemplateBuildInfo {
	t.Helper()
	var info TemplateBuildInfo
	require.Eventually(t, func() bool {
		w := do(t, h, http.MethodGet, "/templates/app/builds/"+buildID+"/status", "", testKey)
		if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &info) != nil {
			return false
		}
		return info.Status == e2bbuild.StatusReady || info.Status == e2bbuild.StatusError
	}, 5*time.Second, 5*time.Millisecond)
	return info
}

func withBuilds() serverOption {
	return func(o *Options) { o.Builds = BuildOptions{Parallel: 2, Timeout: time.Minute, LogLines: 100} }
}

// withBuildFleet serves node n with app's earlier build and node m with a stale copy under a smaller key.
func withBuildFleet(store *fakeStore) serverOption {
	inv := scale.NewStaticInventorySource()
	inv.Put(&scale.NodeInventory{Node: "n", Templates: []scale.PromotedTemplate{{Template: "e2b/sandboxes/app", Net: "none", Size: "medium", ContentDigest: "sha256:old"}}})
	inv.Put(&scale.NodeInventory{Node: "m", Templates: []scale.PromotedTemplate{{Template: "e2b/sandboxes/app", Net: "none", Size: "small", ContentDigest: "sha256:old"}}})
	store.fleet = inv
	return func(o *Options) { o.Inventory = inv }
}
