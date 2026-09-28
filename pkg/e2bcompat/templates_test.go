package e2bcompat

import (
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

func TestTheTemplateListCarriesTheNamespacesBuiltTemplates(t *testing.T) {
	store := &fakeStore{}
	h := newTestServer(t, store, withTemplateFleet(store))

	w := do(t, h, http.MethodGet, "/templates", "", testKey)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var got []Template
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	byID := map[string]Template{}
	for _, tpl := range got {
		byID[tpl.TemplateID] = tpl
	}
	require.Len(t, got, 3, "the pool image plus app and app2, and nothing from another namespace, a tenant or outside e2b/")
	assert.Equal(t, buildReady, byID["reg/rt:24.04"].BuildStatus)
	app := byID["app"]
	assert.Equal(t, [4]any{buildUUID("sha256:aa"), int32(2), int32(1024), "2026-09-28T01:02:03Z"}, [4]any{app.BuildID, app.CPUCount, app.MemoryMB, app.CreatedAt})
	assert.Equal(t, []string{"app"}, app.Names)
	assert.Equal(t, buildReady, app.BuildStatus)
	assert.Equal(t, buildUUID("sha256:cc"), byID["app2"].BuildID, "the newest holder's digest")
	assert.Contains(t, w.Body.String(), `"createdBy":null,"lastSpawnedAt":null`)
	assert.Equal(t, "a2b927ee-9198-5f91-852e-671c845fc253", buildUUID("sha256:aa"), "the UUIDv5 namespace is part of the contract")
}

func TestGetTemplateAnswersOneBuildPerDigest(t *testing.T) {
	store := &fakeStore{}
	h := newTestServer(t, store, withTemplateFleet(store))

	w := do(t, h, http.MethodGet, "/templates/app2", "", testKey)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var got TemplateWithBuilds
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, "app2", got.TemplateID)
	require.Len(t, got.Builds, 2)
	assert.Equal(t, []string{buildUUID("sha256:bb"), buildUUID("sha256:cc")}, []string{got.Builds[0].BuildID, got.Builds[1].BuildID})
	assert.Equal(t, buildReady, got.Builds[0].Status)

	w = do(t, h, http.MethodGet, "/templates/reg%2Frt:24.04", "", testKey)
	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Contains(t, w.Body.String(), "is a pool image, not a built template")
	assert.Equal(t, http.StatusNotFound, do(t, h, http.MethodGet, "/templates/nope", "", testKey).Code)
	assert.Equal(t, http.StatusNotFound, do(t, h, http.MethodGet, "/templates/"+strings.Repeat("x", 60), "", testKey).Code, "a name over the budget is no built template")
	assert.Equal(t, http.StatusOK, do(t, h, http.MethodPatch, "/templates/app", `{"public":false}`, testKey).Code)
	assert.Equal(t, http.StatusNotFound, do(t, h, http.MethodPatch, "/templates/nope", `{}`, testKey).Code)
}

func TestTemplateTagsRoundTripInMemory(t *testing.T) {
	store := &fakeStore{}
	h := newTestServer(t, store, withTemplateFleet(store))

	w := do(t, h, http.MethodPost, "/templates/tags", `{"target":"app","tags":["v1","stable"]}`, testKey)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	assert.JSONEq(t, `{"tags":["v1","stable"],"buildID":"`+buildUUID("sha256:aa")+`"}`, w.Body.String())
	w = do(t, h, http.MethodPost, "/templates/tags", `{"target":"app:v1","tags":["prod"]}`, testKey)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	assert.Equal(t, http.StatusNotFound, do(t, h, http.MethodPost, "/templates/tags", `{"target":"app:nope","tags":["x"]}`, testKey).Code)
	assert.Equal(t, http.StatusNotFound, do(t, h, http.MethodPost, "/templates/tags", `{"target":"nope","tags":["x"]}`, testKey).Code)
	assert.Equal(t, http.StatusBadRequest, do(t, h, http.MethodPost, "/templates/tags", `{"target":"app","tags":[]}`, testKey).Code)

	var tags []TemplateTag
	w = do(t, h, http.MethodGet, "/templates/app/tags", "", testKey)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &tags))
	assert.Equal(t, []string{"prod", "stable", "v1"}, tagNames(tags))
	assert.Equal(t, buildUUID("sha256:aa"), tags[0].BuildID)
	assert.Equal(t, map[string]string{"prod": "sha256:aa", "stable": "sha256:aa", "v1": "sha256:aa"}, fleetLabels(t, store, "node-b", "e2b/sandboxes/app"), "every holder carries the tags")
	var list []Template
	require.NoError(t, json.Unmarshal(do(t, h, http.MethodGet, "/templates", "", testKey).Body.Bytes(), &list))
	i := slices.IndexFunc(list, func(tpl Template) bool { return tpl.TemplateID == "app" })
	assert.Equal(t, []string{"app", "app:prod", "app:stable", "app:v1"}, list[i].Names)

	assert.Equal(t, http.StatusNoContent, do(t, h, http.MethodDelete, "/templates/tags", `{"name":"app","tags":["stable","prod"]}`, testKey).Code)
	require.NoError(t, json.Unmarshal(do(t, h, http.MethodGet, "/templates/app/tags", "", testKey).Body.Bytes(), &tags))
	assert.Equal(t, []string{"v1"}, tagNames(tags))
	assert.Equal(t, http.StatusNotFound, do(t, h, http.MethodGet, "/templates/nope/tags", "", testKey).Code)
	assert.JSONEq(t, `[]`, do(t, h, http.MethodGet, "/templates/app2/tags", "", testKey).Body.String())
}

func TestDeleteTemplateRemovesItOnEveryHolder(t *testing.T) {
	store := &fakeStore{}
	h := newTestServer(t, store, withTemplateFleet(store))
	require.Equal(t, http.StatusCreated, do(t, h, http.MethodPost, "/templates/tags", `{"target":"app","tags":["v1"]}`, testKey).Code)

	assert.Equal(t, http.StatusNoContent, do(t, h, http.MethodDelete, "/templates/app", "", testKey).Code)
	slices.Sort(store.deletedTemplates)
	assert.Equal(t, []string{"node-a e2b/sandboxes/app medium", "node-b e2b/sandboxes/app medium"}, store.deletedTemplates)
	assert.Empty(t, store.deletedSnapshotID, "a built template's delete never falls through to the snapshot path")

	failing := &fakeStore{deleteTemplateErr: map[string]error{"node-b": errors.New("connection refused")}}
	h = newTestServer(t, failing, withTemplateFleet(failing))
	assert.Equal(t, http.StatusInternalServerError, do(t, h, http.MethodDelete, "/templates/app", "", testKey).Code)
	slices.Sort(failing.deletedTemplates)
	assert.Equal(t, []string{"node-a e2b/sandboxes/app medium", "node-b e2b/sandboxes/app medium"}, failing.deletedTemplates)
}

func TestCreateAnswers404OnlyWhenAFailedClaimNamesNothingKnown(t *testing.T) {
	for templateID, want := range map[string]int{
		"nope":         http.StatusNotFound,
		"app":          http.StatusServiceUnavailable,
		"base":         http.StatusServiceUnavailable,
		"reg/rt:24.04": http.StatusServiceUnavailable,
	} {
		store := &fakeStore{claimErr: scale.ErrNoWarmCapacity}
		h := newTestServer(t, store, withTemplateFleet(store), withAliases("base reg/rt:24.04"))
		w := do(t, h, http.MethodPost, "/sandboxes", `{"templateID":"`+templateID+`"}`, testKey)
		assert.Equal(t, want, w.Code, "%s: %s", templateID, w.Body.String())
	}
}

func TestACreateNamingABuiltTemplateClaimsItsKey(t *testing.T) {
	store := &fakeStore{firstClaimErr: scale.ErrNoWarmCapacity, assign: scale.Assignment{SandboxName: "sb_1", Node: "node-a"}}
	h := newTestServer(t, store, withTemplateFleet(store))

	w := do(t, h, http.MethodPost, "/sandboxes", `{"templateID":"app"}`, testKey)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	assert.Equal(t, 2, store.claimCalls, "the built template is tried only after the pool claim finds nothing")
	assert.Equal(t, scale.PoolKey{Template: "e2b/sandboxes/app", Net: "none", Size: "medium"}, store.claimPool)
	var got Sandbox
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, "app", got.TemplateID)

	assert.False(t, store.claimOpts.NoEgress)

	for body, closed := range map[string]bool{`{"templateID":"app","allow_internet_access":true}`: false, `{"templateID":"app","allow_internet_access":false}`: true} {
		store = &fakeStore{firstClaimErr: scale.ErrNoWarmCapacity}
		h = newTestServer(t, store, withTemplateFleet(store))
		w = do(t, h, http.MethodPost, "/sandboxes", body, testKey)
		require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
		assert.Equal(t, [2]any{"none", closed}, [2]any{store.claimPool.Net, store.claimOpts.NoEgress}, "%s: the template's own lane, egress off only when asked", body)
	}

	store = &fakeStore{firstClaimErr: scale.ErrNoWarmCapacity}
	h = newTestServer(t, store, withTemplateFleet(store), withAliases("app reg/rt:24.04"))
	w = do(t, h, http.MethodPost, "/sandboxes", `{"templateID":"app"}`, testKey)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
	assert.Equal(t, 1, store.claimCalls, "a drained alias pool never falls through to a built template of the same name")
}

func TestABuiltTemplateCreateKeepsTheTemplateDefaultsAndLayersTheRequestEnvs(t *testing.T) {
	store := &fakeStore{firstClaimErr: scale.ErrNoWarmCapacity, assign: scale.Assignment{SandboxName: "sb_1", Node: "node-a", Token: "tok"}}
	h := newTestServer(t, store, withTemplateFleet(store))
	w := do(t, h, http.MethodPost, "/sandboxes", `{"templateID":"app"}`, testKey)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	require.Len(t, store.envdCalls, 1, "no envVars asked for means no envd read")
	init, ok := strings.CutPrefix(store.envdCalls[0], "sb_1 POST /init ")
	require.True(t, ok, store.envdCalls[0])
	assert.Contains(t, init, `"accessToken":"`+AccessToken([]byte(testEnvdSecret), "tok")+`"`)
	assert.NotContains(t, init, "envVars", "a nil map keeps the template's environment")
	assert.NotContains(t, init, "default", "envd keeps the template's user and workdir")

	store = &fakeStore{firstClaimErr: scale.ErrNoWarmCapacity, assign: scale.Assignment{SandboxName: "sb_1", Node: "node-a"}, guestEnvs: map[string]string{"A": "1", "B": "old"}}
	h = newTestServer(t, store, withTemplateFleet(store))
	w = do(t, h, http.MethodPost, "/sandboxes", `{"templateID":"app","envVars":{"B":"2"}}`, testKey)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	require.Len(t, store.envdCalls, 2)
	assert.Equal(t, "sb_1 GET /envs ", store.envdCalls[0])
	init, _ = strings.CutPrefix(store.envdCalls[1], "sb_1 POST /init ")
	var got envdInit
	require.NoError(t, json.Unmarshal([]byte(init), &got))
	assert.Equal(t, map[string]string{"A": "1", "B": "2"}, got.EnvVars, "envd replaces its environment, so the request's go on top of the template's")
	assert.Empty(t, got.DefaultUser+got.DefaultWorkdir)
}

func TestTheAliasLookupFindsABuiltTemplateAfterTheTable(t *testing.T) {
	store := &fakeStore{}
	h := newTestServer(t, store, withTemplateFleet(store), withAliases("app reg/rt:24.04"))

	w := do(t, h, http.MethodGet, "/templates/aliases/app", "", testKey)
	assert.JSONEq(t, `{"templateID":"reg/rt:24.04","public":true}`, w.Body.String(), "the alias table wins a clash")
	w = do(t, h, http.MethodGet, "/templates/aliases/app2", "", testKey)
	assert.JSONEq(t, `{"templateID":"app2","public":true}`, w.Body.String())
	assert.Equal(t, http.StatusNotFound, do(t, h, http.MethodGet, "/templates/aliases/other", "", testKey).Code)
}

func TestAFullE2bKeyNeverReachesAnotherNamespacesTemplate(t *testing.T) {
	store := &fakeStore{}
	h := newTestServer(t, store, withBuilds(), withTemplateFleet(store))
	w := do(t, h, http.MethodPost, "/sandboxes", `{"templateID":"e2b/others/app"}`, testKey)
	assert.Equal(t, http.StatusNotFound, w.Code, w.Body.String())
	assert.Equal(t, 0, store.claimCalls, "a full key is refused before any claim")

	id := requestBuild(t, h, "app")
	w = do(t, h, http.MethodPost, "/v2/templates/app/builds/"+id, `{"fromImage":"e2b/others/app"}`, testKey)
	assert.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
}

func withTemplateFleet(store *fakeStore) serverOption {
	created := &metav1.Time{Time: time.Date(2026, 9, 28, 1, 2, 3, 0, time.UTC)}
	built := func(name, digest string) scale.PromotedTemplate {
		return scale.PromotedTemplate{Template: name, Net: "none", Size: "medium", ContentDigest: digest, CreatedAt: created, CPUCount: 2, MemoryBytes: 1 << 30}
	}
	inv := scale.NewStaticInventorySource()
	inv.Put(&scale.NodeInventory{Node: "node-a", Pools: []scale.PoolCapacity{{Template: "reg/rt:24.04", Warm: 1}}, Templates: []scale.PromotedTemplate{
		withLabels(built("e2b/sandboxes/app", "sha256:aa"), map[string]string{"old": "sha256:gone"}), built("e2b/sandboxes/app2", "sha256:bb"),
		built("e2b/others/other", "sha256:dd"), built("tpl:plain", "sha256:ee"),
		{Template: "e2b/sandboxes/tenants", ContentDigest: "sha256:ff", Tenant: "acme", CreatedAt: created},
	}})
	inv.Put(&scale.NodeInventory{Node: "node-b", Templates: []scale.PromotedTemplate{
		withLabels(built("e2b/sandboxes/app", "sha256:aa"), map[string]string{"old": "sha256:gone"}), later(built("e2b/sandboxes/app2", "sha256:cc")),
	}})
	inv.Put(&scale.NodeInventory{Node: "node-c", Templates: []scale.PromotedTemplate{later(built("e2b/sandboxes/app2", "sha256:cc"))}})
	store.fleet = inv
	return func(o *Options) { o.Inventory = inv }
}

func withLabels(t scale.PromotedTemplate, labels map[string]string) scale.PromotedTemplate {
	t.Labels = labels
	return t
}

func later(t scale.PromotedTemplate) scale.PromotedTemplate {
	t.CreatedAt = &metav1.Time{Time: t.CreatedAt.Add(time.Minute)}
	return t
}

func fleetLabels(t *testing.T, store *fakeStore, node, template string) map[string]string {
	t.Helper()
	held, err := store.NodeTemplates(t.Context(), node)
	require.NoError(t, err)
	i := slices.IndexFunc(held, func(p scale.PromotedTemplate) bool { return p.Template == template })
	require.GreaterOrEqual(t, i, 0, "%s not held on %s", template, node)
	return held[i].Labels
}

func tagNames(tags []TemplateTag) []string {
	out := make([]string, 0, len(tags))
	for _, t := range tags {
		out = append(out, t.Tag)
	}
	return out
}
