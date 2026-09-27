package e2bcompat

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

func TestACreateNamingAnAliasClaimsFromItsPool(t *testing.T) {
	for templateID, pool := range map[string]string{"base": "reg/rt:24.04", "reg/py:3.12": "reg/py:3.12"} {
		t.Run(templateID, func(t *testing.T) {
			store := &fakeStore{assign: scale.Assignment{SandboxName: "sb_1", Node: "n"}}
			h := newTestServer(t, store, withAliases("base reg/rt:24.04"))

			w := do(t, h, http.MethodPost, "/sandboxes", `{"templateID":"`+templateID+`"}`, testKey)
			require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
			assert.Equal(t, pool, store.claimPool.Template)
			var got Sandbox
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
			assert.Equal(t, pool, got.TemplateID, "templateID is the pool image whichever spelling created it")
			if pool == "reg/rt:24.04" {
				assert.Equal(t, "base", got.Alias)
			} else {
				assert.Empty(t, got.Alias)
			}
		})
	}
}

func TestReadsKeepTheImageAndCarryTheAlias(t *testing.T) {
	store := &fakeStore{items: []sandboxv1beta1.Sandbox{
		liveSandbox("a", "sb_a", "node-a", "reg/rt:24.04"),
		liveSandbox("b", "sb_b", "node-a", "reg/py:3.12"),
	}}
	h := newTestServer(t, store, withAliases("base reg/rt:24.04", "rt reg/rt:24.04"))

	a, b := getDetailWith(t, h, "sb-a"), getDetailWith(t, h, "sb-b")
	assert.Equal(t, [2]string{"reg/rt:24.04", "base"}, [2]string{a.TemplateID, a.Alias}, "the first alias in order names the pool")
	assert.Equal(t, [2]string{"reg/py:3.12", ""}, [2]string{b.TemplateID, b.Alias})
	for query, want := range map[string][]string{
		"":                       {"sb-a", "sb-b"},
		"?template=base":         {"sb-a"},
		"?template=rt":           {"sb-a"},
		"?template=reg/rt:24.04": {"sb-a"},
		"?template=reg/py:3.12":  {"sb-b"},
	} {
		ids, _ := pageOfList(t, h, "/v2/sandboxes"+query)
		slices.Sort(ids)
		assert.Equal(t, want, ids, query)
	}
}

func TestTheAliasLookupAnswersTheSpecShape(t *testing.T) {
	inv := scale.NewStaticInventorySource()
	inv.Put(&scale.NodeInventory{Node: "node-a", Pools: []scale.PoolCapacity{{Template: "reg/py:3.12", Warm: 1}}})
	h := newTestServer(t, &fakeStore{}, withAliases("base reg/rt:24.04"), func(o *Options) { o.Inventory = inv })

	w := do(t, h, http.MethodGet, "/templates/aliases/base", "", testKey)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, `{"templateID":"reg/rt:24.04","public":true}`, w.Body.String())
	w = do(t, h, http.MethodGet, "/templates/aliases/reg%2Fpy:3.12", "", testKey)
	assert.Equal(t, http.StatusOK, w.Code, "an image the fleet advertises is a name exists() accepts")
	assert.JSONEq(t, `{"templateID":"reg/py:3.12","public":true}`, w.Body.String())
	assert.Equal(t, http.StatusNotFound, do(t, h, http.MethodGet, "/templates/aliases/code-interpreter-v1", "", testKey).Code)
	assert.Equal(t, http.StatusUnauthorized, do(t, h, http.MethodGet, "/templates/aliases/base", "", "").Code)
}

func TestTheTemplateListCarriesEachPoolsAliases(t *testing.T) {
	inv := scale.NewStaticInventorySource()
	inv.Put(&scale.NodeInventory{Node: "node-a", Pools: []scale.PoolCapacity{{Template: "reg/rt:24.04", Warm: 1}, {Template: "reg/py:3.12", Warm: 1}}})
	h := newTestServer(t, &fakeStore{}, withAliases("rt reg/rt:24.04", "base reg/rt:24.04"), func(o *Options) { o.Inventory = inv })

	w := do(t, h, http.MethodGet, "/templates", "", testKey)
	var got []Template
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	aliases := map[string][]string{}
	for _, tpl := range got {
		aliases[tpl.TemplateID] = tpl.Aliases
	}
	assert.Equal(t, map[string][]string{"reg/rt:24.04": {"base", "rt"}, "reg/py:3.12": {}}, aliases)
}

func TestNewServerRefusesAMalformedAliasTable(t *testing.T) {
	for _, entries := range [][]string{{"base"}, {"base reg/rt:24.04 extra"}, {"base reg/rt:24.04", "base reg/py:3.12"}} {
		_, err := NewServer(&fakeStore{}, Options{Domain: testDomain, APIKeys: []string{testKey}, TemplateAliases: entries})
		assert.Error(t, err, strings.Join(entries, " | "))
	}
}

func withAliases(entries ...string) func(*Options) {
	return func(o *Options) { o.TemplateAliases = entries }
}
