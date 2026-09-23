package e2bcompat

import (
	"encoding/json"
	"net/http"
	"slices"
	"testing"
	"time"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

func TestV2CreateAndConnectAreRouted(t *testing.T) {
	store := &lifecycleStore{}
	store.assign = scale.Assignment{SandboxName: "sb_1", Node: "n"}
	store.items = []sandboxv1beta1.Sandbox{liveSandbox("s1", "sb_abc", "node-a", "img")}
	h := newTestServer(t, store)

	if w := do(t, h, http.MethodPost, "/v2/sandboxes", `{"templateID":"t","iam":{},"network":{}}`, testKey); w.Code != http.StatusCreated {
		t.Fatalf("v2 create status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if w := do(t, h, http.MethodPost, "/v2/sandboxes/sb-abc/connect", `{"timeout":300}`, testKey); w.Code != http.StatusOK {
		t.Fatalf("v2 connect status = %d, want 200: %s", w.Code, w.Body.String())
	}
}

func TestConnectRefusesRebootResume(t *testing.T) {
	store := &lifecycleStore{}
	store.items = []sandboxv1beta1.Sandbox{pausedSandbox("s1", "sb_abc", "node-a", "img")}
	store.nodePaused = true
	h := newTestServer(t, store)

	if w := do(t, h, http.MethodPost, "/v2/sandboxes/sb-abc/connect", `{"timeout":300,"memory":false}`, testKey); w.Code != http.StatusBadRequest {
		t.Fatalf("memory=false status = %d, want 400: %s", w.Code, w.Body.String())
	}
	if store.resumedID != "" {
		t.Fatalf("a refused connect resumed %q", store.resumedID)
	}
	if w := do(t, h, http.MethodPost, "/v2/sandboxes/sb-abc/connect", `{"timeout":300,"memory":true}`, testKey); w.Code != http.StatusCreated {
		t.Fatalf("memory=true status = %d, want 201: %s", w.Code, w.Body.String())
	}
}

func TestKeysScopeSandboxesToTheirNamespace(t *testing.T) {
	a, b := liveSandbox("a", "sb_a", "node-a", "img"), liveSandbox("b", "sb_b", "node-a", "img")
	a.Namespace, b.Namespace = "team-a", "team-b"
	store := &fakeStore{assign: scale.Assignment{SandboxName: "sb_new", Node: "n"}, items: []sandboxv1beta1.Sandbox{a, b}}
	h := newTestServer(t, store, func(o *Options) { o.APIKeys = []string{"key-a team-a", "key-b team-b", "key-default"} })

	if w := do(t, h, http.MethodPost, "/sandboxes", `{"templateID":"t"}`, "key-a"); w.Code != http.StatusCreated || store.claimNS != "team-a" {
		t.Fatalf("create with key-a: status %d, claimed in %q, want team-a: %s", w.Code, store.claimNS, w.Body.String())
	}
	if w := do(t, h, http.MethodPost, "/sandboxes", `{"templateID":"t"}`, "key-default"); w.Code != http.StatusCreated || store.claimNS != "sandboxes" {
		t.Fatalf("create with a namespace-less key: status %d, claimed in %q, want the configured namespace", w.Code, store.claimNS)
	}
	if w := do(t, h, http.MethodGet, "/sandboxes/sb-a", "", "key-b"); w.Code != http.StatusNotFound {
		t.Fatalf("key-b reading team-a's sandbox: status = %d, want 404", w.Code)
	}
	if w := do(t, h, http.MethodDelete, "/sandboxes/sb-a", "", "key-b"); w.Code != http.StatusNotFound || store.releasedID != "" {
		t.Fatalf("key-b deleting team-a's sandbox: status = %d, released %q, want 404 and nothing released", w.Code, store.releasedID)
	}
	w := do(t, h, http.MethodGet, "/sandboxes", "", "key-a")
	var listed []SandboxDetail
	if err := json.Unmarshal(w.Body.Bytes(), &listed); err != nil || w.Code != http.StatusOK {
		t.Fatalf("list with key-a: status %d, decode %v", w.Code, err)
	}
	if len(listed) != 1 || listed[0].SandboxID != "sb-a" {
		t.Fatalf("key-a lists %+v, want only team-a's sandbox", listed)
	}
}

func TestSnapshotsAreScopedToTheKeysNamespace(t *testing.T) {
	inv := scale.NewStaticInventorySource()
	inv.Put(&scale.NodeInventory{Node: "node-a"})
	store := &lifecycleStore{snapshot: scale.Snapshot{ID: "ck_1", Name: "team-a/before", Node: "node-a"}}
	store.items = []sandboxv1beta1.Sandbox{liveSandbox("s1", "sb_abc", "node-a", "img")}
	store.items[0].Namespace = "team-a"
	store.snaps = []scale.Snapshot{
		{ID: "ck_1", Name: "team-a/before", Node: "node-a"},
		{ID: "ck_2", Name: "team-b/theirs", Node: "node-a"},
		{ID: "ck_3", Name: "unscoped", Node: "node-a"},
	}
	h := newTestServer(t, store, func(o *Options) {
		o.APIKeys = []string{"key-a team-a", "key-b team-b"}
		o.Inventory = inv
	})

	w := do(t, h, http.MethodPost, "/sandboxes/sb-abc/snapshots", `{"name":"before"}`, "key-a")
	var created SnapshotInfo
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil || w.Code != http.StatusCreated {
		t.Fatalf("snapshot: status %d, decode %v: %s", w.Code, err, w.Body.String())
	}
	if store.snapshotName != "team-a/before" || len(created.Names) != 1 || created.Names[0] != "before" {
		t.Fatalf("snapshot recorded as %q and echoed %v, want the namespace stamped on the node and stripped for the caller", store.snapshotName, created.Names)
	}

	w = do(t, h, http.MethodGet, "/snapshots", "", "key-a")
	var listed []SnapshotInfo
	if err := json.Unmarshal(w.Body.Bytes(), &listed); err != nil || w.Code != http.StatusOK {
		t.Fatalf("list: status %d, decode %v", w.Code, err)
	}
	if len(listed) != 1 || listed[0].SnapshotID != "ck_1" || listed[0].Names[0] != "before" {
		t.Fatalf("key-a lists %+v, want only its own checkpoint, unprefixed", listed)
	}

	if w := do(t, h, http.MethodDelete, "/templates/ck_2", "", "key-a"); w.Code != http.StatusNotFound || store.deletedSnapshotID != "" {
		t.Fatalf("key-a deleting team-b's checkpoint: status %d, deleted %q, want 404 and nothing deleted", w.Code, store.deletedSnapshotID)
	}
	if w := do(t, h, http.MethodDelete, "/templates/ck_1", "", "key-a"); w.Code != http.StatusNoContent || store.deletedSnapshotNode != "node-a" || store.deletedSnapshotID != "ck_1" {
		t.Fatalf("key-a deleting its checkpoint: status %d, deleted %q on %q", w.Code, store.deletedSnapshotID, store.deletedSnapshotNode)
	}
}

func TestSnapshotDeleteReportsANodeThatDidNotAnswer(t *testing.T) {
	inv := scale.NewStaticInventorySource()
	inv.Put(&scale.NodeInventory{Node: "node-a"})
	inv.Put(&scale.NodeInventory{Node: "node-b"})
	store := &fakeStore{snaps: []scale.Snapshot{{ID: "ck_1", Name: "sandboxes/mine", Node: "node-a"}}, snapshotsDownNode: "node-b"}
	h := newTestServer(t, store, func(o *Options) { o.Inventory = inv })

	if w := do(t, h, http.MethodDelete, "/templates/ck_missing", "", testKey); w.Code != http.StatusInternalServerError {
		t.Fatalf("delete with a node down: status %d, want 500 rather than not found: %s", w.Code, w.Body.String())
	}
	if w := do(t, h, http.MethodDelete, "/templates/ck_1", "", testKey); w.Code != http.StatusNoContent || store.deletedSnapshotID != "ck_1" {
		t.Fatalf("delete of a listed checkpoint: status %d, deleted %q, want 204 and ck_1", w.Code, store.deletedSnapshotID)
	}
	store.snapshotsDownNode = ""
	if w := do(t, h, http.MethodDelete, "/templates/ck_missing", "", testKey); w.Code != http.StatusNotFound {
		t.Fatalf("delete with every node answering: status %d, want 404", w.Code)
	}
}

func TestSnapshotListFiltersBySandboxAndName(t *testing.T) {
	inv := scale.NewStaticInventorySource()
	inv.Put(&scale.NodeInventory{Node: "node-a"})
	store := &fakeStore{snaps: []scale.Snapshot{
		{ID: "ck_1", Name: "sandboxes/first", SandboxID: "sb_abc", Node: "node-a"},
		{ID: "ck_2", Name: "sandboxes/second", SandboxID: "sb_abc", Node: "node-a"},
		{ID: "ck_3", Name: "sandboxes/first", SandboxID: "sb_other", Node: "node-a"},
	}}
	h := newTestServer(t, store, func(o *Options) { o.Inventory = inv })
	for _, tc := range []struct {
		query string
		want  []string
	}{
		{"", []string{"ck_1", "ck_2", "ck_3"}},
		{"?sandboxID=sb-abc", []string{"ck_1", "ck_2"}},
		{"?name=first", []string{"ck_1", "ck_3"}},
		{"?sandboxID=sb-abc&name=first", []string{"ck_1"}},
	} {
		w := do(t, h, http.MethodGet, "/snapshots"+tc.query, "", testKey)
		var listed []SnapshotInfo
		if err := json.Unmarshal(w.Body.Bytes(), &listed); err != nil || w.Code != http.StatusOK {
			t.Fatalf("%s: status %d, decode %v", tc.query, w.Code, err)
		}
		ids := make([]string, 0, len(listed))
		for _, snap := range listed {
			ids = append(ids, snap.SnapshotID)
		}
		if !slices.Equal(ids, tc.want) {
			t.Errorf("%s lists %v, want %v", tc.query, ids, tc.want)
		}
	}
}

func TestListHonorsStateAndTemplateAndRefusesMetadata(t *testing.T) {
	running, paused := liveSandbox("a", "sb_a", "node-a", "img"), pausedSandbox("b", "sb_b", "node-a", "other")
	store := &fakeStore{items: []sandboxv1beta1.Sandbox{running, paused}}
	h := newTestServer(t, store)
	for _, tc := range []struct {
		query string
		want  []string
	}{
		{"", []string{"sb-a", "sb-b"}},
		{"?state=paused", []string{"sb-b"}},
		{"?state=running,paused", []string{"sb-a", "sb-b"}},
		{"?state=running&state=paused", []string{"sb-a", "sb-b"}},
		{"?template=other", []string{"sb-b"}},
		{"?startedAfter=2000-01-01T00:00:00Z", []string{"sb-a", "sb-b"}},
		{"?startedAfter=" + running.CreationTimestamp.UTC().Truncate(time.Second).Add(100*time.Millisecond).Format(time.RFC3339Nano), []string{"sb-a", "sb-b"}},
		{"?startedAfter=2999-01-01T00:00:00Z", []string{}},
		{"?metadata=", []string{"sb-a", "sb-b"}},
	} {
		w := do(t, h, http.MethodGet, "/v2/sandboxes"+tc.query, "", testKey)
		var listed []SandboxDetail
		if err := json.Unmarshal(w.Body.Bytes(), &listed); err != nil || w.Code != http.StatusOK {
			t.Fatalf("%s: status %d, decode %v: %s", tc.query, w.Code, err, w.Body.String())
		}
		ids := make([]string, 0, len(listed))
		for _, d := range listed {
			ids = append(ids, d.SandboxID)
		}
		if !slices.Equal(ids, tc.want) {
			t.Errorf("%s lists %v, want %v", tc.query, ids, tc.want)
		}
	}
	for _, query := range []string{"?metadata=owner%3Dme", "?state=sleeping", "?startedAfter=yesterday"} {
		if w := do(t, h, http.MethodGet, "/v2/sandboxes"+query, "", testKey); w.Code != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400: %s", query, w.Code, w.Body.String())
		}
	}
}

func TestAKeyEntryIsAKeyOrAKeyAndANamespace(t *testing.T) {
	if _, err := NewServer(&fakeStore{}, Options{Domain: testDomain, APIKeys: []string{"key ns extra"}}); err == nil {
		t.Fatal("a three-field key entry must fail startup")
	}
}
