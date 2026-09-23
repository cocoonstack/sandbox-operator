package e2bcompat

import (
	"encoding/json"
	"net/http"
	"testing"

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

func TestCreateRefusesV2OptionsItCannotHonor(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want int
	}{
		{"network rules", `{"templateID":"t","network":{"denyOut":["10.0.0.0/8"]}}`, http.StatusBadRequest},
		{"volume mounts", `{"templateID":"t","volumeMounts":[{"name":"v","path":"/data"}]}`, http.StatusBadRequest},
		{"auto pause memory", `{"templateID":"t","autoPauseMemory":true}`, http.StatusBadRequest},
		{"auto resume", `{"templateID":"t","autoResume":{"enabled":true}}`, http.StatusBadRequest},
		{"mcp", `{"templateID":"t","mcp":{"a":{}}}`, http.StatusBadRequest},
		{"iam tokens", `{"templateID":"t","iam":{"tokens":{"x":{"audience":"a","tokenType":"t"}}}}`, http.StatusBadRequest},
		{"auto resume off", `{"templateID":"t","autoResume":{"enabled":false}}`, http.StatusCreated},
		{"empty network", `{"templateID":"t","network":{}}`, http.StatusCreated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{assign: scale.Assignment{SandboxName: "sb_1", Node: "n"}}
			h := newTestServer(t, store)
			if w := do(t, h, http.MethodPost, "/v2/sandboxes", tc.body, testKey); w.Code != tc.want {
				t.Fatalf("status = %d, want %d: %s", w.Code, tc.want, w.Body.String())
			}
		})
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

func TestAKeyEntryIsAKeyOrAKeyAndANamespace(t *testing.T) {
	if _, err := NewServer(&fakeStore{}, Options{Domain: testDomain, APIKeys: []string{"key ns extra"}}); err == nil {
		t.Fatal("a three-field key entry must fail startup")
	}
}
