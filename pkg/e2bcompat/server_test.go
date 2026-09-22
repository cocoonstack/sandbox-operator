package e2bcompat

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

const (
	testKey    = "e2b_testkey"
	testDomain = "sandbox.example.com"
)

func TestCreateClaimsFromTemplatePool(t *testing.T) {
	store := &fakeStore{assign: scale.Assignment{SandboxName: "sb_abc123", Node: "node-a", Token: "tok-1"}}
	h := newTestServer(t, store, func(o *Options) { o.Domain = "sandbox.example.com" })

	w := do(t, h, http.MethodPost, "/sandboxes", `{"templateID":"registry/rt:24.04","timeout":300}`, testKey)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (the e2b create contract): %s", w.Code, w.Body.String())
	}
	var got Sandbox
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if got.SandboxID != "sb-abc123" {
		t.Errorf("sandboxID = %q, want the DNS-safe rendering of the claim id", got.SandboxID)
	}
	if got.TemplateID != "registry/rt:24.04" {
		t.Errorf("templateID = %q, want it echoed back", got.TemplateID)
	}
	if got.EnvdAccessToken != "tok-1" {
		t.Errorf("envdAccessToken = %q, want the claim token", got.EnvdAccessToken)
	}
	if got.Domain != "sandbox.example.com" {
		t.Errorf("domain = %q, want the configured domain", got.Domain)
	}

	if got.EnvdVersion == "" {
		t.Error("envdVersion is empty; the e2b SDK kills the sandbox and throws when it cannot parse this")
	}
	if store.claimPool.Template != "registry/rt:24.04" {
		t.Errorf("claimed pool template = %q, want the requested templateID", store.claimPool.Template)
	}
	if store.claimNS != "sandboxes" {
		t.Errorf("claim namespace = %q, want the configured compat namespace", store.claimNS)
	}
	if !strings.HasPrefix(store.claimName, namePrefix) {
		t.Errorf("claim name = %q, want the %q prefix so compat claims are recognizable", store.claimName, namePrefix)
	}
	if store.claimTTL != 300 {
		t.Errorf("claim ttl = %d, want the requested timeout passed through to the node", store.claimTTL)
	}
}

func TestCreateNetworkLane(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{"default is the hardened lane", `{"templateID":"t"}`, scale.NetDefault},
		{"internet access picks egress", `{"templateID":"t","allow_internet_access":true}`, "egress"},
		{"explicit false stays hardened", `{"templateID":"t","allow_internet_access":false}`, scale.NetDefault},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{assign: scale.Assignment{SandboxName: "sb_1", Node: "n"}}
			h := newTestServer(t, store)
			if w := do(t, h, http.MethodPost, "/sandboxes", tc.body, testKey); w.Code != http.StatusCreated {
				t.Fatalf("status = %d: %s", w.Code, w.Body.String())
			}
			if store.claimPool.Net != tc.want {
				t.Errorf("pool net = %q, want %q", store.claimPool.Net, tc.want)
			}
		})
	}
}

func TestCreateNoWarmCapacityIsRetryable(t *testing.T) {
	store := &fakeStore{claimErr: scale.ErrNoWarmCapacity}
	h := newTestServer(t, store)

	w := do(t, h, http.MethodPost, "/sandboxes", `{"templateID":"t"}`, testKey)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 so the client retries as warm capacity refills", w.Code)
	}
}

func TestCreateRejectsMissingTemplate(t *testing.T) {
	h := newTestServer(t, &fakeStore{})
	if w := do(t, h, http.MethodPost, "/sandboxes", `{}`, testKey); w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for a body with no templateID", w.Code)
	}
}

func TestCreateRejectsNegativeTimeout(t *testing.T) {
	h := newTestServer(t, &fakeStore{})
	body := `{"templateID":"img","timeout":-1}`
	if w := do(t, h, http.MethodPost, "/sandboxes", body, testKey); w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for a negative timeout", w.Code)
	}
}

func TestAuthRequiresAPIKey(t *testing.T) {
	store := &fakeStore{assign: scale.Assignment{SandboxName: "sb_1", Node: "n"}}
	h := newTestServer(t, store)

	for _, tc := range []struct{ name, key string }{
		{"no key", ""},
		{"wrong key", "e2b_wrong"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := do(t, h, http.MethodPost, "/sandboxes", `{"templateID":"t"}`, tc.key)
			if w.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", w.Code)
			}
			if store.claimName != "" {
				t.Fatal("an unauthenticated request reached the claim path")
			}
		})
	}
}

func TestCreateRefusesGuaranteesThisBackendCannotGive(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"secure false", `{"templateID":"t","secure":false}`},
		{"envVars", `{"templateID":"t","envVars":{"A":"1"}}`},
		{"autoPause", `{"templateID":"t","autoPause":true}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{}
			h := newTestServer(t, store)
			if w := do(t, h, http.MethodPost, "/sandboxes", tc.body, testKey); w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
			}
			if store.claimName != "" {
				t.Error("an unhonorable option still reached the claim path")
			}
		})
	}
}

func TestCreateAcceptsTheHonorableForms(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"secure true", `{"templateID":"t","secure":true}`},
		{"empty envVars", `{"templateID":"t","envVars":{}}`},
		{"autoPause false", `{"templateID":"t","autoPause":false}`},
		{"metadata is accepted and dropped", `{"templateID":"t","metadata":{"a":"b"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestServer(t, &fakeStore{})
			if w := do(t, h, http.MethodPost, "/sandboxes", tc.body, testKey); w.Code != http.StatusCreated {
				t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestCreateWithoutATimeoutTakesTheConfiguredDefault(t *testing.T) {
	store := &fakeStore{}
	h := newTestServer(t, store, func(o *Options) { o.DefaultTimeoutSeconds = 900 })
	if w := do(t, h, http.MethodPost, "/sandboxes", `{"templateID":"t"}`, testKey); w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if store.claimTTL != 900 {
		t.Errorf("claimed with ttl %ds, want the configured 900s default", store.claimTTL)
	}
}

func TestNewServerRefusesOpenByDefault(t *testing.T) {
	if _, err := NewServer(&fakeStore{}, Options{Namespace: "x", Domain: testDomain}); err == nil {
		t.Fatal("NewServer accepted no API key without explicit anonymous access")
	}
	if _, err := NewServer(&fakeStore{}, Options{Namespace: "x", Domain: testDomain, AllowAnonymous: true}); err != nil {
		t.Fatalf("NewServer rejected explicit anonymous access: %v", err)
	}
}

func TestNewServerRequiresADomain(t *testing.T) {
	if _, err := NewServer(&fakeStore{}, Options{Namespace: "x", APIKeys: []string{testKey}}); err == nil {
		t.Fatal("NewServer accepted an empty domain; a sandbox handed out with one has no reachable data plane")
	}
	if _, err := NewServer(&fakeStore{}, Options{Namespace: "x", Domain: "   ", APIKeys: []string{testKey}}); err == nil {
		t.Fatal("NewServer accepted a blank domain")
	}
}

func TestHealthNeedsNoKey(t *testing.T) {
	h := newTestServer(t, &fakeStore{})
	if w := do(t, h, http.MethodGet, "/health", "", ""); w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204 without a key so probes work", w.Code)
	}
}

func TestDeleteReleasesTheClaimedID(t *testing.T) {
	store := &fakeStore{items: []sandboxv1beta1.Sandbox{
		liveSandbox("e2b-aaa", "sb_one", "node-a", "img:1"),
		liveSandbox("e2b-bbb", "sb_two", "node-b", "img:1"),
	}}
	h := newTestServer(t, store)

	if w := do(t, h, http.MethodDelete, "/sandboxes/sb_two", "", testKey); w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %s", w.Code, w.Body.String())
	}
	if store.releasedID != "sb_two" || store.releasedNode != "node-b" {
		t.Errorf("released (node=%q id=%q), want (node-b, sb_two)", store.releasedNode, store.releasedID)
	}
}

func TestDeleteUnknownSandboxIs404(t *testing.T) {
	store := &fakeStore{items: []sandboxv1beta1.Sandbox{liveSandbox("a", "sb_one", "node-a", "i")}}
	h := newTestServer(t, store)

	if w := do(t, h, http.MethodDelete, "/sandboxes/sb_missing", "", testKey); w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
	if store.releasedID != "" {
		t.Errorf("released %q for an unknown sandbox", store.releasedID)
	}
}

func TestGetReportsDetail(t *testing.T) {
	got := getDetail(t, liveSandbox("e2b-aaa", "sb_one", "node-a", "registry/rt:24.04"))
	if got.SandboxID != "sb-one" || got.TemplateID != "registry/rt:24.04" || got.ClientID != "node-a" {
		t.Errorf("detail = %+v, want the live sandbox's id/template/node", got)
	}
	if got.State != StateRunning {
		t.Errorf("state = %q, want %q", got.State, StateRunning)
	}

	if got.StartedAt == "" || got.EndAt == "" {
		t.Errorf("startedAt/endAt = %q/%q, want RFC3339 timestamps", got.StartedAt, got.EndAt)
	}
}

func TestGetReportsTheClaimTimeAsStartedAt(t *testing.T) {
	sb := liveSandbox("e2b-aaa", "sb_one", "node-a", "registry/rt:24.04")
	sb.CreationTimestamp = metav1.NewTime(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	if got := getDetail(t, sb); got.StartedAt != "2030-01-02T03:04:05Z" {
		t.Errorf("startedAt = %q, want the claim time the node published", got.StartedAt)
	}
}

func TestGetReportsTheGrantedDeadlineAsEndAt(t *testing.T) {
	sb := liveSandbox("e2b-aaa", "sb_one", "node-a", "registry/rt:24.04")
	sb.Annotations[scale.DeadlineAnnotation] = "2030-01-02T03:04:05Z"
	if got := getDetail(t, sb); got.EndAt != "2030-01-02T03:04:05Z" {
		t.Errorf("endAt = %q, want the granted deadline", got.EndAt)
	}
}

func TestGetReportsTheDefaultEndAtWithoutADeadline(t *testing.T) {
	sb := liveSandbox("e2b-aaa", "sb_one", "node-a", "registry/rt:24.04")
	sb.CreationTimestamp = metav1.NewTime(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	if got := getDetail(t, sb); got.EndAt != "2030-01-02T03:09:05Z" {
		t.Errorf("endAt = %q, want startedAt + %ds", got.EndAt, DefaultTimeoutSeconds)
	}
}

func TestListReturnsArray(t *testing.T) {
	store := &fakeStore{items: []sandboxv1beta1.Sandbox{
		liveSandbox("a", "sb_one", "node-a", "i:1"),
		liveSandbox("b", "sb_two", "node-b", "i:2"),
	}}
	h := newTestServer(t, store)

	for _, path := range []string{"/sandboxes", "/v2/sandboxes"} {
		t.Run(path, func(t *testing.T) {
			w := do(t, h, http.MethodGet, path, "", testKey)
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", w.Code)
			}
			var got []SandboxDetail
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatalf("decode as array: %v", err)
			}
			if len(got) != 2 {
				t.Fatalf("len = %d, want 2", len(got))
			}
		})
	}
}

func TestListEmptyIsArrayNotNull(t *testing.T) {
	h := newTestServer(t, &fakeStore{})
	w := do(t, h, http.MethodGet, "/sandboxes", "", testKey)
	if body := strings.TrimSpace(w.Body.String()); body != "[]" {
		t.Fatalf("body = %s, want []", body)
	}
}

func TestTimeoutAndRefreshRenewTheLease(t *testing.T) {
	for _, tc := range []struct {
		name, path, body string
		wantTTL          int
	}{
		{"timeout carries the requested seconds", "/sandboxes/sb_one/timeout", `{"timeout":60}`, 60},
		{"refresh with a duration", "/sandboxes/sb_one/refreshes", `{"duration":30}`, 30},
		{"refresh without a body takes the default", "/sandboxes/sb_one/refreshes", "", DefaultTimeoutSeconds},
		{"refresh with an unrecognized body takes the default", "/sandboxes/sb_one/refreshes", `{}`, DefaultTimeoutSeconds},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newRenewStore()
			h := newTestServer(t, store)
			if w := do(t, h, http.MethodPost, tc.path, tc.body, testKey); w.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want 204: %s", w.Code, w.Body.String())
			}
			if store.node != "node-a" || store.id != "sb_one" {
				t.Errorf("renewed %s/%s, want node-a/sb_one", store.node, store.id)
			}
			if store.ttl != tc.wantTTL {
				t.Errorf("renewed for %ds, want %ds", store.ttl, tc.wantTTL)
			}
		})
	}
}

func TestTimeoutAndRefreshReportNodeFailures(t *testing.T) {
	for _, path := range []string{"/sandboxes/sb_one/timeout", "/sandboxes/sb_one/refreshes"} {
		t.Run(path, func(t *testing.T) {
			store := newRenewStore()
			store.err = errors.New("node unreachable")
			h := newTestServer(t, store)
			if w := do(t, h, http.MethodPost, path, `{"timeout":60}`, testKey); w.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, want 500 when the node refuses the renew", w.Code)
			}
		})
	}
}

func TestTimeoutAndRefreshOnUnknownSandboxIs404(t *testing.T) {
	store := newRenewStore()
	h := newTestServer(t, store)
	for _, path := range []string{"/sandboxes/sb_gone/timeout", "/sandboxes/sb_gone/refreshes"} {
		t.Run(path, func(t *testing.T) {
			if w := do(t, h, http.MethodPost, path, `{"timeout":60}`, testKey); w.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404", w.Code)
			}
			if store.id != "" {
				t.Errorf("an unknown id reached the node as %q", store.id)
			}
		})
	}
}

func TestTimeoutRejectsNegative(t *testing.T) {
	store := newRenewStore()
	h := newTestServer(t, store)
	if w := do(t, h, http.MethodPost, "/sandboxes/sb_one/timeout", `{"timeout":-1}`, testKey); w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
	if store.id != "" {
		t.Error("a negative timeout reached the node")
	}
}

func TestErrorBodyCarriesMessage(t *testing.T) {
	h := newTestServer(t, &fakeStore{})
	w := do(t, h, http.MethodPost, "/sandboxes", `{}`, testKey)
	var got APIError
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode error envelope: %v", err)
	}
	if got.Message == "" || got.Code != http.StatusBadRequest {
		t.Errorf("error = %+v, want code 400 and a message", got)
	}
}

func TestLookupUsesClaimIDResolver(t *testing.T) {
	src := scale.NewStaticInventorySource()
	src.Put(&scale.NodeInventory{
		Name:    "node-a",
		Node:    "node-a",
		Address: "10.0.0.1:7777",
		Entries: []scale.InventoryEntry{
			{Name: "sandboxes/sb-1", ID: "sb_0123abcd", Phase: "Running", Address: "10.0.0.1:7777"},
			{Name: "elsewhere/sb-2", ID: "sb_ffff0000", Phase: "Running", Address: "10.0.0.1:7777"},
		},
	})
	s, err := NewServer(scale.NewScatterGatherStore(src), Options{Namespace: "sandboxes", Domain: testDomain, AllowAnonymous: true})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/sandboxes/x", nil)

	for _, id := range []string{"sb_0123abcd", PublicID("sb_0123abcd")} {
		sb, err := s.lookup(req, id)
		if err != nil {
			t.Fatalf("lookup(%q): %v", id, err)
		}
		if sb.Name != "sb-1" || sb.Namespace != "sandboxes" {
			t.Fatalf("lookup(%q) = %s/%s, want sandboxes/sb-1", id, sb.Namespace, sb.Name)
		}
	}
	if _, err := s.lookup(req, "sb_ffff0000"); err != errSandboxNotFound {
		t.Fatalf("cross-namespace id resolved, want errSandboxNotFound, got %v", err)
	}
	if _, err := s.lookup(req, "sb_missing"); err != errSandboxNotFound {
		t.Fatalf("missing id: want errSandboxNotFound, got %v", err)
	}
}

func TestDetailStateFollowsThePhaseLabel(t *testing.T) {
	s, err := NewServer(&fakeStore{}, Options{Domain: testDomain, AllowAnonymous: true})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	sb := liveSandbox("sb-1", "sb_0123abcd", "node-a", "img")
	if got := s.detailFor(&sb).State; got != StateRunning {
		t.Fatalf("running sandbox state = %q, want %q", got, StateRunning)
	}
	sb.Labels = map[string]string{scale.PhaseLabel: phaseHibernated}
	if got := s.detailFor(&sb).State; got != StatePaused {
		t.Fatalf("hibernated sandbox state = %q, want %q", got, StatePaused)
	}
}

func TestDeleteWithoutAnOwningNodeIs500(t *testing.T) {
	sb := liveSandbox("e2b-aaa", "sb_one", "node-a", "img:1")
	sb.Status.NodeName = ""
	store := &fakeStore{items: []sandboxv1beta1.Sandbox{sb}}
	h := newTestServer(t, store)

	if w := do(t, h, http.MethodDelete, "/sandboxes/sb_one", "", testKey); w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500: a 204 would report an unreleased sandbox as freed", w.Code)
	}
	if store.releasedID != "" {
		t.Errorf("released %q with no owning node", store.releasedID)
	}
}

type fakeStore struct {
	claimPool scale.PoolKey
	claimNS   string
	claimName string
	claimTTL  int
	claimErr  error
	assign    scale.Assignment

	items []sandboxv1beta1.Sandbox

	releasedNode string
	releasedID   string
	releaseErr   error
	listErr      error
}

func (f *fakeStore) List(context.Context, scale.ListOptions) (*sandboxv1beta1.SandboxList, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return &sandboxv1beta1.SandboxList{Items: f.items}, nil
}

func (f *fakeStore) Get(context.Context, string, string) (*sandboxv1beta1.Sandbox, error) {
	return nil, nil
}

func (f *fakeStore) GetByClaimID(_ context.Context, _, _ string, match func(string) bool) (*sandboxv1beta1.Sandbox, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	for i := range f.items {
		if match(f.items[i].Annotations[scale.ClaimIDAnnotation]) {
			return &f.items[i], nil
		}
	}
	return nil, k8serrors.NewNotFound(sandboxv1beta1.Resource("sandboxes"), "")
}

func (f *fakeStore) Watch(context.Context, scale.ListOptions) (watch.Interface, error) {
	return nil, nil
}

func (f *fakeStore) Claim(_ context.Context, ns, name string, pool scale.PoolKey, ttlSeconds int) (scale.Assignment, error) {
	f.claimNS, f.claimName, f.claimPool, f.claimTTL = ns, name, pool, ttlSeconds
	if f.claimErr != nil {
		return scale.Assignment{}, f.claimErr
	}
	return f.assign, nil
}

func (f *fakeStore) Release(_ context.Context, node, id string) error {
	f.releasedNode, f.releasedID = node, id
	return f.releaseErr
}

func (f *fakeStore) Pause(context.Context, string, string) error { return nil }

func (f *fakeStore) Resume(context.Context, string, string) error { return nil }

func (f *fakeStore) Renew(context.Context, string, string, int) (time.Time, error) {
	return time.Time{}, nil
}

func (f *fakeStore) Fork(context.Context, string, string, int, int) ([]scale.Assignment, error) {
	return nil, nil
}

func (f *fakeStore) Snapshot(context.Context, string, string, string) (scale.Snapshot, error) {
	return scale.Snapshot{}, nil
}

func (f *fakeStore) Snapshots(context.Context, string) ([]scale.Snapshot, error) { return nil, nil }

func (f *fakeStore) DeleteSnapshot(context.Context, string, string) error { return nil }

func (f *fakeStore) Stats(context.Context, string, string) (scale.SandboxStats, error) {
	return scale.SandboxStats{}, nil
}

func getDetail(t *testing.T, sb sandboxv1beta1.Sandbox) SandboxDetail {
	t.Helper()
	h := newTestServer(t, &fakeStore{items: []sandboxv1beta1.Sandbox{sb}})
	w := do(t, h, http.MethodGet, "/sandboxes/sb_one", "", testKey)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var got SandboxDetail
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return got
}

type renewStore struct {
	fakeStore

	node, id string
	ttl      int
	err      error
}

func newRenewStore() *renewStore {
	s := &renewStore{}
	s.items = []sandboxv1beta1.Sandbox{liveSandbox("a", "sb_one", "node-a", "i")}
	return s
}

func (f *renewStore) Renew(_ context.Context, node, id string, ttlSeconds int) (time.Time, error) {
	if f.err != nil {
		return time.Time{}, f.err
	}
	f.node, f.id, f.ttl = node, id, ttlSeconds
	return time.Now().Add(time.Duration(ttlSeconds) * time.Second), nil
}

func newTestServer(t *testing.T, store scale.SandboxStore, opts ...func(*Options)) http.Handler {
	t.Helper()
	o := Options{Namespace: "sandboxes", Domain: testDomain, APIKeys: []string{testKey}, Log: logr.Discard()}
	for _, fn := range opts {
		fn(&o)
	}
	s, err := NewServer(store, o)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return s.Handler()
}

func do(t *testing.T, h http.Handler, method, path, body, key string) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	if key != "" {
		r.Header.Set(apiKeyHeader, key)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func liveSandbox(name, claimID, node, template string) sandboxv1beta1.Sandbox {
	sb := sandboxv1beta1.Sandbox{
		Name:              name,
		Namespace:         "sandboxes",
		CreationTimestamp: metav1.Now(),
		Labels:            map[string]string{scale.NodeLabel: node, scale.TemplateLabel: template},
		Annotations:       map[string]string{scale.ClaimIDAnnotation: claimID},
	}
	sb.Status.NodeName = node
	return sb
}
