package e2bcompat

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cocoonstack/sandbox-operator/pkg/e2bbuild"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

func TestAnUploadLinkIsSignedAndOnlyItsSignatureTakesThePut(t *testing.T) {
	dir := t.TempDir()
	h := newTestServer(t, &fakeStore{}, withBuilds(), withUploads(t, dir, 16))
	hash := strings.Repeat("ab", 32)

	link := uploadLink(t, h, hash)
	require.False(t, link.Present)
	u, err := url.Parse(link.URL)
	require.NoError(t, err)
	assert.Equal(t, "/templates/app/files/"+hash, u.Path)
	q := u.Query()

	sign := func(template, h, ns, expires string) url.Values {
		return url.Values{"ns": {ns}, "expires": {expires}, "sig": {uploadSig([]byte(testEnvdSecret), uploadKey{ns: ns, template: template, hash: h}, expires)}}
	}
	past := strconv.FormatInt(time.Now().Add(-time.Minute).Unix(), 10)
	other := strings.Repeat("cd", 32)
	for name, tc := range map[string]struct {
		path  string
		query url.Values
		want  int
	}{
		"expired":        {"/templates/app/files/" + hash, sign("app", hash, "sandboxes", past), http.StatusUnauthorized},
		"another hash":   {"/templates/app/files/" + other, q, http.StatusUnauthorized},
		"another name":   {"/templates/app2/files/" + hash, q, http.StatusUnauthorized},
		"another ns":     {"/templates/app/files/" + hash, url.Values{"ns": {"others"}, "expires": q["expires"], "sig": q["sig"]}, http.StatusUnauthorized},
		"no signature":   {"/templates/app/files/" + hash, url.Values{"ns": q["ns"], "expires": q["expires"]}, http.StatusUnauthorized},
		"not a hash":     {"/templates/app/files/" + strings.Repeat("z", 64), q, http.StatusBadRequest},
		"a path in name": {"/templates/a%2F..%2Fb/files/" + hash, q, http.StatusBadRequest},
	} {
		assert.Equal(t, tc.want, put(h, tc.path+"?"+tc.query.Encode(), "archive").Code, name)
	}
	assert.Equal(t, http.StatusRequestEntityTooLarge, put(h, u.RequestURI(), strings.Repeat("x", 17)).Code)
	entries, _ := os.ReadDir(filepath.Join(dir, "sandboxes", "app"))
	assert.Empty(t, entries, "a refused or oversized upload leaves nothing behind")

	require.Equal(t, http.StatusOK, put(h, u.RequestURI(), "archive").Code)
	got, err := os.ReadFile(filepath.Join(dir, "sandboxes", "app", hash+".tar"))
	require.NoError(t, err)
	assert.Equal(t, "archive", string(got))
	assert.Equal(t, TemplateBuildFileUpload{Present: true}, uploadLink(t, h, hash))
	assert.Equal(t, http.StatusNotFound, do(t, newTestServer(t, &fakeStore{}, withBuilds()), http.MethodGet, "/templates/app/files/"+hash, "", testKey).Code, "no store serves no upload link")
}

func TestACopyStepSendsItsUploadIntoTheSandbox(t *testing.T) {
	dir := t.TempDir()
	hash := strings.Repeat("ab", 32)
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "sandboxes", "app"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "sandboxes", "app", hash+".tar"), []byte("archive"), 0o600))
	store := &fakeStore{assign: scale.Assignment{SandboxName: "sb_1", Node: "n"}}
	h := newTestServer(t, store, withBuilds(), withUploads(t, dir, 1<<20), withBuildFleet(store))

	id := requestBuild(t, h, "app")
	body := `{"fromImage":"img","steps":[{"type":"COPY","args":["src","/srv/","",""],"filesHash":"` + hash + `"}]}`
	require.Equal(t, http.StatusAccepted, do(t, h, http.MethodPost, "/v2/templates/app/builds/"+id, body, testKey).Code)
	info := waitBuild(t, h, id)
	require.Equal(t, e2bbuild.StatusReady, info.Status, info)
	require.GreaterOrEqual(t, len(store.envdCalls), 2)
	assert.Equal(t, "sb_1 POST /files archive", store.envdCalls[0])
	assert.True(t, strings.HasPrefix(store.envdCalls[1], "sb_1 RUN root  archive='/tmp/"+hash+".tar'"), store.envdCalls[1])
}

func withUploads(t *testing.T, dir string, maxBytes int64) func(*Options) {
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = root.Close() })
	return func(o *Options) {
		o.Builds.Uploads = &dirUploads{root: root, secret: []byte(testEnvdSecret), max: maxBytes}
	}
}

func uploadLink(t *testing.T, h http.Handler, hash string) TemplateBuildFileUpload {
	t.Helper()
	w := do(t, h, http.MethodGet, "/templates/app/files/"+hash, "", testKey)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	var link TemplateBuildFileUpload
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &link))
	return link
}

func put(h http.Handler, target, body string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPut, target, strings.NewReader(body)))
	return w
}
