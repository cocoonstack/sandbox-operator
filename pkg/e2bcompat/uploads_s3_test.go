package e2bcompat

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3UploadsPresignAPutAndReadTheObjectBack(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
	hash := strings.Repeat("ab", 32)
	object := "/bucket/root/e2b-files/sandboxes/app/" + hash + ".tar"
	var stored atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path != object:
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodHead && !stored.Load():
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodGet:
			_, _ = io.WriteString(w, "archive")
		}
	}))
	t.Cleanup(srv.Close)
	u, err := newS3Uploads(t.Context(), s3Config{Bucket: "bucket", Prefix: "root", Endpoint: srv.URL, Region: "us-east-1", ForcePathStyle: true})
	require.NoError(t, err)
	key := uploadKey{ns: "sandboxes", template: "app", hash: hash}

	present, err := u.present(t.Context(), key)
	require.NoError(t, err)
	assert.False(t, present)
	link, _, err := u.uploadURL(httptest.NewRequest(http.MethodGet, "/", nil), key)
	require.NoError(t, err)
	parsed, err := url.Parse(link)
	require.NoError(t, err)
	assert.Equal(t, object, parsed.Path)
	assert.NotEmpty(t, parsed.Query().Get("X-Amz-Signature"))
	assert.Equal(t, "3600", parsed.Query().Get("X-Amz-Expires"))

	stored.Store(true)
	present, err = u.present(t.Context(), key)
	require.NoError(t, err)
	assert.True(t, present)
	body, err := u.open(t.Context(), key)
	require.NoError(t, err)
	got, _ := io.ReadAll(body)
	_ = body.Close()
	assert.Equal(t, "archive", string(got))
}
