package e2bcompat

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox-operator/pkg/e2bbuild"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

const uploadURLTTL = time.Hour

// uploadKey names one COPY archive: the SDK hashes the step's sources into hash.
type uploadKey struct {
	ns, template, hash string
}

func (k uploadKey) path() string {
	return k.ns + "/" + k.template + "/" + k.hash + ".tar"
}

// uploadStore holds the archives a build's COPY steps read.
type uploadStore interface {
	present(ctx context.Context, key uploadKey) (bool, error)
	// uploadURL is where the SDK PUTs the archive and the headers that PUT carries.
	uploadURL(r *http.Request, key uploadKey) (string, map[string]string, error)
	open(ctx context.Context, key uploadKey) (io.ReadCloser, error)
}

// dirUploads keeps archives under a directory and takes them through this surface's own signed PUT.
type dirUploads struct {
	root   *os.Root
	secret []byte
	max    int64
}

// ServeHTTP takes an archive on a link fileUploadLink signed; the signature stands in for the API key the SDK does not send.
func (d *dirUploads) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	key, ok := uploadKeyOf(w, r, q.Get("ns"))
	if !ok {
		return
	}
	expires := q.Get("expires")
	if exp, err := strconv.ParseInt(expires, 10, 64); err != nil || time.Now().Unix() > exp {
		writeError(w, http.StatusUnauthorized, "the upload link has expired")
		return
	}
	if !hmac.Equal([]byte(q.Get("sig")), []byte(uploadSig(d.secret, key, expires))) {
		writeError(w, http.StatusUnauthorized, "the upload link's signature does not match")
		return
	}
	err := d.put(key, http.MaxBytesReader(w, r.Body, d.max))
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("an upload is at most %d bytes", d.max))
		return
	}
	if err != nil {
		log.WithFunc("e2bcompat.dirUploads.ServeHTTP").Errorf(r.Context(), err, "e2b upload failed template=%s hash=%s", key.template, key.hash)
		writeError(w, http.StatusInternalServerError, "failed to store the upload")
	}
}

func (d *dirUploads) present(_ context.Context, key uploadKey) (bool, error) {
	_, err := d.root.Stat(key.path())
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func (d *dirUploads) uploadURL(r *http.Request, key uploadKey) (string, map[string]string, error) {
	scheme := "http"
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
		scheme = "https"
	}
	expires := strconv.FormatInt(time.Now().Add(uploadURLTTL).Unix(), 10)
	q := url.Values{"ns": {key.ns}, "expires": {expires}, "sig": {uploadSig(d.secret, key, expires)}}
	return scheme + "://" + r.Host + "/templates/" + url.PathEscape(key.template) + "/files/" + key.hash + "?" + q.Encode(), nil, nil
}

func (d *dirUploads) open(_ context.Context, key uploadKey) (io.ReadCloser, error) {
	return d.root.Open(key.path())
}

// put writes body next to its final name and renames it there, so a reader never sees a partial archive.
func (d *dirUploads) put(key uploadKey, body io.Reader) error {
	dst := key.path()
	if err := d.root.MkdirAll(path.Dir(dst), 0o750); err != nil {
		return err
	}
	tmp := path.Dir(dst) + "/." + key.hash + "." + rand.Text()
	f, err := d.root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, err = io.Copy(f, body)
	err = errors.Join(err, f.Close())
	if err == nil {
		err = d.root.Rename(tmp, dst)
	}
	if err != nil {
		_ = d.root.Remove(tmp)
	}
	return err
}

// fileUploadLink answers the SDK's check for a COPY archive: present, or where to PUT it.
func (s *Server) fileUploadLink(w http.ResponseWriter, r *http.Request) {
	key, ok := uploadKeyOf(w, r, s.namespace(r))
	if !ok {
		return
	}
	logger := log.WithFunc("e2bcompat.fileUploadLink")
	present, err := s.opts.Builds.Uploads.present(r.Context(), key)
	if err != nil {
		logger.Errorf(r.Context(), err, "e2b upload lookup failed template=%s hash=%s", key.template, key.hash)
		writeError(w, http.StatusInternalServerError, "failed to look up the upload")
		return
	}
	if present {
		writeJSON(w, http.StatusCreated, TemplateBuildFileUpload{Present: true})
		return
	}
	link, headers, err := s.opts.Builds.Uploads.uploadURL(r, key)
	if err != nil {
		logger.Errorf(r.Context(), err, "e2b upload link failed template=%s hash=%s", key.template, key.hash)
		writeError(w, http.StatusInternalServerError, "failed to sign the upload")
		return
	}
	writeJSON(w, http.StatusCreated, TemplateBuildFileUpload{URL: link, Headers: headers})
}

// archive opens the uploads of one build's template for its COPY steps.
func (s *Server) archive(ns, name string) func(context.Context, string) (io.ReadCloser, error) {
	return func(ctx context.Context, hash string) (io.ReadCloser, error) {
		return s.opts.Builds.Uploads.open(ctx, uploadKey{ns: ns, template: name, hash: hash})
	}
}

// uploadKeyOf writes the refusal and reports false unless the path names a template and a files hash a store can key by.
func uploadKeyOf(w http.ResponseWriter, r *http.Request, ns string) (uploadKey, bool) {
	key := uploadKey{ns: ns, template: r.PathValue("templateID"), hash: r.PathValue("hash")}
	if _, err := scale.StampedName("template", templatePrefix, ns, key.template); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return uploadKey{}, false
	}
	if strings.ContainsAny(key.template, `/\`) || strings.HasPrefix(key.template, ".") || !e2bbuild.FilesHash.MatchString(key.hash) {
		writeError(w, http.StatusBadRequest, "the upload names no template and files hash")
		return uploadKey{}, false
	}
	return key, true
}

func uploadSig(secret []byte, key uploadKey, expires string) string {
	return AccessToken(secret, "e2b-upload\x00"+key.ns+"\x00"+key.template+"\x00"+key.hash+"\x00"+expires)
}
