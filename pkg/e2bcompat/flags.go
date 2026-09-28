package e2bcompat

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

// Flags are the --e2b-* command-line options of every binary that serves the surface.
type Flags struct {
	Addr           string
	Namespace      string
	Domain         string
	EnvdVersion    string
	TimeoutSeconds int
	APIKeyFile     string
	AllowAnonymous bool
	AliasesFile    string
	EnvdSecretFile string

	Builds        bool
	BuildParallel int
	BuildTimeout  time.Duration
	BuildLogLines int
	BuildDir      string
	UploadMax     int64
	BuildS3       s3Config
}

// NewFlags returns the flag defaults.
func NewFlags() *Flags {
	return &Flags{Addr: ":8080", Namespace: "default", BuildParallel: 2, BuildTimeout: 30 * time.Minute, BuildLogLines: 10000, UploadMax: 1 << 30}
}

// AddFlags registers the --e2b-* flags on fs.
func (f *Flags) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&f.Addr, "e2b-bind-address", f.Addr,
		"Address the e2b-compatible surface listens on.")
	fs.StringVar(&f.Namespace, "e2b-namespace", f.Namespace,
		"Namespace a key that names none claims in, and where anonymous claims land; e2b has no namespace concept.")
	fs.StringVar(&f.Domain, "e2b-domain", f.Domain,
		"Base domain the SDK derives the in-sandbox envd host from, as {port}-{sandboxID}.{domain}. Required whenever the surface is served: without it a created sandbox has no reachable data plane.")
	fs.StringVar(&f.EnvdVersion, "e2b-envd-version", f.EnvdVersion,
		"envd version reported to the SDK. It must name the envd actually installed in the pool's image; the SDK version-compares it and kills the sandbox when it cannot parse one.")
	fs.IntVar(&f.TimeoutSeconds, "e2b-default-timeout", f.TimeoutSeconds,
		"Lease in seconds granted to a create that names no timeout, and the lease an SDK refresh renews for.")
	fs.StringVar(&f.APIKeyFile, "e2b-api-key-file", f.APIKeyFile,
		"Path to a file (Secret mount) of accepted e2b API keys, one per line as \"key\" or \"key namespace\", presented by the SDK as X-API-KEY; a key sees only the sandboxes and snapshots of its namespace, --e2b-namespace when none is given.")
	fs.BoolVar(&f.AllowAnonymous, "e2b-allow-anonymous", f.AllowAnonymous,
		"Serve the e2b surface with NO API key. Development only: it leaves the claim endpoint open to anyone who can reach the port.")
	fs.StringVar(&f.AliasesFile, "e2b-template-alias-file", f.AliasesFile,
		"Path to a file of e2b template aliases, one per line as \"alias pool-image [size]\", so a create naming the alias (the SDK's default is \"base\") claims from that image's pool at size, small when none is given.")
	AddEnvdSecretFlag(fs, &f.EnvdSecretFile)
	fs.BoolVar(&f.Builds, "e2b-builds", f.Builds,
		"Serve the e2b template build API (Template.build). Builds run in the process that took the request and live in its memory, so every status poll must reach the same replica.")
	fs.IntVar(&f.BuildParallel, "e2b-build-parallel", f.BuildParallel,
		"Builds that run at once; a start beyond them answers 429 and the build stays waiting.")
	fs.DurationVar(&f.BuildTimeout, "e2b-build-timeout", f.BuildTimeout,
		"Bound on one build, claim through publish; it is also the lease of the build's sandbox.")
	fs.IntVar(&f.BuildLogLines, "e2b-build-log-lines", f.BuildLogLines,
		"Log lines kept per build; later lines are dropped.")
	fs.StringVar(&f.BuildDir, "e2b-build-dir", f.BuildDir,
		"Directory that keeps the archives COPY steps upload, taken through this surface's own signed PUT. Every replica must see the same directory, or use --e2b-build-store-s3-bucket.")
	fs.Int64Var(&f.UploadMax, "e2b-build-upload-max", f.UploadMax,
		"Largest archive in bytes a signed PUT to --e2b-build-dir takes.")
	fs.StringVar(&f.BuildS3.Bucket, "e2b-build-store-s3-bucket", f.BuildS3.Bucket,
		"S3 bucket that keeps the archives COPY steps upload, through presigned PUTs; credentials come from the default AWS chain.")
	fs.StringVar(&f.BuildS3.Prefix, "e2b-build-store-s3-prefix", f.BuildS3.Prefix,
		"Key prefix for the archives in --e2b-build-store-s3-bucket; they live under <prefix>/e2b-files/.")
	fs.StringVar(&f.BuildS3.Endpoint, "e2b-build-store-s3-endpoint", f.BuildS3.Endpoint,
		"Endpoint of an S3-compatible store instead of AWS.")
	fs.StringVar(&f.BuildS3.Region, "e2b-build-store-s3-region", f.BuildS3.Region,
		"Region of --e2b-build-store-s3-bucket.")
	fs.BoolVar(&f.BuildS3.ForcePathStyle, "e2b-build-store-s3-force-path-style", f.BuildS3.ForcePathStyle,
		"Address the bucket in the path, as most S3-compatible stores need.")
}

// ServerOptions reads the key and alias files, opens the build upload store, and returns the server options over inv.
func (f *Flags) ServerOptions(ctx context.Context, inv scale.InventorySource) (Options, error) {
	keys, err := fileLines(f.APIKeyFile, "api key")
	if err != nil {
		return Options{}, err
	}
	aliases, err := fileLines(f.AliasesFile, "template aliases")
	if err != nil {
		return Options{}, err
	}
	secret, err := EnvdSecret(f.EnvdSecretFile)
	if err != nil {
		return Options{}, err
	}
	builds, err := f.buildOptions(ctx, secret)
	if err != nil {
		return Options{}, err
	}
	return Options{
		Namespace:             f.Namespace,
		Domain:                f.Domain,
		EnvdVersion:           f.EnvdVersion,
		DefaultTimeoutSeconds: f.TimeoutSeconds,
		APIKeys:               keys,
		AllowAnonymous:        f.AllowAnonymous,
		Inventory:             inv,
		TemplateAliases:       aliases,
		EnvdSecret:            secret,
		Builds:                builds,
	}, nil
}

func (f *Flags) buildOptions(ctx context.Context, secret []byte) (BuildOptions, error) {
	if !f.Builds {
		return BuildOptions{}, nil
	}
	opts := BuildOptions{Parallel: f.BuildParallel, Timeout: f.BuildTimeout, LogLines: f.BuildLogLines}
	switch {
	case f.BuildParallel < 1 || f.BuildTimeout <= 0:
		return BuildOptions{}, errors.New("--e2b-builds needs --e2b-build-parallel of at least 1 and a positive --e2b-build-timeout")
	case f.BuildDir != "" && f.BuildS3.Bucket != "":
		return BuildOptions{}, errors.New("--e2b-build-dir and --e2b-build-store-s3-bucket are exclusive")
	case f.BuildDir != "":
		if err := os.MkdirAll(f.BuildDir, 0o750); err != nil {
			return BuildOptions{}, fmt.Errorf("create --e2b-build-dir: %w", err)
		}
		root, err := os.OpenRoot(f.BuildDir)
		if err != nil {
			return BuildOptions{}, fmt.Errorf("open --e2b-build-dir: %w", err)
		}
		opts.Uploads = &dirUploads{root: root, secret: secret, max: f.UploadMax}
	case f.BuildS3.Bucket != "":
		store, err := newS3Uploads(ctx, f.BuildS3)
		if err != nil {
			return BuildOptions{}, err
		}
		opts.Uploads = store
	}
	return opts, nil
}

// AddEnvdSecretFlag registers --e2b-envd-secret-file, which the e2b surface and the envd-proxy must share.
func AddEnvdSecretFlag(fs *pflag.FlagSet, path *string) {
	fs.StringVar(path, "e2b-envd-secret-file", *path,
		"Path to a file (Secret mount) holding the key every sandbox's envd access token derives from; the e2b surface and the envd-proxy must read the same one.")
}

// EnvdSecret reads the envd secret file, trimmed; an empty or missing one is an error.
func EnvdSecret(path string) ([]byte, error) {
	if path == "" {
		return nil, errors.New("--e2b-envd-secret-file is required")
	}
	b, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("read e2b envd secret file %q: %w", path, err)
	}
	if b = bytes.TrimSpace(b); len(b) == 0 {
		return nil, fmt.Errorf("e2b envd secret file %q is empty", path)
	}
	return b, nil
}

func fileLines(path, what string) ([]string, error) {
	if path == "" {
		return nil, nil
	}
	b, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("read e2b %s file %q: %w", what, path, err)
	}
	var lines []string
	for line := range strings.SplitSeq(string(b), "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			lines = append(lines, line)
		}
	}
	return lines, nil
}
