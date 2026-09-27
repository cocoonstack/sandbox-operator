package e2bcompat

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/projecteru2/core/log"
	"github.com/spf13/pflag"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

const (
	readHeaderTimeout = 10 * time.Second
	shutdownTimeout   = 10 * time.Second
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
}

// NewFlags returns the flag defaults.
func NewFlags() *Flags {
	return &Flags{Addr: ":8080", Namespace: "default"}
}

// AddFlags registers the --e2b-* flags on fs.
func (f *Flags) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&f.Addr, "e2b-bind-address", f.Addr,
		"Address the e2b-compatible surface listens on.")
	fs.StringVar(&f.Namespace, "e2b-namespace", f.Namespace,
		"Namespace a key that names none claims in, and where anonymous claims land; e2b has no namespace concept.")
	fs.StringVar(&f.Domain, "e2b-domain", f.Domain,
		"Base domain the SDK derives the in-sandbox envd host from, as {port}-{sandboxID}.{domain}. Required: without it a created sandbox has no reachable data plane.")
	fs.StringVar(&f.EnvdVersion, "e2b-envd-version", f.EnvdVersion,
		"envd version reported to the SDK. It must name the envd actually installed in the pool's image; the SDK version-compares it and kills the sandbox when it cannot parse one.")
	fs.IntVar(&f.TimeoutSeconds, "e2b-default-timeout", f.TimeoutSeconds,
		"Lease in seconds granted to a create that names no timeout, and the lease an SDK refresh renews for.")
	fs.StringVar(&f.APIKeyFile, "e2b-api-key-file", f.APIKeyFile,
		"Path to a file (Secret mount) of accepted e2b API keys, one per line as \"key\" or \"key namespace\", presented by the SDK as X-API-KEY; a key sees only the sandboxes and snapshots of its namespace, --e2b-namespace when none is given.")
	fs.BoolVar(&f.AllowAnonymous, "e2b-allow-anonymous", f.AllowAnonymous,
		"Serve the e2b surface with NO API key. Development only: it leaves the claim endpoint open to anyone who can reach the port.")
	fs.StringVar(&f.AliasesFile, "e2b-template-aliases", f.AliasesFile,
		"Path to a file of e2b template aliases, one per line as \"alias pool-image\", so a create naming the alias (the SDK's default is \"base\") claims from that image's pool.")
}

// ServerOptions reads the key and alias files and returns the server options over inv.
func (f *Flags) ServerOptions(inv scale.InventorySource) (Options, error) {
	keys, err := fileLines(f.APIKeyFile, "api key")
	if err != nil {
		return Options{}, err
	}
	aliases, err := fileLines(f.AliasesFile, "template aliases")
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
	}, nil
}

// Serve listens on addr and serves the surface in the background; the returned stop drains it.
func (s *Server) Serve(ctx context.Context, addr string) (func(), error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on e2b address %q: %w", addr, err)
	}
	httpSrv := &http.Server{Addr: addr, Handler: s.Handler(), ReadHeaderTimeout: readHeaderTimeout}
	logger := log.WithFunc("e2bcompat.Serve")
	logger.Infof(ctx, "serving e2b-compatible API address=%s namespace=%s authenticated=%t", addr, s.opts.Namespace, len(s.keys) > 0)
	go func() {
		if err := httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error(ctx, err, "e2b-compatible API server exited")
		}
	}()
	e2bCtx, stop := context.WithCancel(ctx)
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		<-e2bCtx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		defer cancel()
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			logger.Error(ctx, err, "e2b-compatible API server shutdown")
		}
	}()
	return func() { stop(); <-drained }, nil
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
