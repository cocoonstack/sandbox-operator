package e2bcompat

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

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
	secret, err := EnvdSecret(f.EnvdSecretFile)
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
	}, nil
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
