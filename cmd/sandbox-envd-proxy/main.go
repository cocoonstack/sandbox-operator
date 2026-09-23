// Command sandbox-envd-proxy is the edge data plane for the e2b-compatible
// surface. It is the one public entry point an unmodified e2b SDK reaches for
// files, commands and pty traffic: it resolves the sandbox a request names from
// node inventory and relays the bytes through the owning node's guest-port
// endpoint.
//
// It runs as its own Deployment, never on a compute node — no client learns a
// node address, and a node's sandboxd stays the only thing holding sandboxes.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/spf13/pflag"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/cocoonstack/sandbox-operator/pkg/envdproxy"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
	"github.com/cocoonstack/sandbox-operator/version"
)

const (
	readHeaderTimeout = 10 * time.Second
	// shutdownTimeout bounds the graceful drain. Data-plane streams are long by
	// design, so this is a floor on restart latency, not a wait for idleness.
	shutdownTimeout = 10 * time.Second
)

type options struct {
	Addr       string
	Domain     string
	Namespace  string
	CertFile   string
	KeyFile    string
	GuestHTTP2 bool
}

func (o *options) addFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.Addr, "bind-address", o.Addr, "Address the proxy listens on.")
	fs.StringVar(&o.Domain, "domain", o.Domain,
		"Base domain sandbox hosts are derived from, as {port}-{sandboxID}.{domain}. Must match the apiserver's --e2b-domain.")
	fs.StringVar(&o.Namespace, "namespace", o.Namespace,
		"Namespace inventory lookups are filtered to; empty matches every namespace. Not an access boundary: a caller holding a sandbox's token reaches it in any namespace.")
	fs.StringVar(&o.CertFile, "tls-cert-file", o.CertFile,
		"Wildcard certificate for *.{domain}. Omit to serve cleartext h2c behind an edge that terminates TLS.")
	fs.StringVar(&o.KeyFile, "tls-private-key-file", o.KeyFile, "Private key for --tls-cert-file.")
	fs.BoolVar(&o.GuestHTTP2, "guest-http2", o.GuestHTTP2,
		"Forward to the guest over cleartext HTTP/2. Off by default: envd 0.8.0 installs no h2c handler and refuses it. Clients still reach this proxy over HTTP/2.")
}

func main() {
	ctx := ctrl.SetupSignalHandler()
	o := &options{Addr: ":8443", Namespace: "default"}
	fs := pflag.NewFlagSet("sandbox-envd-proxy", pflag.ExitOnError)
	o.addFlags(fs)
	if err := fs.Parse(os.Args[1:]); err != nil {
		klog.ErrorS(err, "parse flags")
		os.Exit(1)
	}
	if err := run(ctx, o); err != nil {
		klog.ErrorS(err, "sandbox-envd-proxy exited")
		os.Exit(1)
	}
}

func run(ctx context.Context, o *options) error {
	if (o.CertFile == "") != (o.KeyFile == "") {
		return errors.New("--tls-cert-file and --tls-private-key-file must be set together")
	}
	klog.InfoS("starting sandbox-envd-proxy", "version", version.VERSION, "revision", version.REVISION, "builtAt", version.BUILTAT)
	restCfg, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("load kube config: %w", err)
	}
	reader, err := scale.NewInventoryCache(ctx, restCfg)
	if err != nil {
		return err
	}
	inv := scale.NewClientInventorySource(reader)
	resolver, err := envdproxy.NewResolver(scale.NewScatterGatherStore(inv), inv, o.Namespace)
	if err != nil {
		return err
	}
	srv, err := envdproxy.NewServer(resolver, envdproxy.Options{
		Domain:     o.Domain,
		GuestHTTP2: o.GuestHTTP2,
		Log:        ctrl.Log.WithName("envd-proxy"),
	})
	if err != nil {
		return err
	}
	return serve(ctx, o, srv.Handler())
}

func serve(ctx context.Context, o *options, h http.Handler) error {
	ln, err := net.Listen("tcp", o.Addr)
	if err != nil {
		return fmt.Errorf("listen on %q: %w", o.Addr, err)
	}
	return serveOn(ctx, o, ln, h)
}

func serveOn(ctx context.Context, o *options, ln net.Listener, h http.Handler) error {
	httpSrv := &http.Server{
		Addr:              o.Addr,
		Handler:           h,
		Protocols:         envdproxy.Protocols(),
		ReadHeaderTimeout: readHeaderTimeout,
	}
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		defer cancel()
		if shutdownErr := httpSrv.Shutdown(shutdownCtx); shutdownErr != nil {
			klog.ErrorS(shutdownErr, "envd-proxy shutdown")
		}
	}()
	klog.InfoS("serving envd-proxy", "address", o.Addr, "domain", o.Domain, "tls", o.CertFile != "")
	var err error
	if o.CertFile != "" {
		httpSrv.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		err = httpSrv.ServeTLS(ln, o.CertFile, o.KeyFile)
	} else {
		err = httpSrv.Serve(ln)
	}
	if errors.Is(err, http.ErrServerClosed) {
		<-drained
		return nil
	}
	return err
}
