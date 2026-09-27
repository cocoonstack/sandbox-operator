package envdproxy

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/projecteru2/core/log"
	"github.com/spf13/pflag"
)

const (
	readHeaderTimeout = 10 * time.Second
	// data-plane streams are long, so this bounds restart latency and does not wait for idleness
	shutdownTimeout = 10 * time.Second
)

// Flags are the proxy listener's command-line options of every binary that serves it.
type Flags struct {
	Addr       string
	CertFile   string
	KeyFile    string
	GuestHTTP2 bool
}

// AddFlags registers the listener flags on fs, each name behind prefix.
func (f *Flags) AddFlags(fs *pflag.FlagSet, prefix string) {
	fs.StringVar(&f.Addr, prefix+"bind-address", f.Addr, "Address the proxy listens on.")
	fs.StringVar(&f.CertFile, prefix+"tls-cert-file", f.CertFile,
		"Wildcard certificate for *.{domain}. Omit to serve cleartext h2c behind an edge that terminates TLS.")
	fs.StringVar(&f.KeyFile, prefix+"tls-private-key-file", f.KeyFile, "Private key for --"+prefix+"tls-cert-file.")
	fs.BoolVar(&f.GuestHTTP2, prefix+"guest-http2", f.GuestHTTP2,
		"Forward to the guest over cleartext HTTP/2. Off by default: envd 0.8.0 installs no h2c handler and refuses it. Clients still reach this proxy over HTTP/2.")
}

// Serve serves h on ln until ctx ends, over TLS when a certificate is set, then drains it.
func (f *Flags) Serve(ctx context.Context, ln net.Listener, h http.Handler) error {
	if (f.CertFile == "") != (f.KeyFile == "") {
		return errors.New("the proxy's TLS certificate and private key must be set together")
	}
	httpSrv := &http.Server{
		Addr:              f.Addr,
		Handler:           h,
		Protocols:         Protocols(),
		ReadHeaderTimeout: readHeaderTimeout,
	}
	logger := log.WithFunc("envdproxy.Serve")
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		defer cancel()
		if shutdownErr := httpSrv.Shutdown(shutdownCtx); shutdownErr != nil {
			logger.Error(ctx, shutdownErr, "envd-proxy shutdown")
		}
	}()
	logger.Infof(ctx, "serving envd-proxy address=%s tls=%t", ln.Addr(), f.CertFile != "")
	var err error
	if f.CertFile != "" {
		httpSrv.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		err = httpSrv.ServeTLS(ln, f.CertFile, f.KeyFile)
	} else {
		err = httpSrv.Serve(ln)
	}
	if errors.Is(err, http.ErrServerClosed) {
		<-drained
		return nil
	}
	return err
}
