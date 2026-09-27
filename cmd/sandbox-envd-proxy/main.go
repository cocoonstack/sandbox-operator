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
	"cmp"
	"context"
	"fmt"
	"net"
	"os"

	"github.com/projecteru2/core/log"
	"github.com/projecteru2/core/types"
	"github.com/spf13/pflag"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/cocoonstack/sandbox-operator/pkg/envdproxy"
	"github.com/cocoonstack/sandbox-operator/pkg/logbridge"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
	"github.com/cocoonstack/sandbox-operator/pkg/scale/kubeinventory"
	"github.com/cocoonstack/sandbox-operator/version"
)

type options struct {
	Domain    string
	Namespace string
	Proxy     envdproxy.Flags
}

func (o *options) addFlags(fs *pflag.FlagSet) {
	o.Proxy.AddFlags(fs, "")
	fs.StringVar(&o.Domain, "domain", o.Domain,
		"Base domain sandbox hosts are derived from, as {port}-{sandboxID}.{domain}. Must match the apiserver's --e2b-domain.")
	fs.StringVar(&o.Namespace, "namespace", o.Namespace,
		"Namespace inventory lookups are filtered to; empty matches every namespace. Not an access boundary: a caller holding a sandbox's token reaches it in any namespace.")
}

func main() {
	ctx := ctrl.SetupSignalHandler()
	level := cmp.Or(os.Getenv("OPERATOR_LOG_LEVEL"), "info")
	if err := log.SetupLog(ctx, &types.ServerLogConfig{Level: level, UseJSON: !stderrIsTerminal()}, ""); err != nil {
		fmt.Fprintf(os.Stderr, "setup log: %v\n", err)
		os.Exit(1)
	}
	ctrl.SetLogger(logbridge.New(ctx))
	klog.SetLogger(logbridge.New(ctx).WithName("klog"))
	o := &options{Namespace: "default", Proxy: envdproxy.Flags{Addr: ":8443"}}
	fs := pflag.NewFlagSet("sandbox-envd-proxy", pflag.ExitOnError)
	o.addFlags(fs)
	_ = fs.Parse(os.Args[1:])
	if err := run(ctx, o); err != nil {
		log.WithFunc("main").Fatalf(ctx, err, "sandbox-envd-proxy exited")
	}
}

func run(ctx context.Context, o *options) error {
	log.WithFunc("main.run").Infof(ctx, "starting sandbox-envd-proxy version=%s revision=%s builtAt=%s domain=%s", version.VERSION, version.REVISION, version.BUILTAT, o.Domain)
	restCfg, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("load kube config: %w", err)
	}
	reader, err := kubeinventory.NewCache(ctx, restCfg)
	if err != nil {
		return err
	}
	inv := kubeinventory.New(reader)
	resolver, err := envdproxy.NewResolver(scale.NewScatterGatherStore(inv), inv, o.Namespace)
	if err != nil {
		return err
	}
	srv, err := envdproxy.NewServer(resolver, envdproxy.Options{
		Domain:     o.Domain,
		GuestHTTP2: o.Proxy.GuestHTTP2,
	})
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", o.Proxy.Addr)
	if err != nil {
		return fmt.Errorf("listen on %q: %w", o.Proxy.Addr, err)
	}
	return o.Proxy.Serve(ctx, ln, srv.Handler())
}

func stderrIsTerminal() bool {
	fi, err := os.Stderr.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}
