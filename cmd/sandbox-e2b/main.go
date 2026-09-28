// Command sandbox-e2b serves the e2b-compatible API on a sandboxd mesh with no Kubernetes, and the envd data plane when --envd-proxy-bind-address is set.
package main

import (
	"cmp"
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/projecteru2/core/log"
	"github.com/projecteru2/core/types"
	"github.com/spf13/pflag"

	"github.com/cocoonstack/sandbox-operator/pkg/e2bcompat"
	"github.com/cocoonstack/sandbox-operator/pkg/envdproxy"
	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
	"github.com/cocoonstack/sandbox-operator/pkg/scale/meshinventory"
	"github.com/cocoonstack/sandbox-operator/version"
)

type options struct {
	Seeds             []string
	SandboxdToken     string
	SandboxdTokenFile string
	PollInterval      time.Duration
	E2B               *e2bcompat.Flags
	Proxy             envdproxy.Flags
}

func (o *options) addFlags(fs *pflag.FlagSet) {
	fs.StringSliceVar(&o.Seeds, "sandboxd-seeds", o.Seeds,
		"Comma-separated sandboxd addresses dialed at start, each naming one node; each node reports its own advertise_addr as its key, and the rest of the mesh is found through their gossip.")
	sandboxd.AddTokenFlags(fs, &o.SandboxdToken, &o.SandboxdTokenFile)
	fs.DurationVar(&o.PollInterval, "inventory-poll", o.PollInterval,
		"How often every node's info and sandbox list are read; List and Watch lag a change by up to one tick.")
	o.E2B.AddFlags(fs)
	o.Proxy.AddFlags(fs, "envd-proxy-")
}

func run() error {
	o := &options{PollInterval: 10 * time.Second, E2B: e2bcompat.NewFlags()}
	fs := pflag.NewFlagSet("sandbox-e2b", pflag.ExitOnError)
	o.addFlags(fs)
	_ = fs.Parse(os.Args[1:])
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	level := cmp.Or(os.Getenv("OPERATOR_LOG_LEVEL"), "info")
	if err := log.SetupLog(ctx, &types.ServerLogConfig{Level: level, UseJSON: !stderrIsTerminal()}, ""); err != nil {
		return fmt.Errorf("setup log: %w", err)
	}
	logger := log.WithFunc("main.run")
	logger.Infof(ctx, "starting sandbox-e2b version=%s revision=%s builtAt=%s", version.VERSION, version.REVISION, version.BUILTAT)

	token, err := sandboxd.TokenFrom(o.SandboxdToken, o.SandboxdTokenFile)
	if err != nil {
		return err
	}
	hc := scale.NewSandboxdHTTPClient()
	src, err := meshinventory.New(ctx, func(addr string) meshinventory.NodeReader {
		return sandboxd.New(scale.SandboxdBaseURL(addr), token, sandboxd.WithHTTPClient(hc))
	}, meshinventory.Options{Seeds: o.Seeds, PollInterval: o.PollInterval})
	if err != nil {
		return err
	}
	nodes, err := src.ListNodes(ctx)
	if err != nil {
		return err
	}
	logger.Infof(ctx, "mesh inventory up seeds=%v nodes=%v poll=%s", o.Seeds, nodes, o.PollInterval)
	store := scale.NewScatterGatherStore(src, scale.WithClaimRouting(token, scale.NewSandboxdClientFactory()))

	opts, err := o.E2B.ServerOptions(ctx, src)
	if err != nil {
		return err
	}
	srv, err := e2bcompat.NewServer(store, opts)
	if err != nil {
		return err
	}
	stopE2B, err := srv.Serve(ctx, o.E2B.Addr)
	if err != nil {
		return err
	}
	defer stopE2B()
	if o.Proxy.Addr == "" {
		<-ctx.Done()
		return nil
	}
	resolver, err := envdproxy.NewResolver(store, src, "", opts.EnvdSecret)
	if err != nil {
		return err
	}
	proxy, err := envdproxy.NewServer(resolver, envdproxy.Options{Domain: o.E2B.Domain, GuestHTTP2: o.Proxy.GuestHTTP2, NodeToken: token})
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", o.Proxy.Addr)
	if err != nil {
		return fmt.Errorf("listen on envd-proxy address %q: %w", o.Proxy.Addr, err)
	}
	return o.Proxy.Serve(ctx, ln, proxy.Handler())
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "sandbox-e2b:", err)
		os.Exit(1)
	}
}

func stderrIsTerminal() bool {
	fi, err := os.Stderr.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}
