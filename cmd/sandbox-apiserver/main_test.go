package main

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/spf13/pflag"
	"k8s.io/apiserver/pkg/authorization/authorizerfactory"
	genericapiserver "k8s.io/apiserver/pkg/server"
	restclient "k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
	sandboxapiserver "github.com/cocoonstack/sandbox-operator/pkg/scale/apiserver"
)

func TestServerConfigAppliesTheFeatureFlags(t *testing.T) {
	for _, tt := range []struct {
		args        []string
		profiling   bool
		debugSocket string
		wantErr     bool
	}{
		{profiling: true},
		{args: []string{"--profiling=false"}},
		{args: []string{"--debug-socket-path=/run/sandbox-apiserver/debug.sock"}, profiling: true, debugSocket: "/run/sandbox-apiserver/debug.sock"},
		{args: []string{"--enable-priority-and-fairness"}, wantErr: true},
	} {
		o := newOptions()
		fs := pflag.NewFlagSet("sandbox-apiserver", pflag.ContinueOnError)
		o.addFlags(fs)
		if err := fs.Parse(append([]string{"--secure-port=0"}, tt.args...)); err != nil {
			t.Fatalf("parse %v: %v", tt.args, err)
		}
		cfg, err := o.serverConfig()
		if tt.wantErr {
			if err == nil {
				t.Errorf("%v: serverConfig accepted a flag it cannot honor", tt.args)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%v: serverConfig: %v", tt.args, err)
		}
		if cfg.EnableProfiling != tt.profiling || cfg.DebugSocketPath != tt.debugSocket {
			t.Errorf("%v: profiling=%v debug socket=%q, want %v and %q", tt.args, cfg.EnableProfiling, cfg.DebugSocketPath, tt.profiling, tt.debugSocket)
		}
	}
}

func TestRunRestartingRebuildsTheDriverAfterALostLease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		var builds atomic.Int32
		build := func() (manager.Runnable, error) {
			n := builds.Add(1)
			return manager.RunnableFunc(func(ctx context.Context) error {
				if n <= 2 {
					return errors.New("leader election lost")
				}
				<-ctx.Done()
				return nil
			}), nil
		}
		first, _ := build()
		done := make(chan struct{})
		go func() {
			runRestarting(ctx, first, build, driverRestartDelay)
			close(done)
		}()
		time.Sleep(driverRestartDelay - time.Millisecond)
		synctest.Wait()
		if got := builds.Load(); got != 1 {
			t.Fatalf("builds before the restart delay = %d, want 1", got)
		}
		time.Sleep(driverRestartDelay + 2*time.Millisecond)
		synctest.Wait()
		if got := builds.Load(); got != 3 {
			t.Fatalf("builds after two lost leases = %d, want 3", got)
		}
		cancel()
		<-done
	})
}

func TestRunRestartingRetriesAFailedRebuild(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var attempts atomic.Int32
		build := func() (manager.Runnable, error) {
			if attempts.Add(1) == 1 {
				return nil, errors.New("apiserver unreachable")
			}
			return manager.RunnableFunc(func(ctx context.Context) error {
				<-ctx.Done()
				return nil
			}), nil
		}
		lost := manager.RunnableFunc(func(context.Context) error { return errors.New("leader election lost") })
		go runRestarting(t.Context(), lost, build, driverRestartDelay)
		time.Sleep(2*driverRestartDelay + time.Millisecond)
		synctest.Wait()
		if got := attempts.Load(); got != 2 {
			t.Fatalf("rebuild attempts = %d, want a retry after the failed one", got)
		}
	})
}

func TestStartWarmPoolDriverBuildsAgainInTheSameProcess(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for i := range 3 {
		if err := startWarmPoolDriver(ctx, &restclient.Config{Host: "http://127.0.0.1:1"}, "", 0, nil); err != nil {
			t.Fatalf("build %d: %v", i+1, err)
		}
	}
}

func TestShutdownClosesAnOpenWatchPromptly(t *testing.T) {
	o := newOptions()
	fs := pflag.NewFlagSet("sandbox-apiserver", pflag.ContinueOnError)
	o.addFlags(fs)
	if err := fs.Parse([]string{"--bind-address=127.0.0.1"}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	o.SecureServing.Listener = ln
	o.SecureServing.ServerCert.CertDirectory, o.SecureServing.ServerCert.PairName = "", ""
	cfg, err := o.serverConfig()
	if err != nil {
		t.Fatalf("serverConfig: %v", err)
	}
	cfg.Authorization.Authorizer = authorizerfactory.NewAlwaysAllowAuthorizer()
	server, err := cfg.Complete(nil).New("sandbox-apiserver", genericapiserver.NewEmptyDelegate())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if err := sandboxapiserver.InstallSandboxAPI(server, scale.NewScatterGatherStore(scale.NewStaticInventorySource())); err != nil {
		t.Fatalf("install: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan error, 1)
	go func() { stopped <- server.PrepareRun().RunWithContext(ctx) }()

	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	url := "https://" + cfg.SecureServing.Listener.Addr().String() + "/apis/agents.x-k8s.io/v1beta1/namespaces/default/sandboxes?watch=true"
	closed := make(chan struct{})
	go func(body io.ReadCloser) {
		defer body.Close()
		_, _ = io.Copy(io.Discard, body)
		close(closed)
	}(openWatch(t, hc, url))

	start := time.Now()
	cancel()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("RunWithContext still running 10 s after the stop with a watch open")
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("shutdown with an open watch took %v, want under 5 s", took)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Error("the watch client never saw the stream close")
	}
}

func openWatch(t *testing.T, hc *http.Client, url string) io.ReadCloser {
	t.Helper()
	for range 50 {
		resp, err := hc.Get(url)
		if err == nil && resp.StatusCode == http.StatusOK {
			return resp.Body
		}
		if err == nil {
			resp.Body.Close()
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("the watch never answered 200")
	return nil
}
