package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/spf13/pflag"
	restclient "k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
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
