package main

import (
	"testing"

	"github.com/spf13/pflag"
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
