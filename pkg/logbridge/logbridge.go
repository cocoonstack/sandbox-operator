// Package logbridge forwards logr output from controller-runtime and klog into core/log.
package logbridge

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/go-logr/logr"
	"github.com/projecteru2/core/log"
)

// New returns a logr.Logger that writes through core/log under ctx.
func New(ctx context.Context) logr.Logger { return logr.New(&sink{ctx: ctx}) }

type sink struct {
	ctx  context.Context
	name string
	kv   []any
}

func (s *sink) Init(logr.RuntimeInfo) {}

func (s *sink) Enabled(level int) bool { return level == 0 }

func (s *sink) Info(_ int, msg string, kvs ...any) {
	log.WithFunc(s.funcName()).Info(s.ctx, s.line(msg, kvs))
}

func (s *sink) Error(err error, msg string, kvs ...any) {
	if err == nil {
		// core/log drops a nil err, and logr and klog.Errorf pass nil for an error with no value.
		err = errors.New(msg)
	}
	log.WithFunc(s.funcName()).Error(s.ctx, err, s.line(msg, kvs))
}

func (s *sink) WithValues(kvs ...any) logr.LogSink {
	next := *s
	next.kv = slices.Concat(s.kv, kvs)
	return &next
}

func (s *sink) WithName(name string) logr.LogSink {
	next := *s
	next.name = name
	if s.name != "" {
		next.name = s.name + "." + name
	}
	return &next
}

func (s *sink) funcName() string {
	return cmp.Or(s.name, "controller-runtime")
}

func (s *sink) line(msg string, kvs []any) string {
	pairs := slices.Concat(s.kv, kvs)
	if len(pairs) == 0 {
		return msg
	}
	var b strings.Builder
	b.WriteString(msg)
	for i := 0; i+1 < len(pairs); i += 2 {
		fmt.Fprintf(&b, " %v=%v", pairs[i], pairs[i+1])
	}
	return b.String()
}
