package logbridge

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/projecteru2/core/log"
)

func TestErrorKeepsTheErrorLevel(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want string
	}{
		{"nil err", nil, `"error":"shutdown timed out"`},
		{"err", errors.New("boom"), `"error":"boom"`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			buf := captureLog(t)
			New(t.Context()).WithName("klog").Error(tt.err, "shutdown timed out", "server", "e2b")
			got := buf.String()
			for _, part := range []string{`"level":"error"`, `"func":"klog"`, `"message":"shutdown timed out server=e2b"`, tt.want} {
				if !strings.Contains(got, part) {
					t.Fatalf("missing %s in %s", part, got)
				}
			}
		})
	}
}

func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	global := log.GetGlobalLogger()
	prev := *global
	var buf bytes.Buffer
	*global = prev.Output(&buf)
	t.Cleanup(func() { *global = prev })
	return &buf
}
