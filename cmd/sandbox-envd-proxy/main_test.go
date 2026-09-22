package main

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestServeOnDrainsInFlightRequestsBeforeReturning(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusNoContent)
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- serveOn(ctx, &options{Addr: ln.Addr().String()}, ln, h) }()
	status := make(chan int, 1)
	go func() {
		resp, getErr := http.Get("http://" + ln.Addr().String() + "/files")
		if getErr != nil {
			status <- 0
			return
		}
		_ = resp.Body.Close()
		status <- resp.StatusCode
	}()

	<-entered
	cancel()
	select {
	case serveErr := <-served:
		t.Fatalf("serveOn returned (%v) with a request still in flight", serveErr)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if serveErr := <-served; serveErr != nil {
		t.Fatalf("serveOn: %v", serveErr)
	}
	if got := <-status; got != http.StatusNoContent {
		t.Fatalf("in-flight request answered %d, want 204", got)
	}
}
