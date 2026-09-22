package main

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestRunIssuesExactlyTotal(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(sandboxv1beta1.AddToScheme(scheme))
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	o := &options{
		namespace:      "t",
		image:          "img",
		namegen:        "t",
		concurrency:    4,
		total:          25,
		timeout:        5 * time.Second,
		cleanup:        true,
		releaseTimeout: 5 * time.Second,
	}
	s := createBatch(t.Context(), cl, o, new(int64), o.total)

	if s.issued != 25 {
		t.Fatalf("issued = %d, want exactly total (25)", s.issued)
	}
	if s.created != 25 {
		t.Fatalf("created = %d, want 25 (fake client never fails)", s.created)
	}
	if s.released != 25 {
		t.Fatalf("released = %d, want 25 (cleanup must confirm every release)", s.released)
	}
	if s.leaked != 0 {
		t.Fatalf("leaked = %d, want 0", s.leaked)
	}

	list := &sandboxv1beta1.SandboxList{}
	if err := cl.List(t.Context(), list); err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.Items) != 0 {
		t.Fatalf("%d sandboxes left behind after a cleanup run, want 0", len(list.Items))
	}
}

func TestConcurrencyClampedToTotal(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(sandboxv1beta1.AddToScheme(scheme))
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	o := &options{
		namespace:      "t",
		image:          "img",
		namegen:        "t",
		concurrency:    10,
		total:          2,
		timeout:        5 * time.Second,
		cleanup:        true,
		releaseTimeout: 5 * time.Second,
	}
	s := createBatch(t.Context(), cl, o, new(int64), o.total)
	if s.issued != 2 || s.created != 2 {
		t.Fatalf("issued=%d created=%d, want exactly 2/2", s.issued, s.created)
	}
}
