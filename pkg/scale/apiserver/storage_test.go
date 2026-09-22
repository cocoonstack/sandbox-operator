package apiserver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"

	cocoonv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

func TestDelete_ReleasesByClaimIDAnnotation(t *testing.T) {
	sb := &sandboxv1beta1.Sandbox{
		Namespace:   "ns",
		Name:        "s1",
		Annotations: map[string]string{ClaimIDAnnotation: "sb_abc123"},
		Status:      sandboxv1beta1.SandboxStatus{NodeName: "n1"},
	}
	store := &fakeStore{getSandbox: sb}
	r := NewSandboxREST(store).(*sandboxREST)

	obj, ok, err := r.Delete(nsCtx(t, "ns"), "s1", nil, &metav1.DeleteOptions{})
	require.NoError(t, err)
	assert.True(t, ok)
	assert.NotNil(t, obj)
	assert.True(t, store.released, "expected the node-local claim to be released")
	assert.Equal(t, "n1", store.releaseNode)
	assert.Equal(t, "sb_abc123", store.releaseID, "must release by the sandboxd claim id, not the k8s name")
}

func TestDelete_FailsLoudWithoutClaimID(t *testing.T) {
	sb := &sandboxv1beta1.Sandbox{
		Namespace: "ns", Name: "s1",
		Status: sandboxv1beta1.SandboxStatus{NodeName: "n1"},
	}
	store := &fakeStore{getSandbox: sb}
	r := NewSandboxREST(store).(*sandboxREST)

	_, ok, err := r.Delete(nsCtx(t, "ns"), "s1", nil, &metav1.DeleteOptions{})
	require.Error(t, err)
	assert.False(t, ok)
	assert.True(t, apierrors.IsInternalError(err), "expected an internal error, got %v", err)
	assert.False(t, store.released, "must not release when the sandboxd claim id is unknown")
}

func TestCreate_RejectsDryRunWithoutClaiming(t *testing.T) {
	store := &fakeStore{}
	r := NewSandboxREST(store).(*sandboxREST)
	obj, err := r.Create(nsCtx(t, "ns"), submittedSandbox("s1", nil), nil, &metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	require.True(t, apierrors.IsBadRequest(err), "expected BadRequest, got %v", err)
	assert.Nil(t, obj)
	assert.Zero(t, store.claimCalls)
}

func TestDelete_RejectsDryRunWithoutReleasing(t *testing.T) {
	store := &fakeStore{getSandbox: &sandboxv1beta1.Sandbox{
		Name:        "s1",
		Namespace:   "ns",
		Annotations: map[string]string{ClaimIDAnnotation: "sb_1"},
		Status:      sandboxv1beta1.SandboxStatus{NodeName: "n1"},
	}}
	r := NewSandboxREST(store).(*sandboxREST)
	obj, deleted, err := r.Delete(nsCtx(t, "ns"), "s1", nil, &metav1.DeleteOptions{DryRun: []string{metav1.DryRunAll}})
	require.True(t, apierrors.IsBadRequest(err), "expected BadRequest, got %v", err)
	assert.Nil(t, obj)
	assert.False(t, deleted)
	assert.Zero(t, store.getCalls)
	assert.False(t, store.released)
}

func TestLifecycleVerbs_RejectDryRunBeforeStoreAccess(t *testing.T) {
	store := &fakeStore{}
	for name, storage := range map[string]rest.Storage{
		"pause":    NewSandboxPauseREST(store),
		"resume":   NewSandboxResumeREST(store),
		"fork":     NewSandboxForkREST(store),
		"snapshot": NewSandboxSnapshotREST(store),
	} {
		t.Run(name, func(t *testing.T) {
			obj, err := storage.(*lifecycleREST).Create(nsCtx(t, "ns"), "s1", storage.New(), nil, &metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
			require.True(t, apierrors.IsBadRequest(err), "expected BadRequest, got %v", err)
			assert.Nil(t, obj)
			assert.Zero(t, store.getCalls)
		})
	}
}

func TestTTLSecondsForSandbox(t *testing.T) {
	now := time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC)
	for name, tc := range map[string]struct {
		shutdown time.Time
		ttl      string
		want     int
		wantErr  bool
	}{
		"shutdownTime":                        {shutdown: now.Add(90 * time.Second), want: 90},
		"sub-second rounds up":                {shutdown: now.Add(90*time.Second + 500*time.Millisecond), want: 91},
		"annotation":                          {ttl: "120", want: 120},
		"spec wins over annotation":           {shutdown: now.Add(time.Hour), ttl: "10", want: 3600},
		"explicit zero asks the node default": {ttl: "0"},
		"no lifetime asks the node default":   {},
		"expired shutdownTime":                {shutdown: now, wantErr: true},
		"malformed annotation":                {ttl: "banana", wantErr: true},
		"negative annotation":                 {ttl: "-5", wantErr: true},
	} {
		t.Run(name, func(t *testing.T) {
			sb := submittedSandbox("s", nil)
			if tc.ttl != "" {
				sb.Annotations = map[string]string{TTLSecondsAnnotation: tc.ttl}
			}
			if !tc.shutdown.IsZero() {
				sb.Spec.ShutdownTime = &metav1.Time{Time: tc.shutdown}
			}
			got, err := ttlSecondsForSandbox(sb, now)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestCreate_TTLRidesTheClaim(t *testing.T) {
	f := &fakeStore{claimAssign: scale.Assignment{SandboxName: "sb_1", Node: "n1"}}
	r := NewSandboxREST(f).(*sandboxREST)

	_, err := r.Create(nsCtx(t, "ns"), submittedSandbox("s1", map[string]string{TTLSecondsAnnotation: "120"}), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, 120, f.claimTTL, "the derived lease must reach the store")
}

func TestCreate_RejectsUnusableLifetime(t *testing.T) {
	f := &fakeStore{}
	r := NewSandboxREST(f).(*sandboxREST)

	_, err := r.Create(nsCtx(t, "ns"), submittedSandbox("s1", map[string]string{TTLSecondsAnnotation: "banana"}), nil, nil)
	require.Error(t, err)
	assert.True(t, apierrors.IsBadRequest(err), "expected BadRequest, got %v", err)
	assert.Equal(t, 0, f.claimCalls, "no claim may be spent on a rejected request")
}

func TestCreate_ReportsGrantedDeadline(t *testing.T) {
	deadline := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	f := &fakeStore{claimAssign: scale.Assignment{SandboxName: "sb_1", Node: "n1", Deadline: deadline}}
	r := NewSandboxREST(f).(*sandboxREST)

	obj, err := r.Create(nsCtx(t, "ns"), submittedSandbox("s1", map[string]string{TTLSecondsAnnotation: "999999"}), nil, nil)
	require.NoError(t, err)
	out, ok := obj.(*sandboxv1beta1.Sandbox)
	require.True(t, ok)
	assert.Equal(t, "2026-08-17T12:00:00Z", out.Annotations[DeadlineAnnotation], "the node-granted deadline, not the caller's ask")
	assert.Nil(t, out.Spec.ShutdownTime, "the submitted spec is echoed, not rewritten")
}

func TestLifecycleVerbs_NodeUnknownSandboxIsNotFound(t *testing.T) {
	sb := &sandboxv1beta1.Sandbox{
		Namespace:   "ns",
		Name:        "s1",
		Annotations: map[string]string{ClaimIDAnnotation: "sb_abc123"},
		Status:      sandboxv1beta1.SandboxStatus{NodeName: "n1"},
	}
	gone := apierrors.NewNotFound(sandboxv1beta1.Resource("sandboxes"), "sb_abc123")
	store := &fakeStore{getSandbox: sb, verbErr: gone}
	for name, tc := range map[string]struct {
		storage rest.Storage
		body    runtime.Object
	}{
		"pause":    {NewSandboxPauseREST(store), &cocoonv1beta1.SandboxPauseOptions{}},
		"resume":   {NewSandboxResumeREST(store), &cocoonv1beta1.SandboxResumeOptions{}},
		"fork":     {NewSandboxForkREST(store), &cocoonv1beta1.SandboxForkOptions{}},
		"snapshot": {NewSandboxSnapshotREST(store), &cocoonv1beta1.SandboxSnapshotOptions{}},
	} {
		_, err := tc.storage.(*lifecycleREST).Create(nsCtx(t, "ns"), "s1", tc.body, nil, &metav1.CreateOptions{})
		require.Error(t, err, name)
		assert.True(t, apierrors.IsNotFound(err), "%s: the node's 404 must stay a NotFound, got %v", name, err)
	}
}

type fakeStore struct {
	getSandbox *sandboxv1beta1.Sandbox
	getErr     error
	getCalls   int
	verbErr    error

	claimCalls  int
	claimTTL    int
	claimAssign scale.Assignment

	released    bool
	releaseNode string
	releaseID   string
	releaseErr  error
}

func (f *fakeStore) List(context.Context, scale.ListOptions) (*sandboxv1beta1.SandboxList, error) {
	return &sandboxv1beta1.SandboxList{}, nil
}

func (f *fakeStore) Get(context.Context, string, string) (*sandboxv1beta1.Sandbox, error) {
	f.getCalls++
	return f.getSandbox, f.getErr
}

func (f *fakeStore) Watch(context.Context, scale.ListOptions) (watch.Interface, error) {
	return watch.NewFake(), nil
}

func (f *fakeStore) Claim(_ context.Context, _, _ string, _ scale.PoolKey, ttlSeconds int) (scale.Assignment, error) {
	f.claimCalls++
	f.claimTTL = ttlSeconds
	return f.claimAssign, nil
}

func (f *fakeStore) Release(_ context.Context, node, id string) error {
	f.released, f.releaseNode, f.releaseID = true, node, id
	return f.releaseErr
}

func (f *fakeStore) Pause(context.Context, string, string) error { return f.verbErr }

func (f *fakeStore) Resume(context.Context, string, string) error { return f.verbErr }

func (f *fakeStore) Renew(context.Context, string, string, int) (time.Time, error) {
	return time.Time{}, f.verbErr
}

func (f *fakeStore) Fork(context.Context, string, string, int, int) ([]scale.Assignment, error) {
	return nil, f.verbErr
}

func (f *fakeStore) Snapshot(context.Context, string, string, string) (scale.Snapshot, error) {
	return scale.Snapshot{}, f.verbErr
}

func (f *fakeStore) Snapshots(context.Context, string) ([]scale.Snapshot, error) { return nil, nil }

func (f *fakeStore) DeleteSnapshot(context.Context, string, string) error { return nil }

func (f *fakeStore) Stats(context.Context, string, string) (scale.SandboxStats, error) {
	return scale.SandboxStats{}, nil
}

func nsCtx(t *testing.T, ns string) context.Context {
	return genericapirequest.WithNamespace(t.Context(), ns)
}

func submittedSandbox(name string, anns map[string]string) *sandboxv1beta1.Sandbox {
	sb := &sandboxv1beta1.Sandbox{Name: name, Annotations: anns}
	sb.Spec.PodTemplate.Spec.Containers = []corev1.Container{{Name: "c", Image: "img"}}
	return sb
}
