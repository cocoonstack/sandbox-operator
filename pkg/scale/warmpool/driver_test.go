package warmpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

const testImage = "ghcr.io/cocoonstack/sandbox/rt@sha256:deadbeef"

func TestReconcileDistributesAndMatchesPoolKey(t *testing.T) {
	d, setter, inv, kube := newTestDriver(t, warmPool("p", 100), template())
	putNodes(inv, 26)

	if err := d.reconcileOnce(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if len(setter.byAddr) != 26 {
		t.Fatalf("PUT %d nodes, want 26", len(setter.byAddr))
	}
	want := scale.PoolKeyFor([]corev1.Container{{Image: testImage}}, "")
	sum, minWarm, maxWarm := 0, 1<<30, 0
	for _, specs := range setter.byAddr {
		if len(specs) != 1 {
			t.Fatalf("node got %d pool specs, want 1", len(specs))
		}
		s := specs[0]
		if s.Template != want.Template || s.Net != want.Net || s.Size != want.Size {
			t.Fatalf("pool key {%s,%s,%s} != apiserver-derived {%s,%s,%s}",
				s.Template, s.Net, s.Size, want.Template, want.Net, want.Size)
		}
		sum += s.Warm
		minWarm, maxWarm = min(minWarm, s.Warm), max(maxWarm, s.Warm)
	}
	if sum != 100 {
		t.Fatalf("targets sum to %d, want 100", sum)
	}
	if maxWarm-minWarm > 1 {
		t.Fatalf("uneven spread: min=%d max=%d", minWarm, maxWarm)
	}

	var got extv1beta1.SandboxWarmPool
	if err := kube.Get(t.Context(), client.ObjectKey{Namespace: "ns", Name: "p"}, &got); err != nil {
		t.Fatalf("get pool: %v", err)
	}
	if got.Status.Replicas != 0 {
		t.Fatalf("status.replicas = %d, want 0 (no warm reported yet)", got.Status.Replicas)
	}
}

func TestThePoolKeyReadsTheNetLaneWhereThePodDoes(t *testing.T) {
	tmpl := template()
	tmpl.Annotations = map[string]string{scale.NetAnnotation: "egress"}
	d, setter, inv, _ := newTestDriver(t, warmPool("p", 1), tmpl)
	putNodes(inv, 1)

	if err := d.reconcileOnce(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	for _, specs := range setter.byAddr {
		if specs[0].Net != scale.NetDefault {
			t.Fatalf("pool net = %q, want %q: the pod carries only its template's pod annotations, so a lane set on the template object alone provisions a pool no pod claims", specs[0].Net, scale.NetDefault)
		}
	}
}

func TestReconcileWritesWarmStatus(t *testing.T) {
	d, setter, inv, kube := newTestDriver(t, warmPool("p", 8), template())
	key := scale.PoolKeyFor([]corev1.Container{{Image: testImage}}, "")

	for _, name := range []string{"a", "b"} {
		inv.Put(&scale.NodeInventory{
			Name:    name,
			Node:    name,
			Address: "10.0.0." + name + ":7777",
			Pools:   []scale.PoolCapacity{{Template: key.Template, Net: key.Net, Size: key.Size, Warm: 4, Target: 4}},
		})
		setter.reportWarm("10.0.0."+name+":7777", 4)
	}
	if err := d.reconcileOnce(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got extv1beta1.SandboxWarmPool
	if err := kube.Get(t.Context(), client.ObjectKey{Namespace: "ns", Name: "p"}, &got); err != nil {
		t.Fatalf("get pool: %v", err)
	}
	if got.Status.Replicas != 8 || got.Status.ReadyReplicas != 8 {
		t.Fatalf("status replicas=%d ready=%d, want 8/8", got.Status.Replicas, got.Status.ReadyReplicas)
	}
}

func TestStatusPrefersPutResponseOverStaleInventory(t *testing.T) {
	d, setter, inv, kube := newTestDriver(t, warmPool("p", 20), template())
	key := scale.PoolKeyFor([]corev1.Container{{Image: testImage}}, "")
	for _, name := range []string{"a", "b"} {
		addr := "10.0.0." + name + ":7777"

		inv.Put(&scale.NodeInventory{
			Name:    name,
			Node:    name,
			Address: addr,
			Pools:   []scale.PoolCapacity{{Template: key.Template, Net: key.Net, Size: key.Size, Warm: 4, Target: 10}},
		})

		setter.reportWarm(addr, 7)
	}
	if err := d.reconcileOnce(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got extv1beta1.SandboxWarmPool
	if err := kube.Get(t.Context(), client.ObjectKey{Namespace: "ns", Name: "p"}, &got); err != nil {
		t.Fatalf("get pool: %v", err)
	}
	if got.Status.Replicas != 14 {
		t.Fatalf("status.replicas = %d, want 14 (live 7+7, not stale 4+4)", got.Status.Replicas)
	}
}

func TestStatusFallsBackToInventoryWhenPutFails(t *testing.T) {
	d, setter, inv, kube := newTestDriver(t, warmPool("p", 20), template())
	key := scale.PoolKeyFor([]corev1.Container{{Image: testImage}}, "")
	for _, name := range []string{"a", "b"} {
		addr := "10.0.0." + name + ":7777"
		inv.Put(&scale.NodeInventory{
			Name:    name,
			Node:    name,
			Address: addr,
			Pools:   []scale.PoolCapacity{{Template: key.Template, Net: key.Net, Size: key.Size, Warm: 5, Target: 10}},
		})
		setter.reportWarm(addr, 9)
	}
	setter.failAddr = "10.0.0.b:7777"
	if err := d.reconcileOnce(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got extv1beta1.SandboxWarmPool
	if err := kube.Get(t.Context(), client.ObjectKey{Namespace: "ns", Name: "p"}, &got); err != nil {
		t.Fatalf("get pool: %v", err)
	}
	if got.Status.Replicas != 14 {
		t.Fatalf("status.replicas = %d, want 14 (live 9 from a, inventory 5 from unreachable b)", got.Status.Replicas)
	}
}

func TestTwoPoolsSameKeyAggregate(t *testing.T) {
	d, setter, inv, _ := newTestDriver(t, warmPool("p1", 3), warmPool("p2", 5), template())
	putNodes(inv, 2)
	if err := d.reconcileOnce(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	sum := 0
	for addr, specs := range setter.byAddr {
		if len(specs) != 1 {
			t.Fatalf("node %s got %d specs, want 1 aggregated (no duplicate key): %+v", addr, len(specs), specs)
		}
		if specs[0].Template != testImage {
			t.Fatalf("node %s wrong key %q", addr, specs[0].Template)
		}
		sum += specs[0].Warm
	}

	if sum != 8 {
		t.Fatalf("fleet warm total = %d, want 8 (3+5 summed)", sum)
	}
}

func TestTwoPoolsSameKeyReportTheirOwnShareOfWarm(t *testing.T) {
	d, setter, inv, kube := newTestDriver(t, warmPool("p1", 3), warmPool("p2", 5), template())
	putNodes(inv, 2)
	for _, tt := range []struct {
		warmPerNode int
		p1, p2      int32
	}{
		{4, 3, 5},
		{2, 2, 2},
		{0, 0, 0},
	} {
		setter.reportWarm("10.0.0.a:7777", tt.warmPerNode)
		setter.reportWarm("10.0.0.b:7777", tt.warmPerNode)
		if err := d.reconcileOnce(t.Context()); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		for _, want := range []struct {
			name string
			warm int32
		}{{"p1", tt.p1}, {"p2", tt.p2}} {
			var p extv1beta1.SandboxWarmPool
			if err := kube.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: want.name}, &p); err != nil {
				t.Fatalf("get %s: %v", want.name, err)
			}
			if p.Status.Replicas != want.warm || p.Status.ReadyReplicas != want.warm {
				t.Errorf("fleet warm %d: %s status = %d/%d, want %d/%d (the key's warm is shared, not repeated)",
					2*tt.warmPerNode, want.name, p.Status.Replicas, p.Status.ReadyReplicas, want.warm, want.warm)
			}
		}
	}
}

func TestDrainOnZeroReplicas(t *testing.T) {
	d, setter, inv, _ := newTestDriver(t, warmPool("p", 0), template())
	putNodes(inv, 3)
	if err := d.reconcileOnce(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	for addr, specs := range setter.byAddr {
		if len(specs) != 1 || specs[0].Warm != 0 {
			t.Fatalf("node %s target != 0 on drain: %+v", addr, specs)
		}
	}
}

func TestATransientTemplateReadFailureLeavesNodeTargetsUntouched(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := extv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&extv1beta1.SandboxWarmPool{}).
		WithObjects(warmPool("p", 100), template()).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*extv1beta1.SandboxTemplate); ok {
					return k8serrors.NewInternalError(errors.New("apiserver unavailable"))
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).Build()
	inv := scale.NewStaticInventorySource()
	putNodes(inv, 3)
	setter := &fakeSetter{byAddr: map[string][]sandboxd.PoolSpec{}}
	d := New(kube, inv, "tok", setter.factory(), Options{Interval: 0})

	err := d.reconcileOnce(t.Context())
	if err == nil {
		t.Fatal("reconcile returned nil on a transient template read failure; the tick must fail so the workqueue backs off")
	}
	if len(setter.byAddr) != 0 {
		t.Fatalf("PUT reached %d nodes on a failed tick; an omitted pool drains every node's warm target: %+v", len(setter.byAddr), setter.byAddr)
	}
}

func TestADeletedTemplateStillDrainsItsPool(t *testing.T) {
	d, setter, inv, _ := newTestDriver(t, warmPool("p", 4))
	putNodes(inv, 2)
	if err := d.reconcileOnce(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(setter.byAddr) != 2 {
		t.Fatalf("PUT reached %d nodes, want 2: a pool whose template is gone is drained", len(setter.byAddr))
	}
	for addr, specs := range setter.byAddr {
		if len(specs) != 0 {
			t.Fatalf("node %s still carries %d pool specs for a template-less pool", addr, len(specs))
		}
	}
}

func TestEveryTriggerCollapsesOntoTheSyncKey(t *testing.T) {
	for name, obj := range map[string]client.Object{
		"pool":      warmPool("p", 1),
		"inventory": &scale.NodeInventory{Name: "n1", Node: "n1"},
	} {
		got := syncRequest(t.Context(), obj)
		if len(got) != 1 || got[0].Name != "sync" || got[0].Namespace != "" {
			t.Fatalf("%s event enqueued %v, want the single fleet key {Name: sync}: every key runs the whole fleet loop and re-arms its own timer", name, got)
		}
	}
}

func BenchmarkReconcileOnce(b *testing.B) {
	objs := []client.Object{template()}
	for i := range 4 {
		objs = append(objs, warmPool(fmt.Sprintf("p%d", i), 500))
	}
	d, _, inv, _ := newTestDriver(b, objs...)
	putNodes(inv, 26)
	ctx := b.Context()
	b.ReportAllocs()
	for b.Loop() {
		if err := d.reconcileOnce(ctx); err != nil {
			b.Fatalf("reconcile: %v", err)
		}
	}
}

type fakeSetter struct {
	mu     sync.Mutex
	byAddr map[string][]sandboxd.PoolSpec

	warm map[string]int

	failAddr string
}

func (f *fakeSetter) reportWarm(addr string, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.warm == nil {
		f.warm = map[string]int{}
	}
	f.warm[addr] = n
}

func (f *fakeSetter) factory() ClientFactory {
	return func(addr, _ string) PoolSetter { return &fakeNode{addr: addr, parent: f} }
}

type fakeNode struct {
	addr   string
	parent *fakeSetter
}

func (n *fakeNode) SetPools(_ context.Context, pools []sandboxd.PoolSpec) (*sandboxd.NodeInfo, error) {
	n.parent.mu.Lock()
	defer n.parent.mu.Unlock()
	if n.parent.failAddr == n.addr {
		return nil, errors.New("node unreachable")
	}
	n.parent.byAddr[n.addr] = pools

	info := &sandboxd.NodeInfo{}
	for _, p := range pools {
		info.Pools = append(info.Pools, sandboxd.NodePool{
			Key:    sandboxd.PoolKey{Template: p.Template, Net: p.Net, Size: p.Size},
			Warm:   n.parent.warm[n.addr],
			Target: p.Warm,
		})
	}
	return info, nil
}

func newTestDriver(t testing.TB, objs ...client.Object) (*Driver, *fakeSetter, *scale.StaticInventorySource, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := extv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&extv1beta1.SandboxWarmPool{}).
		WithObjects(objs...).Build()
	inv := scale.NewStaticInventorySource()
	setter := &fakeSetter{byAddr: map[string][]sandboxd.PoolSpec{}}
	d := New(kube, inv, "tok", setter.factory(), Options{Interval: 0})
	return d, setter, inv, kube
}

func warmPool(name string, replicas int32) *extv1beta1.SandboxWarmPool {
	return &extv1beta1.SandboxWarmPool{
		Namespace: "ns", Name: name,
		Spec: extv1beta1.SandboxWarmPoolSpec{
			Replicas:    new(replicas),
			TemplateRef: extv1beta1.SandboxTemplateRef{Name: "tpl"},
		},
	}
}

func template() *extv1beta1.SandboxTemplate {
	return &extv1beta1.SandboxTemplate{
		Namespace: "ns", Name: "tpl",
		Spec: extv1beta1.SandboxTemplateSpec{
			SandboxBlueprint: sandboxv1beta1.SandboxBlueprint{
				PodTemplate: sandboxv1beta1.PodTemplate{
					Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: testImage}}},
				},
			},
		},
	}
}

func putNodes(inv *scale.StaticInventorySource, n int) {
	for i := range n {
		name := string(rune('a' + i))
		inv.Put(&scale.NodeInventory{
			Name:    name,
			Node:    name,
			Address: "10.0.0." + name + ":7777",
		})
	}
}
