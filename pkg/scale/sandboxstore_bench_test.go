package scale

import (
	"fmt"
	"testing"
)

var benchFleets = []struct {
	name    string
	nodes   int
	perNode int
}{
	{"26x100", 26, 100},
	{"26x2000", 26, 2000},
	{"200x2000", 200, 2000},
}

func BenchmarkStoreGet(b *testing.B) {
	for _, fleet := range benchFleets {
		b.Run(fleet.name, func(b *testing.B) {
			store, _ := benchStore(b, fleet.nodes, fleet.perNode)
			last := benchNodeName(fleet.nodes - 1)
			target := fmt.Sprintf("default/sb-%s-%d", last, fleet.perNode-1)
			ns, name := splitNamespacedName(target)
			ctx := b.Context()
			b.ReportAllocs()
			for b.Loop() {
				if _, err := store.Get(ctx, ns, name); err != nil {
					b.Fatalf("get: %v", err)
				}
			}
		})
	}
}

func BenchmarkStoreWarmCandidates(b *testing.B) {
	for _, fleet := range benchFleets {
		b.Run(fleet.name, func(b *testing.B) {
			store, pool := benchStore(b, fleet.nodes, fleet.perNode)
			ctx := b.Context()
			b.ReportAllocs()
			for b.Loop() {
				candidates, err := store.warmCandidates(ctx, pool)
				if err != nil {
					b.Fatalf("warm candidates: %v", err)
				}
				if len(candidates) != fleet.nodes {
					b.Fatalf("got %d candidates, want %d", len(candidates), fleet.nodes)
				}
			}
		})
	}
}

func benchStore(b *testing.B, nodes, perNode int) (*scatterGatherStore, PoolKey) {
	b.Helper()
	invs, pool := benchInventories(nodes, perNode)
	src := NewStaticInventorySource()
	for _, inv := range invs {
		src.Put(inv)
	}
	return NewScatterGatherStore(src), pool
}

func benchInventories(nodes, perNode int) ([]*NodeInventory, PoolKey) {
	pool := PoolKey{Template: "ghcr.io/cocoonstack/sandbox/rt:24.04", Net: NetDefault, Size: SizeClassSmall}
	invs := make([]*NodeInventory, 0, nodes)
	for n := range nodes {
		name := benchNodeName(n)
		entries := make([]InventoryEntry, perNode)
		for i := range entries {
			entries[i] = InventoryEntry{
				Name:    fmt.Sprintf("default/sb-%s-%d", name, i),
				ID:      fmt.Sprintf("sb_%s_%d", name, i),
				Phase:   "Running",
				Address: "10.0.0.1:7777",
			}
		}
		invs = append(invs, &NodeInventory{
			Name:    name,
			Node:    name,
			Address: "10.0.0.1:7777",
			Entries: entries,
			Pools:   []PoolCapacity{{Template: pool.Template, Net: pool.Net, Size: pool.Size, Warm: 5, Target: 5}},
		})
	}
	return invs, pool
}

func benchNodeName(n int) string { return fmt.Sprintf("node-%03d", n) }
