package e2bcompat

import (
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

func BenchmarkLookupByID(b *testing.B) {
	const nodes, perNode = 200, 2000
	src := scale.NewStaticInventorySource()
	for n := range nodes {
		name := fmt.Sprintf("node-%03d", n)
		entries := make([]scale.InventoryEntry, perNode)
		for i := range entries {
			entries[i] = scale.InventoryEntry{
				Name:    fmt.Sprintf("sandboxes/sb-%s-%d", name, i),
				ID:      fmt.Sprintf("sb_%s_%d", name, i),
				Phase:   "Running",
				Address: "10.0.0.1:7777",
			}
		}
		src.Put(&scale.NodeInventory{
			Name:    name,
			Node:    name,
			Address: "10.0.0.1:7777",
			Entries: entries,
		})
	}
	s, err := NewServer(scale.NewScatterGatherStore(src), Options{Namespace: "sandboxes", AllowAnonymous: true})
	if err != nil {
		b.Fatalf("NewServer: %v", err)
	}
	id := PublicID(fmt.Sprintf("sb_node-%03d_%d", nodes-1, perNode-1))
	req := httptest.NewRequest("GET", "/sandboxes/"+id, nil)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := s.lookup(req, id); err != nil {
			b.Fatalf("lookup: %v", err)
		}
	}
}
