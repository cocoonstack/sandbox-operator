package e2bcompat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

func BenchmarkListSandboxes(b *testing.B) {
	const nodes, perNode = 26, 100
	for _, pairs := range []int{0, 3} {
		b.Run(fmt.Sprintf("26x100/meta%d", pairs), func(b *testing.B) {
			src := scale.NewStaticInventorySource()
			for n := range nodes {
				name := fmt.Sprintf("node-%03d", n)
				entries := make([]scale.InventoryEntry, perNode)
				for i := range entries {
					entries[i] = scale.InventoryEntry{
						Name:        fmt.Sprintf("sandboxes/sb-%s-%d", name, i),
						ID:          fmt.Sprintf("sb_%s_%d", name, i),
						Phase:       "Running",
						Address:     "10.0.0.1:7777",
						Metadata:    listBenchMetadata(pairs),
						CPUCount:    2,
						MemoryBytes: 1 << 30,
					}
				}
				src.Put(&scale.NodeInventory{Name: name, Node: name, Address: "10.0.0.1:7777", Entries: entries})
			}
			s, err := NewServer(scale.NewScatterGatherStore(src), Options{Namespace: "sandboxes", Domain: testDomain, AllowAnonymous: true})
			if err != nil {
				b.Fatalf("NewServer: %v", err)
			}
			h := s.Handler()
			b.ReportAllocs()
			for b.Loop() {
				w := httptest.NewRecorder()
				h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v2/sandboxes", nil))
				if w.Code != http.StatusOK {
					b.Fatalf("list: %d %s", w.Code, w.Body.String())
				}
			}
		})
	}
}

func listBenchMetadata(pairs int) string {
	if pairs == 0 {
		return ""
	}
	md := make(map[string]string, pairs)
	for i := range pairs {
		md[fmt.Sprintf("key-%d", i)] = fmt.Sprintf("value-%d", i)
	}
	b, _ := json.Marshal(md)
	return string(b)
}
