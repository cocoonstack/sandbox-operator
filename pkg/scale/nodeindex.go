package scale

import "sync"

// nodeIndexMaxEntries bounds one generation of the resolution index.
const nodeIndexMaxEntries = 8192

// nodeIndex maps a lookup key to the node last seen holding it, so a repeat
// resolve reads one node's inventory instead of sweeping the fleet.
type nodeIndex struct {
	mu   sync.Mutex
	max  int
	cur  map[string]string
	prev map[string]string
}

func newNodeIndex(maxEntries int) *nodeIndex {
	return &nodeIndex{max: maxEntries, cur: map[string]string{}, prev: map[string]string{}}
}

func (x *nodeIndex) lookup(key string) (string, bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if node, ok := x.cur[key]; ok {
		return node, true
	}
	node, ok := x.prev[key]
	if ok {
		x.storeLocked(key, node)
	}
	return node, ok
}

func (x *nodeIndex) remember(key, node string) {
	if key == "" || node == "" {
		return
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	x.storeLocked(key, node)
}

// storeLocked retires the whole generation once it fills, which bounds the
// index at 2*max keys without per-entry recency bookkeeping.
func (x *nodeIndex) storeLocked(key, node string) {
	if len(x.cur) >= x.max {
		x.prev, x.cur = x.cur, make(map[string]string, x.max/2)
	}
	x.cur[key] = node
}

func nameKey(namespace, name string) string { return "name/" + namespace + "/" + name }

func claimKey(namespace, id string) string { return "claim/" + namespace + "/" + id }
