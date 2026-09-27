package meshinventory

import (
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

func TestNewJoinsTheSeedsAndTheirGossipedPeers(t *testing.T) {
	a, b := newStubNode(t), newStubNode(t)
	a.set(func(n *stubNode) { n.peers = []string{b.addr()} })
	b.set(func(n *stubNode) {
		n.peers = []string{a.addr()}
		n.rows = []sandboxd.SandboxSummary{{ID: "sb_1", ClaimRef: "ns/s1", Key: sandboxd.PoolKey{Template: "rt:24.04"}}}
	})

	s := newSource(t, a.addr())

	nodes, err := s.ListNodes(t.Context())
	require.NoError(t, err)
	assert.Equal(t, sortedAddrs(a, b), nodes)
	inv, err := s.NodeInventory(t.Context(), b.addr())
	require.NoError(t, err)
	assert.Equal(t, b.addr(), inv.Address)
	require.Len(t, inv.Entries, 1)
	assert.Equal(t, "ns/s1", inv.Entries[0].Name)
	addr, pools, err := s.NodeCapacity(t.Context(), a.addr())
	require.NoError(t, err)
	assert.Equal(t, a.addr(), addr)
	assert.Equal(t, []scale.PoolCapacity{{Template: "rt:24.04", Net: "none", Size: "small", Warm: 2, Target: 3}}, pools)
	_, err = s.NodeInventory(t.Context(), "10.9.9.9:7777")
	require.ErrorIs(t, err, errUnknownNode)
}

func TestASeedStaysAMemberWhileItIsDown(t *testing.T) {
	a, c := newStubNode(t), newStubNode(t)
	c.set(func(n *stubNode) { n.status = http.StatusServiceUnavailable })
	s := newSource(t, a.addr(), c.addr())
	assert.Equal(t, []string{a.addr()}, listNodes(t, s))

	c.set(func(n *stubNode) { n.status = 0 })
	s.tick(t.Context())
	assert.Equal(t, sortedAddrs(a, c), listNodes(t, s))

	c.set(func(n *stubNode) { n.status = http.StatusServiceUnavailable })
	for range defaultMaxStale {
		s.tick(t.Context())
		assert.Equal(t, sortedAddrs(a, c), listNodes(t, s), "a silent node keeps its last snapshot for MaxStale ticks")
	}
	s.tick(t.Context())
	assert.Equal(t, []string{a.addr()}, listNodes(t, s))

	before := c.calls()
	s.tick(t.Context())
	assert.Greater(t, c.calls(), before, "a seed is dialed again although it is silent")
	c.set(func(n *stubNode) { n.status = 0 })
	s.tick(t.Context())
	assert.Equal(t, sortedAddrs(a, c), listNodes(t, s))
}

func TestASilentPeerLeavesTheSnapshotAfterMaxStaleWhileItIsNamed(t *testing.T) {
	a, b := newStubNode(t), newStubNode(t)
	a.set(func(n *stubNode) { n.peers = []string{b.addr()} })
	s := newSource(t, a.addr())
	require.Equal(t, sortedAddrs(a, b), listNodes(t, s))

	b.set(func(n *stubNode) { n.status = http.StatusServiceUnavailable })
	for range defaultMaxStale {
		s.tick(t.Context())
	}
	assert.Equal(t, sortedAddrs(a, b), listNodes(t, s))
	s.tick(t.Context())
	assert.Equal(t, []string{a.addr()}, listNodes(t, s))
	assert.Contains(t, s.members, b.addr(), "a peer the mesh still names stays a member")
}

func TestAPeerNoLongerNamedLeavesAtItsFirstSilence(t *testing.T) {
	a, b := newStubNode(t), newStubNode(t)
	a.set(func(n *stubNode) { n.peers = []string{b.addr()} })
	s := newSource(t, a.addr())
	require.Equal(t, sortedAddrs(a, b), listNodes(t, s))

	a.set(func(n *stubNode) { n.peers = nil })
	b.set(func(n *stubNode) { n.status = http.StatusServiceUnavailable })
	s.tick(t.Context())
	assert.Equal(t, []string{a.addr()}, listNodes(t, s))
	assert.NotContains(t, s.members, b.addr())
}

func TestNewFailsLoud(t *testing.T) {
	for name, tc := range map[string]struct {
		status    int
		advertise string
		want      string
	}{
		"no seed answers":             {status: http.StatusServiceUnavailable, want: "no seed answered"},
		"the seed refuses the token":  {status: http.StatusUnauthorized, want: "fleet root api_token"},
		"the seed advertises no host": {advertise: "-", want: "no advertise_addr"},
	} {
		t.Run(name, func(t *testing.T) {
			a := newStubNode(t)
			a.set(func(n *stubNode) {
				n.status = tc.status
				if tc.advertise == "-" {
					n.advertise = ""
				}
			})
			_, err := New(t.Context(), dial, Options{Seeds: []string{a.addr()}, PollInterval: time.Hour})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestAPeerThatAdvertisesNoHostIsSkipped(t *testing.T) {
	a, b := newStubNode(t), newStubNode(t)
	a.set(func(n *stubNode) { n.peers = []string{b.addr()} })
	b.set(func(n *stubNode) { n.advertise = "" })
	s := newSource(t, a.addr())
	assert.Equal(t, []string{a.addr()}, listNodes(t, s))
}

func TestASeedSpelledDifferentlyIsOneNode(t *testing.T) {
	a, b := newStubNode(t), newStubNode(t)
	a.set(func(n *stubNode) { n.peers = []string{b.addr()} })
	b.set(func(n *stubNode) { n.peers = []string{a.addr()} })
	seed := strings.Replace(a.addr(), "127.0.0.1", "localhost", 1)
	s := newSource(t, seed)

	before := a.calls()
	s.tick(t.Context())
	assert.Equal(t, 1, a.calls()-before, "the gossiped spelling of the seed must not be dialed as a second member")
	assert.Equal(t, sortedAddrs(a, b), listNodes(t, s))
}

type stubNode struct {
	mu        sync.Mutex
	srv       *httptest.Server
	advertise string
	peers     []string
	rows      []sandboxd.SandboxSummary
	status    int
	infoCalls int
}

func newStubNode(t *testing.T) *stubNode {
	n := &stubNode{}
	n.srv = httptest.NewServer(http.HandlerFunc(n.serve))
	t.Cleanup(n.srv.Close)
	n.advertise = n.addr()
	return n
}

func (n *stubNode) addr() string { return n.srv.Listener.Addr().String() }

func (n *stubNode) set(f func(*stubNode)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	f(n)
}

func (n *stubNode) calls() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.infoCalls
}

func (n *stubNode) serve(w http.ResponseWriter, r *http.Request) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if r.URL.Path == "/v1/info" {
		n.infoCalls++
	}
	if n.status != 0 {
		w.WriteHeader(n.status)
		return
	}
	switch r.URL.Path {
	case "/v1/info":
		_ = json.MarshalWrite(w, sandboxd.NodeInfo{
			AdvertiseAddr: n.advertise,
			Peers:         n.peers,
			Pools:         []sandboxd.NodePool{{Key: sandboxd.PoolKey{Template: "rt:24.04", Net: "none", Size: "small"}, Warm: 2, Refilling: 1, Target: 3}},
		})
	case "/v1/sandboxes":
		_ = json.MarshalWrite(w, map[string][]sandboxd.SandboxSummary{"sandboxes": n.rows})
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func dial(addr string) NodeReader { return sandboxd.New(scale.SandboxdBaseURL(addr), "root") }

func newSource(t *testing.T, seeds ...string) *Source {
	s, err := New(t.Context(), dial, Options{Seeds: seeds, PollInterval: time.Hour})
	require.NoError(t, err)
	return s
}

func listNodes(t *testing.T, s *Source) []string {
	nodes, err := s.ListNodes(t.Context())
	require.NoError(t, err)
	return nodes
}

func sortedAddrs(nodes ...*stubNode) []string {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.addr())
	}
	slices.Sort(out)
	return out
}
