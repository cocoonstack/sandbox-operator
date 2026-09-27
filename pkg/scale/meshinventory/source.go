// Package meshinventory is the InventorySource of a sandboxd mesh: it polls every member's GET /v1/info and GET /v1/sandboxes and serves the store from the last snapshot.
package meshinventory

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

const (
	defaultPollInterval = 10 * time.Second
	defaultMaxStale     = 3
)

var (
	errUnknownNode     = errors.New("meshinventory: node not in the mesh snapshot")
	errNoAdvertiseAddr = errors.New("GET /v1/info reports no advertise_addr; a mesh node must advertise a routable host")
)

// Options configures a Source.
type Options struct {
	// Seeds are the sandboxd addresses dialed at start, each naming one node; the node key is each node's own advertise_addr.
	Seeds []string
	// PollInterval is the tick between polls, 10s by default.
	PollInterval time.Duration
	// MaxStale is how many failed ticks a node keeps its last snapshot, 3 by default.
	MaxStale int
}

// DialFunc returns the NodeReader for one member address.
type DialFunc func(addr string) NodeReader

// NodeReader is what one mesh member answers; *sandboxd.Client satisfies it with the fleet root token.
type NodeReader interface {
	Info(ctx context.Context) (*sandboxd.NodeInfo, error)
	Sandboxes(ctx context.Context) ([]sandboxd.SandboxSummary, error)
}

type member struct {
	reader NodeReader
	seed   bool
	fails  int
	inv    *scale.NodeInventory
}

type snapshot struct {
	nodes []string
	byKey map[string]*scale.NodeInventory
}

type answer struct {
	inv   *scale.NodeInventory
	peers []string
	err   error
}

var _ scale.InventorySource = (*Source)(nil)

// Source serves ListNodes, NodeInventory and NodeCapacity from memory; a background tick refreshes it.
type Source struct {
	dial    DialFunc
	opts    Options
	members map[string]*member
	snap    atomic.Pointer[snapshot]
}

// New runs the first tick before returning and polls until ctx ends, so ctx is the process context; it fails when no seed answers, a seed refuses the token, or a seed has no advertise_addr.
func New(ctx context.Context, dial DialFunc, opts Options) (*Source, error) {
	if len(opts.Seeds) == 0 {
		return nil, errors.New("meshinventory: at least one seed is required")
	}
	opts.PollInterval = cmp.Or(opts.PollInterval, defaultPollInterval)
	opts.MaxStale = cmp.Or(opts.MaxStale, defaultMaxStale)
	s := &Source{dial: dial, opts: opts, members: map[string]*member{}}
	for _, addr := range opts.Seeds {
		s.members[addr] = &member{reader: dial(addr), seed: true}
	}
	seeded := len(s.members)
	answers := s.tick(ctx)
	answered := false
	for _, addr := range opts.Seeds {
		switch err := answers[addr].err; {
		case err == nil:
			answered = true
		case isUnauthorized(err):
			return nil, fmt.Errorf("meshinventory: seed %s refused the token; GET /v1/info needs the fleet root api_token: %w", addr, err)
		case errors.Is(err, errNoAdvertiseAddr):
			return nil, fmt.Errorf("meshinventory: seed %s: %w", addr, err)
		}
	}
	if !answered {
		return nil, fmt.Errorf("meshinventory: no seed answered: %w", answers[opts.Seeds[0]].err)
	}
	if len(s.members) > seeded {
		s.tick(ctx)
	}
	go s.run(ctx)
	return s, nil
}

func (s *Source) ListNodes(_ context.Context) ([]string, error) {
	return s.snap.Load().nodes, nil
}

func (s *Source) NodeInventory(_ context.Context, node string) (*scale.NodeInventory, error) {
	if inv, ok := s.snap.Load().byKey[node]; ok {
		return inv, nil
	}
	return nil, fmt.Errorf("%w: %s", errUnknownNode, node)
}

func (s *Source) NodeCapacity(ctx context.Context, node string) (string, []scale.PoolCapacity, error) {
	inv, err := s.NodeInventory(ctx, node)
	if err != nil {
		return "", nil, err
	}
	return inv.Address, inv.Pools, nil
}

func (s *Source) run(ctx context.Context) {
	t := time.NewTicker(s.opts.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.tick(ctx)
		}
	}
}

func (s *Source) tick(ctx context.Context) map[string]answer {
	logger := log.WithFunc("meshinventory.tick")
	answers := s.poll(ctx)
	named, heard := map[string]bool{}, false
	for _, a := range answers {
		heard = heard || a.err == nil
		for _, p := range a.peers {
			named[p] = true
		}
	}
	for addr, m := range s.members {
		a := answers[addr]
		if a.err == nil {
			m.fails, m.inv = 0, a.inv
			continue
		}
		m.fails++
		logger.Warnf(ctx, "mesh member did not answer addr=%s fails=%d err=%v", addr, m.fails, a.err)
		if !m.seed && heard && !named[addr] && m.fails > s.opts.MaxStale {
			delete(s.members, addr)
		}
	}
	owner := map[string]string{}
	for addr, m := range s.members {
		if a, ok := answers[addr]; !ok || a.err != nil {
			continue
		}
		key := m.inv.Node
		other, dup := owner[key]
		switch {
		case !dup:
			owner[key] = addr
		case m.seed && s.members[other].seed:
		case m.seed || (!s.members[other].seed && addr == key):
			delete(s.members, other)
			owner[key] = addr
		default:
			delete(s.members, addr)
		}
	}
	for addr := range named {
		if _, ok := s.members[addr]; ok {
			continue
		}
		if _, known := owner[addr]; !known {
			s.members[addr] = &member{reader: s.dial(addr)}
		}
	}
	s.publish()
	return answers
}

func (s *Source) poll(ctx context.Context) map[string]answer {
	ctx, cancel := context.WithTimeout(ctx, s.opts.PollInterval)
	defer cancel()
	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	answers := make(map[string]answer, len(s.members))
	for addr, m := range s.members {
		wg.Go(func() {
			a := read(ctx, m.reader)
			mu.Lock()
			answers[addr] = a
			mu.Unlock()
		})
	}
	wg.Wait()
	return answers
}

func (s *Source) publish() {
	best := map[string]*member{}
	for _, m := range s.members {
		if m.inv == nil || m.fails > s.opts.MaxStale {
			continue
		}
		if b, ok := best[m.inv.Node]; !ok || m.fails < b.fails {
			best[m.inv.Node] = m
		}
	}
	next := &snapshot{byKey: make(map[string]*scale.NodeInventory, len(best))}
	for key, m := range best {
		next.byKey[key] = m.inv
	}
	next.nodes = slices.Sorted(maps.Keys(next.byKey))
	s.snap.Store(next)
}

func read(ctx context.Context, r NodeReader) answer {
	info, err := r.Info(ctx)
	if err != nil {
		return answer{err: err}
	}
	if info.AdvertiseAddr == "" {
		return answer{err: errNoAdvertiseAddr}
	}
	rows, err := r.Sandboxes(ctx)
	if err != nil {
		return answer{err: err}
	}
	entries := make([]scale.InventoryEntry, 0, len(rows))
	for _, row := range rows {
		entries = append(entries, scale.EntryFromSummary(row))
	}
	key := info.AdvertiseAddr
	return answer{
		inv:   &scale.NodeInventory{Node: key, Address: key, Pools: scale.PoolCapacityFromInfo(info), Entries: entries},
		peers: info.Peers,
	}
}

func isUnauthorized(err error) bool {
	he, ok := errors.AsType[*sandboxd.HTTPError](err)
	return ok && he.StatusCode == http.StatusUnauthorized
}
