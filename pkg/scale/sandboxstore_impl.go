package scale

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"math/rand/v2"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/sync/errgroup"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sandboxv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	extv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
)

const (
	// Synthesized-Sandbox label keys. The aggregated store stamps these onto every
	// Sandbox it materializes from a NodeInventory entry so label selectors (the
	// `kubectl get sandboxes -l ...` path) have real axes to filter on without any
	// per-sandbox etcd object.
	// NodeLabel carries the owning node of a synthesized Sandbox.
	NodeLabel = "sandbox.cocoonstack.io/node"
	// PhaseLabel carries the entry phase of a synthesized Sandbox.
	PhaseLabel = "sandbox.cocoonstack.io/phase"
	// ClaimLabel carries the claim name a synthesized Sandbox is bound to.
	ClaimLabel = "sandbox.cocoonstack.io/claim"
	// TemplateLabel carries the pool template a synthesized Sandbox was claimed
	// from; it is the only recoverable source, since no per-sandbox object holds
	// the pod spec the template would otherwise be read off.
	TemplateLabel = "sandbox.cocoonstack.io/template"

	// ClaimIDAnnotation carries the owning node's sandboxd claim id ("sb_...") on a
	// synthesized Sandbox. Unlike the label keys above it is an annotation — an
	// opaque node-local handle, not a selector axis: the aggregated apiserver reads
	// it on Delete to release exactly the microVM this Sandbox stands for (releasing
	// by k8s name would target the wrong claim). This is the single definition of
	// the key; apiserver.ClaimIDAnnotation aliases it so both write it identically.
	ClaimIDAnnotation = "sandbox.cocoonstack.io/claim-id"
	// DeadlineAnnotation carries the node-granted lease expiry (RFC3339) of a
	// Sandbox: stamped from inventory on reads and from the claim on Create.
	// apiserver.DeadlineAnnotation aliases it.
	DeadlineAnnotation = "sandbox.cocoonstack.io/deadline"
	// NetAnnotation selects the pool network mode. Create and the warm-pool
	// driver must read the same key or a claim never matches provisioned warm
	// capacity (perpetual 503).
	NetAnnotation = "sandbox.cocoonstack.io/net"
	// TokenAnnotation carries the per-sandbox ownership token handed back on Create.
	TokenAnnotation = "sandbox.cocoonstack.io/token"

	// Connection pooling for the node-local claim path. Idle conns per host are
	// sized to the per-node claim fan-out so a burst reuses connections instead
	// of handshaking; the timeout bounds a wedged sandboxd.
	sandboxdRequestTimeout      = 10 * time.Second
	sandboxdMaxIdleConns        = 256
	sandboxdMaxIdleConnsPerHost = 32
	sandboxdIdleConnTimeout     = 90 * time.Second
)

// NodeInventoryGVK is the GroupVersionKind of the O(nodes) intent object the
// publisher server-side-applies. It lives in the extensions CRD group next to
// SandboxClaim/Template/WarmPool — NOT in the aggregated agents.x-k8s.io group:
// the APIService hands that entire group-version to the aggregated server,
// which serves only `sandboxes`, so a NodeInventory registered there would 404
// once the APIService cuts over.
var (
	NodeInventoryGVK = extv1beta1.GroupVersion.WithKind("NodeInventory")

	// ErrNoWarmCapacity lets the aggregated apiserver map an exhausted pool to a retryable 503 instead of writing an object.
	ErrNoWarmCapacity = errors.New("scale: no node has warm capacity for the requested pool")
)

// IsNoWarmCapacity reports whether err means Claim found no warm node.
func IsNoWarmCapacity(err error) bool { return errors.Is(err, ErrNoWarmCapacity) }

// InventorySource enumerates the per-node NodeInventory objects that back the
// aggregated store. It is deliberately granular — a node enumeration plus a
// per-node fetch — rather than one cluster-wide read, so:
//
//   - List fans out per node with bounded concurrency and a single partitioned
//     node degrades to eventual consistency (its sandboxes are briefly absent)
//     instead of failing the whole list, and
//   - Get can route to the single owning node (the README "route Get to the
//     owning node, not the summary" contract).
//
// In production ListNodes/NodeInventory are served from a cache-fed client
// listing NodeInventory objects at ResourceVersion=0 (O(nodes), never a hot-path
// LIST off etcd); tests inject StaticInventorySource.
type InventorySource interface {
	// ListNodes returns the nodes that publish inventory. O(nodes), cache-fed.
	ListNodes(ctx context.Context) ([]string, error)
	// NodeInventory returns one node's authoritative inventory. A partitioned or
	// not-yet-published node returns an error, which List logs and skips.
	NodeInventory(ctx context.Context, node string) (*NodeInventory, error)
	// NodeCapacity returns one node's advertise address and warm pools without
	// decoding its entry list, which the claim and routing paths never read.
	NodeCapacity(ctx context.Context, node string) (address string, pools []PoolCapacity, err error)
}

// warmCandidate is one node advertising warm capacity for a requested pool.
type warmCandidate struct {
	node string
	addr string
	warm int
}

// StoreOption configures a scatterGatherStore.
type StoreOption func(*scatterGatherStore)

// WithLogger sets the store logger. The zero logr.Logger discards.
func WithLogger(log logr.Logger) StoreOption {
	return func(s *scatterGatherStore) { s.log = log }
}

// WithWatchPollInterval sets how often Watch re-derives node inventory to emit
// deltas. Defaults to one second.
func WithWatchPollInterval(d time.Duration) StoreOption {
	return func(s *scatterGatherStore) { s.watchPoll = d }
}

// SandboxdClientFactory builds a sandboxd client for one node's advertise address
// and the uniform fleet api_token. It is injected so tests need no live node and
// production wires the real HTTP client (NewSandboxdClientFactory).
type SandboxdClientFactory func(addr, token string) SandboxdClient

// WithClaimRouting enables the Create/Delete write path: token is the uniform
// fleet-wide sandboxd api_token presented on claim/release, and factory builds a
// per-node sandboxd client for a node's advertise address. Without it, Claim and
// Release fail closed and the store stays read-only.
func WithClaimRouting(token string, factory SandboxdClientFactory) StoreOption {
	return func(s *scatterGatherStore) {
		s.sandboxdToken = token
		s.sandboxdFactory = factory
	}
}

// NewSandboxdHTTPClient returns one HTTP client for the whole fleet. A per-call
// client would fall back to http.DefaultTransport, whose MaxIdleConnsPerHost of
// 2 forces a fresh TCP handshake on every concurrent claim past the second to
// the same node.
func NewSandboxdHTTPClient() *http.Client {
	return &http.Client{
		Timeout: sandboxdRequestTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        sandboxdMaxIdleConns,
			MaxIdleConnsPerHost: sandboxdMaxIdleConnsPerHost,
			IdleConnTimeout:     sandboxdIdleConnTimeout,
		},
	}
}

// SandboxdBaseURL renders a node advertise address as a sandboxd base URL: a
// bare "host:port" is given the http scheme, an address that already carries
// one is used verbatim.
func SandboxdBaseURL(addr string) string {
	if strings.Contains(addr, "://") {
		return addr
	}
	return "http://" + addr
}

// NewSandboxdClientFactory returns the production SandboxdClientFactory: an HTTP
// sandboxd client per node advertise address, over the shared client.
func NewSandboxdClientFactory() SandboxdClientFactory {
	hc := NewSandboxdHTTPClient()
	return func(addr, token string) SandboxdClient {
		return sandboxd.New(SandboxdBaseURL(addr), token, sandboxd.WithHTTPClient(hc))
	}
}

type inventoryMatch func(inv *NodeInventory, i int) bool

var _ SandboxStore = (*scatterGatherStore)(nil)

// scatterGatherStore is the concrete SandboxStore: List/Get/Watch synthesize
// Sandbox objects from live NodeInventory rather than reading any per-sandbox
// etcd object, and Create/Delete are node-local claim/release — exactly the
// metrics.k8s.io aggregation pattern extended with a synchronous write path.
type scatterGatherStore struct {
	src         InventorySource
	log         logr.Logger
	concurrency int
	watchPoll   time.Duration
	index       *nodeIndex

	// sandboxdToken is the uniform fleet api_token; sandboxdFactory builds a
	// per-node sandboxd client. Both are nil/empty until WithClaimRouting is set,
	// which is what gates the write path (Claim/Release).
	sandboxdToken   string
	sandboxdFactory SandboxdClientFactory
}

// NewScatterGatherStore builds the aggregated store over src.
func NewScatterGatherStore(src InventorySource, opts ...StoreOption) *scatterGatherStore {
	s := &scatterGatherStore{
		src:         src,
		concurrency: 16,
		watchPoll:   time.Second,
		index:       newNodeIndex(nodeIndexMaxEntries),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// List assembles a SandboxList by fanning out to every node inventory with
// bounded concurrency, flattening entries into Sandboxes and honoring the
// namespace/label/field filters. A node whose inventory is unavailable
// (partitioned, or its NodeInventory lost before the next publish) is logged and
// omitted — eventual consistency, never a whole-list failure.
func (s *scatterGatherStore) List(ctx context.Context, opts ListOptions) (*sandboxv1beta1.SandboxList, error) {
	labelSel, fieldSel, err := parseSelectors(opts)
	if err != nil {
		return nil, err
	}
	items, err := fanOutNodes(ctx, s, func(gctx context.Context, node string) []sandboxv1beta1.Sandbox {
		inv, invErr := s.src.NodeInventory(gctx, node)
		if invErr != nil {
			// Partitioned / lost inventory: skip this node's sandboxes rather
			// than failing the list. They reappear on the node's next publish.
			s.log.V(1).Info("node inventory unavailable; omitting from list (eventual consistency)",
				"node", node, "err", invErr.Error())
			return nil
		}
		return s.materialize(inv, opts.Namespace, labelSel, fieldSel)
	})
	if err != nil {
		return nil, err
	}
	list := &sandboxv1beta1.SandboxList{}
	list.SetGroupVersionKind(sandboxv1beta1.GroupVersion.WithKind("SandboxList"))
	list.Items = items
	if list.Items == nil {
		list.Items = []sandboxv1beta1.Sandbox{} // an empty list serializes as [], not null
	}
	slices.SortFunc(list.Items, func(a, b sandboxv1beta1.Sandbox) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name))
	})
	return list, nil
}

// Get routes to the owning node's authoritative inventory. It resolves which
// node holds namespace/name and returns that entry synthesized as a Sandbox.
// The per-node sweep fans out like List and cancels on the first hit — a
// sandbox lives on exactly one node, so first-found is the answer.
//
// In production this would RPC the owning node's live sandboxd for a
// read-after-write answer; in this substrate the node's published NodeInventory
// is the authoritative view available, so Get returns from it directly rather
// than from an eventually-consistent cluster-wide summary.
func (s *scatterGatherStore) Get(ctx context.Context, namespace, name string) (*sandboxv1beta1.Sandbox, error) {
	found, err := s.findEntry(ctx, "get", nameKey(namespace, name), func(inv *NodeInventory, i int) bool {
		ens, ename := splitNamespacedName(inv.Entries[i].Name)
		return ens == namespace && ename == name
	})
	if err != nil {
		return nil, err
	}
	if found == nil {
		return nil, k8serrors.NewNotFound(sandboxv1beta1.Resource("sandboxes"), name)
	}
	return found, nil
}

// GetByClaimID resolves the sandbox whose node-local claim id satisfies match,
// fanning out per node and canceling on the first hit; only the matching entry
// is materialized. An empty namespace matches every namespace. id is the
// caller's spelling of the claim id and keys the owning-node index; match owns
// which node-local id it accepts.
func (s *scatterGatherStore) GetByClaimID(ctx context.Context, namespace, id string, match func(claimID string) bool) (*sandboxv1beta1.Sandbox, error) {
	found, err := s.findEntry(ctx, "claim-id get", claimKey(namespace, id), func(inv *NodeInventory, i int) bool {
		if inv.Entries[i].ID == "" || !match(inv.Entries[i].ID) {
			return false
		}
		ns, _ := splitNamespacedName(inv.Entries[i].Name)
		return namespace == "" || ns == namespace
	})
	if err != nil {
		return nil, err
	}
	if found == nil {
		return nil, k8serrors.NewNotFound(sandboxv1beta1.Resource("sandboxes"), "")
	}
	return found, nil
}

// Claim picks the node with the most warm capacity for pool and hands over one of
// its already-running microVMs via that node's sandboxd, returning the assignment.
// No per-sandbox object is written to etcd. It fails closed if claim routing is
// not configured, and returns ErrNoWarmCapacity when no warm node is available.
func (s *scatterGatherStore) Claim(ctx context.Context, namespace, name string, pool PoolKey, ttlSeconds int) (Assignment, error) {
	if s.sandboxdFactory == nil {
		return Assignment{}, fmt.Errorf("scale: claim routing not configured (call WithClaimRouting)")
	}
	candidates, err := s.warmCandidates(ctx, pool)
	if err != nil {
		return Assignment{}, err
	}

	// Inventory is 5-30s stale, so a node can advertise warm capacity it no
	// longer has. Reporting the whole fleet exhausted because one sampled node
	// raced to zero would 503 a caller that other nodes could still serve, so
	// each capacity miss drops that node and re-samples the rest.
	for len(candidates) > 0 {
		best, idx := pickPowerOfTwo(candidates)
		res, claimErr := s.sandboxdFactory(best.addr, s.sandboxdToken).Claim(ctx, sandboxd.ClaimSpec{
			Template:   pool.Template,
			Net:        pool.Net,
			Size:       pool.Size,
			TTLSeconds: ttlSeconds,
			// Name the claim by the k8s object so the node's operator index echoes
			// it back and the aggregated read path (List/Get) resolves this sandbox
			// by "<namespace>/<name>".
			ClaimRef: namespace + "/" + name,
		})
		if claimErr == nil {
			s.index.remember(nameKey(namespace, name), best.node)
			return Assignment{SandboxName: res.ID, Node: best.node, Address: res.OwnerAddr, Token: res.Token, Deadline: res.Deadline}, nil
		}
		if !errors.Is(claimErr, sandboxd.ErrNodeAtCapacity) {
			return Assignment{}, fmt.Errorf("scale: claim %s/%s on node %q: %w", namespace, name, best.node, claimErr)
		}
		s.log.V(1).Info("node warm-raced to zero during claim; trying another node",
			"node", best.node, "remaining", len(candidates)-1)
		candidates = slices.Delete(candidates, idx, idx+1)
	}
	return Assignment{}, fmt.Errorf("scale: claim %s/%s: every warm node raced to zero: %w", namespace, name, ErrNoWarmCapacity)
}

// Release returns the claimed microVM id to node's pool via that node's sandboxd,
// resolving the sandboxd address from the node's NodeInventory. It fails closed if
// claim routing is not configured. Callers must only reach this on owner-authorized
// teardown (the delete-authorization contract); it never destroys a VM on pod state.
func (s *scatterGatherStore) Release(ctx context.Context, node, id string) error {
	if id == "" {
		return fmt.Errorf("scale: release requires a claim id")
	}
	cl, err := s.nodeClient(ctx, node, "release", id)
	if err != nil {
		return err
	}
	// The uniform fleet api_token authorizes release by id.
	if err := cl.Release(ctx, id, s.sandboxdToken); err != nil {
		return fmt.Errorf("scale: sandboxd release of %q on node %q: %w", id, node, err)
	}
	return nil
}

// Watch merges per-node inventory into a single Sandbox event stream. This
// minimal-correct implementation re-derives the fanned-out list on a slow cadence
// and translates the diff into Added/Modified/Deleted events; a production
// implementation would merge real per-node watch streams instead of polling.
func (s *scatterGatherStore) Watch(ctx context.Context, opts ListOptions) (watch.Interface, error) {
	if _, _, err := parseSelectors(opts); err != nil {
		return nil, err
	}
	w := newInventoryWatcher()
	go s.runWatch(ctx, opts, w)
	return w, nil
}

// findEntry resolves the first entry matching match, synthesized as a Sandbox.
// It reads the node the index last saw holding key and only sweeps the whole
// fleet on a miss, canceling the rest of the sweep on the hit. Nil with a nil
// error means no entry matched.
func (s *scatterGatherStore) findEntry(ctx context.Context, op, key string, match inventoryMatch) (*sandboxv1beta1.Sandbox, error) {
	if node, ok := s.index.lookup(key); ok {
		if sb := s.matchOnNode(ctx, node, match); sb != nil {
			return sb, nil
		}
		s.index.forget(key)
	}
	nodes, err := s.src.ListNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("scale: enumerate node inventories: %w", err)
	}
	gctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g := &errgroup.Group{}
	if s.concurrency > 0 {
		g.SetLimit(s.concurrency)
	}
	var (
		mu    sync.Mutex
		found *sandboxv1beta1.Sandbox
	)
	for _, node := range nodes {
		g.Go(func() error {
			if gctx.Err() != nil {
				return nil
			}
			inv, err := s.src.NodeInventory(gctx, node)
			if err != nil {
				// A sibling goroutine's hit cancels gctx, which fails every read
				// still in flight; those are not unavailable nodes.
				if gctx.Err() == nil {
					s.log.V(1).Info("node inventory unavailable during "+op+"; skipping node",
						"node", node, "err", err.Error())
				}
				return nil
			}
			for i := range inv.Entries {
				if !match(inv, i) {
					continue
				}
				mu.Lock()
				if found == nil {
					found = entryToSandbox(inv.Node, inv.Entries[i])
					s.index.remember(key, inv.Node)
				}
				mu.Unlock()
				cancel()
				return nil
			}
			return nil
		})
	}
	_ = g.Wait()
	return found, nil
}

// matchOnNode resolves match against one node's inventory, returning nil when
// that node is unreadable or no longer holds the entry.
func (s *scatterGatherStore) matchOnNode(ctx context.Context, node string, match inventoryMatch) *sandboxv1beta1.Sandbox {
	inv, err := s.src.NodeInventory(ctx, node)
	if err != nil {
		return nil
	}
	for i := range inv.Entries {
		if match(inv, i) {
			return entryToSandbox(inv.Node, inv.Entries[i])
		}
	}
	return nil
}

// warmCandidates lists every node advertising warm capacity for pool, fanning
// out per node like List. A node whose inventory is unavailable is skipped, not
// fatal: the fleet stays claimable while one node is partitioned.
func (s *scatterGatherStore) warmCandidates(ctx context.Context, pool PoolKey) ([]warmCandidate, error) {
	return fanOutNodes(ctx, s, func(gctx context.Context, n string) []warmCandidate {
		addr, pools, err := s.src.NodeCapacity(gctx, n)
		if err != nil {
			s.log.V(1).Info("node inventory unavailable during claim node-pick; skipping",
				"node", n, "err", err.Error())
			return nil
		}
		if addr == "" {
			return nil
		}
		var out []warmCandidate
		for j := range pools {
			if pc := pools[j]; pc.Warm > 0 && poolCapacityMatches(pc, pool) {
				out = append(out, warmCandidate{node: n, addr: addr, warm: pc.Warm})
			}
		}
		return out
	})
}

func (s *scatterGatherStore) runWatch(ctx context.Context, opts ListOptions, w *inventoryWatcher) {
	defer w.terminate()

	known := map[string]*sandboxv1beta1.Sandbox{}
	emit := func(t watch.EventType, sb *sandboxv1beta1.Sandbox) bool {
		return w.send(ctx, watch.Event{Type: t, Object: sb})
	}

	if list, err := s.List(ctx, opts); err != nil {
		s.log.Error(err, "initial watch list failed")
	} else {
		for i := range list.Items {
			sb := list.Items[i].DeepCopy()
			known[objKey(sb)] = sb
			if !emit(watch.Added, sb) {
				return
			}
		}
	}

	// A fixed cadence, deliberately: backing off while quiet would let a sandbox
	// that is created and deleted inside the widened gap produce neither an Added
	// nor a Deleted. Re-deriving the fleet view costs 6.5ms at 26 nodes and 2600
	// sandboxes, and 1.2s at the 200x2000 projection, so one watcher per fleet is
	// the supported shape.
	ticker := time.NewTicker(s.watchPoll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.done:
			return
		case <-ticker.C:
			list, err := s.List(ctx, opts)
			if err != nil {
				s.log.Error(err, "watch poll list failed")
				continue
			}
			cur := make(map[string]*sandboxv1beta1.Sandbox, len(list.Items))
			for i := range list.Items {
				sb := list.Items[i].DeepCopy()
				k := objKey(sb)
				cur[k] = sb
				prev, ok := known[k]
				switch {
				case !ok:
					if !emit(watch.Added, sb) {
						return
					}
				case prev.ResourceVersion != sb.ResourceVersion:
					if !emit(watch.Modified, sb) {
						return
					}
				}
			}
			for k, prev := range known {
				if _, ok := cur[k]; !ok {
					if !emit(watch.Deleted, prev) {
						return
					}
				}
			}
			known = cur
		}
	}
}

// materialize turns one node's inventory entries into filtered Sandboxes.
func (s *scatterGatherStore) materialize(inv *NodeInventory, namespace string, labelSel labels.Selector, fieldSel fields.Selector) []sandboxv1beta1.Sandbox {
	out := make([]sandboxv1beta1.Sandbox, 0, len(inv.Entries))
	for i := range inv.Entries {
		sb := entryToSandbox(inv.Node, inv.Entries[i])
		if namespace != "" && sb.Namespace != namespace {
			continue
		}
		if !labelSel.Matches(labels.Set(sb.Labels)) {
			continue
		}
		if !fieldSel.Matches(sandboxFields(sb)) {
			continue
		}
		out = append(out, *sb)
	}
	return out
}

// AddressIPs strips the port from a "host:port" address, yielding the pod IP
// list a synthesized Sandbox status carries. Shared with the aggregated
// apiserver so both stamp identical PodIPs.
func AddressIPs(addr string) []string {
	if addr == "" {
		return nil
	}
	if host, _, err := net.SplitHostPort(addr); err == nil && host != "" {
		return []string{host}
	}
	return []string{addr}
}

// pickPowerOfTwo samples two candidates and keeps the warmer one. Node inventory
// is 5-30s stale, so always taking the global maximum funnels an entire burst onto
// whichever node looked best in that snapshot; sampling spreads the burst while
// still biasing toward warm capacity. A stale pick costs one gossip redirect.
func pickPowerOfTwo(candidates []warmCandidate) (warmCandidate, int) {
	if len(candidates) == 1 {
		return candidates[0], 0
	}
	//nolint:gosec // load spreading, not a security decision
	i := rand.IntN(len(candidates))
	//nolint:gosec // load spreading, not a security decision
	j := rand.IntN(len(candidates))
	if candidates[j].warm > candidates[i].warm {
		return candidates[j], j
	}
	return candidates[i], i
}

// inventoryWatcher is a watch.Interface fed by the store's poll-diff goroutine.
type inventoryWatcher struct {
	result chan watch.Event
	done   chan struct{}
	once   sync.Once
}

func newInventoryWatcher() *inventoryWatcher {
	return &inventoryWatcher{
		result: make(chan watch.Event, 64),
		done:   make(chan struct{}),
	}
}

func (w *inventoryWatcher) ResultChan() <-chan watch.Event { return w.result }

// Stop signals the producer to exit; safe to call multiple times.
func (w *inventoryWatcher) Stop() {
	w.once.Do(func() { close(w.done) })
}

// terminate is called once by the producer goroutine as it exits, closing the
// result channel so consumers observe end-of-stream.
func (w *inventoryWatcher) terminate() { close(w.result) }

// send delivers ev unless the watch has been stopped or the context is done.
func (w *inventoryWatcher) send(ctx context.Context, ev watch.Event) bool {
	select {
	case w.result <- ev:
		return true
	case <-w.done:
		return false
	case <-ctx.Done():
		return false
	}
}

// NodeLiveSource is a node's own live sandbox state — the sandboxd inventory /
// L0 node-scoped cache — NOT a cluster-wide LIST. A lost NodeInventory object is
// rebuilt from this on the next publish.
type NodeLiveSource interface {
	LiveSandboxes(ctx context.Context) ([]InventoryEntry, error)
}

// InventoryApplier server-side-applies a NodeInventory object. The default
// implementation resolves the resource through a RESTMapper (never a naive
// kind+"s"); tests inject a fake.
type InventoryApplier interface {
	Apply(ctx context.Context, inv *NodeInventory) error
}

var _ InventoryApplier = (*ssaInventoryApplier)(nil)

type ssaInventoryApplier struct {
	c          client.Client
	fieldOwner string
}

// NewSSAInventoryApplier returns the default server-side-apply InventoryApplier.
func NewSSAInventoryApplier(c client.Client, fieldOwner string) InventoryApplier {
	fieldOwner = cmp.Or(fieldOwner, "cocoon-node-inventory-publisher")
	return &ssaInventoryApplier{c: c, fieldOwner: fieldOwner}
}

func (a *ssaInventoryApplier) Apply(ctx context.Context, inv *NodeInventory) error {
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(inv)
	if err != nil {
		return fmt.Errorf("scale: encode node inventory: %w", err)
	}
	u := &unstructured.Unstructured{Object: raw}
	u.SetGroupVersionKind(NodeInventoryGVK)
	u.SetName(inv.Node)
	ac := client.ApplyConfigurationFromUnstructured(u)
	if err := a.c.Apply(ctx, ac, client.FieldOwner(a.fieldOwner), client.ForceOwnership); err != nil {
		return fmt.Errorf("scale: server-side-apply node inventory: %w", err)
	}
	return nil
}

var (
	_ InventorySource  = (*StaticInventorySource)(nil)
	_ InventoryApplier = (*StaticInventorySource)(nil)
)

// StaticInventorySource is an in-memory InventorySource that doubles as an
// InventoryApplier: publishers Apply into it (the O(nodes) "etcd" writes) and the
// store reads from it (the cache-fed NodeInventory reads). Production swaps in a
// client-backed source listing NodeInventory objects at ResourceVersion=0.
type StaticInventorySource struct {
	mu        sync.RWMutex
	inv       map[string]*NodeInventory
	partition map[string]struct{}
	applies   int
}

// NewStaticInventorySource returns an empty source.
func NewStaticInventorySource() *StaticInventorySource {
	return &StaticInventorySource{
		inv:       map[string]*NodeInventory{},
		partition: map[string]struct{}{},
	}
}

// Put stores a node inventory directly (test seeding).
func (s *StaticInventorySource) Put(inv *NodeInventory) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inv[inv.Node] = inv.DeepCopy()
}

func (s *StaticInventorySource) Apply(_ context.Context, inv *NodeInventory) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inv[inv.Node] = inv.DeepCopy()
	s.applies++
	return nil
}

// Partition keeps node in ListNodes but makes NodeInventory(node) fail, modeling
// a node partitioned from the aggregated server.
func (s *StaticInventorySource) Partition(node string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.partition[node] = struct{}{}
}

// Remove drops a node's inventory object entirely (lost inventory).
func (s *StaticInventorySource) Remove(node string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inv, node)
	delete(s.partition, node)
}

func (s *StaticInventorySource) ListNodes(_ context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Sorted(maps.Keys(s.inv)), nil
}

func (s *StaticInventorySource) NodeInventory(_ context.Context, node string) (*NodeInventory, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.partition[node]; ok {
		return nil, fmt.Errorf("scale: node %q partitioned from aggregated server", node)
	}
	inv, ok := s.inv[node]
	if !ok {
		return nil, fmt.Errorf("scale: no inventory published for node %q", node)
	}
	return inv.DeepCopy(), nil
}

func (s *StaticInventorySource) NodeCapacity(_ context.Context, node string) (string, []PoolCapacity, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.partition[node]; ok {
		return "", nil, fmt.Errorf("scale: node %q partitioned from aggregated server", node)
	}
	inv, ok := s.inv[node]
	if !ok {
		return "", nil, fmt.Errorf("scale: no inventory published for node %q", node)
	}
	return inv.Address, slices.Clone(inv.Pools), nil
}

// ObjectCount is the number of durable NodeInventory objects held — the O(nodes)
// etcd object count backing every synthesized sandbox.
func (s *StaticInventorySource) ObjectCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.inv)
}

// ApplyCount is the number of Apply calls, i.e. the size of the write path.
func (s *StaticInventorySource) ApplyCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.applies
}

var _ InventorySource = (*ClientInventorySource)(nil)

// ClientInventorySource is the production InventorySource: it reads NodeInventory
// objects through a controller-runtime reader. Back it with a cache-fed reader
// (cmd/sandbox-apiserver builds one scoped to exactly this GVK) so the O(nodes)
// enumeration is served from an informer, never a hot-path LIST off etcd.
// Objects are read as unstructured so any reader works without scheme wiring.
type ClientInventorySource struct {
	reader client.Reader
}

// NewClientInventorySource builds a ClientInventorySource over reader (use a
// cache-fed client in production).
func NewClientInventorySource(reader client.Reader) *ClientInventorySource {
	return &ClientInventorySource{reader: reader}
}

func (s *ClientInventorySource) ListNodes(ctx context.Context) ([]string, error) {
	ul := &unstructured.UnstructuredList{}
	ul.SetGroupVersionKind(NodeInventoryGVK.GroupVersion().WithKind(NodeInventoryGVK.Kind + "List"))
	if err := s.reader.List(ctx, ul); err != nil {
		return nil, fmt.Errorf("scale: list node inventories: %w", err)
	}
	nodes := make([]string, 0, len(ul.Items))
	for i := range ul.Items {
		nodes = append(nodes, ul.Items[i].GetName())
	}
	slices.Sort(nodes)
	return nodes, nil
}

func (s *ClientInventorySource) NodeInventory(ctx context.Context, node string) (*NodeInventory, error) {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(NodeInventoryGVK)
	if err := s.reader.Get(ctx, types.NamespacedName{Name: node}, u); err != nil {
		return nil, fmt.Errorf("scale: get node %q inventory: %w", node, err)
	}
	inv := &NodeInventory{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, inv); err != nil {
		return nil, fmt.Errorf("scale: decode node %q inventory: %w", node, err)
	}
	return inv, nil
}

func (s *ClientInventorySource) NodeCapacity(ctx context.Context, node string) (string, []PoolCapacity, error) {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(NodeInventoryGVK)
	if err := s.reader.Get(ctx, types.NamespacedName{Name: node}, u); err != nil {
		return "", nil, fmt.Errorf("scale: get node %q inventory: %w", node, err)
	}
	addr, _, err := unstructured.NestedString(u.Object, "address")
	if err != nil {
		return "", nil, fmt.Errorf("scale: decode node %q address: %w", node, err)
	}
	raw, _, err := unstructured.NestedSlice(u.Object, "pools")
	if err != nil {
		return "", nil, fmt.Errorf("scale: decode node %q pools: %w", node, err)
	}
	pools := make([]PoolCapacity, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		pc := PoolCapacity{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(m, &pc); err != nil {
			return "", nil, fmt.Errorf("scale: decode node %q pool capacity: %w", node, err)
		}
		pools = append(pools, pc)
	}
	return addr, pools, nil
}

// fanOutNodes enumerates the node inventories and runs work per node with
// List's bounded concurrency, concatenating per-node results in node order.
// A node the work skips contributes nil.
func fanOutNodes[T any](ctx context.Context, s *scatterGatherStore, work func(ctx context.Context, node string) []T) ([]T, error) {
	nodes, err := s.src.ListNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("scale: enumerate node inventories: %w", err)
	}
	perNode := make([][]T, len(nodes))
	g, gctx := errgroup.WithContext(ctx)
	if s.concurrency > 0 {
		g.SetLimit(s.concurrency)
	}
	for i, node := range nodes {
		g.Go(func() error {
			perNode[i] = work(gctx, node)
			return nil
		})
	}
	_ = g.Wait()
	return slices.Concat(perNode...), nil
}

// poolCapacityMatches reports whether a node's advertised pool capacity serves the
// requested pool key, normalizing the net/size defaults on both sides so an unset
// axis matches its default-named pool.
func poolCapacityMatches(pc PoolCapacity, key PoolKey) bool {
	return pc.Template == key.Template &&
		cmp.Or(pc.Net, NetDefault) == cmp.Or(key.Net, NetDefault) &&
		cmp.Or(pc.Size, SizeClassSmall) == cmp.Or(key.Size, SizeClassSmall)
}

// parseSelectors turns the string selectors on ListOptions into matchers. Empty
// strings become everything-matchers.
func parseSelectors(opts ListOptions) (labels.Selector, fields.Selector, error) {
	labelSel, err := labels.Parse(opts.LabelSelector)
	if err != nil {
		return nil, nil, fmt.Errorf("scale: parse label selector %q: %w", opts.LabelSelector, err)
	}
	fieldSel, err := fields.ParseSelector(opts.FieldSelector)
	if err != nil {
		return nil, nil, fmt.Errorf("scale: parse field selector %q: %w", opts.FieldSelector, err)
	}
	return labelSel, fieldSel, nil
}

// entryToSandbox synthesizes the Sandbox object served for one inventory entry.
// The entry name is the sandbox's "<namespace>/<name>"; an unqualified name
// lands in the default namespace.
func entryToSandbox(node string, e InventoryEntry) *sandboxv1beta1.Sandbox {
	ns, name := splitNamespacedName(e.Name)
	sb := &sandboxv1beta1.Sandbox{
		Namespace:       ns,
		Name:            name,
		Labels:          synthLabels(node, e),
		Annotations:     synthAnnotations(e),
		ResourceVersion: resourceVersionFor(ns, name, e),
		Status: sandboxv1beta1.SandboxStatus{
			NodeName: node,
			PodIPs:   AddressIPs(e.Address),
			Conditions: []metav1.Condition{{
				Type:    string(sandboxv1beta1.SandboxConditionReady),
				Status:  readyStatus(e.Phase),
				Reason:  readyReason(e.Phase),
				Message: fmt.Sprintf("phase %q reported by node %q inventory", e.Phase, node),
			}},
		},
	}
	return sb
}

func synthLabels(node string, e InventoryEntry) map[string]string {
	l := map[string]string{NodeLabel: node}
	if e.Phase != "" {
		l[PhaseLabel] = e.Phase
	}
	if e.Template != "" {
		l[TemplateLabel] = e.Template
	}
	if e.ClaimRef != "" {
		_, claim := splitNamespacedName(e.ClaimRef)
		if claim != "" {
			l[ClaimLabel] = claim
		}
	}
	return l
}

// synthAnnotations carries the node-reported facts that are not selector axes:
// the sandboxd claim id Delete releases by, and the granted deadline. Neither is
// stamped until the node publishes it, so Delete never guesses a claim by name.
func synthAnnotations(e InventoryEntry) map[string]string {
	a := map[string]string{}
	if e.ID != "" {
		a[ClaimIDAnnotation] = e.ID
	}
	if d := deadlineValue(e); d != "" {
		a[DeadlineAnnotation] = d
	}
	if len(a) == 0 {
		return nil
	}
	return a
}

func deadlineValue(e InventoryEntry) string {
	if e.Deadline == nil || e.Deadline.IsZero() {
		return ""
	}
	return e.Deadline.UTC().Format(time.RFC3339)
}

func sandboxFields(sb *sandboxv1beta1.Sandbox) fields.Set {
	return fields.Set{
		"metadata.name":      sb.Name,
		"metadata.namespace": sb.Namespace,
		"status.nodeName":    sb.Status.NodeName,
	}
}

func readyStatus(phase string) metav1.ConditionStatus {
	if strings.EqualFold(phase, "Running") || strings.EqualFold(phase, "Ready") {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}

func readyReason(phase string) string {
	return cmp.Or(phase, "Unknown")
}

// resourceVersionFor derives a deterministic, content-sensitive ResourceVersion
// so watch can detect a Modified entry and clients see a stable version for an
// unchanged one. It is opaque, as the API contract requires.
func resourceVersionFor(ns, name string, e InventoryEntry) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(ns + "/" + name + "|" + e.ID + "|" + e.Phase + "|" + e.ClaimRef + "|" + e.Address + "|" + e.Template + "|" + deadlineValue(e)))
	return strconv.FormatUint(h.Sum64(), 10)
}

func splitNamespacedName(s string) (namespace, name string) {
	if before, after, ok := strings.Cut(s, "/"); ok {
		return before, after
	}
	return metav1.NamespaceDefault, s
}

func objKey(sb *sandboxv1beta1.Sandbox) string { return sb.Namespace + "/" + sb.Name }
