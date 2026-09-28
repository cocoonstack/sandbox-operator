package scale

import (
	"cmp"
	"context"
	"encoding/json/v2"
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

	"github.com/projecteru2/core/log"
	"golang.org/x/sync/errgroup"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/watch"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"

	cocoonv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
)

const (
	// NodeLabel carries the owning node of a synthesized Sandbox.
	NodeLabel = "sandbox.cocoonstack.io/node"
	// PhaseLabel carries the entry phase of a synthesized Sandbox.
	PhaseLabel      = "sandbox.cocoonstack.io/phase"
	PhaseRunning    = "Running"
	PhaseHibernated = "Hibernated"
	// ClaimLabel carries the claim name a synthesized Sandbox is bound to.
	ClaimLabel = "sandbox.cocoonstack.io/claim"
	// TemplateLabel carries the pool template of a synthesized Sandbox, since no per-sandbox object holds its pod spec.
	TemplateLabel = "sandbox.cocoonstack.io/template"

	// ClaimIDAnnotation carries the sandboxd claim id Delete releases, since one name can hold several claims.
	ClaimIDAnnotation = "sandbox.cocoonstack.io/claim-id"
	// DeadlineAnnotation carries the node-granted lease expiry of a Sandbox, in RFC3339.
	DeadlineAnnotation = "sandbox.cocoonstack.io/deadline"
	// NetAnnotation selects the pool network mode for both Create and the warm-pool driver.
	NetAnnotation = "sandbox.cocoonstack.io/net"
	// TokenAnnotation carries the per-sandbox ownership token handed back on Create.
	TokenAnnotation = "sandbox.cocoonstack.io/token"
	// MetadataAnnotation carries the claim's caller metadata as one JSON object.
	MetadataAnnotation = "sandbox.cocoonstack.io/metadata"
	// CPUCountAnnotation and MemoryBytesAnnotation carry the size tier the VM was booted with.
	CPUCountAnnotation    = "sandbox.cocoonstack.io/cpu-count"
	MemoryBytesAnnotation = "sandbox.cocoonstack.io/memory-bytes"

	// Selector keys are the pod annotations the vk-sandbox provider reads its claim axes from.
	SelectorTemplateKey = "sandbox.cocoonstack.io/template"
	SelectorNetKey      = NetAnnotation
	SelectorSizeKey     = "sandbox.cocoonstack.io/size"

	// Idle conns per host match the per-node claim fan-out, so a burst reuses connections.
	sandboxdRequestTimeout      = 10 * time.Second
	sandboxdDialTimeout         = time.Second
	sandboxdMaxIdleConns        = 256
	sandboxdMaxIdleConnsPerHost = 32
	sandboxdIdleConnTimeout     = 90 * time.Second
)

var (
	// NodeInventoryGVK is in this operator's CRD group, since the APIService hands agents.x-k8s.io to the aggregated server.
	NodeInventoryGVK = cocoonv1beta1.GroupVersion.WithKind("NodeInventory")

	// ErrNoWarmCapacity lets the aggregated apiserver map an exhausted pool to a retryable 503 instead of writing an object.
	ErrNoWarmCapacity = errors.New("scale: no node has warm capacity for the requested pool")
)

// InventorySource enumerates nodes and fetches each one's NodeInventory, so a partitioned node drops out of a List instead of failing it.
type InventorySource interface {
	// ListNodes returns the nodes the source holds inventory for. O(nodes), from memory.
	ListNodes(ctx context.Context) ([]string, error)
	// NodeInventory returns one node's inventory, or an error for an unreadable or unpublished node.
	NodeInventory(ctx context.Context, node string) (*NodeInventory, error)
	// NodeCapacity returns one node's advertise address and warm pools without decoding its entries.
	NodeCapacity(ctx context.Context, node string) (address string, pools []PoolCapacity, err error)
	// NodeCapacities returns every readable node's address, warm pools and promoted templates in ListNodes order; a source may return its own shared slice, so callers never mutate it.
	NodeCapacities(ctx context.Context) ([]NodePools, error)
}

// NodePools is one node's advertise address, warm pools and promoted templates.
type NodePools struct {
	Node      string
	Address   string
	Pools     []PoolCapacity
	Templates []PromotedTemplate
}

// StoreOption configures a scatterGatherStore.
type StoreOption func(*scatterGatherStore)

// WithWatchPollInterval sets how often Watch re-derives the inventories, one second by default.
func WithWatchPollInterval(d time.Duration) StoreOption {
	return func(s *scatterGatherStore) { s.watchPoll = d }
}

// WithClaimRouting lets the store call nodes with the fleet api_token, and without it node calls fail closed and lookups read inventory alone.
func WithClaimRouting(token string, factory SandboxdClientFactory) StoreOption {
	return func(s *scatterGatherStore) {
		s.sandboxdToken = token
		s.sandboxdFactory = factory
	}
}

// SandboxdClient is the subset of the sandboxd HTTP client the store needs.
type SandboxdClient interface {
	Claim(ctx context.Context, spec sandboxd.ClaimSpec) (sandboxd.ClaimResult, error)
	Release(ctx context.Context, id, token string) error

	// The lifecycle verbs use the fleet api_token, so the control plane holds no per-sandbox secret.
	Hibernate(ctx context.Context, id string) error
	Wake(ctx context.Context, id string) error
	Renew(ctx context.Context, id string, spec sandboxd.RenewSpec) (time.Time, error)
	Fork(ctx context.Context, id string, spec sandboxd.ForkSpec) (sandboxd.ForkResult, error)
	Checkpoint(ctx context.Context, id string, spec sandboxd.CheckpointSpec) (sandboxd.Checkpoint, error)
	Checkpoints(ctx context.Context) ([]sandboxd.Checkpoint, error)
	DeleteCheckpoint(ctx context.Context, checkpointID string) error
	DeleteTemplate(ctx context.Context, key sandboxd.PoolKey) error
	Promote(ctx context.Context, id, template string) (sandboxd.PoolKey, string, error)
	SetTemplateLabels(ctx context.Context, key sandboxd.PoolKey, labels map[string]string) error
	Info(ctx context.Context) (*sandboxd.NodeInfo, error)
	DialPort(ctx context.Context, id, token string, port uint16) (net.Conn, error)
	SetInstanceMetadata(ctx context.Context, id string, doc []byte) error

	// Sandbox and SandboxesByClaimRef read the node's own index, which a published inventory lags.
	Sandbox(ctx context.Context, id string) (sandboxd.SandboxSummary, error)
	SandboxesByClaimRef(ctx context.Context, ref string) ([]sandboxd.SandboxSummary, error)
}

// SandboxdClientFactory builds a sandboxd client for one node's advertise address and the fleet api_token.
type SandboxdClientFactory func(addr, token string) SandboxdClient

// NewSandboxdClientFactory returns the production factory, whose clients share one HTTP client.
func NewSandboxdClientFactory() SandboxdClientFactory {
	hc := NewSandboxdHTTPClient()
	return func(addr, token string) SandboxdClient {
		return sandboxd.New(SandboxdBaseURL(addr), token, sandboxd.WithHTTPClient(hc))
	}
}

type inventoryMatch func(inv *NodeInventory, i int) bool

type warmCandidate struct {
	node string
	addr string
	warm int
}

var _ SandboxStore = (*scatterGatherStore)(nil)

type scatterGatherStore struct {
	src         InventorySource
	concurrency int
	watchPoll   time.Duration
	index       *nodeIndex

	sandboxdToken   string
	sandboxdFactory SandboxdClientFactory
}

// NewScatterGatherStore builds the aggregated store over src.
func NewScatterGatherStore(src InventorySource, opts ...StoreOption) SandboxStore {
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

func (s *scatterGatherStore) List(ctx context.Context, opts ListOptions) (*sandboxv1beta1.SandboxList, error) {
	labelSel, fieldSel, err := parseSelectors(opts)
	if err != nil {
		return nil, err
	}
	items, err := s.listItems(ctx, opts.Namespace, labelSel, fieldSel)
	if err != nil {
		return nil, err
	}
	list := &sandboxv1beta1.SandboxList{}
	list.SetGroupVersionKind(sandboxv1beta1.GroupVersion.WithKind("SandboxList"))
	list.Items = items
	if list.Items == nil {
		list.Items = []sandboxv1beta1.Sandbox{} // an empty list serializes as [], not null
	}
	return list, nil
}

func (s *scatterGatherStore) Get(ctx context.Context, namespace, name string) (*sandboxv1beta1.Sandbox, error) {
	found, err := s.lookupName(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	if found == nil {
		return nil, k8serrors.NewNotFound(sandboxv1beta1.Resource("sandboxes"), name)
	}
	return found, nil
}

func (s *scatterGatherStore) GetByClaimID(ctx context.Context, namespace, id string, match func(claimID string) bool) (*sandboxv1beta1.Sandbox, error) {
	found, err := s.resolve(ctx, "claim-id get", claimKey(namespace, id), rowByID(id), func(inv *NodeInventory, i int) bool {
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

func (s *scatterGatherStore) Claim(ctx context.Context, namespace, name string, pool PoolKey, opts ClaimOptions) (Assignment, error) {
	if s.sandboxdFactory == nil {
		return Assignment{}, fmt.Errorf("scale: claim routing not configured (call WithClaimRouting)")
	}
	nodes, err := s.src.NodeCapacities(ctx)
	if err != nil {
		return Assignment{}, fmt.Errorf("scale: enumerate node capacity: %w", err)
	}
	candidates := warmCandidates(nodes, pool)
	promoted := len(candidates) == 0
	if promoted {
		candidates = templateCandidates(nodes, pool)
	}
	if len(candidates) == 0 {
		return Assignment{}, fmt.Errorf("scale: claim %s/%s: no node advertises warm capacity for template %q net %q size %q: %w", namespace, name, pool.Template, pool.Net, pool.Size, ErrNoWarmCapacity)
	}

	spec := sandboxd.ClaimSpec{
		Template:   pool.Template,
		Net:        pool.Net,
		Size:       pool.Size,
		TTLSeconds: opts.TTLSeconds,
		// The claim ref is the object's namespace/name, which the read path resolves by.
		ClaimRef:        namespacedName(namespace, name),
		Metadata:        opts.Metadata,
		OnExpire:        opts.OnExpire,
		RequirePromoted: promoted,
	}
	if opts.NoEgress {
		spec.Egress = new(false)
	}
	// Inventory is 5-30s stale, so a capacity miss drops that node and re-samples the rest instead of failing.
	for len(candidates) > 0 {
		best := pickPowerOfTwo(candidates)
		node := best.node
		res, claimErr := s.sandboxdFactory(best.addr, s.sandboxdToken).Claim(ctx, spec)
		redirect, _ := errors.AsType[*sandboxd.RedirectError](claimErr)
		if redirect != nil {
			node, res, claimErr = s.claimRedirected(ctx, nodes, node, redirect, spec)
		}
		if claimErr == nil {
			s.index.remember(nameKey(namespace, name), node)
			s.index.remember(claimKey(namespace, res.ID), node)
			return Assignment{SandboxName: res.ID, Node: node, Address: res.OwnerAddr, Token: res.Token, Deadline: res.Deadline, NetRoute: res.NetRoute}, nil
		}
		if tryNext := claimUndelivered(claimErr) || promoted && templateGone(claimErr); !tryNext {
			return Assignment{}, fmt.Errorf("scale: claim %s/%s on node %q: %w", namespace, name, node, claimErr)
		}
		candidates = slices.DeleteFunc(candidates, func(c warmCandidate) bool {
			return c.node == best.node || redirect != nil && slices.Contains(redirect.Targets, c.addr)
		})
		log.WithFunc("scale.Claim").Debugf(ctx, "node delivered nothing for the claim; trying another node node=%s err=%v remaining=%d", node, claimErr, len(candidates))
	}
	return Assignment{}, fmt.Errorf("scale: claim %s/%s: no warm node delivered: %w", namespace, name, ErrNoWarmCapacity)
}

func (s *scatterGatherStore) Release(ctx context.Context, node, id string) error {
	if id == "" {
		return fmt.Errorf("scale: release requires a claim id")
	}
	cl, err := s.nodeClient(ctx, node, "release", id)
	if err != nil {
		return err
	}
	if err := cl.Release(ctx, id, s.sandboxdToken); err != nil {
		return fmt.Errorf("scale: sandboxd release of %q on node %q: %w", id, node, err)
	}
	return nil
}

func (s *scatterGatherStore) Watch(ctx context.Context, opts ListOptions) (watch.Interface, error) {
	labelSel, fieldSel, err := parseSelectors(opts)
	if err != nil {
		return nil, err
	}
	ch := make(chan watch.Event, 64)
	w := watch.NewProxyWatcher(ch)
	go s.runWatch(ctx, opts, labelSel, fieldSel, w, ch)
	return w, nil
}

// resolve tries the indexed node's inventory, that node, every inventory, then every node, and returns nil, nil on a miss.
func (s *scatterGatherStore) resolve(ctx context.Context, op, key string, rows nodeRows, match inventoryMatch) (*sandboxv1beta1.Sandbox, error) {
	if node, ok := s.index.lookup(key); ok {
		if sb := s.matchOnNode(ctx, op, node, match); sb != nil {
			return sb, nil
		}
		if sb := s.liveOnNode(ctx, op, node, rows, match); sb != nil {
			return sb, nil
		}
	}
	found, err := FirstHit(ctx, s.src, s.concurrency, func(gctx context.Context, node string) *sandboxv1beta1.Sandbox {
		return s.matchOnNode(gctx, op, node, match)
	})
	if found == nil && err == nil && s.sandboxdFactory != nil {
		found, err = FirstHit(ctx, s.src, s.concurrency, func(gctx context.Context, node string) *sandboxv1beta1.Sandbox {
			return s.liveOnNode(gctx, op, node, rows, match)
		})
	}
	if found != nil {
		s.index.remember(key, found.Status.NodeName)
	}
	return found, err
}

func (s *scatterGatherStore) matchOnNode(ctx context.Context, op, node string, match inventoryMatch) *sandboxv1beta1.Sandbox {
	inv, err := s.src.NodeInventory(ctx, node)
	if err != nil {
		// A sibling's hit cancels ctx, so a read that fails from it is not an unavailable node.
		if ctx.Err() == nil {
			log.WithFunc("scale.matchOnNode").Debugf(ctx, "node inventory unavailable during %s; skipping node node=%s err=%v", op, node, err)
		}
		return nil
	}
	for i := range inv.Entries {
		if match(inv, i) {
			return entryToSandbox(inv.Node, inv.Entries[i])
		}
	}
	return nil
}

func (s *scatterGatherStore) claimRedirected(ctx context.Context, nodes []NodePools, from string, redirect *sandboxd.RedirectError, spec sandboxd.ClaimSpec) (string, sandboxd.ClaimResult, error) {
	logger := log.WithFunc("scale.claimRedirected")
	spec.NoRedirect = true
	for _, target := range redirect.Targets {
		node := nodeForAddress(nodes, target)
		if node == "" {
			logger.Debugf(ctx, "redirect target is no known node; skipping from=%s target=%s", from, target)
			continue
		}
		res, err := s.sandboxdFactory(target, s.sandboxdToken).Claim(ctx, spec)
		if err == nil {
			logger.Debugf(ctx, "claim followed a redirect from=%s node=%s id=%s", from, node, res.ID)
			return node, res, nil
		}
		if !claimUndelivered(err) {
			return node, res, err
		}
		logger.Debugf(ctx, "redirect target delivered nothing; skipping from=%s node=%s err=%v", from, node, err)
	}
	return from, sandboxd.ClaimResult{}, redirect
}

func (s *scatterGatherStore) runWatch(ctx context.Context, opts ListOptions, labelSel labels.Selector, fieldSel fields.Selector, w *watch.ProxyWatcher, ch chan watch.Event) {
	defer close(ch)

	logger := log.WithFunc("scale.runWatch")
	name, pinned := pinnedName(opts.Namespace, fieldSel)
	known := map[string]*sandboxv1beta1.Sandbox{}
	poll := func() ([]sandboxv1beta1.Sandbox, error) {
		if pinned {
			return s.pollPinned(ctx, opts.Namespace, name, known[namespacedName(opts.Namespace, name)], labelSel, fieldSel)
		}
		return s.listInventories(ctx, opts.Namespace, labelSel, fieldSel)
	}
	emit := func(t watch.EventType, sb *sandboxv1beta1.Sandbox) bool {
		select {
		case ch <- watch.Event{Type: t, Object: sb}:
			return true
		case <-w.StopChan():
			return false
		case <-ctx.Done():
			return false
		}
	}

	if items, err := s.listItems(ctx, opts.Namespace, labelSel, fieldSel); err != nil {
		logger.Error(ctx, err, "initial watch list failed")
	} else {
		for i := range items {
			sb := items[i].DeepCopy()
			known[objKey(sb)] = sb
			if !emit(watch.Added, sb) {
				return
			}
		}
	}
	if opts.WatchList && !emit(watch.Bookmark, &sandboxv1beta1.Sandbox{Annotations: map[string]string{metav1.InitialEventsAnnotationKey: "true"}}) {
		return
	}

	// A fixed cadence, since backing off would let a sandbox created and deleted in the widened gap emit no event.
	ticker := time.NewTicker(s.watchPoll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.StopChan():
			return
		case <-ticker.C:
			items, err := poll()
			if err != nil {
				logger.Error(ctx, err, "watch poll list failed")
				continue
			}
			cur := make(map[string]*sandboxv1beta1.Sandbox, len(items))
			for i := range items {
				sb := items[i].DeepCopy()
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
				if _, ok := cur[k]; ok {
					continue
				}
				if pinned && s.heldByNode(ctx, prev, labelSel, fieldSel) {
					cur[k] = prev
					continue
				}
				if !emit(watch.Deleted, prev) {
					return
				}
			}
			known = cur
		}
	}
}

func (s *scatterGatherStore) listItems(ctx context.Context, namespace string, labelSel labels.Selector, fieldSel fields.Selector) ([]sandboxv1beta1.Sandbox, error) {
	if name, ok := pinnedName(namespace, fieldSel); ok {
		return s.listPinned(ctx, namespace, name, labelSel, fieldSel)
	}
	return s.listInventories(ctx, namespace, labelSel, fieldSel)
}

func (s *scatterGatherStore) listInventories(ctx context.Context, namespace string, labelSel labels.Selector, fieldSel fields.Selector) ([]sandboxv1beta1.Sandbox, error) {
	items, err := fanOutNodes(ctx, s, func(gctx context.Context, node string) []sandboxv1beta1.Sandbox {
		inv, invErr := s.src.NodeInventory(gctx, node)
		if invErr != nil {
			log.WithFunc("scale.listInventories").Debugf(gctx, "node inventory unavailable; omitting from list (eventual consistency) node=%s err=%v", node, invErr)
			return nil
		}
		return s.materialize(inv, namespace, labelSel, fieldSel)
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(items, func(a, b sandboxv1beta1.Sandbox) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name))
	})
	return items, nil
}

func (s *scatterGatherStore) listPinned(ctx context.Context, namespace, name string, labelSel labels.Selector, fieldSel fields.Selector) ([]sandboxv1beta1.Sandbox, error) {
	sb, err := s.lookupName(ctx, namespace, name)
	if sb != nil && !selected(sb, labelSel, fieldSel) {
		return s.pollPinned(ctx, namespace, name, nil, labelSel, fieldSel)
	}
	return selectedOne(sb, err, labelSel, fieldSel)
}

func (s *scatterGatherStore) pollPinned(ctx context.Context, namespace, name string, prev *sandboxv1beta1.Sandbox, labelSel labels.Selector, fieldSel fields.Selector) ([]sandboxv1beta1.Sandbox, error) {
	match := nameMatch(namespace, name)
	if prev != nil {
		return selectedOne(s.matchOnNode(ctx, "watch", prev.Status.NodeName, match), nil, labelSel, fieldSel)
	}
	sb, err := FirstHit(ctx, s.src, s.concurrency, func(gctx context.Context, node string) *sandboxv1beta1.Sandbox {
		if sb := s.matchOnNode(gctx, "watch", node, match); sb != nil && selected(sb, labelSel, fieldSel) {
			return sb
		}
		return nil
	})
	return selectedOne(sb, err, labelSel, fieldSel)
}

func (s *scatterGatherStore) lookupName(ctx context.Context, namespace, name string) (*sandboxv1beta1.Sandbox, error) {
	return s.resolve(ctx, "get", nameKey(namespace, name), rowsByClaimRef(namespacedName(namespace, name)), nameMatch(namespace, name))
}

func (s *scatterGatherStore) heldByNode(ctx context.Context, sb *sandboxv1beta1.Sandbox, labelSel labels.Selector, fieldSel fields.Selector) bool {
	live := s.liveOnNode(ctx, "watch", sb.Status.NodeName, rowsByClaimRef(objKey(sb)), nameMatch(sb.Namespace, sb.Name))
	return live != nil && selected(live, labelSel, fieldSel)
}

func (s *scatterGatherStore) materialize(inv *NodeInventory, namespace string, labelSel labels.Selector, fieldSel fields.Selector) []sandboxv1beta1.Sandbox {
	out := make([]sandboxv1beta1.Sandbox, 0, len(inv.Entries))
	for i := range inv.Entries {
		if ns, _ := splitNamespacedName(inv.Entries[i].Name); namespace != "" && ns != namespace {
			continue
		}
		if sb := entryToSandbox(inv.Node, inv.Entries[i]); selected(sb, labelSel, fieldSel) {
			out = append(out, *sb)
		}
	}
	return out
}

// NodeLiveSource is a node's own live sandbox state, from which a lost NodeInventory is rebuilt.
type NodeLiveSource interface {
	LiveSandboxes(ctx context.Context) ([]InventoryEntry, error)
}

// InventoryApplier server-side-applies a NodeInventory object.
type InventoryApplier interface {
	Apply(ctx context.Context, inv *NodeInventory) error
}

var (
	_ InventorySource  = (*StaticInventorySource)(nil)
	_ InventoryApplier = (*StaticInventorySource)(nil)
)

// StaticInventorySource is an in-memory InventorySource that is also an InventoryApplier.
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

// Partition keeps node in ListNodes but makes its NodeInventory fail, like a partitioned node.
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

func (s *StaticInventorySource) NodeCapacities(context.Context) ([]NodePools, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]NodePools, 0, len(s.inv))
	for _, node := range slices.Sorted(maps.Keys(s.inv)) {
		if _, partitioned := s.partition[node]; partitioned {
			continue
		}
		inv := s.inv[node]
		out = append(out, NodePools{Node: node, Address: inv.Address, Pools: slices.Clone(inv.Pools), Templates: slices.Clone(inv.Templates)})
	}
	return out, nil
}

// ObjectCount is the number of NodeInventory objects held, the O(nodes) etcd object count.
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

// IsNoWarmCapacity reports whether err means Claim found no warm node.
func IsNoWarmCapacity(err error) bool { return errors.Is(err, ErrNoWarmCapacity) }

// NewSandboxdHTTPClient returns one HTTP client for the fleet, since the default transport keeps only 2 idle conns per host.
func NewSandboxdHTTPClient() *http.Client {
	return &http.Client{
		Timeout: sandboxdRequestTimeout,
		Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: sandboxdDialTimeout}).DialContext,
			MaxIdleConns:        sandboxdMaxIdleConns,
			MaxIdleConnsPerHost: sandboxdMaxIdleConnsPerHost,
			IdleConnTimeout:     sandboxdIdleConnTimeout,
		},
	}
}

// SandboxdBaseURL gives a bare host:port the http scheme and keeps an address that already has one.
func SandboxdBaseURL(addr string) string {
	if strings.Contains(addr, "://") {
		return addr
	}
	return "http://" + addr
}

// AddressIPs strips the port from a host:port address to give a synthesized Sandbox's pod IPs.
func AddressIPs(addr string) []string {
	if addr == "" {
		return nil
	}
	if _, rest, ok := strings.Cut(addr, "://"); ok {
		addr = rest
	}
	if host, _, err := net.SplitHostPort(addr); err == nil && host != "" {
		return []string{host}
	}
	return []string{addr}
}

// MetadataOf decodes a synthesized Sandbox's caller metadata, nil when it carries none.
func MetadataOf(sb *sandboxv1beta1.Sandbox) map[string]string {
	raw := sb.Annotations[MetadataAnnotation]
	if raw == "" {
		return nil
	}
	var md map[string]string
	if json.Unmarshal([]byte(raw), &md) != nil {
		return nil
	}
	return md
}

// FirstHit runs find on every node src lists, concurrency at a time, and returns the first non-zero result and cancels the rest.
func FirstHit[T comparable](ctx context.Context, src InventorySource, concurrency int, find func(ctx context.Context, node string) T) (T, error) {
	var found T
	nodes, err := src.ListNodes(ctx)
	if err != nil {
		return found, fmt.Errorf("scale: enumerate node inventories: %w", err)
	}
	gctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g := &errgroup.Group{}
	if concurrency > 0 {
		g.SetLimit(concurrency)
	}
	var (
		mu   sync.Mutex
		zero T
	)
	for _, node := range nodes {
		g.Go(func() error {
			if gctx.Err() != nil {
				return nil
			}
			hit := find(gctx, node)
			if hit == zero {
				return nil
			}
			mu.Lock()
			found = cmp.Or(found, hit)
			mu.Unlock()
			cancel()
			return nil
		})
	}
	_ = g.Wait()
	return found, nil
}

// fanOutNodes runs work on every node at the store's concurrency and concatenates the results in node order.
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

// poolCapacityMatches compares pc against key, defaulting each unset net/size axis.
func nodeForAddress(nodes []NodePools, addr string) string {
	if i := slices.IndexFunc(nodes, func(n NodePools) bool { return n.Address == addr }); i >= 0 {
		return nodes[i].Node
	}
	return ""
}

func warmCandidates(nodes []NodePools, pool PoolKey) []warmCandidate {
	var out []warmCandidate
	for _, n := range nodes {
		if n.Address == "" {
			continue
		}
		for _, pc := range n.Pools {
			if pc.Warm > 0 && poolCapacityMatches(pc, pool) {
				out = append(out, warmCandidate{node: n.Node, addr: n.Address, warm: pc.Warm})
			}
		}
	}
	return out
}

// templateCandidates lists the nodes whose inventory holds pool as a promoted template, for a claim no warm pool serves.
func templateCandidates(nodes []NodePools, pool PoolKey) []warmCandidate {
	var out []warmCandidate
	for _, n := range nodes {
		if n.Address != "" && slices.ContainsFunc(n.Templates, func(t PromotedTemplate) bool {
			return poolCapacityMatches(PoolCapacity{Template: t.Template, Net: t.Net, Size: t.Size}, pool)
		}) {
			out = append(out, warmCandidate{node: n.Node, addr: n.Address, warm: 1})
		}
	}
	return out
}

func poolCapacityMatches(pc PoolCapacity, key PoolKey) bool {
	return pc.Template == key.Template &&
		cmp.Or(pc.Net, NetDefault) == cmp.Or(key.Net, NetDefault) &&
		cmp.Or(pc.Size, SizeClassSmall) == cmp.Or(key.Size, SizeClassSmall)
}

// pickPowerOfTwo keeps the warmer of two sampled candidates, so a burst spreads instead of funneling onto one node.
func pickPowerOfTwo(candidates []warmCandidate) warmCandidate {
	//nolint:gosec // load spreading, not a security decision
	i := rand.IntN(len(candidates))
	//nolint:gosec // load spreading, not a security decision
	j := rand.IntN(len(candidates))
	if candidates[j].warm > candidates[i].warm {
		return candidates[j]
	}
	return candidates[i]
}

func parseSelectors(opts ListOptions) (labels.Selector, fields.Selector, error) {
	labelSel, err := labels.Parse(opts.LabelSelector)
	if err != nil {
		return nil, nil, k8serrors.NewBadRequest(fmt.Sprintf("parse label selector %q: %v", opts.LabelSelector, err))
	}
	fieldSel, err := fields.ParseSelector(opts.FieldSelector)
	if err != nil {
		return nil, nil, k8serrors.NewBadRequest(fmt.Sprintf("parse field selector %q: %v", opts.FieldSelector, err))
	}
	known := sandboxFields(&sandboxv1beta1.Sandbox{})
	for _, req := range fieldSel.Requirements() {
		if _, ok := known[req.Field]; !ok {
			return nil, nil, k8serrors.NewBadRequest(fmt.Sprintf("field selector %q is not supported on sandboxes", req.Field))
		}
	}
	return labelSel, fieldSel, nil
}

// entryToSandbox puts an entry name without a namespace in the default namespace.
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
				Reason:  cmp.Or(e.Phase, "Unknown"),
				Message: fmt.Sprintf("phase %q reported by node %q inventory", e.Phase, node),
			}},
		},
	}
	if e.ClaimedAt != nil {
		sb.CreationTimestamp = *e.ClaimedAt
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

// synthAnnotations stamps only what the node reported, so Delete never guesses a claim by name.
func synthAnnotations(e InventoryEntry) map[string]string {
	a := map[string]string{}
	if e.ID != "" {
		a[ClaimIDAnnotation] = e.ID
	}
	if d := deadlineValue(e); d != "" {
		a[DeadlineAnnotation] = d
	}
	if e.Metadata != "" {
		a[MetadataAnnotation] = e.Metadata
	}
	if e.CPUCount > 0 {
		a[CPUCountAnnotation] = strconv.FormatInt(int64(e.CPUCount), 10)
	}
	if e.MemoryBytes > 0 {
		a[MemoryBytesAnnotation] = strconv.FormatInt(e.MemoryBytes, 10)
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

func claimedAtValue(e InventoryEntry) string {
	if e.ClaimedAt == nil {
		return ""
	}
	return strconv.FormatInt(e.ClaimedAt.Unix(), 10)
}

func pinnedName(namespace string, fieldSel fields.Selector) (string, bool) {
	name, ok := fieldSel.RequiresExactMatch("metadata.name")
	return name, ok && name != "" && namespace != ""
}

func nameMatch(namespace, name string) inventoryMatch {
	return func(inv *NodeInventory, i int) bool {
		ens, ename := splitNamespacedName(inv.Entries[i].Name)
		return ens == namespace && ename == name
	}
}

func selected(sb *sandboxv1beta1.Sandbox, labelSel labels.Selector, fieldSel fields.Selector) bool {
	return labelSel.Matches(labels.Set(sb.Labels)) && fieldSel.Matches(sandboxFields(sb))
}

func selectedOne(sb *sandboxv1beta1.Sandbox, err error, labelSel labels.Selector, fieldSel fields.Selector) ([]sandboxv1beta1.Sandbox, error) {
	if err != nil || sb == nil || !selected(sb, labelSel, fieldSel) {
		return nil, err
	}
	return []sandboxv1beta1.Sandbox{*sb}, nil
}

func sandboxFields(sb *sandboxv1beta1.Sandbox) fields.Set {
	return fields.Set{
		"metadata.name":      sb.Name,
		"metadata.namespace": sb.Namespace,
		"status.nodeName":    sb.Status.NodeName,
	}
}

func readyStatus(phase string) metav1.ConditionStatus {
	if strings.EqualFold(phase, PhaseRunning) || strings.EqualFold(phase, "Ready") {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}

// resourceVersionFor hashes an entry's content, so watch sees a change as Modified and an unchanged entry keeps its version.
func resourceVersionFor(ns, name string, e InventoryEntry) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(ns + "/" + name + "|" + e.ID + "|" + e.Phase + "|" + e.ClaimRef + "|" + e.Address + "|" + e.Template + "|" + deadlineValue(e) + "|" + claimedAtValue(e)))
	return strconv.FormatUint(h.Sum64(), 10)
}

func namespacedName(namespace, name string) string { return namespace + "/" + name }

func splitNamespacedName(s string) (namespace, name string) {
	if before, after, ok := strings.Cut(s, "/"); ok {
		return before, after
	}
	return metav1.NamespaceDefault, s
}

func objKey(sb *sandboxv1beta1.Sandbox) string { return namespacedName(sb.Namespace, sb.Name) }

// A timeout after the request went out may have delivered a microVM, so it is never retried elsewhere.
func claimUndelivered(err error) bool {
	if errors.Is(err, sandboxd.ErrNodeAtCapacity) {
		return true
	}
	if opErr, ok := errors.AsType[*net.OpError](err); ok && opErr.Op == "dial" {
		return true
	}
	httpErr, ok := errors.AsType[*sandboxd.HTTPError](err)
	return ok && httpErr.StatusCode >= http.StatusInternalServerError
}

// templateGone is an advertiser that no longer holds the template, which a stale inventory still lists.
func templateGone(err error) bool {
	httpErr, ok := errors.AsType[*sandboxd.HTTPError](err)
	return ok && httpErr.StatusCode == http.StatusNotFound
}
