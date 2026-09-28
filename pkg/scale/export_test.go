package scale

import "context"

var (
	BenchFleets         = benchFleets
	BenchInventories    = benchInventories
	BenchNodeName       = benchNodeName
	ParseSelectors      = parseSelectors
	SplitNamespacedName = splitNamespacedName

	LookupName      = (*scatterGatherStore).lookupName
	ListInventories = (*scatterGatherStore).listInventories
	PollPinned      = (*scatterGatherStore).pollPinned
)

type ScatterGatherStore = scatterGatherStore

func WarmCandidates(ctx context.Context, s *scatterGatherStore, pool PoolKey) ([]warmCandidate, error) {
	nodes, err := s.src.NodeCapacities(ctx)
	if err != nil {
		return nil, err
	}
	return warmCandidates(nodes, pool), nil
}
