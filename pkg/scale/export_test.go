package scale

var (
	BenchFleets         = benchFleets
	BenchInventories    = benchInventories
	BenchNodeName       = benchNodeName
	ParseSelectors      = parseSelectors
	SplitNamespacedName = splitNamespacedName

	WarmCandidates  = (*scatterGatherStore).warmCandidates
	LookupName      = (*scatterGatherStore).lookupName
	ListInventories = (*scatterGatherStore).listInventories
	PollPinned      = (*scatterGatherStore).pollPinned
)

type ScatterGatherStore = scatterGatherStore
