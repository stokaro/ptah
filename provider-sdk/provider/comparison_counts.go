package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// ComparisonCountsFeature lets a comparison count the objects it compared, in
// ComparedModels.counts. A facet comparison counts the objects its settings
// stand for. It is a two-way feature: a provider lists it beside
// CompareModelsFeature, and so does each engine whose comparisons count. A
// service counts only for a request whose target features list it. Contract
// 25.13.
const ComparisonCountsFeature = "models.compare-counts"

// validateComparisonCounts checks the counts of a reply against its changes.
// Every change of a kind adds an object, removes one, or changes one both
// sides hold, and no change does two of these. So the additions and removals
// of a kind together are never more than its changes, and its changes are
// never more than its three counts together.
func validateComparisonCounts(description *protocol.Description, engine *protocol.Engine, request *protocol.CompareModelsRequest,
	service *protocol.ModelComparison, result *protocol.ComparedModels,
) error {
	counts := result.GetCounts()
	if len(counts) == 0 {
		return nil
	}
	if !EngineOffers(description, engine.GetId(), ComparisonCountsFeature) || !slices.Contains(request.GetTargetFeatures(), ComparisonCountsFeature) {
		return fmt.Errorf("%w: comparison counts need %s", ErrProtocol, ComparisonCountsFeature)
	}
	changed := make(map[string]uint64)
	for _, change := range result.GetChanges() {
		changed[change.GetValue().GetValue().GetKind()]++
	}
	counted := make(map[string]bool, len(counts))
	for _, count := range counts {
		kind := count.GetKind()
		if !slices.Contains(service.GetChangeKinds(), kind) || counted[kind] {
			return fmt.Errorf("%w: comparison counted a change kind %q the service lacks, or counted it twice", ErrProtocol, kind)
		}
		counted[kind] = true
		// Each count fits in 32 bits, so their sum cannot overflow 64.
		matched, added, removed := uint64(count.GetMatched()), uint64(count.GetAdded()), uint64(count.GetRemoved())
		if added+removed > changed[kind] || changed[kind] > matched+added+removed {
			return fmt.Errorf("%w: comparison counted %d matched, %d added and %d removed objects of change kind %q, which its %d changes of the kind contradict",
				ErrProtocol, matched, added, removed, kind, changed[kind])
		}
	}
	return nil
}
