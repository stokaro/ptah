package providerhost

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/coverage"
	"ptah.run/core/schemaext"
)

func goHeaderKinds(description *protocol.Description) []string {
	var kinds []string
	for _, engine := range description.GetEngines() {
		if !slices.Contains(engine.GetFeatures(), provider.SourceLimitsFeature) {
			continue
		}
		kinds = append(kinds, engine.GetGoHeaderKinds()...)
	}
	slices.Sort(kinds)
	return slices.Compact(kinds)
}

// goCoverage keeps the cached empty-source claim only for documents without
// headers. A header is interpreted by its owner for this document, so a prior
// complete source cannot erase the limitation.
func (h *Host) goCoverage(limits []coverage.Object) (schemaext.Coverage, error) {
	if len(limits) == 0 {
		return h.goBaseCoverage()
	}
	claimed := goHeaderKinds(h.description)
	for _, limit := range limits {
		if !slices.Contains(claimed, string(limit.Kind)) || !limit.Reason.Valid() || !limit.Provenance.Valid() {
			return schemaext.Coverage{}, fmt.Errorf("%w: unclaimed or invalid Go source limit", provider.ErrProtocol)
		}
	}
	var combined schemaext.Coverage
	for _, engine := range h.description.GetEngines() {
		if len(engine.GetGoSourceDirectives()) == 0 {
			continue
		}
		var selected []coverage.Object
		for _, limit := range limits {
			if slices.Contains(engine.GetGoHeaderKinds(), string(limit.Kind)) {
				selected = append(selected, limit)
			}
		}
		decoded, err := h.decodeGoSource(engine, nil, nil, selected)
		if err != nil {
			return schemaext.Coverage{}, err
		}
		known, err := h.sourceCoverage(engine.GetId(), protocol.SourceDocument_GO_ANNOTATIONS, decoded.GetCoverage())
		if err != nil {
			return schemaext.Coverage{}, err
		}
		if combined, err = combined.Combine(known); err != nil {
			return schemaext.Coverage{}, err
		}
	}
	return combined, nil
}
