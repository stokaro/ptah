package providerhost

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// The wire indexes only assigned objects. Core diagnostics index the whole
// owner-local capture so aggregation can remap a refusal across services.
func remapObjectDiagnostics(remote *protocol.PlanFeaturesRequest, all []objectidentity.ID, result *featureplan.Result) error {
	subjects := provider.PlanningObjectSubjects(remote)
	for i, diagnostic := range result.Diagnostics {
		if diagnostic.ContextObject == nil {
			continue
		}
		index := *diagnostic.ContextObject
		if index < 0 || index >= len(subjects) {
			return fmt.Errorf("%w: planning diagnostic has an invalid object index", schemaext.ErrInvalidValue)
		}
		subject := boundIdentity(subjects[index])
		mapped := slices.IndexFunc(all, func(candidate objectidentity.ID) bool { return candidate.Key() == subject.Key() })
		if mapped < 0 {
			return fmt.Errorf("%w: planning diagnostic has no captured object", schemaext.ErrInvalidValue)
		}
		result.Diagnostics[i].ContextObject = new(mapped)
	}
	return nil
}
