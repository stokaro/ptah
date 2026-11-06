package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// PrimaryKeyChangesFeature states that an engine plans the change of the
// primary key of a table whose lifecycle it owns. With it negotiated, a host
// that compares a declared key with the ordered key a read reports
// (ObservedPrimaryKeysFeature) hands a difference in members or in order to
// the engine as an ALTER of the table: the desired declaration carries the
// declared key, the current observation carries the held key as its
// PRIMARY KEY constraint, and the engine plans the change or refuses it in
// its own words. Without it the host refuses such a change by name rather
// than plan the table as if its key matched. The provider and the engine
// list it; the engine needs ObservedPrimaryKeysFeature and a planning
// service with parent kinds, and does not list CommonTableIdentityFeature,
// under which the host plans the table's operations itself.
const PrimaryKeyChangesFeature = "planning.primary-key-changes"

func validatePrimaryKeyChangesFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), PrimaryKeyChangesFeature) {
		return nil
	}
	owns := slices.ContainsFunc(engine.GetPlanning(), func(assignment *protocol.ModelPlanning) bool { return len(assignment.GetParentKinds()) > 0 })
	if !slices.Contains(description.GetFeatures(), PrimaryKeyChangesFeature) || !EngineOffers(description, engine.GetId(), ObservedPrimaryKeysFeature) ||
		!owns || slices.Contains(engine.GetFeatures(), CommonTableIdentityFeature) {
		return fmt.Errorf("%w: primary key changes: the engine requires the provider's feature, observed primary keys "+
			"and a planning service that owns its tables", ErrProtocol)
	}
	return nil
}
