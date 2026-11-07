package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// ConstraintChangesFeature states that an engine plans the addition and the
// removal of a FOREIGN KEY or CHECK constraint on a table whose lifecycle it
// owns and the plan keeps. With it negotiated, a host that compares the
// declared constraints with the ones a read reports (ObservedConstraintsFeature)
// hands a table whose constraints differ to the engine as an ALTER of the
// table: the desired declaration carries the declared constraints, in
// DeclaredTable.constraints, DeclaredTable.checks and the columns' own
// checks and references, and the current observation carries the held ones
// in ObservedTable.constraints. The engine plans the change or refuses it in
// its own words, and the host adds no step of its own for it. Without it the
// host refuses such a change by name rather than plan the table as if its
// constraints matched. The provider and the engine list it; the engine needs
// ObservedConstraintsFeature and a planning service with parent kinds, and
// does not list CommonTableIdentityFeature, under which the host plans the
// table's operations itself. Contract 25.28.
const ConstraintChangesFeature = "planning.constraint-changes"

func validateConstraintChangesFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), ConstraintChangesFeature) {
		return nil
	}
	owns := slices.ContainsFunc(engine.GetPlanning(), func(assignment *protocol.ModelPlanning) bool { return len(assignment.GetParentKinds()) > 0 })
	if !slices.Contains(description.GetFeatures(), ConstraintChangesFeature) || !EngineOffers(description, engine.GetId(), ObservedConstraintsFeature) ||
		!owns || slices.Contains(engine.GetFeatures(), CommonTableIdentityFeature) {
		return fmt.Errorf("%w: constraint changes: the engine requires the provider's feature, observed constraints "+
			"and a planning service that owns its tables", ErrProtocol)
	}
	return nil
}
