package provider

import (
	"fmt"
	"slices"
	"strings"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// IndexPredicatesFeature has an engine state the predicate of a partial index
// on both sides in one spelling, the one its server holds. Table preparation
// states the predicate of each declared index in PreparedIndex.condition,
// as the engine would hold the declared one, and a read states
// ObservedIndex.condition of a held index the same way, leaving out what the
// engine adds to an index by itself, such as a filter a unique index carries
// implicitly. A host that receives both compares them exactly, so a
// predicate the server rewrote converges and a changed one is a change. It is
// a two-way feature that requires ObservedIndexesFeature and
// PrepareTablesFeature: the provider lists it, and so does each engine that
// states predicates. Contract 25.29.
const IndexPredicatesFeature = "schema.index-predicates"

func validateIndexPredicatesFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), IndexPredicatesFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), IndexPredicatesFeature) || !EngineOffers(description, engine.GetId(), ObservedIndexesFeature) ||
		!EngineOffers(description, engine.GetId(), PrepareTablesFeature) {
		return fmt.Errorf("%w: index predicates require the provider's feature, observed indexes and table preparation", ErrProtocol)
	}
	return nil
}

// validatePreparedIndexCondition checks the predicate one prepared index
// states under a negotiated feature: one exactly when the declaration states
// one, without surrounding white space.
func validatePreparedIndexCondition(declared *protocol.DeclaredIndex, prepared *protocol.PreparedIndex) error {
	condition := prepared.GetCondition()
	if (strings.TrimSpace(declared.GetCondition()) == "") != (condition == "") {
		return fmt.Errorf("%w: table preparation states a predicate for index %q exactly when the declaration states one", ErrProtocol, declared.GetName())
	}
	if strings.TrimSpace(condition) != condition {
		return fmt.Errorf("%w: table preparation states the predicate of index %q with surrounding white space", ErrProtocol, declared.GetName())
	}
	return nil
}

// validateInspectedIndexPredicates checks that a read under negotiated index
// predicates states each predicate without surrounding white space, the form a
// prepared predicate takes. A read without the feature is checked as before
// it.
func validateInspectedIndexPredicates(description *protocol.Description, engine *protocol.Engine, snapshot *protocol.Schema) error {
	if !EngineOffers(description, engine.GetId(), IndexPredicatesFeature) || !slices.Contains(snapshot.GetTargetFeatures(), IndexPredicatesFeature) {
		return nil
	}
	for _, table := range snapshot.GetTables() {
		for _, index := range table.GetIndexes() {
			if condition := index.GetCondition(); strings.TrimSpace(condition) != condition {
				return fmt.Errorf("%w: observed index %q states its predicate with surrounding white space", ErrProtocol, index.GetName())
			}
		}
	}
	return nil
}
