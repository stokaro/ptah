package provider

import (
	"fmt"
	"slices"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// DeclaredRelationsFeature gives CompareModelsRequest.declared_relations its
// complete meaning: every table, view and materialized view the desired schema
// declares, each identity's kind naming which ("table", "view" or
// "materialized view"). A comparison of models that reference a relation,
// such as a transfer into a table, then knows whether the relation is
// declared. A host that does not list the feature sends the declared views
// and materialized views alone, as a host of contract 25.31 does. An object
// of the provider's own models, such as a topic, is not a relation and is
// never listed: the comparison's state carries the objects of its own models,
// and DeclaredObjectsFeature the declared objects of a model another
// comparison owns. It is a two-way feature: the provider lists it, and so
// does each engine whose comparisons read it. Contract 25.32.
const DeclaredRelationsFeature = "models.declared-relations"

// Kinds of a declared relation, as a ComparisonIdentity names them.
const (
	relationTable            = "table"
	relationView             = "view"
	relationMaterializedView = "materialized view"
)

// validateDeclaredRelationsFeature refuses an engine that lists the feature
// without the provider's, or without a model comparison to read the list.
func validateDeclaredRelationsFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), DeclaredRelationsFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), DeclaredRelationsFeature) || !slices.Contains(description.GetFeatures(), CompareModelsFeature) ||
		len(engine.GetComparisons()) == 0 {
		return fmt.Errorf("%w: declared relations require the provider's feature and a model comparison", ErrProtocol)
	}
	return nil
}

// validateDeclaredRelations checks the kind of each declared relation: a
// view or a materialized view, or a table as well under the feature.
func validateDeclaredRelations(description *protocol.Description, request *protocol.CompareModelsRequest) error {
	kinds := []string{relationView, relationMaterializedView}
	if EngineOffers(description, request.GetEngine(), DeclaredRelationsFeature) && slices.Contains(request.GetTargetFeatures(), DeclaredRelationsFeature) {
		kinds = append(kinds, relationTable)
	}
	for _, relation := range request.GetDeclaredRelations() {
		if !slices.Contains(kinds, relation.GetKind()) {
			return fmt.Errorf("%w: a declared relation of kind %q is not one this comparison carries", ErrProtocol, relation.GetKind())
		}
	}
	return nil
}
