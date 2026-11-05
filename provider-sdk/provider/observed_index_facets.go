package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// ObservedIndexFacetsFeature enables ObservedIndex.facets: the settings of a
// common index a table holds, read into the engine's own models and owned by
// the index. A source states the same settings as a facet of the index its
// index directive or entry declares, so the host compares the two sides of one
// owner. The provider and the engine list it, and the engine needs
// ObservedIndexesFeature, which carries the index the facets belong to.
//
// An engine that lists it keeps an index's facets through a rename: the
// rename is the engine's own step, and the host compares a renamed index's
// facets under the name the declaration gives it.
const ObservedIndexFacetsFeature = "schema.observed-index-facets"

func validateObservedIndexFacetsFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), ObservedIndexFacetsFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), ObservedIndexFacetsFeature) || !EngineOffers(description, engine.GetId(), ObservedIndexesFeature) {
		return fmt.Errorf("%w: observed index facets require the provider's feature and observed indexes", ErrProtocol)
	}
	return nil
}

// IndexReference returns the owner of a facet of the index name of table: the
// table's engine, database and schema, kind "index", the table as the only
// path component, and the index's name. It returns nil for a nil table.
func IndexReference(table *protocol.Reference, name string) *protocol.Reference {
	if table == nil {
		return nil
	}
	result := proto.CloneOf(table)
	result.Path = append(result.Path, result.Name)
	result.Kind, result.Name, result.DisplayName = "index", name, ""
	return result
}

// validateInspectedIndexFacets refuses a facet of an observed index owned by
// anything but that index.
func validateInspectedIndexFacets(table *protocol.Table, index *protocol.ObservedIndex) error {
	owner := IndexReference(table.GetIdentity(), index.GetName())
	for _, value := range index.GetFacets() {
		if value == nil || compareReferences(value.GetOwner(), owner) != 0 {
			return fmt.Errorf("%w: inspected facet of index %q belongs to another index", ErrProtocol, index.GetName())
		}
	}
	return nil
}
