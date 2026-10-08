package schemapreparation

import (
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// retainEqualFeatures compares opaque values through their owners. Reusing the
// accepted immutable collections then lets the common-state guard inspect only
// common representation, without prescribing a provider's private layout.
// Both tables must be independent clones because this replaces their slots.
func retainEqualFeatures(before, after *Table) bool {
	if !before.Desired.OwnedObjects.Equal(after.Desired.OwnedObjects) ||
		!before.Current.OwnedObjects.Equal(after.Current.OwnedObjects) ||
		!before.Desired.FeatureCoverage.Equal(after.Desired.FeatureCoverage) ||
		!before.Current.FeatureCoverage.Equal(after.Current.FeatureCoverage) {
		return false
	}
	left, right := capturedFacetSlots(before), capturedFacetSlots(after)
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !equalFacets(*left[i], *right[i]) {
			return false
		}
		*right[i] = *left[i]
	}
	after.Desired.OwnedObjects = before.Desired.OwnedObjects
	after.Current.OwnedObjects = before.Current.OwnedObjects
	after.Desired.FeatureCoverage = before.Desired.FeatureCoverage
	after.Current.FeatureCoverage = before.Current.FeatureCoverage
	return true
}

func equalFacets(before, after schemaext.Facets) bool {
	if !before.Equal(after) || !slices.Equal(before.DeclaredKinds(), after.DeclaredKinds()) {
		return false
	}
	for _, kind := range before.DeclaredKinds() {
		if !slices.Equal(before.TargetScope(kind), after.TargetScope(kind)) {
			return false
		}
	}
	return true
}

func capturedFacetSlots(table *Table) []*schemaext.Facets {
	// Reuse each model's facet inventory. Child slices still address the cloned
	// capture; the table-level slots address it directly instead of a view copy.
	d := table.Desired
	o := table.Current
	declared := schemamodel.Database{Fields: d.Fields, Enums: d.Enums, Constraints: d.Constraints, Indexes: d.Indexes, Triggers: d.Triggers}
	observed := catalog.Database{Tables: []catalog.Table{{Columns: o.Table.Columns}}, Indexes: o.Indexes, Constraints: o.Constraints, Triggers: o.Triggers}
	slots := []*schemaext.Facets{&table.Desired.Table.Facets, &table.Current.Table.Facets}
	slots = append(slots, declared.FacetSlots()...)
	return append(slots, observed.FacetSlots()...)
}
