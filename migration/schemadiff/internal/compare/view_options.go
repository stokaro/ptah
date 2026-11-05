package compare

import (
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
)

// HeldMaterializedViewOptions is desired with each materialized view whose
// storage parameters desired does not describe given the parameters the
// database holds for it. A [coverage.MaterializedViewOptions] record says a
// view's are not described, and an HCL document makes one for every
// materialized view it declares, since the block has no attribute for them.
//
// A source with no spelling for the parameters states nothing about them, so
// the comparison plans neither a change nor a removal, and a rebuild that
// creates the view again writes back the ones it held. desired is not
// changed: a copy is returned when a view takes the parameters it holds.
func HeldMaterializedViewOptions(desired *schemamodel.Database, current *catalog.Database, semantics identifier.Semantics) *schemamodel.Database {
	if desired.NotDescribed.IsZero() {
		return desired
	}
	var out *schemamodel.Database
	for i, view := range desired.MaterializedViews {
		// The record names the view as the declaration does, so it is asked
		// by that name: a read reports a view in the default schema without
		// one.
		if desired.NotDescribed.Describes(coverage.MaterializedViewOptions, view.Name) {
			continue
		}
		held, found := heldMaterializedView(current, view.Name, semantics)
		if !found {
			continue
		}
		if out == nil {
			clone := *desired
			clone.MaterializedViews = slices.Clone(desired.MaterializedViews)
			out = &clone
		}
		out.MaterializedViews[i].Options = slices.Clone(held.Options)
	}
	if out == nil {
		return desired
	}
	return out
}

// heldMaterializedView is the materialized view the database holds under name,
// a name without a schema being one in the default schema.
func heldMaterializedView(current *catalog.Database, name string, semantics identifier.Semantics) (catalog.MaterializedView, bool) {
	key := semantics.QualifiedTableIdentityKey(name)
	for _, view := range current.MatViews {
		if semantics.QualifiedTableIdentityKey(view.QualifiedName()) == key {
			return view, true
		}
	}
	return catalog.MaterializedView{}, false
}
