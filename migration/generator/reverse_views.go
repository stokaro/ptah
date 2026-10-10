package generator

// Reversing views and materialized views: the families whose prior state is a
// body rather than a set of attributes.

import (
	"slices"
	"strings"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/objectlookup"
	"ptah.run/migration/schemadiff/difftypes"
)

// reverseViewDiffs carries modified views into the down direction.
//
// The entry is carried across rather than swapped with anything: the planner
// renders a modified view from the schema it is given (the pre-change database
// schema, in the down direction), so the entry itself is what selects the prior
// definition.
//
// PreviousBody is different in kind: it names the body the view HAS when the
// statement runs, not a change. When the rollback runs, the database holds what
// the up migration wrote, which is the generated schema's body -- so that is
// what the reversed entry must carry. Getting this wrong is not cosmetic: the
// PostgreSQL planner reads it to decide whether CREATE OR REPLACE VIEW is legal
// for the rollback, and PostgreSQL refuses the replace for every column-list
// change except a trailing append.
//
// A nil schema (the deprecated reverseSchemaDiff entry point) leaves it empty,
// which planners read as "not known" and answer with drop-and-recreate. That is
// the safe direction: it always applies.
//
// Rollback is set for the same reason and is the other half of it. Where a
// planner can neither prove the replace legal nor prove it refused, the answer
// it should give differs by direction, and this is the only place that knows
// which direction is being built.
func reverseViewDiffs(
	viewDiffs []difftypes.ViewDiff,
	schema, prior *schemamodel.Database,
	semantics identifier.Semantics,
) []difftypes.ViewDiff {
	reversed := make([]difftypes.ViewDiff, len(viewDiffs))
	for i, viewDiff := range viewDiffs {
		reversed[i] = difftypes.ViewDiff{
			ViewName:     viewDiff.ViewName,
			Changes:      reverseChangeMap(viewDiff.Changes),
			PreviousBody: generatedViewBody(schema, viewDiff.ViewName),
			// The view the database HAD, which is what this rollback restores.
			// The forward entry carries the declaration; reversing the change
			// map without reversing the operand would have the down direction
			// reapply the very body it is undoing (stokaro/ptah#2315).
			Desired:  priorView(prior, viewDiff.ViewName, semantics),
			Rollback: true,
		}
	}
	return reversed
}

// priorView is the view the pre-change database held, resolved on the terms the
// planner resolved it on when it was handed that schema directly: the diff
// spells a name the declaration used, and a database view carries the schema
// the server reports it under.
func priorView(prior *schemamodel.Database, name string, semantics identifier.Semantics) schemamodel.View {
	if prior == nil {
		return schemamodel.View{}
	}
	if view := objectlookup.View(prior.Views, name, semantics); view != nil {
		return *view
	}
	return schemamodel.View{}
}

func generatedViewBody(schema *schemamodel.Database, viewName string) string {
	if schema == nil {
		return ""
	}
	for _, view := range schema.Views {
		if view.Name == viewName {
			return strings.TrimSpace(view.Body)
		}
	}
	return ""
}

// reverseMaterializedViewDiffs carries modified materialized views into the
// down direction, on the same terms as reverseViewDiffs. A materialized view
// has no in-place replace at all, so there is no prior body to record: both
// directions drop and recreate it.
func reverseMaterializedViewDiffs(
	viewDiffs []difftypes.MaterializedViewDiff,
	prior *schemamodel.Database,
	semantics identifier.Semantics,
) []difftypes.MaterializedViewDiff {
	reversed := make([]difftypes.MaterializedViewDiff, len(viewDiffs))
	for i, viewDiff := range viewDiffs {
		reversed[i] = difftypes.MaterializedViewDiff{
			ViewName: viewDiff.ViewName,
			Changes:  reverseChangeMap(viewDiff.Changes),
			// The recreate half renders from the operand, so reversing the
			// change map without reversing the operand would rebuild the very
			// definition the rollback is undoing (stokaro/ptah#2315). The prior
			// view carries the settings the pre-change database held, such as
			// a refresh schedule, so a rollback that replaces the view
			// restores them; its feature changes are reversed by their owners
			// afterwards (stokaro/ptah#2418).
			Desired: priorMaterializedView(prior, viewDiff.ViewName, semantics),
		}
	}
	return reversed
}

// priorMaterializedViews are the removed views as the pre-change database held
// them. A removal carries the view in the shape the database reported it,
// without settings an owner would have to convert; the converted pre-change
// schema has them. A view it does not hold keeps the carried declaration.
func priorMaterializedViews(removed difftypes.MaterializedViewChanges, prior *schemamodel.Database, semantics identifier.Semantics) difftypes.MaterializedViewChanges {
	restored := slices.Clone(removed)
	for i, view := range restored {
		if held := priorMaterializedView(prior, view.Name, semantics); held.Name != "" {
			restored[i] = held
		}
	}
	return restored
}

// priorMaterializedView is the materialized view the pre-change database held,
// resolved the way [priorView] resolves a plain one: the diff spells a name the
// declaration used, and a database view carries the schema the server reports it
// under.
func priorMaterializedView(
	prior *schemamodel.Database,
	name string,
	semantics identifier.Semantics,
) schemamodel.MaterializedView {
	if prior == nil {
		return schemamodel.MaterializedView{}
	}
	if view := objectlookup.MaterializedView(prior.MaterializedViews, name, semantics); view != nil {
		return *view
	}
	return schemamodel.MaterializedView{}
}
