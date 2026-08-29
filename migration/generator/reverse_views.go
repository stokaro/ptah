package generator

// Reversing views, materialized views, hypertables and continuous aggregates:
// the families whose prior state is a body rather than a set of attributes.

import (
	"strings"

	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/planner/objectlookup"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// reverseHypertableDiffs swaps the two sides of each partitioning change, so a
// rollback describes the partitioning the database had.
func reverseHypertableDiffs(changes []difftypes.HypertableDiff) []difftypes.HypertableDiff {
	reversed := make([]difftypes.HypertableDiff, 0, len(changes))
	for _, change := range changes {
		reversed = append(reversed, difftypes.HypertableDiff{
			Table:            change.Table,
			OldColumn:        change.NewColumn,
			NewColumn:        change.OldColumn,
			OldChunkInterval: change.NewChunkInterval,
			NewChunkInterval: change.OldChunkInterval,
		})
	}
	return reversed
}

// reverseContinuousAggregateDiffs swaps the two sides of each aggregate change,
// so a rollback restores the body and the option the database had.
func reverseContinuousAggregateDiffs(
	changes []difftypes.ContinuousAggregateDiff,
	prior *schemamodel.Database,
	semantics identifier.Semantics,
) []difftypes.ContinuousAggregateDiff {
	reversed := make([]difftypes.ContinuousAggregateDiff, 0, len(changes))
	for _, change := range changes {
		reversed = append(reversed, difftypes.ContinuousAggregateDiff{
			Name:                change.Name,
			OldBody:             change.NewBody,
			NewBody:             change.OldBody,
			OldMaterializedOnly: change.NewMaterializedOnly,
			NewMaterializedOnly: change.OldMaterializedOnly,
			// Swapping the bodies without swapping the operand would have the
			// down direction drop the aggregate and create the very definition
			// it is undoing, since the operand is what the create renders from
			// (stokaro/ptah#2315).
			Desired: priorContinuousAggregate(prior, change.Name, semantics),
		})
	}
	return reversed
}

// priorContinuousAggregate is the aggregate the pre-change database held.
//
// The diff spells a qualified name the declaration produced, and the aggregate
// a read reports carries the schema the server puts it under, so the two are
// compared as qualified names on the connection's own identifier terms.
func priorContinuousAggregate(
	prior *schemamodel.Database,
	name string,
	semantics identifier.Semantics,
) schemamodel.ContinuousAggregate {
	if prior == nil {
		return schemamodel.ContinuousAggregate{}
	}
	if aggregate := objectlookup.Qualified(prior.ContinuousAggregates, name, semantics); aggregate != nil {
		return *aggregate
	}
	return schemamodel.ContinuousAggregate{}
}

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
			// A ClickHouse refresh schedule is a Desired/Current pair for the
			// reason the table's TTL is: the planner needs both sides to tell
			// an ALTER from a rebuild. Dropping it meant a rollback restored
			// the view without the schedule it had, so its rows would be right
			// once and never again (stokaro/ptah#2418).
			RefreshChange: reverseRefreshChange(viewDiff.RefreshChange),
			// The recreate half renders from the operand, so reversing the
			// change map without reversing the operand would rebuild the very
			// definition the rollback is undoing (stokaro/ptah#2315).
			Desired: priorMaterializedView(prior, viewDiff.ViewName, semantics),
		}
	}
	return reversed
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
