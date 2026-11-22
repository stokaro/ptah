package clickhouse

import (
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/internal/droppedviews"
	"ptah.run/internal/objectlookup"
)

// nameHandovers are the relations a plan drops for good while it creates a
// relation of another kind under the same name: a view or materialized view
// that gives its name to a table, and a table that gives its name to a view
// or materialized view. ClickHouse keeps one namespace for all of them and
// refuses the creation while the old relation holds the name -- measured on
// 24.10 and 26.10, every direction answers "Code: 57. DB::Exception: Table
// d1.v already exists. (TABLE_ALREADY_EXISTS)" -- so each drop moves ahead of
// the creation that takes the name.
//
// The two directions move to different places. A view's drop goes ahead of
// CREATE TABLE, at the head of the phase that creates tables, since the
// table's own columns and features follow it there. A table's drop goes
// ahead of the view creations, at the head of the phase after storage, so the
// changes the plan makes to the table before it goes, such as dropping its
// row policies and grants, still find it. The drops are moved rather than
// added: the removal lists skip a relation a handover holds, so the plan still
// names each object once.
//
// A view or materialized view a table takes the name of drops its row
// policies first, as one dropped for good does ([rowPolicyDrops]). ClickHouse
// keeps a policy bound to the name after DROP VIEW and applies it to the
// table at once, so a policy left on the view would filter the new table, and
// the comparison stops reporting such a policy because the plan drops it
// ([droppedviews.Vacated]).
//
// A view replaced by a materialized view, or the other way round, is not a
// handover: [reportViewLikes] drops it ahead of the creation already.
type nameHandovers struct {
	// beforeTables are the drops of the views and materialized views a table
	// takes the name of, views first, each after the drops of its row
	// policies.
	beforeTables []ast.Node
	// beforeViews are the drops of the tables a view or materialized view
	// takes the name of.
	beforeViews []ast.Node
	// viewLikes are the removals beforeTables drops, for the removal list to
	// skip.
	viewLikes []droppedviews.Removal
	// tables are the removal list elements beforeViews drops, by exact
	// string, for the removal list to skip.
	tables []string
}

// planNameHandovers finds the handovers of diff. A name matches under
// objectlookup's identity rule, since the reader qualifies what a declaration
// leaves bare.
func (p *Planner) planNameHandovers(diff *difftypes.SchemaDiff) nameHandovers {
	semantics := diff.EffectiveIdentifierSemantics(platform.ClickHouse)
	tablesAdded := diff.TablesAdded.Names()
	viewLikesAdded := slices.Concat(diff.ViewsAdded.Names(), diff.MaterializedViewsAdded.Names())
	var handovers nameHandovers
	for _, removal := range droppedviews.Vacated(diff, semantics) {
		if !objectlookup.Contains(tablesAdded, removal.Name, semantics) {
			continue
		}
		handovers.viewLikes = append(handovers.viewLikes, removal)
		handovers.beforeTables = append(handovers.beforeTables, rowPolicyDrops(diff, removal.Name, semantics)...)
		handovers.beforeTables = append(handovers.beforeTables, dropViewLike(removal))
	}
	for _, name := range diff.TablesRemoved.Names() {
		if objectlookup.Contains(viewLikesAdded, name, semantics) {
			handovers.tables = append(handovers.tables, name)
			handovers.beforeViews = append(handovers.beforeViews, p.dropTable(name))
		}
	}
	return handovers
}
