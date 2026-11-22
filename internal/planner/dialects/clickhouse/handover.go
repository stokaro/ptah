package clickhouse

import (
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
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
// row policies, still find it. The drops are moved rather than added: the
// removal lists skip a relation a handover holds, so the plan still names each
// object once.
//
// In both directions the grants the plan removes on the old relation are
// revoked directly ahead of its drop, as they are ahead of a table dropped
// for good. ClickHouse binds a grant to the name too, so a REVOKE left to the
// grant phase would run after the new relation's CREATE, and the old grant
// would apply to the new relation until then. The grant phase skips the
// revokes a handover holds.
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
	// revoked are the grant removals on a handed-over relation, which
	// beforeTables and beforeViews revoke ahead of its drop, for the grant
	// phase to skip.
	revoked []difftypes.GrantRef
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
		handovers.beforeTables = handovers.revokeOn(handovers.beforeTables, diff, removal.Name, semantics)
		handovers.beforeTables = append(handovers.beforeTables, rowPolicyDrops(diff, removal.Name, semantics)...)
		handovers.beforeTables = append(handovers.beforeTables, dropViewLike(removal))
	}
	for _, name := range diff.TablesRemoved.Names() {
		if objectlookup.Contains(viewLikesAdded, name, semantics) {
			handovers.tables = append(handovers.tables, name)
			handovers.beforeViews = handovers.revokeOn(handovers.beforeViews, diff, name, semantics)
			handovers.beforeViews = append(handovers.beforeViews, p.dropTable(name))
		}
	}
	return handovers
}

// revokeOn appends the revokes of the grants diff removes on the relation
// named name, and records them for the grant phase to skip. ClickHouse binds
// a grant to a name as it binds a row policy, so a grant revoked only after
// the handover would apply to the new relation from its CREATE until the
// REVOKE.
func (h *nameHandovers) revokeOn(result []ast.Node, diff *difftypes.SchemaDiff, name string, semantics identifier.Semantics) []ast.Node {
	for _, grant := range diff.GrantsRemoved {
		if !strings.EqualFold(grant.ObjectType, "TABLE") || !objectlookup.Contains([]string{name}, grant.ObjectName, semantics) {
			continue
		}
		h.revoked = append(h.revoked, grant)
		result = append(result, revokeNode(grant))
	}
	return result
}
