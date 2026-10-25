package postgres

import (
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/internal/deporder"
)

// viewsReadingRetypedColumns finds the views and materialized views that have to
// leave the database while a column they read changes its type.
//
// PostgreSQL refuses ALTER COLUMN ... TYPE on a column a view reads, and it does
// so whether or not the view itself changes:
//
//	ALTER TABLE t ALTER COLUMN a TYPE varchar(128);
//	ERROR:  cannot alter type of a column used by a view or rule
//
// A plan that leaves the view in place therefore fails when it runs, after
// golang-migrate-style runners have already marked the version dirty. The
// dependents are dropped before the column step and put back after the view
// step, which is the one position where every table and column they read
// exists in its final shape.
//
// Dependency is read the way the cascade logic reads it, off the bodies, so a
// view that reads another view that reads the table is found too, whether the
// declaration keeps it or not. The result holds only objects the live database
// has: a view this plan adds does not exist yet, and the plan creates it after
// the columns anyway.
//
// The second list is the live views the declaration no longer holds. They also
// block the ALTER, or block the drop of a view that does, and the removal step
// runs after it, so they are dropped here and need no recreate.
//
// Every reader is on one of the two lists, so the drops can run without
// CASCADE once each reader goes before what it reads; see
// [Planner.dropViewsBeforeRetype].
func viewsReadingRetypedColumns(diff *difftypes.SchemaDiff) (recreate, removeOnly []deporder.ViewLike) {
	tables := retypedTables(diff)
	if len(tables) == 0 {
		return nil, nil
	}

	added := make(map[string]bool)
	for _, view := range diff.ViewsAdded {
		added[view.Name] = true
	}
	for _, view := range diff.MaterializedViewsAdded {
		added[view.Name] = true
	}

	candidates := make([]deporder.ViewLike, 0, len(diff.DeclaredViewLikes.Views)+len(diff.DeclaredViewLikes.MaterializedViews))
	for _, view := range diff.DeclaredViewLikes.Views {
		if !added[view.Name] {
			candidates = append(candidates, deporder.ViewLike{Name: view.Name, Body: view.Body})
		}
	}
	for _, view := range diff.DeclaredViewLikes.MaterializedViews {
		if !added[view.Name] {
			candidates = append(candidates, deporder.ViewLike{Name: view.Name, Body: view.Body, Materialized: true})
		}
	}
	declared := len(candidates)
	candidates = append(candidates, removedViewLikes(diff)...)

	removed := make(map[string]bool, len(candidates)-declared)
	for _, object := range candidates[declared:] {
		removed[object.Name] = true
	}
	for _, object := range readersOf(candidates, tables) {
		if removed[object.Name] {
			removeOnly = append(removeOnly, object)
			continue
		}
		recreate = append(recreate, object)
	}
	return recreate, removeOnly
}

// removedViewLikes is the views and materialized views the declaration no
// longer holds, with the bodies the database holds them with.
func removedViewLikes(diff *difftypes.SchemaDiff) []deporder.ViewLike {
	removed := make([]deporder.ViewLike, 0, len(diff.ViewsRemoved)+len(diff.MaterializedViewsRemoved))
	for _, view := range diff.MaterializedViewsRemoved {
		removed = append(removed, deporder.ViewLike{Name: view.Name, Body: view.Body, Materialized: true})
	}
	for _, view := range diff.ViewsRemoved {
		removed = append(removed, deporder.ViewLike{Name: view.Name, Body: view.Body})
	}
	return removed
}

// retypeDropped names every object [Planner.dropViewsBeforeRetype] drops. A
// later step that would drop one of them again leaves it out, and one that
// creates it again creates it outright: it is gone when that step runs.
func retypeDropped(diff *difftypes.SchemaDiff) map[string]bool {
	recreate, removeOnly := viewsReadingRetypedColumns(diff)
	gone := make(map[string]bool, len(recreate)+len(removeOnly))
	for _, object := range recreate {
		gone[object.Name] = true
	}
	for _, object := range removeOnly {
		gone[object.Name] = true
	}
	return gone
}

// readersOf returns the candidates that read one of the named relations,
// directly or through another returned candidate.
func readersOf(candidates []deporder.ViewLike, names []string) []deporder.ViewLike {
	var found []deporder.ViewLike
	taken := make(map[string]bool)
	frontier := append([]string(nil), names...)
	for len(frontier) > 0 {
		name := frontier[0]
		frontier = frontier[1:]
		for _, candidate := range candidates {
			if taken[candidate.Name] || !deporder.ReferencesIdentifier(candidate.Body, name) {
				continue
			}
			taken[candidate.Name] = true
			frontier = append(frontier, candidate.Name)
			found = append(found, candidate)
		}
	}
	return found
}

// retypedTables names the tables with at least one column whose type changes.
func retypedTables(diff *difftypes.SchemaDiff) []string {
	var tables []string
	for _, tableDiff := range diff.TablesModified {
		for _, colDiff := range tableDiff.ColumnsModified {
			if _, retyped := colDiff.Changes["type"]; retyped {
				tables = append(tables, tableDiff.TableName)
				break
			}
		}
	}
	return tables
}

// dropViewsBeforeRetype renders the drops that clear the way for a column type
// change. It runs before the column step.
//
// Each object is dropped after every object on the lists that reads it, so no
// drop relies on CASCADE to take a reader the plan knows of: that reader went
// first. CASCADE is then written only in Ptah's own statement style, where it
// also takes the readers the plan does not know of, such as a view in a
// schema the run leaves out; the exact style leaves the server to refuse
// those.
func (p *Planner) dropViewsBeforeRetype(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	recreate, removeOnly := viewsReadingRetypedColumns(diff)
	objects := append(append([]deporder.ViewLike(nil), recreate...), removeOnly...)
	for _, object := range deporder.ViewLikesForDropForDialect(objects, p.targetDialect()) {
		result = append(result, p.dropViewLikeNode(object, p.cascades(false)))
	}
	return result
}

// recreateViewsAfterRetype puts back what dropViewsBeforeRetype took. It runs
// after the view step, in creation order, and skips an object that step already
// renders: a modified view or materialized view is written there from the same
// declared body, and so is a view that step puts back after one it dropped.
//
// Each object is created outright rather than re-asserted with CREATE OR
// REPLACE: dropViewsBeforeRetype dropped it by name, so it is known to be gone.
func (p *Planner) recreateViewsAfterRetype(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	recreate, _ := viewsReadingRetypedColumns(diff)
	if len(recreate) == 0 {
		return result
	}

	rendered := p.viewStepRenders(diff)
	for _, viewDiff := range diff.MaterializedViewsModified {
		rendered[viewDiff.Desired.Name] = true
	}
	pending := make([]deporder.ViewLike, 0, len(recreate))
	dropped := make([]string, 0, len(recreate))
	for _, object := range recreate {
		if !rendered[object.Name] {
			pending = append(pending, object)
			dropped = append(dropped, object.Name)
		}
	}

	for _, object := range deporder.ViewLikesForCreateForDialect(pending, p.targetDialect()) {
		result = p.appendViewLikeRecreate(result, diff.DeclaredViewLikes, object, dropped, semantics)
	}
	return result
}

// dropViewLikeNode is the statement that drops one view or materialized view.
func (p *Planner) dropViewLikeNode(object deporder.ViewLike, cascade bool) ast.Node {
	if object.Materialized {
		return dropMaterializedViewNode(object.Name, cascade)
	}
	return p.dropViewNode(object.Name, cascade)
}
