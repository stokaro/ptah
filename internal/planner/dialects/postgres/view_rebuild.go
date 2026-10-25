package postgres

import (
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/deporder"
)

// A view rebuild takes views and materialized views out of the database before
// the column step and puts them back after the view step. Two changes need it.
//
// A column type change. PostgreSQL refuses ALTER COLUMN ... TYPE on a column a
// view reads, whether or not the view itself changes:
//
//	ALTER TABLE t ALTER COLUMN a TYPE varchar(128);
//	ERROR:  cannot alter type of a column used by a view or rule
//
// A plan that leaves the view in place fails when it runs, after
// golang-migrate-style runners have already marked the version dirty.
//
// A materialized view change. No engine replaces one in place, so the plan
// drops it and creates it again, and the drop takes every view and
// materialized view that reads it. The declaration still holds those readers,
// so the rebuild puts them back; a plan that only dropped and created the
// materialized view applied, the readers were gone, and the next plan created
// them again (stokaro/ptah#4964).
//
// The objects leave before the column step and return after the view step,
// which is the one position where every table and column they read exists in
// its final shape, and so does every view the view step changes.
//
// Dependency is read the way the cascade logic reads it, off the declared
// bodies, so a view that reads another view that reads the table is found too,
// whether the declaration keeps it or not. Every object that reads a rebuilt
// object is rebuilt with it, so the drops run without CASCADE once each object
// goes after every object that reads it; see [Planner.dropRebuiltViewLikes].
//
// The rebuild owns its objects. No other step drops or creates one of them,
// and [viewRebuild.owns] names them for those steps.
type viewRebuild struct {
	// recreate is the objects the database holds and the declaration keeps:
	// dropped, then created from the declaration.
	recreate []deporder.ViewLike
	// removeOnly is the objects the database holds and the declaration no
	// longer does. They also stand in the way of the ALTER or of a drop, and
	// the removal step runs only after the column step, so they are dropped
	// here and not created again.
	removeOnly []deporder.ViewLike
	// deferred is the objects this plan adds that read one of the others.
	// The step that creates added objects runs while the rebuilt ones are
	// absent, so these are created with them, after the objects they read.
	deferred []deporder.ViewLike
}

// planViewRebuild finds the objects the plan rebuilds: every view and
// materialized view that reads a column whose type changes, every
// materialized view that changes, and everything that reads one of those.
func planViewRebuild(diff *difftypes.SchemaDiff) viewRebuild {
	modified := modifiedMaterializedViews(diff)
	seeds := retypedTables(diff)
	for _, view := range modified {
		seeds = append(seeds, view.Name)
	}
	if len(seeds) == 0 {
		return viewRebuild{}
	}

	// A changed materialized view is rebuilt whether or not anything reads
	// it, from the declaration its change carries. Its name is a seed, so
	// what reads it is found without it being a candidate.
	rebuild := viewRebuild{recreate: slices.Clone(modified)}
	candidates, kinds := rebuildCandidates(diff, &rebuild)
	for _, i := range readerIndexes(candidates, seeds) {
		*kinds[i] = append(*kinds[i], candidates[i])
	}
	return rebuild
}

// rebuildCandidates is every object a rebuild may take, and beside each the
// list of rebuild it joins if it reads a rebuilt object: a declared object the
// database holds is recreated, a removed one is only dropped, and one the plan
// adds is deferred. The changed materialized views already on the recreate
// list are not candidates. A view the plan turns into a materialized view, or
// back, is two candidates of one name.
func rebuildCandidates(diff *difftypes.SchemaDiff, rebuild *viewRebuild) ([]deporder.ViewLike, []*[]deporder.ViewLike) {
	skip := make(map[string]bool)
	for _, object := range rebuild.recreate {
		skip[object.Name] = true
	}
	added := make([]deporder.ViewLike, 0, len(diff.ViewsAdded)+len(diff.MaterializedViewsAdded))
	for _, view := range diff.ViewsAdded {
		skip[view.Name] = true
		added = append(added, deporder.ViewLike{Name: view.Name, Body: view.Body})
	}
	for _, view := range diff.MaterializedViewsAdded {
		skip[view.Name] = true
		added = append(added, deporder.ViewLike{Name: view.Name, Body: view.Body, Materialized: true})
	}

	var candidates []deporder.ViewLike
	var kinds []*[]deporder.ViewLike
	join := func(object deporder.ViewLike, kind *[]deporder.ViewLike) {
		candidates = append(candidates, object)
		kinds = append(kinds, kind)
	}
	for _, view := range diff.DeclaredViewLikes.Views {
		if !skip[view.Name] {
			join(deporder.ViewLike{Name: view.Name, Body: view.Body}, &rebuild.recreate)
		}
	}
	for _, view := range diff.DeclaredViewLikes.MaterializedViews {
		if !skip[view.Name] {
			join(deporder.ViewLike{Name: view.Name, Body: view.Body, Materialized: true}, &rebuild.recreate)
		}
	}
	for _, object := range removedViewLikes(diff) {
		join(object, &rebuild.removeOnly)
	}
	for _, object := range added {
		join(object, &rebuild.deferred)
	}
	return candidates, kinds
}

// modifiedMaterializedViews is the materialized views the plan changes, with
// the declarations the changes carry. A change carrying none names a view the
// pre-change database did not hold, and there is nothing to rebuild.
func modifiedMaterializedViews(diff *difftypes.SchemaDiff) []deporder.ViewLike {
	modified := make([]deporder.ViewLike, 0, len(diff.MaterializedViewsModified))
	for _, viewDiff := range diff.MaterializedViewsModified {
		view := viewDiff.Desired
		if view.Name == "" || slices.ContainsFunc(modified, func(seen deporder.ViewLike) bool { return seen.Name == view.Name }) {
			continue
		}
		modified = append(modified, deporder.ViewLike{Name: view.Name, Body: view.Body, Materialized: true})
	}
	return modified
}

// dropped is every object the rebuild drops: the ones it creates again and
// the ones it only removes.
func (r viewRebuild) dropped() []deporder.ViewLike {
	return append(slices.Clone(r.recreate), r.removeOnly...)
}

// created is every view and materialized view the rebuild creates: the ones it
// dropped and keeps, and the deferred ones.
func (r viewRebuild) created() []deporder.ViewLike {
	return append(slices.Clone(r.recreate), r.deferred...)
}

// owns is every object the rebuild drops or creates. A step that would drop
// one of them leaves it out, and a step that would create one leaves that to
// [Planner.createRebuiltViewLikes]: the object is absent while that step runs.
func (r viewRebuild) owns() viewLikeSet {
	owned := make(viewLikeSet, len(r.recreate)+len(r.removeOnly)+len(r.deferred))
	for _, objects := range [][]deporder.ViewLike{r.recreate, r.removeOnly, r.deferred} {
		for _, object := range objects {
			owned[viewLikeKey(object)] = true
		}
	}
	return owned
}

// viewLikeSet holds view-like objects by [viewLikeID].
type viewLikeSet map[viewLikeID]bool

func (s viewLikeSet) has(object deporder.ViewLike) bool {
	return s[viewLikeKey(object)]
}

// viewLikeID tells apart the objects a view-like ordering holds: a view and a
// materialized view of one name, as a plan that turns one into the other has,
// and a routine.
type viewLikeID struct {
	name                  string
	materialized, routine bool
}

func viewLikeKey(object deporder.ViewLike) viewLikeID {
	return viewLikeID{name: object.Name, materialized: object.Materialized, routine: object.Routine}
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

// readersOf returns the candidates that read one of the named relations,
// directly or through another returned candidate.
func readersOf(candidates []deporder.ViewLike, names []string) []deporder.ViewLike {
	indexes := readerIndexes(candidates, names)
	found := make([]deporder.ViewLike, 0, len(indexes))
	for _, i := range indexes {
		found = append(found, candidates[i])
	}
	return found
}

// readerIndexes is [readersOf] as positions in candidates.
func readerIndexes(candidates []deporder.ViewLike, names []string) []int {
	var found []int
	taken := make([]bool, len(candidates))
	frontier := append([]string(nil), names...)
	for len(frontier) > 0 {
		name := frontier[0]
		frontier = frontier[1:]
		for i, candidate := range candidates {
			if taken[i] || !deporder.ReferencesIdentifier(candidate.Body, name) {
				continue
			}
			taken[i] = true
			frontier = append(frontier, candidate.Name)
			found = append(found, i)
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

// dropRebuiltViewLikes renders the drops of a view rebuild. It runs before the
// column step.
//
// Each object is dropped after every object of the rebuild that reads it, so
// no drop relies on CASCADE to take a reader the plan knows of: that reader
// went first. CASCADE is then written only in Ptah's own statement style, where
// it also takes the readers the plan does not know of, such as a view in a
// schema the run leaves out; the exact style leaves the server to refuse
// those. The exception is an object a later removal depends on, a removed
// routine or a policy of a removed table; see [removalOutlivesDrop].
func (p *Planner) dropRebuiltViewLikes(result []ast.Node, diff *difftypes.SchemaDiff, rebuild viewRebuild) []ast.Node {
	for _, object := range deporder.ViewLikesForDropForDialect(rebuild.dropped(), p.targetDialect()) {
		result = append(result, p.dropViewLikeNode(object, p.cascades(removalOutlivesDrop(diff, object.Name, routinesAhead{}))))
	}
	return result
}

// createRebuiltViewLikes puts back what dropRebuiltViewLikes took, creates the
// deferred objects, and writes the routines placed with the relations that
// name any of them. It runs after the view step, in creation order.
//
// Each object is created outright rather than re-asserted with CREATE OR
// REPLACE: dropRebuiltViewLikes dropped it by name, or the plan adds it, so it
// is known to be absent.
func (p *Planner) createRebuiltViewLikes(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
	rebuild viewRebuild,
	routines relationRoutines,
) []ast.Node {
	pending := rebuild.created()
	if len(pending) == 0 {
		return result
	}
	absent := make([]string, 0, len(pending))
	for _, object := range pending {
		absent = append(absent, object.Name)
	}
	pending = append(pending, deferredRoutines(rebuild, routines)...)

	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	declared := rebuildDeclarations(diff)
	for _, object := range deporder.ViewLikesForCreateForDialect(pending, p.targetDialect()) {
		if object.Routine {
			result = append(result, routines.nodes[object.Name]...)
			continue
		}
		result = p.appendViewLikeRecreate(result, declared, object, absent, semantics)
	}
	return result
}

// rebuildDeclarations is the declarations rebuilt objects are created from:
// every declared view and materialized view, and the operand of each changed
// one the vocabulary does not hold, as a diff built by hand may leave out.
func rebuildDeclarations(diff *difftypes.SchemaDiff) difftypes.ViewLikeVocabulary {
	declared := difftypes.ViewLikeVocabulary{
		Views:             slices.Clone(diff.DeclaredViewLikes.Views),
		MaterializedViews: slices.Clone(diff.DeclaredViewLikes.MaterializedViews),
	}
	for _, viewDiff := range diff.MaterializedViewsModified {
		view := viewDiff.Desired
		if view.Name != "" && !slices.ContainsFunc(declared.MaterializedViews, func(held schemamodel.MaterializedView) bool {
			return held.Name == view.Name
		}) {
			declared.MaterializedViews = append(declared.MaterializedViews, view)
		}
	}
	for _, viewDiff := range diff.ViewsModified {
		view := viewDiff.Desired
		if view.Name != "" && !slices.ContainsFunc(declared.Views, func(held schemamodel.View) bool {
			return held.Name == view.Name
		}) {
			declared.Views = append(declared.Views, view)
		}
	}
	return declared
}

// deferredRoutines is the routines placed with the relations whose definitions
// name an object the rebuild creates, directly or through another such
// routine. [Planner.addNewViewLikeObjects] leaves them out: it runs while that
// object is absent, and PostgreSQL resolves a SQL-language body and a SETOF
// return type when the routine is created.
func deferredRoutines(rebuild viewRebuild, routines relationRoutines) []deporder.ViewLike {
	created := rebuild.created()
	if len(created) == 0 {
		return nil
	}
	names := make([]string, 0, len(created))
	for _, object := range created {
		names = append(names, object.Name)
	}
	return readersOf(routines.viewLikes(), names)
}

// dropViewLikeNode is the statement that drops one view or materialized view.
func (p *Planner) dropViewLikeNode(object deporder.ViewLike, cascade bool) ast.Node {
	if object.Materialized {
		return dropMaterializedViewNode(object.Name, cascade)
	}
	return p.dropViewNode(object.Name, cascade)
}
