package postgres

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/deporder"
	"ptah.run/internal/modelast"
)

// A view rebuild takes views and materialized views out of the database before
// the column step and puts them back after the view step. Three changes need
// it.
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
// drops it and creates it again (stokaro/ptah#4964).
//
// A view change CREATE OR REPLACE VIEW cannot make, such as a column that moves;
// see [Planner.modifyExistingViews] for which changes those are.
//
// The drop takes every view and materialized view that reads the dropped one,
// every routine whose parameters or result use its row type, and the indexes,
// triggers and privileges on each of those. The declaration still holds them,
// so the rebuild puts back all of them (stokaro/ptah#4976, stokaro/ptah#4980).
//
// The objects leave before the column step and return after the view step,
// which is the one position where every table and column they read exists in
// its final shape, and so does every view the view step replaces.
//
// Dependency is read the way the cascade logic reads it, off the declared
// bodies and signatures, so a view that reads another view that reads the table
// is found too, whether the declaration keeps it or not. Every object that
// depends on a rebuilt object is rebuilt with it, so the drops run without
// CASCADE once each object goes after every object that depends on it; see
// [Planner.dropRebuiltViewLikes].
//
// The rebuild owns its objects. No other step drops or creates one of them,
// and [viewRebuild.owns] and [viewRebuild.ownsRoutine] name them for those
// steps.
type viewRebuild struct {
	// recreate is the objects the database holds and the declaration keeps:
	// dropped, then created from the declaration.
	recreate []deporder.ViewLike
	// removeOnly is the objects the database holds and the declaration no
	// longer does. They also stand in the way of the ALTER or of a drop, and
	// the removal step runs only after the column step, so they are dropped
	// here and not created again.
	removeOnly []deporder.ViewLike
	// deferred is the objects this plan adds that depend on one of the
	// others. The step that creates added objects runs while the rebuilt ones
	// are absent, so these are created with them, after what they depend on.
	deferred []deporder.ViewLike
	// replaceFirst is the views the plan replaces in place whose definition in
	// the database reads an object the rebuild drops, and whose new one reads
	// none: replaced before the drops, they hold nothing back and keep their
	// policies and privileges. See [planViewRebuild].
	replaceFirst []deporder.ViewLike
	// routines is, for each routine the rebuild owns, keyed by [bareName],
	// the overloads it drops and the ones it creates. A routine is owned by
	// name: every overload the database or the declaration holds goes with it.
	routines map[string]rebuiltRoutine
}

// routineInRebuild is the placement of a routine a view rebuild owns. No
// routine step matches it, so none of them creates or replaces the routine;
// [Planner.createRebuiltViewLikes] does.
const routineInRebuild deporder.RoutinePlacement = 255

// rebuiltRoutine is the overloads of one routine name a rebuild drops and
// creates.
type rebuiltRoutine struct {
	// held is the overloads the database holds, each with the argument list a
	// DROP addresses it by.
	held []difftypes.RoutineChange
	// declared is the overloads the declaration holds.
	declared []schemamodel.Function
}

// rebuildCandidate is one object a rebuild may take: the object as the drop
// and create orders see it, the text that says what it depends on, and the
// list it joins.
type rebuildCandidate struct {
	object deporder.ViewLike
	// reads is what the object depends on: a view's body, and a routine's
	// parameters, result and SQL-standard body.
	reads string
	kind  *[]deporder.ViewLike
}

// planViewRebuild finds the objects the plan rebuilds: every view and
// materialized view that reads a column whose type changes, every
// materialized view that changes, every view a change drops and creates
// again, and everything that depends on one of those.
//
// A view the plan replaces in place can depend on a rebuilt object only in
// the definition the database holds, as a view that stops reading a changed
// materialized view does. The drop would take it, with its policies and
// privileges, or stop on it. Such a view is replaced before the drops instead,
// which leaves nothing for them to take, where its new definition reads only
// what exists at that point; otherwise it is rebuilt too. See
// [viewRebuild.replaceFirst].
func planViewRebuild(diff *difftypes.SchemaDiff) viewRebuild {
	var extra []deporder.ViewLike
	for {
		rebuild := closeViewRebuild(diff, extra)
		first, rebuilt := heldReaders(diff, rebuild)
		if len(rebuilt) == 0 {
			rebuild.replaceFirst = first
			return rebuild
		}
		extra = append(extra, rebuilt...)
	}
}

// closeViewRebuild is the rebuild that starts from the seeds the diff names and
// from extra, and takes everything that depends on them.
func closeViewRebuild(diff *difftypes.SchemaDiff, extra []deporder.ViewLike) viewRebuild {
	seeded := append(seededViewLikes(diff), extra...)
	tables := retypedTables(diff)
	if len(seeded) == 0 && len(tables) == 0 {
		return viewRebuild{}
	}

	// A changed materialized view, and a view the plan cannot replace in
	// place, is rebuilt whether or not anything depends on it, from the
	// declaration its change carries. Its name is a seed, so what depends on
	// it is found without it being a candidate.
	rebuild := viewRebuild{recreate: slices.Clone(seeded), routines: make(map[string]rebuiltRoutine)}
	candidates := rebuildCandidates(diff, &rebuild)
	frontier := make([]rebuildFrontier, 0, len(tables)+len(seeded))
	for _, table := range tables {
		frontier = append(frontier, rebuildFrontier{name: table})
	}
	for _, object := range seeded {
		frontier = append(frontier, rebuildFrontier{name: object.Name, viewLike: true})
	}
	for _, i := range dependentIndexes(candidates, frontier) {
		*candidates[i].kind = append(*candidates[i].kind, candidates[i].object)
	}
	for name := range rebuild.routines {
		if !slices.ContainsFunc(rebuild.dropped(), func(object deporder.ViewLike) bool {
			return object.Routine && bareName(object.Name) == name
		}) && !slices.ContainsFunc(rebuild.deferred, func(object deporder.ViewLike) bool {
			return object.Routine && bareName(object.Name) == name
		}) {
			delete(rebuild.routines, name)
		}
	}
	return rebuild
}

// heldReaders is the views the plan replaces in place, outside rebuild, whose
// definition in the database reads an object rebuild drops or a column whose
// type changes, while their new definition reads none of them. first is the
// ones the plan can replace before the drops; rebuilt is the ones it cannot,
// because their new definition reads something the plan creates or changes
// only after that point.
func heldReaders(diff *difftypes.SchemaDiff, rebuild viewRebuild) (first, rebuilt []deporder.ViewLike) {
	held := retypedTables(diff)
	for _, object := range rebuild.dropped() {
		held = append(held, object.Name)
	}
	if len(held) == 0 {
		return nil, nil
	}
	owned := rebuild.owns()
	for _, viewDiff := range diff.ViewsModified {
		object := deporder.ViewLike{Name: viewDiff.Desired.Name, Body: viewDiff.Desired.Body}
		if object.Name == "" || owned.has(object) || !viewReplaceKeepsDependents(viewDiff, object.Body) ||
			!slices.ContainsFunc(held, func(name string) bool {
				return deporder.ReferencesIdentifier(viewDiff.PreviousBody, unqualified(name))
			}) {
			continue
		}
		if readsLaterObjects(diff, object.Body) {
			rebuilt = append(rebuilt, object)
			continue
		}
		first = append(first, object)
	}
	return first, rebuilt
}

// readsLaterObjects reports whether body reads an object the plan creates or
// changes after a view rebuild's drops: a view or materialized view it adds, a
// routine it adds or changes, or a table whose columns it adds or changes.
func readsLaterObjects(diff *difftypes.SchemaDiff, body string) bool {
	var later []string
	for _, view := range diff.ViewsAdded {
		later = append(later, view.Name)
	}
	for _, view := range diff.MaterializedViewsAdded {
		later = append(later, view.Name)
	}
	for _, routine := range diff.FunctionsAdded {
		later = append(later, routine.Name)
	}
	for _, fnDiff := range diff.FunctionsModified {
		later = append(later, fnDiff.FunctionName)
	}
	for _, tableDiff := range diff.TablesModified {
		if len(tableDiff.ColumnsAdded) > 0 || len(tableDiff.ColumnsModified) > 0 {
			later = append(later, tableDiff.TableName)
		}
	}
	return slices.ContainsFunc(later, func(name string) bool {
		return deporder.ReferencesIdentifier(body, unqualified(name))
	})
}

// rebuildFrontier is a name the closure has reached: a table, whose row type
// no routine of the closure is held to, or a view-like, whose row type is.
type rebuildFrontier struct {
	name     string
	viewLike bool
}

// dependentIndexes returns the candidates that depend on a frontier name,
// directly or through another returned candidate. A view depends on what its
// body names, including a routine it calls; a routine depends on the views
// and materialized views its signature or SQL-standard body names, and not on
// a table: a column type change under a table's row type is one the server
// makes with the routine in place.
func dependentIndexes(candidates []rebuildCandidate, frontier []rebuildFrontier) []int {
	var found []int
	taken := make([]bool, len(candidates))
	for len(frontier) > 0 {
		reached := frontier[0]
		frontier = frontier[1:]
		for i, candidate := range candidates {
			if taken[i] || (candidate.object.Routine && !reached.viewLike) ||
				!deporder.ReferencesIdentifier(candidate.reads, unqualified(reached.name)) {
				continue
			}
			taken[i] = true
			frontier = append(frontier, rebuildFrontier{name: candidate.object.Name, viewLike: !candidate.object.Routine})
			found = append(found, i)
		}
	}
	return found
}

// seededViewLikes is the objects a rebuild starts from: the materialized views
// the plan changes and the views it cannot replace in place, with the
// declarations their changes carry.
func seededViewLikes(diff *difftypes.SchemaDiff) []deporder.ViewLike {
	seeded := modifiedMaterializedViews(diff)
	for _, view := range modifiedViewPaths(diff).dropped {
		if !slices.ContainsFunc(seeded, func(seen deporder.ViewLike) bool { return !seen.Materialized && seen.Name == view.Name }) {
			seeded = append(seeded, view)
		}
	}
	return seeded
}

// rebuildCandidates is every object a rebuild may take, and beside each the
// list of rebuild it joins if it depends on a rebuilt object: a declared object
// the database holds is recreated, a removed one is only dropped, and one the
// plan adds is deferred. The seeded objects already on the recreate list are
// not candidates. A view the plan turns into a materialized view, or back, is
// two candidates of one name.
func rebuildCandidates(diff *difftypes.SchemaDiff, rebuild *viewRebuild) []rebuildCandidate {
	skip := make(map[viewLikeID]bool)
	for _, object := range rebuild.recreate {
		skip[viewLikeKey(object)] = true
	}
	added := make([]deporder.ViewLike, 0, len(diff.ViewsAdded)+len(diff.MaterializedViewsAdded))
	for _, view := range diff.ViewsAdded {
		added = append(added, deporder.ViewLike{Name: view.Name, Body: view.Body})
	}
	for _, view := range diff.MaterializedViewsAdded {
		added = append(added, deporder.ViewLike{Name: view.Name, Body: view.Body, Materialized: true})
	}
	isAdded := func(name string) bool {
		return slices.ContainsFunc(added, func(object deporder.ViewLike) bool { return object.Name == name })
	}

	var candidates []rebuildCandidate
	join := func(object deporder.ViewLike, kind *[]deporder.ViewLike) {
		candidates = append(candidates, rebuildCandidate{object: object, reads: object.Body, kind: kind})
	}
	for _, view := range diff.DeclaredViewLikes.Views {
		object := deporder.ViewLike{Name: view.Name, Body: view.Body}
		if !skip[viewLikeKey(object)] && !isAdded(view.Name) {
			join(object, &rebuild.recreate)
		}
	}
	for _, view := range diff.DeclaredViewLikes.MaterializedViews {
		object := deporder.ViewLike{Name: view.Name, Body: view.Body, Materialized: true}
		if !skip[viewLikeKey(object)] && !isAdded(view.Name) {
			join(object, &rebuild.recreate)
		}
	}
	for _, object := range removedViewLikes(diff) {
		join(object, &rebuild.removeOnly)
	}
	for _, object := range added {
		join(object, &rebuild.deferred)
	}
	return append(candidates, routineCandidates(diff, rebuild)...)
}

// routineCandidates is the routines a rebuild may take, one candidate per
// routine name with every overload the plan knows: the ones the database holds
// and the declaration keeps or changes, which the rebuild drops and creates;
// the ones it removes, which it only drops; and the ones it adds, which it only
// creates. Each name's overloads are recorded in rebuild.routines, from which
// the ones the closure does not reach are removed again.
func routineCandidates(diff *difftypes.SchemaDiff, rebuild *viewRebuild) []rebuildCandidate {
	var order []string
	hold := func(name string, held *difftypes.RoutineChange, declared *schemamodel.Function) {
		key := bareName(name)
		routine, seen := rebuild.routines[key]
		if !seen {
			order = append(order, key)
		}
		if held != nil {
			routine.held = append(routine.held, *held)
		}
		if declared != nil {
			routine.declared = append(routine.declared, *declared)
		}
		rebuild.routines[key] = routine
	}
	for _, kept := range diff.RoutinesKept {
		hold(kept.Name, &kept, &kept.Function)
	}
	for _, fnDiff := range diff.FunctionsModified {
		if fnDiff.Desired.Name == "" {
			continue
		}
		held := difftypes.RoutineChange{Function: fnDiff.Desired, Signature: fnDiff.CurrentSignature}
		hold(fnDiff.Desired.Name, &held, &fnDiff.Desired)
	}
	for _, added := range diff.FunctionsAdded {
		hold(added.Name, nil, &added.Function)
	}
	for _, removed := range diff.FunctionsRemoved {
		hold(removed.Name, &removed, nil)
	}
	for _, removed := range diff.ProceduresRemoved {
		removed.Kind = schemamodel.FunctionKindProcedure
		hold(removed.Name, &removed, nil)
	}

	candidates := make([]rebuildCandidate, 0, len(order))
	for _, key := range order {
		routine := rebuild.routines[key]
		candidates = append(candidates, rebuildCandidate{
			object: deporder.ViewLike{Name: routine.name(), Body: routine.orderingBody(), Routine: true},
			reads:  routine.dependencyText(),
			kind:   routine.kind(rebuild),
		})
	}
	return candidates
}

// name is the routine's name without its schema, as a view calls it.
func (r rebuiltRoutine) name() string {
	if len(r.declared) > 0 {
		return unqualified(r.declared[0].Name)
	}
	return unqualified(r.held[0].Name)
}

// overloads is every overload of the routine, declared ones first.
func (r rebuiltRoutine) overloads() []schemamodel.Function {
	all := slices.Clone(r.declared)
	for _, held := range r.held {
		all = append(all, held.Function)
	}
	return all
}

// dependencyText is what the routine depends on: the parameters and results of
// its overloads, and their SQL-standard bodies.
func (r rebuiltRoutine) dependencyText() string {
	var parts []string
	for _, overload := range r.overloads() {
		parts = append(parts, deporder.RoutineSignatureText(overload))
		if sqlStandardBody(overload.Body) {
			parts = append(parts, overload.Body)
		}
	}
	return strings.Join(parts, "\n")
}

// orderingBody is the text the routine is ordered by among the view-likes.
func (r rebuiltRoutine) orderingBody() string {
	var parts []string
	for _, overload := range r.overloads() {
		parts = append(parts, deporder.RoutineOrderingBody(overload, ResolvesBodyAtCreate))
	}
	return strings.Join(parts, "\n")
}

// kind is the rebuild list the routine joins: recreated when the database and
// the declaration both hold an overload, only dropped when only the database
// does, and deferred when only the declaration does.
func (r rebuiltRoutine) kind(rebuild *viewRebuild) *[]deporder.ViewLike {
	switch {
	case len(r.held) > 0 && len(r.declared) > 0:
		return &rebuild.recreate
	case len(r.held) > 0:
		return &rebuild.removeOnly
	default:
		return &rebuild.deferred
	}
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

// created is every object the rebuild creates: the ones it dropped and keeps,
// and the deferred ones.
func (r viewRebuild) created() []deporder.ViewLike {
	return append(slices.Clone(r.recreate), r.deferred...)
}

// owns is every view and materialized view the rebuild drops or creates. A
// step that would drop one of them leaves it out, and a step that would create
// one leaves that to [Planner.createRebuiltViewLikes]: the object is absent
// while that step runs.
func (r viewRebuild) owns() viewLikeSet {
	owned := make(viewLikeSet, len(r.recreate)+len(r.removeOnly)+len(r.deferred))
	for _, objects := range [][]deporder.ViewLike{r.recreate, r.removeOnly, r.deferred} {
		for _, object := range objects {
			owned[viewLikeKey(object)] = true
		}
	}
	return owned
}

// ownsRoutine reports whether the rebuild drops or creates the routine name,
// every overload of it.
func (r viewRebuild) ownsRoutine(name string) bool {
	_, owned := r.routines[bareName(name)]
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
	var found []deporder.ViewLike
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

// refuseUnrebuildable refuses a rebuild that would drop something it cannot
// create again, and names it rather than leave it out:
//
//   - a view or materialized view with options Ptah does not declare, such as
//     security_invoker: the new view would run with its owner's privileges,
//     and row-level security on the tables it reads would stop applying to the
//     callers. The read reports them; see
//     [difftypes.SchemaDiff.CurrentViewOptions].
//   - a routine with a SQL-standard body, `BEGIN ATOMIC ... END` or `RETURN
//     expression`: the routine renderer writes every body in dollar quotes,
//     which PostgreSQL refuses for one of those (stokaro/ptah#4981).
func refuseUnrebuildable(diff *difftypes.SchemaDiff, rebuild viewRebuild) error {
	for _, object := range rebuild.recreate {
		if options := diff.CurrentViewOptions[strings.ToLower(unqualified(object.Name))]; !object.Routine && len(options) > 0 {
			return fmt.Errorf("%w: the plan drops and creates again %s, which the database holds with %s, an option Ptah does not "+
				"declare, so the new one would not have it; remove the option first, or make the change by hand",
				ptaherr.ErrUnsupportedFeature, object.Name, strings.Join(options, ", "))
		}
	}
	for _, object := range rebuild.created() {
		if !object.Routine {
			continue
		}
		for _, overload := range rebuild.routines[bareName(object.Name)].declared {
			if sqlStandardBody(overload.Body) {
				return fmt.Errorf("%w: the plan drops and creates again the routine %s, whose SQL-standard body Ptah cannot write yet "+
					"(stokaro/ptah#4981); give it a quoted body, or change it in a migration of its own",
					ptaherr.ErrUnsupportedFeature, overload.Name)
			}
		}
	}
	return nil
}

// dropRebuiltViewLikes renders the drops of a view rebuild. It runs before the
// column step.
//
// Each object is dropped after every object of the rebuild that depends on
// it, so no drop relies on CASCADE to take a dependent the plan knows of: that
// dependent went first. CASCADE is then written only in Ptah's own statement
// style, where it also takes the dependents the plan does not know of, such
// as a view in a schema the run leaves out; the exact style leaves the server
// to refuse those. The exception is a relation a policy of a removed table
// reads, which goes with its table only later; see [removedTableReads]. A
// routine is dropped without CASCADE in both styles, as the plan drops every
// routine.
//
// The views the plan replaces before the drops come first; see
// [viewRebuild.replaceFirst]. A view the plan changes is ordered by both of
// its definitions: the database holds the old one when the drops run.
func (p *Planner) dropRebuiltViewLikes(result []ast.Node, diff *difftypes.SchemaDiff, rebuild viewRebuild) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	for _, object := range deporder.ViewLikesForCreateForDialect(rebuild.replaceFirst, p.targetDialect()) {
		result = p.appendViewLikeRecreate(result, diff.DeclaredViewLikes, object, nil, semantics)
	}
	for _, object := range deporder.ViewLikesForDropForDialect(heldDefinitions(diff, rebuild.dropped()), p.targetDialect()) {
		if object.Routine {
			result = append(result, rebuild.routines[bareName(object.Name)].dropNodes()...)
			continue
		}
		result = append(result, p.dropViewLikeNode(object, p.cascades(removedTableReads(diff, object.Name, nil))))
	}
	return result
}

// heldDefinitions is objects with each changed view's definition in the
// database added to its body, so the drop order sees what the old definition
// reads.
func heldDefinitions(diff *difftypes.SchemaDiff, objects []deporder.ViewLike) []deporder.ViewLike {
	held := make([]deporder.ViewLike, 0, len(objects))
	for _, object := range objects {
		for _, viewDiff := range diff.ViewsModified {
			if !object.Materialized && !object.Routine && viewDiff.Desired.Name == object.Name && viewDiff.PreviousBody != "" {
				object.Body += "\n" + viewDiff.PreviousBody
			}
		}
		held = append(held, object)
	}
	return held
}

// dropNodes is the statements that drop every overload the database holds.
func (r rebuiltRoutine) dropNodes() []ast.Node {
	nodes := make([]ast.Node, 0, len(r.held))
	for _, held := range r.held {
		drop := ast.NewDropFunction(held.Name).SetKind(held.Kind).SetIfExists()
		if held.Signature != nil {
			drop = drop.SetParameters(*held.Signature)
		}
		nodes = append(nodes, drop)
	}
	return nodes
}

// createRebuiltViewLikes puts back what dropRebuiltViewLikes took, creates the
// deferred objects, and writes the routines placed with the relations that
// name any of them. It runs after the view step, in creation order.
//
// Each object is created outright rather than re-asserted with CREATE OR
// REPLACE: dropRebuiltViewLikes dropped it by name, or the plan adds it, so it
// is known to be absent. What the drop took with it comes back right after
// it: the declared indexes of a materialized view, the declared triggers of a
// view, and the privileges the declaration grants on any of them. A view's,
// materialized view's or routine's comment is part of its CREATE.
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
	for _, object := range deporder.ViewLikesForCreateForDialect(pending, p.targetDialect()) {
		switch {
		case object.Routine && rebuild.ownsRoutine(object.Name):
			for _, overload := range rebuild.routines[bareName(object.Name)].declared {
				result = append(result, modelast.FromFunction(overload))
			}
			result = append(result, rebuiltGrants(diff, object)...)
		case object.Routine:
			result = append(result, routines.nodes[object.Name]...)
		default:
			result = p.appendViewLikeRecreate(result, diff.DeclaredViewLikes, object, absent, semantics)
			result = append(result, rebuiltIndexes(diff, object)...)
			result = append(result, rebuiltTriggers(diff, object)...)
			result = append(result, rebuiltGrants(diff, object)...)
		}
	}
	return result
}

// rebuiltIndexes is the CREATE INDEX of every declared index on the rebuilt
// materialized view object, except the ones the plan adds, which the index
// step creates.
func rebuiltIndexes(diff *difftypes.SchemaDiff, object deporder.ViewLike) []ast.Node {
	if !object.Materialized {
		return nil
	}
	var nodes []ast.Node
	for _, declaration := range diff.DeclaredIndexes {
		if bareName(declaration.TableName) != bareName(object.Name) || slices.ContainsFunc(diff.IndexesAdded, func(added difftypes.IndexChange) bool {
			return bareName(added.Index.Name) == bareName(declaration.Index.Name) && bareName(added.TableName) == bareName(declaration.TableName)
		}) {
			continue
		}
		index := declaration.Index
		index.TableName = declaration.TableName
		nodes = append(nodes, modelast.FromIndex(index))
	}
	return nodes
}

// rebuiltTriggers is the CREATE TRIGGER of every declared trigger on the
// rebuilt view object, except the ones the plan adds or changes, which the
// trigger steps write.
func rebuiltTriggers(diff *difftypes.SchemaDiff, object deporder.ViewLike) []ast.Node {
	if object.Materialized {
		return nil
	}
	planned := func(name, table string) bool {
		same := func(trigger, on string) bool {
			return strings.EqualFold(trigger, name) && bareName(on) == bareName(table)
		}
		return slices.ContainsFunc(diff.TriggersAdded, func(ref difftypes.TriggerRef) bool { return same(ref.TriggerName, ref.TableName) }) ||
			slices.ContainsFunc(diff.TriggersModified, func(change difftypes.TriggerDiff) bool { return same(change.TriggerName, change.TableName) })
	}
	var nodes []ast.Node
	for _, trigger := range diff.DeclaredTriggers {
		if bareName(trigger.Table) == bareName(object.Name) && !planned(trigger.Name, trigger.Table) {
			nodes = append(nodes, modelast.FromTrigger(trigger))
		}
	}
	return nodes
}

// rebuiltGrants is the GRANT of every privilege the declaration grants on the
// rebuilt object, except the ones the plan adds, which the grant step writes.
// A privilege the declaration does not grant is not given back: the
// comparison would revoke it anyway from a role the schema manages.
func rebuiltGrants(diff *difftypes.SchemaDiff, object deporder.ViewLike) []ast.Node {
	var nodes []ast.Node
	for _, grant := range diff.DeclaredGrants {
		if !grantsOn(grant, object) || slices.Contains(diff.GrantsAdded, grant) {
			continue
		}
		nodes = append(nodes, ast.NewGrantPrivilege(grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
			SetArguments(grant.Arguments).
			SetColumns(grantColumns(grant)).
			SetWithOption(grant.WithOption))
	}
	return nodes
}

// grantsOn reports whether grant is a privilege on object: a table privilege
// on a view or materialized view, and a routine privilege on a routine.
func grantsOn(grant difftypes.GrantRef, object deporder.ViewLike) bool {
	if bareName(grant.ObjectName) != bareName(object.Name) {
		return false
	}
	if object.Routine {
		return slices.Contains([]string{"FUNCTION", "PROCEDURE", "ROUTINE"}, strings.ToUpper(grant.ObjectType))
	}
	return strings.EqualFold(grant.ObjectType, "TABLE")
}

// deferredRoutines is the routines placed with the relations, other than the
// ones the rebuild owns, whose definitions name an object the rebuild
// creates, directly or through another such routine.
// [Planner.addNewViewLikeObjects] leaves them out: it runs while that object
// is absent, and PostgreSQL resolves a SQL-language body when the routine is
// created.
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
