package postgres

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/deporder"
	"ptah.run/internal/modelast"
	"ptah.run/internal/objectlookup"
	"ptah.run/internal/routineargs"
	"ptah.run/internal/routinebody"
)

// A view rebuild takes views, materialized views and routines out of the
// database before the column step and puts them back after the view step.
// Three changes need it.
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
// A drop takes every view and materialized view that reads the dropped one,
// every routine whose parameters or result use its row type, and the indexes,
// triggers and privileges on each of those. So the rebuild drops each of them
// itself, after everything that depends on it, and creates it again with the
// indexes, triggers and privileges it had (stokaro/ptah#4976,
// stokaro/ptah#4980).
//
// The objects leave before the column step and return after the view step,
// which is the one position where every table and column they read exists in
// its final shape, and so does every view the view step replaces.
//
// What the database holds decides what has to be dropped: a view the
// declaration keeps reads what its declaration reads, a view it changes reads
// what its definition in the database reads, and a routine depends on the
// relations its parameters and result name. A view the plan replaces in place
// is dropped only when it has to be; see [viewRebuild.replaceFirst] and
// [viewRebuild.replaceAfter].
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
	// policies and privileges. A view that stops reading a changed
	// materialized view is one.
	replaceFirst []deporder.ViewLike
	// replaceAfter is the views the plan replaces in place whose definition in
	// the database reads nothing the rebuild drops, and whose new one reads an
	// object it creates: replaced after that object is back, they are never
	// dropped. A view that starts reading a changed materialized view is one.
	replaceAfter []deporder.ViewLike
	// routines is, for each routine the rebuild owns, keyed by [bareName],
	// the overloads it drops and the ones it creates. A routine is owned by
	// name: every overload the database or the declaration holds goes with it.
	routines map[string]rebuiltRoutine
	// causes says, for each object the rebuild drops, which change the drop
	// is for. A refusal names it.
	causes map[viewLikeID]rebuildCause
	// semantics resolves names exactly as the comparison's session did.
	semantics identifier.Semantics
}

// rebuildSemantics uses the PostgreSQL default for an offline diff and the
// session's captured rules when a live comparison supplied them.
func rebuildSemantics(diff *difftypes.SchemaDiff) identifier.Semantics {
	return diff.EffectiveIdentifierSemantics(platform.Postgres)
}

// rebuildCause is the change a rebuild drops an object for, in the two forms
// a refusal writes it: as a noun, "the type change of tenants.id", and as what
// the author does to make it, "change the type of tenants.id".
type rebuildCause struct {
	change, then string
}

// routineInRebuild is the placement of a routine a view rebuild owns. No
// routine step matches it, so none of them creates or replaces the routine;
// [Planner.createRebuiltViewLikes] does.
const routineInRebuild deporder.RoutinePlacement = 255

// rebuiltRoutine is the overloads of one routine name a rebuild drops and
// creates.
type rebuiltRoutine struct {
	// held is the overloads the database holds.
	held []heldRoutine
	// declared is the overloads the declaration holds.
	declared []schemamodel.Function
}

// heldRoutine is one overload the database holds: its definition there, with
// the argument list a DROP addresses it by, and whether the declaration keeps
// it with the same arguments, so the privileges on it apply to the routine the
// rebuild creates again.
type heldRoutine struct {
	difftypes.RoutineChange
	kept bool
}

// rebuildCandidate is one object a rebuild may take: the object as the drop
// and create orders see it, the text that says what it depends on, and the
// list it joins.
type rebuildCandidate struct {
	object deporder.ViewLike
	// reads is what the object depends on among relations: a view's body,
	// and a routine's parameters, result and SQL-standard body.
	reads string
	// calls is what the object depends on among routines: a view's body, and
	// a routine's SQL-standard body. A body in quotes records nothing.
	calls string
	kind  *[]deporder.ViewLike
}

// rebuildSeed is an object a rebuild starts from, and the change it is
// dropped for.
type rebuildSeed struct {
	object deporder.ViewLike
	cause  rebuildCause
}

// replacedView is a view the plan changes and can replace in place: the view
// the declaration asks for, and the body the database holds it with.
type replacedView struct {
	object   deporder.ViewLike
	previous string
}

// planViewRebuild finds the objects the plan rebuilds: every view and
// materialized view that reads a column whose type changes, every
// materialized view that changes, every view a change drops and creates
// again, and everything that depends on one of those in the database.
//
// A view the plan replaces in place can depend on a rebuilt object in the
// definition the database holds and not in its new one. The drop would take
// it, with its policies and privileges, or stop on it. Such a view is replaced
// before the drops instead, where its new definition reads only what exists at
// that point; otherwise it is rebuilt too, and so is what reads it.
func planViewRebuild(diff *difftypes.SchemaDiff) viewRebuild {
	var extra []rebuildSeed
	for {
		rebuild := closeViewRebuild(diff, extra)
		first, rebuilt := heldReaders(diff, rebuild)
		if len(rebuilt) == 0 {
			rebuild.replaceFirst = first
			deferLateReaders(diff, &rebuild)
			return rebuild
		}
		extra = append(extra, rebuilt...)
	}
}

// closeViewRebuild is the rebuild that starts from the seeds the diff names and
// from extra, and takes everything that depends on them.
func closeViewRebuild(diff *difftypes.SchemaDiff, extra []rebuildSeed) viewRebuild {
	seeds := append(seededViewLikes(diff), extra...)
	retyped := retypedColumns(diff)
	if len(seeds) == 0 && len(retyped) == 0 {
		return viewRebuild{}
	}

	// A changed materialized view, and a view the plan cannot replace in
	// place, is rebuilt whether or not anything depends on it, from the
	// declaration its change carries. Its name is a seed, so what depends on
	// it is found without it being a candidate.
	rebuild := viewRebuild{
		routines:  make(map[string]rebuiltRoutine),
		causes:    make(map[viewLikeID]rebuildCause),
		semantics: rebuildSemantics(diff),
	}
	frontier := make([]rebuildFrontier, 0, len(retyped)+len(seeds))
	for _, table := range retyped {
		frontier = append(frontier, rebuildFrontier{name: table.table, cause: table.cause()})
	}
	for _, seed := range seeds {
		rebuild.recreate = append(rebuild.recreate, seed.object)
		rebuild.causes[viewLikeKey(seed.object)] = seed.cause
		frontier = append(frontier, rebuildFrontier{name: seed.object.Name, viewLike: true, cause: seed.cause})
	}
	candidates := rebuildCandidates(diff, &rebuild)
	for _, reached := range dependents(candidates, frontier, rebuildSemantics(diff)) {
		candidate := candidates[reached.index]
		*candidate.kind = append(*candidate.kind, candidate.object)
		rebuild.causes[viewLikeKey(candidate.object)] = reached.cause
	}
	for name := range rebuild.routines {
		if !slices.ContainsFunc(append(rebuild.dropped(), rebuild.deferred...), func(object deporder.ViewLike) bool {
			return object.Routine && bareName(object.Name) == name
		}) {
			delete(rebuild.routines, name)
		}
	}
	return rebuild
}

// heldReaders is the views the plan replaces in place, outside rebuild, whose
// definition in the database reads an object rebuild drops or a column whose
// type changes. first is the ones the plan can replace before the drops,
// because their new definition reads none of those and nothing the plan
// creates or changes after that point; rebuilt is the others, each with the
// change its drop is for.
func heldReaders(diff *difftypes.SchemaDiff, rebuild viewRebuild) (first []deporder.ViewLike, rebuilt []rebuildSeed) {
	held := make(map[string]rebuildCause)
	for _, table := range retypedColumns(diff) {
		held[table.table] = table.cause()
	}
	dropped := rebuild.dropped()
	for _, object := range dropped {
		held[object.Name] = rebuild.causes[viewLikeKey(object)]
	}
	if len(held) == 0 {
		return nil, nil
	}
	owned := rebuild.owns()
	for _, view := range replacedViews(diff) {
		if owned.has(view.object) {
			continue
		}
		name, reads := firstRead(view.previous, held, rebuildSemantics(diff))
		if !reads {
			continue
		}
		if readsAny(view.object.Body, namesOf(dropped), rebuildSemantics(diff)) || readsLaterObjects(diff, view.object.Body) {
			rebuilt = append(rebuilt, rebuildSeed{object: view.object, cause: held[name]})
			continue
		}
		first = append(first, view.object)
	}
	return first, rebuilt
}

// deferLateReaders completes rebuild with what has to wait for the objects it
// creates: the views the plan replaces in place whose new definition reads one
// of them, replaced after it, and the objects the plan adds that read one of
// those, created after them.
func deferLateReaders(diff *difftypes.SchemaDiff, rebuild *viewRebuild) {
	late := namesOf(rebuild.created())
	if len(late) == 0 {
		return
	}
	settled := rebuild.owns()
	for _, object := range rebuild.replaceFirst {
		settled[viewLikeKey(object)] = true
	}
	var waiting []deporder.ViewLike
	for _, view := range replacedViews(diff) {
		if !settled.has(view.object) {
			waiting = append(waiting, view.object)
		}
	}
	added := addedViewLikes(diff)
	for changed := true; changed; {
		changed = false
		for _, object := range waiting {
			if !settled.has(object) && readsAny(object.Body, late, rebuildSemantics(diff)) {
				rebuild.replaceAfter = append(rebuild.replaceAfter, object)
				settled[viewLikeKey(object)] = true
				late = append(late, object.Name)
				changed = true
			}
		}
		for _, object := range added {
			if !settled.has(object) && readsAny(object.Body, late, rebuildSemantics(diff)) {
				rebuild.deferred = append(rebuild.deferred, object)
				settled[viewLikeKey(object)] = true
				late = append(late, object.Name)
				changed = true
			}
		}
	}
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
	return readsAny(body, later, rebuildSemantics(diff))
}

// readsAny reports whether body names one of names, an unqualified name of
// either resolving in schema; see [deporder.ReferencesRelation].
func readsAny(body string, names []string, semantics identifier.Semantics) bool {
	return slices.ContainsFunc(names, func(name string) bool {
		return deporder.ReferencesRelation(body, name, semantics)
	})
}

// firstRead is the first of held, in name order, that body names, an
// unqualified name of either resolving in schema.
func firstRead(body string, held map[string]rebuildCause, semantics identifier.Semantics) (string, bool) {
	names := make([]string, 0, len(held))
	for name := range held {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		if deporder.ReferencesRelation(body, name, semantics) {
			return name, true
		}
	}
	return "", false
}

// namesOf is the names of objects.
func namesOf(objects []deporder.ViewLike) []string {
	names := make([]string, 0, len(objects))
	for _, object := range objects {
		names = append(names, object.Name)
	}
	return names
}

// rebuildFrontier is a name the closure has reached, and the change it is
// dropped for: a table, whose row type no routine of the closure is held to,
// a view-like, whose row type is, or a routine.
type rebuildFrontier struct {
	name     string
	viewLike bool
	routine  bool
	cause    rebuildCause
}

// reachedCandidate is a candidate the closure reached, and the change its drop
// is for.
type reachedCandidate struct {
	index int
	cause rebuildCause
}

// dependents returns the candidates that depend on a frontier name, directly
// or through another returned candidate. A view depends on the relations and
// routines its body names; a routine depends on the views and materialized
// views its signature or SQL-standard body names, and on the routines that
// body calls, and not on a table: a column type change under a table's row
// type is one the server makes with the routine in place.
//
// An unqualified name, in a candidate or reached, resolves in schema, so a view
// that reads a namesake in another schema is not a dependent.
func dependents(candidates []rebuildCandidate, frontier []rebuildFrontier, semantics identifier.Semantics) []reachedCandidate {
	var found []reachedCandidate
	taken := make([]bool, len(candidates))
	for len(frontier) > 0 {
		reached := frontier[0]
		frontier = frontier[1:]
		for i, candidate := range candidates {
			if taken[i] || !dependsOn(candidate, reached, semantics) {
				continue
			}
			taken[i] = true
			frontier = append(frontier, rebuildFrontier{
				name:     candidate.object.Name,
				viewLike: !candidate.object.Routine,
				routine:  candidate.object.Routine,
				cause:    reached.cause,
			})
			found = append(found, reachedCandidate{index: i, cause: reached.cause})
		}
	}
	return found
}

// dependsOn reports whether candidate depends on the object reached names.
func dependsOn(candidate rebuildCandidate, reached rebuildFrontier, semantics identifier.Semantics) bool {
	switch {
	case reached.routine:
		return deporder.ReferencesRelation(candidate.calls, reached.name, semantics)
	case candidate.object.Routine && !reached.viewLike:
		return false
	default:
		return deporder.ReferencesRelation(candidate.reads, reached.name, semantics)
	}
}

// seededViewLikes is the objects a rebuild starts from: the materialized views
// the plan changes and the views it cannot replace in place, with the
// declarations their changes carry.
func seededViewLikes(diff *difftypes.SchemaDiff) []rebuildSeed {
	var seeds []rebuildSeed
	for _, view := range modifiedMaterializedViews(diff) {
		seeds = append(seeds, rebuildSeed{object: view, cause: rebuildCause{
			change: "the change to materialized view " + view.Name,
			then:   "change materialized view " + view.Name,
		}})
	}
	for _, view := range modifiedViewPaths(diff).dropped {
		if slices.ContainsFunc(seeds, func(seen rebuildSeed) bool { return !seen.object.Materialized && seen.object.Name == view.Name }) {
			continue
		}
		seeds = append(seeds, rebuildSeed{object: view, cause: rebuildCause{
			change: "the change to view " + view.Name + ", which CREATE OR REPLACE VIEW cannot make",
			then:   "change view " + view.Name,
		}})
	}
	return seeds
}

// retypedTable is a table with columns whose type changes.
type retypedTable struct {
	table   string
	columns []string
}

func (t retypedTable) cause() rebuildCause {
	columns := make([]string, 0, len(t.columns))
	for _, column := range t.columns {
		columns = append(columns, t.table+"."+column)
	}
	listed := strings.Join(columns, ", ")
	return rebuildCause{change: "the type change of " + listed, then: "change the type of " + listed}
}

// retypedColumns is the tables with at least one column whose type changes,
// and those columns.
func retypedColumns(diff *difftypes.SchemaDiff) []retypedTable {
	var tables []retypedTable
	for _, tableDiff := range diff.TablesModified {
		var columns []string
		for _, colDiff := range tableDiff.ColumnsModified {
			if _, retyped := colDiff.Changes["type"]; retyped {
				columns = append(columns, colDiff.ColumnName)
			}
		}
		if len(columns) > 0 {
			tables = append(tables, retypedTable{table: tableDiff.TableName, columns: columns})
		}
	}
	return tables
}

// replacedViews is the views the plan changes and can replace in place.
func replacedViews(diff *difftypes.SchemaDiff) []replacedView {
	var views []replacedView
	for _, viewDiff := range diff.ViewsModified {
		view := viewDiff.Desired
		if view.Name == "" || !viewReplaceKeepsDependents(viewDiff, view.Body) {
			continue
		}
		previous := viewDiff.PreviousBody
		if previous == "" {
			// Not known: the new body is the best guess at what the database
			// holds, and it errs toward rebuilding.
			previous = view.Body
		}
		views = append(views, replacedView{object: deporder.ViewLike{Name: view.Name, Body: view.Body}, previous: previous})
	}
	return views
}

// addedViewLikes is the views and materialized views the plan adds.
func addedViewLikes(diff *difftypes.SchemaDiff) []deporder.ViewLike {
	added := make([]deporder.ViewLike, 0, len(diff.ViewsAdded)+len(diff.MaterializedViewsAdded))
	for _, view := range diff.ViewsAdded {
		added = append(added, deporder.ViewLike{Name: view.Name, Body: view.Body})
	}
	for _, view := range diff.MaterializedViewsAdded {
		added = append(added, deporder.ViewLike{Name: view.Name, Body: view.Body, Materialized: true})
	}
	return added
}

// rebuildCandidates is every object a rebuild may take, and beside each the
// list of rebuild it joins if it depends on a rebuilt object: a declared object
// the database holds is recreated, a removed one is only dropped, and one the
// plan adds is deferred. The seeded objects already on the recreate list are
// not candidates, and neither is a view the plan replaces in place, which
// [heldReaders] and [deferLateReaders] place. A view the plan turns into a
// materialized view, or back, is two candidates of one name.
func rebuildCandidates(diff *difftypes.SchemaDiff, rebuild *viewRebuild) []rebuildCandidate {
	skip := make(viewLikeSet)
	for _, object := range rebuild.recreate {
		skip[viewLikeKey(object)] = true
	}
	for _, view := range replacedViews(diff) {
		skip[viewLikeKey(view.object)] = true
	}
	added := addedViewLikes(diff)
	isAdded := func(name string) bool {
		return slices.ContainsFunc(added, func(object deporder.ViewLike) bool { return object.Name == name })
	}

	var candidates []rebuildCandidate
	join := func(object deporder.ViewLike, kind *[]deporder.ViewLike) {
		candidates = append(candidates, rebuildCandidate{object: object, reads: object.Body, calls: object.Body, kind: kind})
	}
	for _, view := range diff.DeclaredViewLikes.Views {
		object := deporder.ViewLike{Name: view.Name, Body: view.Body}
		if !skip.has(object) && !isAdded(view.Name) {
			join(object, &rebuild.recreate)
		}
	}
	for _, view := range diff.DeclaredViewLikes.MaterializedViews {
		object := deporder.ViewLike{Name: view.Name, Body: view.Body, Materialized: true}
		if !skip.has(object) && !isAdded(view.Name) {
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
//
// An overload the plan changes is held with its definition in the database:
// its parameters and result as they were, since that is what the database
// records a dependency for.
func routineCandidates(diff *difftypes.SchemaDiff, rebuild *viewRebuild) []rebuildCandidate {
	var order []string
	hold := func(name string, held *heldRoutine, declared *schemamodel.Function) {
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
		hold(kept.Name, &heldRoutine{RoutineChange: kept, kept: true}, &kept.Function)
	}
	for _, fnDiff := range diff.FunctionsModified {
		if fnDiff.Desired.Name == "" {
			continue
		}
		current := fnDiff.Desired
		if before := previousColumnType(fnDiff.Changes["parameters"]); before != "" {
			current.Parameters = strings.Trim(before, "()")
		}
		if before := previousColumnType(fnDiff.Changes["returns"]); before != "" {
			current.Returns = before
		}
		// The server recorded what the routine depends on from the body it
		// holds, so a SQL-standard body is read as the database holds it. A
		// body that holds " -> " itself is cut short there, which reads less,
		// never more.
		if before := previousColumnType(fnDiff.Changes["body"]); before != "" {
			current.Body = before
		}
		_, retyped := fnDiff.Changes["parameters"]
		held := heldRoutine{RoutineChange: difftypes.RoutineChange{Function: current, Signature: fnDiff.CurrentSignature}, kept: !retyped}
		hold(fnDiff.Desired.Name, &held, &fnDiff.Desired)
	}
	for _, added := range diff.FunctionsAdded {
		hold(added.Name, nil, &added.Function)
	}
	for _, removed := range diff.FunctionsRemoved {
		hold(removed.Name, &heldRoutine{RoutineChange: removed}, nil)
	}
	for _, removed := range diff.ProceduresRemoved {
		removed.Kind = schemamodel.FunctionKindProcedure
		hold(removed.Name, &heldRoutine{RoutineChange: removed}, nil)
	}

	candidates := make([]rebuildCandidate, 0, len(order))
	for _, key := range order {
		routine := rebuild.routines[key]
		candidates = append(candidates, rebuildCandidate{
			object: deporder.ViewLike{Name: routine.name(), Body: routine.orderingBody(), Routine: true},
			reads:  routine.readText(),
			calls:  routine.callText(),
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

// readText is what the routine depends on among relations: the parameters
// and results of its overloads, and their SQL-standard bodies.
func (r rebuiltRoutine) readText() string {
	var parts []string
	for _, overload := range r.overloads() {
		parts = append(parts, deporder.RoutineSignatureText(overload))
	}
	return strings.Join(append(parts, r.callText()), "\n")
}

// callText is what the routine depends on among routines: the SQL-standard
// bodies of its overloads. A body in quotes records nothing.
func (r rebuiltRoutine) callText() string {
	var parts []string
	for _, overload := range r.overloads() {
		if routinebody.SQLStandard(overload.Language, overload.Body) {
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

// late is every object [Planner.createRebuiltViewLikes] writes: the ones the
// rebuild creates, and the views it replaces after them.
func (r viewRebuild) late() []deporder.ViewLike {
	return append(r.created(), r.replaceAfter...)
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

// replaces is every view the rebuild replaces in place, before its drops or
// after its creates. The view step leaves them out.
func (r viewRebuild) replaces() viewLikeSet {
	replaced := make(viewLikeSet, len(r.replaceFirst)+len(r.replaceAfter))
	for _, objects := range [][]deporder.ViewLike{r.replaceFirst, r.replaceAfter} {
		for _, object := range objects {
			replaced[viewLikeKey(object)] = true
		}
	}
	return replaced
}

// dropsForGood reports whether grant is a privilege on an object the rebuild
// drops and does not create again: a view or materialized view the
// declaration removes, or an overload of a routine it removes or whose
// arguments change. The drop takes the privilege with it, before any step that
// would grant or revoke it.
func (r viewRebuild) dropsForGood(grant difftypes.GrantRef, semantics identifier.Semantics) bool {
	if routineObjectTypes[strings.ToUpper(grant.ObjectType)] {
		routine, owned := r.routines[bareName(grant.ObjectName)]
		return owned && slices.ContainsFunc(routine.held, func(held heldRoutine) bool {
			return !held.kept && (held.Signature == nil || held.grantedBy(grant, semantics))
		})
	}
	return grant.ObjectType == "TABLE" && slices.ContainsFunc(r.removeOnly, func(object deporder.ViewLike) bool {
		return !object.Routine && sameObject(grant.ObjectName, object.Name, semantics)
	})
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
// directly or through another returned candidate, an unqualified name of either
// resolving in schema.
func readersOf(candidates []deporder.ViewLike, names []string, semantics identifier.Semantics) []deporder.ViewLike {
	var found []deporder.ViewLike
	taken := make([]bool, len(candidates))
	frontier := append([]string(nil), names...)
	for len(frontier) > 0 {
		name := frontier[0]
		frontier = frontier[1:]
		for i, candidate := range candidates {
			if taken[i] || !deporder.ReferencesRelation(candidate.Body, name, semantics) {
				continue
			}
			taken[i] = true
			frontier = append(frontier, candidate.Name)
			found = append(found, candidate)
		}
	}
	return found
}

// refuseUnrebuildable refuses a rebuild that would drop a view or a
// materialized view the diff carries no declaration of, and names it rather
// than leave it out. Only a diff built by hand can do that: the plan would
// drop the object and create nothing.
func refuseUnrebuildable(diff *difftypes.SchemaDiff, rebuild viewRebuild, semantics identifier.Semantics) error {
	for _, object := range rebuild.created() {
		if object.Routine || declaredViewLike(diff.DeclaredViewLikes, object, semantics) {
			continue
		}
		return fmt.Errorf("%w: the plan drops %s %s and creates it again, and the diff carries no declaration of it",
			ptaherr.ErrInvalidSchemaDiff, viewLikeKind(object), object.Name)
	}
	return nil
}

// declaredViewLike reports whether declared holds object.
func declaredViewLike(declared difftypes.ViewLikeVocabulary, object deporder.ViewLike, semantics identifier.Semantics) bool {
	if object.Materialized {
		return objectlookup.MaterializedView(declared.MaterializedViews, object.Name, semantics) != nil
	}
	return objectlookup.View(declared.Views, object.Name, semantics) != nil
}

// viewLikeKind is the word a message names object's kind with.
func viewLikeKind(object deporder.ViewLike) string {
	switch {
	case object.Routine:
		return "routine"
	case object.Materialized:
		return "materialized view"
	default:
		return "view"
	}
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
	for _, object := range deporder.ViewLikesForCreateIn(rebuild.replaceFirst, p.targetDialect(), semantics) {
		result = appendNode(result, p.viewReplace(diff, object, semantics))
	}
	for _, object := range deporder.ViewLikesForDropIn(heldDefinitions(diff, rebuild.dropped()), p.targetDialect(), semantics) {
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
// deferred objects, replaces the views that have to wait for them, and writes
// the routines placed with the relations that name any of them. It runs after
// the view step, in creation order.
//
// Each object the rebuild dropped, or the plan adds, is created outright rather
// than re-asserted with CREATE OR REPLACE: it is known to be absent. What the
// drop took with it comes back right after it: the declared indexes of a
// materialized view, the declared triggers of a view, the options the database
// held it with, and the privileges it held. A view's, materialized view's or
// routine's comment is part of its CREATE.
func (p *Planner) createRebuiltViewLikes(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
	rebuild viewRebuild,
	routines relationRoutines,
) []ast.Node {
	pending := rebuild.late()
	if len(pending) == 0 {
		return result
	}
	pending = append(pending, deferredRoutines(rebuild, routines)...)
	recreated := make(viewLikeSet, len(rebuild.recreate))
	for _, object := range rebuild.recreate {
		recreated[viewLikeKey(object)] = true
	}
	replaced := rebuild.replaces()

	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	for _, object := range deporder.ViewLikesForCreateIn(pending, p.targetDialect(), semantics) {
		switch {
		case object.Routine && rebuild.ownsRoutine(object.Name):
			routine := rebuild.routines[bareName(object.Name)]
			for _, overload := range routine.declared {
				result = append(result, modelast.FromFunctionFor(overload, p.targetDialect()))
			}
			result = append(result, routine.privilegeNodes(diff, semantics)...)
		case object.Routine:
			result = append(result, routines.nodes[object.Name]...)
		case replaced.has(object):
			result = appendNode(result, p.viewReplace(diff, object, semantics))
		default:
			result = appendNode(result, p.viewLikeCreate(diff, object, semantics))
			if recreated.has(object) {
				result = append(result, rebuiltIndexes(diff, object, semantics)...)
				result = append(result, rebuiltTriggers(diff, object, semantics)...)
				result = append(result, rebuiltRelationGrants(diff, object, semantics)...)
			}
		}
	}
	return result
}

// appendNode appends node to result unless it is nil.
func appendNode(result []ast.Node, node ast.Node) []ast.Node {
	if node == nil {
		return result
	}
	return append(result, node)
}

// rebuiltIndexes is the CREATE INDEX of every declared index on the rebuilt
// materialized view object, except the ones the plan adds, which the index
// step creates.
func rebuiltIndexes(diff *difftypes.SchemaDiff, object deporder.ViewLike, semantics identifier.Semantics) []ast.Node {
	if !object.Materialized {
		return nil
	}
	var nodes []ast.Node
	for _, declaration := range diff.DeclaredIndexes {
		if !sameObject(declaration.TableName, object.Name, semantics) || slices.ContainsFunc(diff.IndexesAdded, func(added difftypes.IndexChange) bool {
			return strings.EqualFold(added.Index.Name, declaration.Index.Name) && sameObject(added.TableName, declaration.TableName, semantics)
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
// trigger steps write after this one. A trigger whose function Ptah writes
// for it is created with CREATE OR REPLACE FUNCTION: the drop of the view
// leaves the function in place.
func rebuiltTriggers(diff *difftypes.SchemaDiff, object deporder.ViewLike, semantics identifier.Semantics) []ast.Node {
	if object.Materialized {
		return nil
	}
	planned := func(name, table string) bool {
		same := func(trigger, on string) bool {
			return strings.EqualFold(trigger, name) && sameObject(on, table, semantics)
		}
		return slices.ContainsFunc(diff.TriggersAdded, func(ref difftypes.TriggerRef) bool { return same(ref.TriggerName, ref.TableName) }) ||
			slices.ContainsFunc(diff.TriggersModified, func(change difftypes.TriggerDiff) bool { return same(change.TriggerName, change.TableName) })
	}
	var nodes []ast.Node
	for _, trigger := range diff.DeclaredTriggers {
		if !sameObject(trigger.Table, object.Name, semantics) || planned(trigger.Name, trigger.Table) {
			continue
		}
		node := modelast.FromTrigger(trigger)
		if !node.ExternalFunction {
			node.SetReplace()
		}
		nodes = append(nodes, node)
	}
	return nodes
}

// rebuiltRelationGrants is the GRANT of every privilege the database held on
// the rebuilt view or materialized view object, column privileges included,
// except the ones the plan revokes or grants itself, which the grant steps
// write after this one.
//
// They are what the database held, not what the declaration grants: a
// comparison leaves alone a privilege of a role the schema does not manage,
// and the drop would take it all the same.
func rebuiltRelationGrants(diff *difftypes.SchemaDiff, object deporder.ViewLike, semantics identifier.Semantics) []ast.Node {
	var nodes []ast.Node
	for _, grant := range diff.CurrentGrants {
		if grant.ObjectType == "TABLE" && sameObject(grant.ObjectName, object.Name, semantics) && !grantPlanned(diff, grant, semantics) {
			nodes = append(nodes, grantNode(grant))
		}
	}
	return nodes
}

// privilegeNodes is the statements that give each overload the rebuild creates
// again the privileges the database held on it, except the ones the plan
// revokes or grants itself.
//
// A new routine is executable by PUBLIC, which the database reports as a
// privilege of its own. An overload PUBLIC could not execute has it revoked
// again; one it could execute needs nothing.
func (r rebuiltRoutine) privilegeNodes(diff *difftypes.SchemaDiff, semantics identifier.Semantics) []ast.Node {
	var nodes []ast.Node
	for _, held := range r.held {
		if !held.kept || held.Signature == nil {
			continue
		}
		public := false
		for _, grant := range diff.CurrentGrants {
			if !held.grantedBy(grant, semantics) {
				continue
			}
			if grant.Role == "PUBLIC" && grant.Privilege == "EXECUTE" && grant.Column == "" {
				public = true
				continue
			}
			if !grantPlanned(diff, grant, semantics) {
				nodes = append(nodes, grantNode(grant))
			}
		}
		if !public {
			nodes = append(nodes, ast.NewRevokePrivilege("PUBLIC", routineObjectType(held.Kind), held.Name, []string{"EXECUTE"}).
				SetArguments(*held.Signature))
		}
	}
	return nodes
}

// grantedBy reports whether grant is a privilege on this overload: the input
// argument types name it, which a declaration may spell with parameter names
// or without.
func (h heldRoutine) grantedBy(grant difftypes.GrantRef, semantics identifier.Semantics) bool {
	return routineObjectTypes[strings.ToUpper(grant.ObjectType)] &&
		sameObject(grant.ObjectName, h.Name, semantics) &&
		routineargs.InputTypes(grant.Arguments) == routineargs.InputTypes(*h.Signature)
}

// routineObjectTypes are the object types a grant names a routine with.
var routineObjectTypes = map[string]bool{"FUNCTION": true, "PROCEDURE": true, "ROUTINE": true}

// routineObjectType is the object type a statement names a routine of kind
// with.
func routineObjectType(kind string) string {
	if strings.EqualFold(kind, schemamodel.FunctionKindProcedure) {
		return "PROCEDURE"
	}
	return "FUNCTION"
}

// grantPlanned reports whether the plan grants or revokes grant in a step of
// its own.
func grantPlanned(diff *difftypes.SchemaDiff, grant difftypes.GrantRef, semantics identifier.Semantics) bool {
	same := func(planned difftypes.GrantRef) bool {
		return planned.Role == grant.Role && planned.Privilege == grant.Privilege && planned.Column == grant.Column &&
			sameObject(planned.ObjectName, grant.ObjectName, semantics) &&
			routineargs.InputTypes(planned.Arguments) == routineargs.InputTypes(grant.Arguments)
	}
	return slices.ContainsFunc(diff.GrantsAdded, same) || slices.ContainsFunc(diff.GrantsRemoved, same)
}

// grantNode is the GRANT that gives grant again.
func grantNode(grant difftypes.GrantRef) ast.Node {
	return ast.NewGrantPrivilege(grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
		SetArguments(grant.Arguments).
		SetColumns(grantColumns(grant)).
		SetWithOption(grant.WithOption)
}

// sameObject reports whether two names spell one object: the same string, or
// the same schema and name once a missing schema is read as the default one.
// Two names in different schemas are never one object, even when one of them
// leaves its schema out.
func sameObject(a, b string, semantics identifier.Semantics) bool {
	return a == b || semantics.QualifiedTableIdentityKey(a) == semantics.QualifiedTableIdentityKey(b)
}

// heldViewOptions is the options the database holds the view or materialized
// view object with, or nil.
func heldViewOptions(diff *difftypes.SchemaDiff, object deporder.ViewLike, semantics identifier.Semantics) []string {
	held := diff.CurrentViewOptions.Views
	if object.Materialized {
		held = diff.CurrentViewOptions.MaterializedViews
	}
	for name, options := range held {
		if sameObject(name, object.Name, semantics) {
			return slices.Clone(options)
		}
	}
	return nil
}

// viewLikeCreate is the statement that creates object from its declaration,
// with the options the database holds it with; nil when the declaration does
// not hold it.
func (p *Planner) viewLikeCreate(diff *difftypes.SchemaDiff, object deporder.ViewLike, semantics identifier.Semantics) ast.Node {
	options := heldViewOptions(diff, object, semantics)
	if object.Materialized {
		view := objectlookup.MaterializedView(diff.DeclaredViewLikes.MaterializedViews, object.Name, semantics)
		if view == nil {
			return nil
		}
		node := modelast.FromMaterializedView(*view)
		node.Options = options
		return node
	}
	view := objectlookup.View(diff.DeclaredViewLikes.Views, object.Name, semantics)
	if view == nil {
		return nil
	}
	node := modelast.FromView(*view)
	node.Options = options
	return node
}

// viewReplace is the CREATE OR REPLACE VIEW that gives the view object the
// definition its declaration holds, with the options the database holds it
// with: the replace sets the options it states and clears the others. It is
// nil when the declaration holds no such view.
func (p *Planner) viewReplace(diff *difftypes.SchemaDiff, object deporder.ViewLike, semantics identifier.Semantics) ast.Node {
	node, isView := p.viewLikeCreate(diff, object, semantics).(*ast.CreateViewNode)
	if !isView {
		return nil
	}
	return node.SetReplace()
}

// deferredRoutines is the routines placed with the relations, other than the
// ones the rebuild owns, whose definitions name an object the rebuild creates
// or replaces after its drops, directly or through another such routine.
// [Planner.addNewViewLikeObjects] leaves them out: it runs while that object
// is absent, or before it takes its new definition, and PostgreSQL resolves a
// SQL-language body when the routine is created.
func deferredRoutines(rebuild viewRebuild, routines relationRoutines) []deporder.ViewLike {
	late := rebuild.late()
	if len(late) == 0 {
		return nil
	}
	return readersOf(routines.viewLikes(), namesOf(late), rebuild.semantics)
}

// dropViewLikeNode is the statement that drops one view or materialized view.
func (p *Planner) dropViewLikeNode(object deporder.ViewLike, cascade bool) ast.Node {
	if object.Materialized {
		return dropMaterializedViewNode(object.Name, cascade)
	}
	return p.dropViewNode(object.Name, cascade)
}
