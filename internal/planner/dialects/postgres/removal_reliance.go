package postgres

import (
	"cmp"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgpolicy"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/deporder"
	"ptah.run/internal/routineargs"
	"ptah.run/internal/routinebody"
	"ptah.run/internal/tableref"
)

// routineNames reports whether routine depends on relation, with names
// resolved as semantics says: the type of a parameter or of its result is the
// relation's row type, or its SQL-standard body reads the relation. A
// parameter or a result column of that name, or a type of the same name in
// another schema, is not the row type.
func routineNames(routine difftypes.RoutineChange, relation string, semantics identifier.Semantics) bool {
	types := append(routineargs.ArgumentTypes(routine.Parameters), routineargs.ResultTypes(routine.Returns)...)
	return slices.ContainsFunc(types, func(spelled string) bool { return typeNamesRelation(spelled, relation, semantics) }) ||
		sqlStandardBodyReads(routine, relation, semantics)
}

// relationSemantics is where the names of diff resolve: the identifier
// semantics of the connection it was compared over, the ones the view rebuild
// and the policy refusal ask with; see [readsRelationIn].
func relationSemantics(diff *difftypes.SchemaDiff) identifier.Semantics {
	return diff.EffectiveIdentifierSemantics("")
}

// typeNamesRelation reports whether spelled, a type as a routine's definition
// writes it, is the row type of relation or an array of it. Names compare
// without case and resolve as [deporder.ReferencesRelation] resolves them: a
// relation named without a schema is in the default schema, and a type
// spelled without one may be in any schema of the search path, since
// PostgreSQL prints a type without its schema wherever the path reaches it.
// What semantics leaves out matches any schema, so a type that might be the
// relation's row type counts as one.
func typeNamesRelation(spelled, relation string, semantics identifier.Semantics) bool {
	spelled = strings.TrimSpace(spelled)
	for strings.HasSuffix(spelled, "[]") {
		spelled = strings.TrimSpace(strings.TrimSuffix(spelled, "[]"))
	}
	named, ok := tableref.Parse(spelled)
	if !ok {
		return false
	}
	want, ok := tableref.Parse(relation)
	if !ok || !strings.EqualFold(named.Name, want.Name) {
		return false
	}
	schema := want.Schema
	if !want.Qualified {
		schema = semantics.DefaultSchema
	}
	switch {
	case schema == "":
		return true
	case named.Qualified:
		return strings.EqualFold(named.Schema, schema)
	default:
		return len(semantics.SearchPath) == 0 || slices.ContainsFunc(semantics.SearchPath, func(onPath string) bool {
			return strings.EqualFold(onPath, schema)
		})
	}
}

// routinesAhead says where the exact style drops each removed routine.
//
// The server records what a routine depends on: the row type its parameters or
// result name, and what a SQL-standard body reads. A routine that depends on a
// relation or a column the plan removes leaves just ahead of the first of them,
// and what calls that routine leaves before it. The drop of the relation or the
// column then relies on nothing and carries no CASCADE, which would also take a
// reader nobody declared, such as a view in a schema the run leaves out. The
// server refuses that drop and names the reader.
//
// A routine that depends on none of them stays in the routine step, after the
// tables, and so does every routine in the annotated style.
//
// What calls such a routine and is not a routine has to leave first too. The
// removed views and the removed tables are ordered for that; see
// [routinesAhead.viewsAfter] and [routinesAhead.tablesAfter]. Where no order
// does it, the plan is refused before the server changes.
type routinesAhead struct {
	// byRelation is the routines that leave just ahead of a relation, by its
	// name. A statement drops each group; callers come first.
	byRelation map[string][]removedRoutineGroup
	// byColumn is the routines that leave just ahead of a column, keyed as
	// [routinesAhead.ofColumn] keys it.
	byColumn map[string][]removedRoutineGroup
	// rest is the routines of the routine step.
	rest []removedRoutineGroup
	// viewsAfter is, by the name of a removed view, the removed views that go
	// after it, because it calls a routine that depends on them.
	viewsAfter map[string][]string
	// tablesAfter is the same for removed tables.
	tablesAfter map[string][]string
	// tablesDropped is the removed tables the plan has written a drop for so far.
	tablesDropped map[string]bool
}

// columnsLeavingEarly is the generated columns the exact style drops ahead of
// the removed views; see [earlyGeneratedColumns]. The annotated style has none.
func (p *Planner) columnsLeavingEarly(diff *difftypes.SchemaDiff) map[string]bool {
	if !p.exact() {
		return nil
	}
	return earlyGeneratedColumns(diff)
}

// earlyGeneratedColumns identifies generated columns on kept tables that call
// a removed routine whose signature or SQL body names a removed relation.
// They leave before that routine and relation. Their own drop uses RESTRICT;
// an unmodeled dependent object must make the server refuse the plan.
func earlyGeneratedColumns(diff *difftypes.SchemaDiff) map[string]bool {
	early := make(map[string]bool)
	relations := removedViewLikes(diff)
	for _, table := range diff.TablesRemoved {
		relations = append(relations, deporder.ViewLike{Name: table.Name})
	}
	for _, table := range diff.TablesModified {
		for _, column := range table.ColumnsRemoved {
			if strings.TrimSpace(column.GeneratedExpression) == "" {
				continue
			}
			for _, routines := range []difftypes.FunctionChanges{diff.FunctionsRemoved, diff.ProceduresRemoved} {
				for _, routine := range routines {
					if !deporder.ReferencesIdentifier(column.GeneratedExpression, unqualified(routine.Name)) {
						continue
					}
					if slices.ContainsFunc(relations, func(relation deporder.ViewLike) bool {
						return routineNames(routine, relation.Name, relationSemantics(diff))
					}) {
						early[table.TableName+"\x00"+column.Name] = true
					}
				}
			}
		}
	}
	return early
}

// routinesAheadOf works out where the plan drops each removed routine; see
// [routinesAhead]. It refuses a plan in which something that calls a routine
// cannot leave before what that routine depends on.
func (p *Planner) routinesAheadOf(diff *difftypes.SchemaDiff, rebuild viewRebuild, earlyColumns map[string]bool, droppedPolicies map[objectidentity.Key]bool) (routinesAhead, error) {
	ahead := routinesAhead{
		byRelation:  make(map[string][]removedRoutineGroup),
		byColumn:    make(map[string][]removedRoutineGroup),
		viewsAfter:  make(map[string][]string),
		tablesAfter: make(map[string][]string),

		tablesDropped: make(map[string]bool),
	}
	groups := p.removedRoutineGroups(diff, rebuild)
	if !p.exact() {
		ahead.rest = groups
		return ahead, nil
	}
	reads := p.routineGroupReads(diff, groups, p.removalSteps(diff, rebuild, earlyColumns, ahead))
	callers := make([][]removalPoint, len(groups))
	for i, group := range groups {
		for _, routine := range group.routines {
			callers[i] = append(callers[i], routineCallers(diff, rebuild, routine, droppedPolicies)...)
		}
		for _, caller := range callers[i] {
			for _, read := range reads[i] {
				ahead.holdBack(caller, read)
			}
		}
	}
	positions := make(map[removalPoint]int)
	for i, step := range p.removalSteps(diff, rebuild, earlyColumns, ahead) {
		for _, point := range step {
			positions[point] = i
		}
	}
	for i, group := range groups {
		if len(reads[i]) == 0 {
			ahead.rest = append(ahead.rest, group)
			continue
		}
		for _, caller := range callers[i] {
			for _, read := range reads[i] {
				if position, ordered := positions[caller]; !ordered || position >= positions[read] {
					return routinesAhead{}, routineWithoutOrder(group, caller, read)
				}
			}
		}
		ahead.add(slices.MinFunc(reads[i], func(a, b removalPoint) int {
			return cmp.Compare(positions[a], positions[b])
		}), group)
	}
	return ahead, nil
}

// routineWithoutOrder is the refusal of a plan in which caller has to leave
// before a routine of group, and that routine before read, and caller cannot
// leave before read.
func routineWithoutOrder(group removedRoutineGroup, caller, read removalPoint) error {
	name := group.routines[0].Name
	return fmt.Errorf("%w: %s calls routine %s, which depends on %s, and the plan has no order that drops them all without CASCADE. "+
		"CASCADE would also take objects the plan does not know of. "+
		"Change %s so it no longer calls %s in one migration, and remove the rest in the next",
		ptaherr.ErrUnsupportedFeature, caller, name, read, caller, name)
}

// removalPoint is one object the plan removes that a routine can depend on or
// be called by: a dropped column, a removed view or table, or a removed domain.
type removalPoint struct {
	kind     string
	relation string
	column   string
}

const (
	columnPoint  = "column"
	viewPoint    = "view"
	matviewPoint = "materialized view"
	tablePoint   = "table"
	typePoint    = "type"
	domainPoint  = "domain"
)

// String is how a refusal names the point.
func (r removalPoint) String() string {
	if r.kind == columnPoint {
		return r.kind + " " + r.relation + "." + r.column
	}
	return r.kind + " " + r.relation
}

func (r removalPoint) isView() bool {
	return r.kind == viewPoint || r.kind == matviewPoint
}

// viewLikePoint is the removal point of a removed view or materialized view.
func viewLikePoint(object deporder.ViewLike) removalPoint {
	if object.Materialized {
		return removalPoint{kind: matviewPoint, relation: object.Name}
	}
	return removalPoint{kind: viewPoint, relation: object.Name}
}

// removalSteps is the statements that drop the columns and relations the plan
// removes ahead of the routine step, in the order the plan makes them, each with
// what it drops: generated columns that leave early, then views, columns and
// tables. See [Planner.removeEarlyGeneratedColumns], [Planner.removeViewLikes],
// [Planner.removeTableColumnsFromDiff] and [Planner.removeTables].
func (p *Planner) removalSteps(diff *difftypes.SchemaDiff, rebuild viewRebuild, earlyColumns map[string]bool, ahead routinesAhead) [][]removalPoint {
	var steps [][]removalPoint
	for _, table := range diff.TablesModified {
		for _, column := range table.ColumnsRemoved {
			if earlyColumns[table.TableName+"\x00"+column.Name] {
				steps = append(steps, []removalPoint{{kind: columnPoint, relation: table.TableName, column: column.Name}})
			}
		}
	}
	for _, object := range p.removedViewOrder(diff, rebuild, ahead) {
		steps = append(steps, []removalPoint{viewLikePoint(object)})
	}
	for _, table := range diff.TablesModified {
		for _, generated := range []bool{true, false} {
			for _, column := range table.ColumnsRemoved {
				if isGenerated(column) == generated && !earlyColumns[table.TableName+"\x00"+column.Name] {
					steps = append(steps, []removalPoint{{kind: columnPoint, relation: table.TableName, column: column.Name}})
				}
			}
		}
	}
	holding, later := p.removedTablesAroundTypes(diff, ahead)
	addTables := func(groups [][]string) {
		for _, group := range groups {
			step := make([]removalPoint, 0, len(group))
			for _, name := range group {
				step = append(step, removalPoint{kind: tablePoint, relation: name})
			}
			steps = append(steps, step)
		}
	}
	addTables(holding)
	for _, name := range typesOverRemovedRows(diff) {
		steps = append(steps, []removalPoint{{kind: typePoint, relation: name}})
	}
	addTables(later)
	return steps
}

// typesOverRemovedRows names the types the plan removes that name the row type of a
// table it removes, directly or through another such type. The type owner drops one
// ahead of the tables, unless a drop that comes later still uses it; see
// [typeorder.DroppedBeforeTables].
func typesOverRemovedRows(diff *difftypes.SchemaDiff) []string {
	semantics := relationSemantics(diff)
	readers := removedTypeReaders(diff)
	over := diff.TablesRemoved.Names()
	var names []string
	for changed := true; changed; {
		changed = false
		for _, typ := range readers {
			name := typeReaderName(typ)
			if !slices.Contains(names, name) && slices.ContainsFunc(over, func(relation string) bool {
				return schemaext.ReadsRelationIn(typ.reader, relation, semantics)
			}) {
				names = append(names, name)
				over = append(over, name)
				changed = true
			}
		}
	}
	return names
}

// typeReaderName is the name a definition spells typ with.
func typeReaderName(typ removedTypeReader) string {
	if typ.id.Schema.Source == "" {
		return typ.id.Name.Source
	}
	return typ.id.Schema.Source + "." + typ.id.Name.Source
}

// tableHoldsType reports whether a column of removal has the type name, in any
// spelling the catalog records it.
func tableHoldsType(diff *difftypes.SchemaDiff, removal difftypes.TableRemoval, name string) bool {
	return slices.ContainsFunc(removedTableTypes(diff, []string{removal.Name}), func(spelled string) bool {
		return typeNamesRelation(spelled, name, relationSemantics(diff))
	})
}

// removedTablesAroundTypes splits the removed tables' groups: those a type in
// [typesOverRemovedRows] is the column type of, which leave ahead of the feature
// removal window so the type can leave in it, and the rest. A type left behind them
// would hold its own table's drop until a CASCADE, which also takes a reader nobody
// declared. The annotated style keeps its order.
func (p *Planner) removedTablesAroundTypes(diff *difftypes.SchemaDiff, ahead routinesAhead) (holding, later [][]string) {
	groups := p.removedTableGroups(diff, ahead)
	if !p.exact() {
		return nil, groups
	}
	types := typesOverRemovedRows(diff)
	if len(types) == 0 {
		return nil, groups
	}
	for _, group := range groups {
		holds := slices.ContainsFunc(group, func(table string) bool {
			return slices.ContainsFunc(diff.TablesRemoved, func(removal difftypes.TableRemoval) bool {
				return removal.Name == table && slices.ContainsFunc(types, func(name string) bool {
					return tableHoldsType(diff, removal, name)
				})
			})
		})
		if holds {
			holding = append(holding, group)
		} else {
			later = append(later, group)
		}
	}
	return holding, later
}

// routineGroupReads is, for each group, what its routines depend on among the
// points of steps: directly, or through a routine they call. Groups come
// callers first, so walking them backward meets every callee before its caller.
func (p *Planner) routineGroupReads(diff *difftypes.SchemaDiff, groups []removedRoutineGroup, steps [][]removalPoint) [][]removalPoint {
	points := slices.Concat(steps...)
	byRoutine := make(map[string][]removalPoint)
	reads := make([][]removalPoint, len(groups))
	for i := len(groups) - 1; i >= 0; i-- {
		var group []removalPoint
		for _, routine := range groups[i].routines {
			for _, point := range points {
				if p.routineDependsOn(routine, point, diff) && !slices.Contains(group, point) {
					group = append(group, point)
				}
			}
		}
		for _, callee := range groups[i].calls {
			for _, point := range byRoutine[callee] {
				if !slices.Contains(group, point) {
					group = append(group, point)
				}
			}
		}
		for _, routine := range groups[i].routines {
			byRoutine[routineKey(routine)] = group
		}
		reads[i] = group
	}
	return reads
}

// routineDependsOn reports whether the server holds point back for routine.
func (p *Planner) routineDependsOn(routine difftypes.RoutineChange, point removalPoint, diff *difftypes.SchemaDiff) bool {
	semantics := relationSemantics(diff)
	if point.kind == columnPoint {
		return routineReadsColumn(routine, point.relation, point.column, semantics)
	}
	return routineNames(routine, point.relation, semantics) || p.quotedBodyHolds(routine, point.relation, diff)
}

// routineCallers is what the plan removes that the server holds routine back
// for: a removed view whose body calls it, a dropped column or a removed table
// with a definition that calls it, a captured object of a removed table that
// reads it and has not left already, and a removed domain whose CHECK or DEFAULT
// calls it. What the plan keeps is not here: while it calls the routine the
// server refuses the drop, and rightly so.
func routineCallers(diff *difftypes.SchemaDiff, rebuild viewRebuild, routine difftypes.RoutineChange, droppedPolicies map[objectidentity.Key]bool) []removalPoint {
	var callers []removalPoint
	owned := rebuild.owns()
	for _, object := range removedViewLikes(diff) {
		if !owned.has(object) && callsRoutine(routine, object.Body) {
			callers = append(callers, viewLikePoint(object))
		}
	}
	for _, table := range diff.TablesModified {
		for _, column := range table.ColumnsRemoved {
			if callsRoutine(routine, column.Default, column.DefaultExpr, column.GeneratedExpression, column.Check) {
				callers = append(callers, removalPoint{kind: columnPoint, relation: table.TableName, column: column.Name})
			}
		}
	}
	for _, removal := range diff.TablesRemoved {
		if callsRoutine(routine, removedTableDefinitions(removal)...) ||
			capturedReadsExcept(removal.Current.OwnedObjects, unqualified(routine.Name), relationSemantics(diff), droppedPolicies) {
			callers = append(callers, removalPoint{kind: tablePoint, relation: removal.Name})
		}
	}
	if domain, calls := removedDomainCalling(diff, routine); calls {
		callers = append(callers, removalPoint{kind: domainPoint, relation: domain})
	}
	return callers
}

// callsRoutine reports whether one of the texts calls routine: its name stands
// in the code, followed by an opening parenthesis. A column of the same name
// does not hold the routine back, and a catalog prints every call with the
// parenthesis against the name.
func callsRoutine(routine difftypes.RoutineChange, texts ...string) bool {
	name := unqualified(routine.Name)
	call := regexp.MustCompile(`(?i)(^|[^\w$])"?` + regexp.QuoteMeta(name) + `"?\s*\(`)
	return slices.ContainsFunc(texts, func(text string) bool {
		return call.MatchString(text) && deporder.ReferencesIdentifier(text, name)
	})
}

// holdBack records that caller has to leave before read, where an order of the
// plan can see to it. Every other pair is in the order its steps give it.
func (a routinesAhead) holdBack(caller, read removalPoint) {
	switch {
	case caller.isView() && read.isView() && caller.relation != read.relation:
		a.viewsAfter[caller.relation] = append(a.viewsAfter[caller.relation], read.relation)
	case caller.kind == tablePoint && read.kind == tablePoint && caller.relation != read.relation:
		a.tablesAfter[caller.relation] = append(a.tablesAfter[caller.relation], read.relation)
	}
}

// add makes group leave just ahead of point.
func (a routinesAhead) add(point removalPoint, group removedRoutineGroup) {
	if point.kind == columnPoint {
		key := point.relation + "\x00" + point.column
		a.byColumn[key] = append(a.byColumn[key], group)
		return
	}
	a.byRelation[point.relation] = append(a.byRelation[point.relation], group)
}

// of is the routines the plan drops just ahead of relation.
func (a routinesAhead) of(relation string) []removedRoutineGroup {
	return a.byRelation[relation]
}

// ofColumn is the routines the plan drops just ahead of a column of table.
func (a routinesAhead) ofColumn(table, column string) []removedRoutineGroup {
	return a.byColumn[table+"\x00"+column]
}

// routineKey identifies a removed routine by its kind, name and signature.
func routineKey(routine difftypes.RoutineChange) string {
	signature := "\x00"
	if routine.Signature != nil {
		signature = *routine.Signature
	}
	return routine.Kind + "\x00" + routine.Name + "\x00" + signature
}

// removedRoutineGroup is one statement of the routine step: the routines it
// drops, of one kind, and whether the step after the type drops holds them.
type removedRoutineGroup struct {
	routines   []difftypes.RoutineChange
	kind       string
	afterTypes bool
	// calls is the routine keys of the functions the group's routines call.
	calls []string
}

// removedRoutineGroups is the routines the routine step drops, in the order the
// server requires: a routine ahead of the functions it calls, so a drop never
// meets a caller the plan removes later. Functions keep their order among
// themselves otherwise, ahead of the procedures.
//
// A routine a removed domain calls leaves after the types, and so does every
// function that routine calls. Functions that call each other go in one
// statement, because PostgreSQL drops the routines one statement names together:
// CREATE OR REPLACE lets a SQL-standard body call its own caller, and neither
// drop runs first without CASCADE, which would also take a reader nobody
// declared. Nothing calls a procedure, so a procedure is never part of a cycle.
func (p *Planner) removedRoutineGroups(diff *difftypes.SchemaDiff, rebuild viewRebuild) []removedRoutineGroup {
	type dropped struct {
		routine difftypes.RoutineChange
		kind    string
	}
	byKey := make(map[string]dropped)
	var keys []string
	var functions []difftypes.RoutineChange
	for _, routines := range []struct {
		changes difftypes.FunctionChanges
		kind    string
	}{{diff.FunctionsRemoved, schemamodel.FunctionKindFunction}, {diff.ProceduresRemoved, schemamodel.FunctionKindProcedure}} {
		for _, routine := range routines.changes {
			if rebuild.ownsRoutine(routine.Name) {
				continue
			}
			key := routineKey(routine)
			byKey[key] = dropped{routine: routine, kind: routines.kind}
			keys = append(keys, key)
			if routines.kind == schemamodel.FunctionKindFunction {
				functions = append(functions, routine)
			}
		}
	}
	calls := make(map[string][]string, len(keys))
	for _, key := range keys {
		for _, function := range functions {
			if callee := routineKey(function); callee != key && p.routineHolds(byKey[key].routine, function, diff) {
				calls[key] = append(calls[key], callee)
			}
		}
	}
	held := make(map[string]bool)
	var groups []removedRoutineGroup
	for _, members := range deporder.StableReverseDependencyGroups(keys, calls) {
		group := removedRoutineGroup{kind: byKey[members[0]].kind}
		for _, key := range members {
			group.routines = append(group.routines, byKey[key].routine)
			group.calls = append(group.calls, calls[key]...)
			group.afterTypes = group.afterTypes || held[key] || removedDomainCallsRoutine(diff, byKey[key].routine)
		}
		if group.afterTypes {
			for _, key := range members {
				for _, callee := range calls[key] {
					held[callee] = true
				}
			}
		}
		groups = append(groups, group)
	}
	return groups
}

// routineHolds reports whether the server refuses to drop callee while caller
// exists. PostgreSQL records what a parameter default calls and what a
// SQL-standard body calls, and nothing for a body in quotes. CockroachDB also
// records what a quoted body calls, measured on v26.3.2. A name that may not be
// a call only orders two drops or joins them in one statement, either of which
// the server accepts.
func (p *Planner) routineHolds(caller, callee difftypes.RoutineChange, diff *difftypes.SchemaDiff) bool {
	semantics := relationSemantics(diff)
	if deporder.ReferencesRelation(caller.Parameters, callee.Name, semantics) {
		return true
	}
	if !routinebody.SQLStandard(caller.Language, caller.Body) && p.targetDialect() != platform.CockroachDB {
		return false
	}
	return deporder.ReferencesRelation(caller.Body, callee.Name, semantics)
}

// removedDomainCallsRoutine reports a removed domain whose CHECK or DEFAULT
// calls routine. PostgreSQL records that dependency, so the domain must leave
// before the routine.
func removedDomainCallsRoutine(diff *difftypes.SchemaDiff, routine difftypes.RoutineChange) bool {
	_, calls := removedDomainCalling(diff, routine)
	return calls
}

// removedDomainCalling is the first removed domain that calls routine, as
// [removedDomainCallsRoutine] asks.
func removedDomainCalling(diff *difftypes.SchemaDiff, routine difftypes.RoutineChange) (string, bool) {
	name := unqualified(routine.Name)
	for _, record := range diff.FeatureChanges {
		change, ok := record.Value.(*pgtype.DomainChange)
		if !ok || change == nil || change.Before == nil || change.After != nil {
			continue
		}
		before := change.Before
		if deporder.ReferencesIdentifier(before.Check, name) || deporder.ReferencesIdentifier(before.Default, name) ||
			slices.ContainsFunc(before.Checks, func(check pgtype.DomainCheck) bool {
				return deporder.ReferencesIdentifier(check.Expression, name)
			}) {
			return record.Subject.Name.Source, true
		}
	}
	return "", false
}

// removedTableDefinitions is the text of every definition the plan drops with a
// removed table that may call a routine: a column's default and generation
// expression, a CHECK clause and an index definition.
func removedTableDefinitions(removal difftypes.TableRemoval) []string {
	var definitions []string
	for _, column := range removal.Current.Table.Columns {
		definitions = append(definitions, derefText(column.ColumnDefault), derefText(column.GeneratedExpression))
	}
	for _, constraint := range removal.Current.Constraints {
		definitions = append(definitions, derefText(constraint.CheckClause))
	}
	for _, index := range removal.Current.Indexes {
		definitions = append(definitions, index.Definition)
	}
	return definitions
}

// derefText is the text text points at, empty for nil.
func derefText(text *string) string {
	if text == nil {
		return ""
	}
	return *text
}

// routineReadsColumn reports whether routine reads column of table: its
// SQL-standard body names the table and the column, or selects `*`, which the
// server expands into every column when it creates the routine. A row type in
// the parameters or the result does not hold a column back.
func routineReadsColumn(routine difftypes.RoutineChange, table, column string, semantics identifier.Semantics) bool {
	return sqlStandardBodyReads(routine, table, semantics) &&
		(strings.Contains(routine.Body, "*") || deporder.ReferencesIdentifier(routine.Body, column))
}

// quotedBodyHolds reports whether the target holds relation back for a routine
// whose body is a string. CockroachDB records what such a body reads and refuses
// the drop with SQLSTATE 2BP01, measured on v26.3.2, where PostgreSQL records
// nothing for it.
func (p *Planner) quotedBodyHolds(routine difftypes.RoutineChange, relation string, diff *difftypes.SchemaDiff) bool {
	return p.targetDialect() == platform.CockroachDB &&
		deporder.ReferencesRelation(routine.Body, relation, relationSemantics(diff))
}

// sqlStandardBodyReads reports whether routine has a SQL-standard body,
// `BEGIN ATOMIC ... END` or `RETURN expression`, that names relation under
// the comparison's identifier semantics. The server parses such a body when the
// routine is created and records what it reads; a body in quotes is a string it
// records nothing for.
func sqlStandardBodyReads(routine difftypes.RoutineChange, relation string, semantics identifier.Semantics) bool {
	return routinebody.SQLStandard(routine.Language, routine.Body) &&
		deporder.ReferencesRelation(routine.Body, relation, semantics)
}

// removedTableReads reports whether a captured object of a table the plan
// removes, other than the tables skip names, reads relation.
func removedTableReads(diff *difftypes.SchemaDiff, relation string, skip map[string]bool) bool {
	return removedTableReadsExcept(diff, relation, skip, nil)
}

// removedTableReadsExcept ignores only captured policies whose standalone
// drops the plan already placed before this relation or column drop.
func removedTableReadsExcept(diff *difftypes.SchemaDiff, relation string, skip map[string]bool, droppedPolicies map[objectidentity.Key]bool) bool {
	for _, removal := range diff.TablesRemoved {
		if skip[removal.Name] || removal.Name == relation {
			continue
		}
		if capturedReadsExcept(removal.Current.OwnedObjects, relation, relationSemantics(diff), droppedPolicies) {
			return true
		}
	}
	return false
}

// capturedReads reports whether one of objects reads relation, with names
// resolved as semantics says.
func capturedReads(objects schemaext.Objects, relation string, semantics identifier.Semantics) bool {
	return capturedReadsExcept(objects, relation, semantics, nil)
}

func capturedReadsExcept(objects schemaext.Objects, relation string, semantics identifier.Semantics, droppedPolicies map[objectidentity.Key]bool) bool {
	all, err := objects.All()
	if err != nil {
		// An unreadable capture cannot say it reads nothing, so the drop keeps
		// CASCADE.
		return true
	}
	for _, object := range all {
		if droppedPolicies[object.Ref.Key()] {
			continue
		}
		if reader, ok := object.Value.(schemaext.RelationReader); ok && schemaext.ReadsRelationIn(reader, relation, semantics) {
			return true
		}
	}
	return false
}

// policiesDroppedBeforeRelations records actual owner operations in the
// prefix emitted before view and column drops. A captured policy counts as
// released only when its matching DROP POLICY is in that prefix.
func policiesDroppedBeforeRelations(nodes []ast.Node, diff *difftypes.SchemaDiff) map[objectidentity.Key]bool {
	dropped := make(map[objectidentity.Key]bool)
	for _, node := range nodes {
		for _, payload := range ast.OwnerOperations(node) {
			operation, ok := payload.(*pgpolicy.PolicyOperation)
			if !ok || operation == nil || operation.Change.Before == nil || operation.Change.After != nil {
				continue
			}
			ref := pgpolicy.PolicyRefWith(relationSemantics(diff), operation.Schema, operation.Table, operation.Name)
			dropped[ref.Key()] = true
		}
	}
	return dropped
}

// removedFeatureReads reports whether a database-scope feature change may drop
// an object that reads relation, which the removal window does only after the
// tables and columns. The object the change removes answers for itself
// through [schemaext.RelationReader]: an extension reads none. A change whose
// owner does not say whether it removes state, or whose object cannot answer,
// counts as one that reads relation.
//
// A type the change removes is left out: a composite type that names a
// table's row type in an attribute, and a domain or a range type over one,
// is dropped ahead of the table, and what uses it leaves ahead of it; see
// [Planner.removedTablesAroundTypes].
func removedFeatureReads(diff *difftypes.SchemaDiff, relation string) bool {
	return removedFeatureMatches(diff, func(object schemaext.Value) bool {
		if _, defines := object.(schemaext.TypeDefiner); defines {
			if _, reads := object.(schemaext.RelationReader); reads {
				return false
			}
		}
		reader, ok := object.(schemaext.RelationReader)
		return !ok || schemaext.ReadsRelationIn(reader, relation, relationSemantics(diff))
	})
}

// removedFeatureReadsColumns is [removedFeatureReads] for a column drop of
// table. A type an owner defines, such as a composite type, depends on a
// table's row type and on no column: PostgreSQL drops a column of a table
// whose row type a composite attribute names without CASCADE, and refuses
// only the table's drop. So a type the plan removes holds back no column drop,
// and any other object answers as it does for the table.
func removedFeatureReadsColumns(diff *difftypes.SchemaDiff, table string) bool {
	return removedFeatureMatches(diff, func(object schemaext.Value) bool {
		if _, defines := object.(schemaext.TypeDefiner); defines {
			return false
		}
		reader, ok := object.(schemaext.RelationReader)
		return !ok || schemaext.ReadsRelationIn(reader, table, relationSemantics(diff))
	})
}

// removedFeatureMatches reports whether a database-scope feature change that
// may remove state removes an object reads answers true for. A change whose
// object the read did not hold counts as a match.
func removedFeatureMatches(diff *difftypes.SchemaDiff, reads func(schemaext.Value) bool) bool {
	for _, change := range diff.FeatureChanges {
		if _, says := change.Value.(schemaext.StateRemoval); says && !schemaext.RemovesState(change.Value) {
			continue
		}
		object, found, err := diff.Features.CurrentObjects.Get(change.Subject)
		if err != nil || !found || reads(object.Value) {
			return true
		}
	}
	return false
}

// unqualified is relation without its schema. A routine's name matched
// without it may name a routine of another schema too, which errs toward
// keeping the routine where it was.
func unqualified(relation string) string {
	if ref, ok := tableref.Parse(relation); ok {
		return ref.Name
	}
	return relation
}
