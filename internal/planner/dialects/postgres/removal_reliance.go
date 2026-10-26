package postgres

import (
	"slices"
	"strings"

	"ptah.run/core/difftypes"
	"ptah.run/core/schemaext"
	"ptah.run/internal/deporder"
	"ptah.run/internal/routineargs"
	"ptah.run/internal/tableref"
)

// The objects below outlive a drop that relies on CASCADE for them. Each is one
// the plan removes too, but in a later step than the relation it depends on,
// so the server still records the dependency when that relation is dropped:
//
//   - a routine the plan removes, whose parameters or result name the
//     relation's row type, or whose SQL-standard body reads it, is dropped
//     with the routines, after every table and view;
//   - a row-level security policy, or another captured object that reads
//     relations, of a table the plan removes is dropped with its table.
//
// A relation one of them depends on cannot be dropped before it without
// CASCADE: the server answers 2BP01, `function list_v() depends on type v` or
// `policy p on table t depends on table allowed`.

// removedRoutineNames reports whether a routine the plan removes depends on the
// relation itself: its parameters or its result name the relation's row
// type, or its SQL-standard body reads it. A routine ahead drops before the
// relation is not one.
func removedRoutineNames(diff *difftypes.SchemaDiff, relation string, ahead routinesAhead) bool {
	return removedRoutineMatches(diff, func(routine difftypes.RoutineChange) bool {
		return !ahead.moves(routine) && routineNames(routine, relation, defaultSchema(diff))
	})
}

// routineNames reports whether routine depends on relation, an unqualified
// name of either resolving in schema: the type of a parameter or of its result
// is the relation's row type, or its SQL-standard body reads the relation. A
// parameter or a result column of that name, or a type of the same name in
// another schema, is not the row type.
func routineNames(routine difftypes.RoutineChange, relation, schema string) bool {
	types := append(routineargs.ArgumentTypes(routine.Parameters), resultTypes(routine.Returns)...)
	return slices.ContainsFunc(types, func(spelled string) bool { return typeNamesRelation(spelled, relation, schema) }) ||
		sqlStandardBodyReads(routine.Body, relation, schema)
}

// defaultSchema is the schema an unqualified name of diff resolves in, empty
// when it is not known.
func defaultSchema(diff *difftypes.SchemaDiff) string {
	return diff.EffectiveIdentifierSemantics("").DefaultSchema
}

// resultTypes are the types a routine's result names: the type, the element
// type of SETOF, or each column type of RETURNS TABLE.
func resultTypes(returns string) []string {
	result := strings.TrimSpace(returns)
	if fields := strings.Fields(result); len(fields) > 1 && strings.EqualFold(fields[0], "SETOF") {
		result = strings.TrimSpace(result[len(fields[0]):])
	}
	if len(result) > len("TABLE") && strings.EqualFold(result[:len("TABLE")], "TABLE") {
		columns := strings.TrimSpace(result[len("TABLE"):])
		if strings.HasPrefix(columns, "(") && strings.HasSuffix(columns, ")") {
			return routineargs.ArgumentTypes(columns[1 : len(columns)-1])
		}
	}
	if result == "" {
		return nil
	}
	return []string{result}
}

// typeNamesRelation reports whether spelled, a type as a routine's definition
// writes it, is the row type of relation or an array of it, an unqualified name
// of either resolving in schema. Names compare without case. An empty schema
// is not known, and an unqualified name then matches the name in any schema,
// so a type that might be the relation's row type counts as one.
func typeNamesRelation(spelled, relation, schema string) bool {
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
	in := func(ref tableref.Ref) string {
		if ref.Qualified {
			return ref.Schema
		}
		return schema
	}
	return in(named) == "" || in(want) == "" || strings.EqualFold(in(named), in(want))
}

// routinesAhead holds the routines the plan removes that the exact style drops
// just ahead of a relation, keyed by the relation, instead of after every
// table and view.
//
// Such a routine depends on the one relation the plan drops: its parameters or
// result name the relation's row type, or its SQL-standard body reads it.
// Dropped first, it leaves the relation's drop relying on nothing, so the drop
// carries no CASCADE, which would also take a reader nobody declared, such as
// a view in a schema the run leaves out. The server then refuses the drop and
// names that reader.
//
// A routine moves only when nothing else the plan knows of still uses it at
// that point: no view or materialized view the declaration holds, removes or
// rebuilds names it, no other removed routine's body does, and no captured
// object of a removed table reads it. It moves only ahead of a relation the
// plan removes, and only when that is the only relation it names that the plan
// drops. Every other routine stays in the routine step, and the relation's
// drop keeps CASCADE for it.
type routinesAhead struct {
	byRelation map[string][]difftypes.RoutineChange
	moved      map[string]bool
}

// routinesAheadOf works out which removed routines the plan drops ahead of a
// removed relation; see [routinesAhead]. Ptah's own style keeps every routine
// in the routine step.
func (p *Planner) routinesAheadOf(diff *difftypes.SchemaDiff, rebuild viewRebuild) routinesAhead {
	ahead := routinesAhead{byRelation: make(map[string][]difftypes.RoutineChange), moved: make(map[string]bool)}
	if !p.exact() {
		return ahead
	}
	removed := make(map[string]bool)
	var dropped []string
	for _, object := range removedViewLikes(diff) {
		removed[object.Name] = true
		dropped = append(dropped, object.Name)
	}
	for _, table := range diff.TablesRemoved {
		removed[table.Name] = true
		dropped = append(dropped, table.Name)
	}
	for _, object := range rebuild.dropped() {
		dropped = append(dropped, object.Name)
	}
	dropped = append(dropped, modifiedViewPaths(diff).dropped...)
	for _, routines := range []difftypes.FunctionChanges{diff.FunctionsRemoved, diff.ProceduresRemoved} {
		for _, routine := range routines {
			var named []string
			for _, relation := range dropped {
				if routineNames(routine, relation, defaultSchema(diff)) && !slices.Contains(named, relation) {
					named = append(named, relation)
				}
			}
			if len(named) != 1 || !removed[named[0]] || routineInUse(diff, routine, rebuild) {
				continue
			}
			ahead.byRelation[named[0]] = append(ahead.byRelation[named[0]], routine)
			ahead.moved[routineKey(routine)] = true
		}
	}
	return ahead
}

// moves reports whether the plan drops routine ahead of a relation.
func (a routinesAhead) moves(routine difftypes.RoutineChange) bool {
	return a.moved[routineKey(routine)]
}

// of is the routines the plan drops just ahead of relation.
func (a routinesAhead) of(relation string) []difftypes.RoutineChange {
	return a.byRelation[relation]
}

// routineKey identifies a removed routine by its kind, name and signature.
func routineKey(routine difftypes.RoutineChange) string {
	signature := "\x00"
	if routine.Signature != nil {
		signature = *routine.Signature
	}
	return routine.Kind + "\x00" + routine.Name + "\x00" + signature
}

// routineInUse reports whether something the plan knows of may still use
// routine when a step before the routine step drops a relation: a view or
// materialized view the declaration holds, removes or rebuilds whose body
// names it, another removed routine whose body names it, a captured object of
// a removed table that reads it, or a definition of a removed table or a
// dropped column that names it, such as a default, a CHECK, a generated
// column or an index expression. A CHECK the plan removes from a table it
// keeps carries no definition to read, so it counts as one that names it.
func routineInUse(diff *difftypes.SchemaDiff, routine difftypes.RoutineChange, rebuild viewRebuild) bool {
	name := unqualified(routine.Name)
	if removesCheckOfKeptTable(diff) {
		return true
	}
	bodies := tableDefinitions(diff)
	for _, view := range diff.DeclaredViewLikes.Views {
		bodies = append(bodies, view.Body)
	}
	for _, view := range diff.DeclaredViewLikes.MaterializedViews {
		bodies = append(bodies, view.Body)
	}
	for _, object := range append(removedViewLikes(diff), rebuild.dropped()...) {
		bodies = append(bodies, object.Body)
	}
	for _, routines := range []difftypes.FunctionChanges{diff.FunctionsRemoved, diff.ProceduresRemoved} {
		for _, other := range routines {
			if routineKey(other) != routineKey(routine) {
				bodies = append(bodies, other.Body)
			}
		}
	}
	if slices.ContainsFunc(bodies, func(body string) bool { return deporder.ReferencesIdentifier(body, name) }) {
		return true
	}
	return slices.ContainsFunc(diff.TablesRemoved, func(removal difftypes.TableRemoval) bool {
		return capturedReads(removal.Current.OwnedObjects, name, defaultSchema(diff))
	})
}

// tableDefinitions is the text of every definition the plan drops with a
// removed table or a dropped column that may call a routine: a column's
// default and generation expression, a CHECK clause and an index definition.
func tableDefinitions(diff *difftypes.SchemaDiff) []string {
	var definitions []string
	for _, removal := range diff.TablesRemoved {
		for _, column := range removal.Current.Table.Columns {
			definitions = append(definitions, derefText(column.ColumnDefault), derefText(column.GeneratedExpression))
		}
		for _, constraint := range removal.Current.Constraints {
			definitions = append(definitions, derefText(constraint.CheckClause))
		}
		for _, index := range removal.Current.Indexes {
			definitions = append(definitions, index.Definition)
		}
	}
	for _, table := range diff.TablesModified {
		for _, column := range table.ColumnsRemoved {
			definitions = append(definitions, column.Default, column.DefaultExpr, column.GeneratedExpression, column.Check)
		}
	}
	return definitions
}

// removesCheckOfKeptTable reports whether the plan removes a CHECK from a
// table it keeps. The removal names the constraint and not its clause, which
// may call a routine.
func removesCheckOfKeptTable(diff *difftypes.SchemaDiff) bool {
	removed := make(map[string]bool, len(diff.TablesRemoved))
	for _, removal := range diff.TablesRemoved {
		removed[bareName(removal.Name)] = true
	}
	return slices.ContainsFunc(diff.ConstraintsRemoved, func(constraint difftypes.ConstraintRemovalInfo) bool {
		return strings.EqualFold(strings.TrimSpace(constraint.Type), "CHECK") && !removed[bareName(constraint.TableName)]
	})
}

// derefText is the text text points at, empty for nil.
func derefText(text *string) string {
	if text == nil {
		return ""
	}
	return *text
}

// removedRoutineReadsColumn reports whether a routine the plan removes reads
// column of table: its SQL-standard body names the table and the column, or
// selects `*`, which the server expands into every column when it creates the
// routine. A row type in the parameters or the result does not hold a column
// back.
func removedRoutineReadsColumn(diff *difftypes.SchemaDiff, table, column string) bool {
	return removedRoutineMatches(diff, func(routine difftypes.RoutineChange) bool {
		return sqlStandardBodyReads(routine.Body, table, defaultSchema(diff)) &&
			(strings.Contains(routine.Body, "*") || deporder.ReferencesIdentifier(routine.Body, column))
	})
}

func removedRoutineMatches(diff *difftypes.SchemaDiff, match func(difftypes.RoutineChange) bool) bool {
	for _, routines := range []difftypes.FunctionChanges{diff.FunctionsRemoved, diff.ProceduresRemoved} {
		if slices.ContainsFunc(routines, match) {
			return true
		}
	}
	return false
}

// sqlStandardBodyReads reports whether body is a SQL-standard routine body,
// `BEGIN ATOMIC ... END` or `RETURN expression`, that names relation, an
// unqualified name resolving in schema. The server parses such a body when the
// routine is created and records what it reads; a body in quotes is a string it
// records nothing for.
func sqlStandardBodyReads(body, relation, schema string) bool {
	head := strings.ToUpper(strings.Join(strings.Fields(body), " "))
	if !strings.HasPrefix(head, "BEGIN ATOMIC") && !strings.HasPrefix(head, "RETURN ") {
		return false
	}
	return deporder.ReferencesRelation(body, relation, schema)
}

// removedTableReads reports whether a captured object of a table the plan
// removes, other than the tables skip names, reads relation.
func removedTableReads(diff *difftypes.SchemaDiff, relation string, skip map[string]bool) bool {
	for _, removal := range diff.TablesRemoved {
		if skip[removal.Name] || removal.Name == relation {
			continue
		}
		if capturedReads(removal.Current.OwnedObjects, relation, defaultSchema(diff)) {
			return true
		}
	}
	return false
}

// capturedReads reports whether one of objects reads relation, an unqualified
// name resolving in schema.
func capturedReads(objects schemaext.Objects, relation, schema string) bool {
	all, err := objects.All()
	if err != nil {
		// An unreadable capture cannot say it reads nothing, so the drop keeps
		// CASCADE.
		return true
	}
	for _, object := range all {
		if reader, ok := object.Value.(schemaext.RelationReader); ok && schemaext.ReadsRelationIn(reader, relation, schema) {
			return true
		}
	}
	return false
}

// removalOutlivesDrop reports whether an object the plan removes in a later
// step depends on relation when the relation is dropped: a removed routine
// that names it, or a captured object of a removed table that reads it.
func removalOutlivesDrop(diff *difftypes.SchemaDiff, relation string, ahead routinesAhead) bool {
	return removedRoutineNames(diff, relation, ahead) || removedTableReads(diff, relation, nil)
}

// removedFeatureReads reports whether a database-scope feature change may drop
// an object that reads relation, which the removal window does only after the
// tables and columns. The object the change removes answers for itself
// through [schemaext.RelationReader]: a composite type names a table's row
// type in an attribute, and an extension reads none. A change whose owner does
// not say whether it removes state, or whose object cannot answer, counts as
// one that reads relation.
func removedFeatureReads(diff *difftypes.SchemaDiff, relation string) bool {
	return removedFeatureMatches(diff, func(object schemaext.Value) bool {
		reader, ok := object.(schemaext.RelationReader)
		return !ok || schemaext.ReadsRelationIn(reader, relation, defaultSchema(diff))
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
		return !ok || schemaext.ReadsRelationIn(reader, table, defaultSchema(diff))
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
