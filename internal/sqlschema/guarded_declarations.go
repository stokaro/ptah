package sqlschema

import (
	"cmp"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/routineargs"
)

// A CREATE statement with a guard may name an object that already exists, and
// the guard says what the server does then. IF NOT EXISTS skips the whole
// statement, so the first declaration stands with nothing of the second: not
// its columns, keys or indexes. OR REPLACE replaces the earlier definition with
// its own. Measured on PostgreSQL 15.19 for a table, a sequence, an index, a
// materialized view, a view, a function, a procedure and a trigger.
//
// Whether a name is in use is the server's question, so on a dialect whose
// namespaces the stored-name pass follows, the PostgreSQL family, the
// statements it found skipped are the ones the conversion skips
// ([Document.skips]): a view or a sequence takes a table's name, a serial
// column takes its sequence's, and an unnamed key takes its index's. Elsewhere
// the conversion looks for a table or a sequence it already declared.
//
// The earlier declaration is looked for in the statements of this read and in
// the earlier files of the document, the files of a schema directory or the
// other sources of one load, and an OR REPLACE replaces it where it is. What
// the server keeps of an object it replaces, its comment, stays when the new
// statement sets none.

// appendGuardedCreateTable converts node unless the server skips it.
func appendGuardedCreateTable(database *schemamodel.Database, document *Document, node *ast.CreateTableNode, sourcePlatform string) error {
	if skipsCreateTable(database, document, node, sourcePlatform) {
		return nil
	}
	return appendCreateTable(database, document, node, sourcePlatform)
}

// appendSequence records the sequence node declares unless the server skips
// the statement.
func appendSequence(database *schemamodel.Database, document *Document, node *ast.CreateSequenceNode, sourcePlatform string) {
	sequence := toSequence(node, sourcePlatform)
	if !skipsCreateSequence(database, document, node, sequence, sourcePlatform) {
		database.Sequences = append(database.Sequences, sequence)
	}
}

// appendFunction records the routine node declares, in the place of the one
// it replaces.
func appendFunction(database *schemamodel.Database, document *Document, node *ast.CreateFunctionNode, sourcePlatform string) {
	function := toFunction(node, sourcePlatform)
	if !node.Replace || !replaceFunction(declaredIn(database, document), function, sourcePlatform) {
		database.Functions = append(database.Functions, function)
	}
}

// appendTrigger records the trigger node declares, in the place of the one it
// replaces.
func appendTrigger(database *schemamodel.Database, document *Document, node *ast.CreateTriggerNode, sourcePlatform string) {
	trigger := toTrigger(node, sourcePlatform)
	if !node.Replace || !replaceTrigger(declaredIn(database, document), trigger, sourcePlatform) {
		database.Triggers = append(database.Triggers, trigger)
	}
}

// appendCreateView records the view node declares, in the place of the one it
// replaces.
func appendCreateView(database *schemamodel.Database, document *Document, node *ast.CreateViewNode, sourcePlatform string) {
	view := toView(node, sourcePlatform)
	if !node.Replace || !replaceView(declaredIn(database, document), view, sourcePlatform) {
		database.Views = append(database.Views, view)
	}
}

// declaredIn is the models an earlier declaration may sit in: this read's,
// then the document's earlier files'.
func declaredIn(database *schemamodel.Database, document *Document) []*schemamodel.Database {
	if document == nil || document.base == nil {
		return []*schemamodel.Database{database}
	}
	return []*schemamodel.Database{database, document.base}
}

// skips reports whether the server skips stmt, as the stored-name pass found.
func (d *Document) skips(stmt ast.Node) bool {
	return d != nil && d.names != nil && d.names.skipped[stmt]
}

// followsNames reports whether the stored-name pass decides what the server
// skips for this document.
func (d *Document) followsNames(sourcePlatform string) bool {
	return d != nil && d.names != nil && d.names.sourcePlatform == sourcePlatform
}

// skipsCreateTable reports whether the server skips node: a CREATE TABLE IF NOT
// EXISTS whose name reaches a table already declared, on a dialect the
// stored-name pass does not follow.
func skipsCreateTable(database *schemamodel.Database, document *Document, node *ast.CreateTableNode, sourcePlatform string) bool {
	if !node.IfNotExists || document.followsNames(sourcePlatform) {
		return false
	}
	declared := []*schemamodel.Database{database}
	if document.base != nil {
		declared = append(declared, document.base)
	}
	return resolveTable(declared, ToTable(node, sourcePlatform).QualifiedName(), sourcePlatform) != nil
}

// skipsCreateSequence reports whether the server skips a CREATE SEQUENCE IF NOT
// EXISTS of sequence: one declared under the same name in the same schema, on
// a dialect the stored-name pass does not follow.
func skipsCreateSequence(database *schemamodel.Database, document *Document, node *ast.CreateSequenceNode, sequence schemamodel.Sequence, sourcePlatform string) bool {
	if !node.IfNotExists || document.followsNames(sourcePlatform) {
		return false
	}
	key := sequenceKey(sequence, sourcePlatform)
	for _, source := range []*schemamodel.Database{database, document.base} {
		if source == nil {
			continue
		}
		for _, declared := range source.Sequences {
			if sequenceKey(declared, sourcePlatform) == key {
				return true
			}
		}
	}
	return false
}

// sequenceKey is the schema and the name a sequence is found by, with the
// dialect's default schema for one written without a schema.
func sequenceKey(sequence schemamodel.Sequence, sourcePlatform string) [2]string {
	schema := sequence.Schema
	if schema == "" {
		schema = factsOf(sourcePlatform).defaultSchema
	}
	return [2]string{schema, sequence.Name}
}

// unqualified drops the dialect's default schema from a qualified name, so
// `public.v` and `v` name one object on PostgreSQL.
func unqualified(name, sourcePlatform string) string {
	if schema := factsOf(sourcePlatform).defaultSchema; schema != "" {
		return strings.TrimPrefix(name, schema+".")
	}
	return name
}

// replaceView puts view in the place of the view of its name an earlier
// statement declared, for a CREATE OR REPLACE VIEW, and reports whether there
// was one.
func replaceView(declared []*schemamodel.Database, view schemamodel.View, sourcePlatform string) bool {
	for _, database := range declared {
		for i := range database.Views {
			if unqualified(database.Views[i].Name, sourcePlatform) == unqualified(view.Name, sourcePlatform) {
				view.Comment = cmp.Or(view.Comment, database.Views[i].Comment)
				database.Views[i] = view
				return true
			}
		}
	}
	return false
}

// replaceFunction puts function in the place of the routine it replaces, for a
// CREATE OR REPLACE FUNCTION or PROCEDURE, or SQL Server's CREATE OR ALTER, and
// reports whether there was one. A routine is found by its kind and its name,
// as the server finds it, and on a server that overloads routines by its
// argument types too: an overload with other arguments is another routine
// there. The comment of the routine replaced stays where the server keeps it.
func replaceFunction(declared []*schemamodel.Database, function schemamodel.Function, sourcePlatform string) bool {
	overloads := routinesOverload(sourcePlatform)
	for _, database := range declared {
		for i := range database.Functions {
			existing := database.Functions[i]
			if existing.IsProcedure() == function.IsProcedure() &&
				unqualified(existing.Name, sourcePlatform) == unqualified(function.Name, sourcePlatform) &&
				(!overloads || routineargs.InputTypes(existing.Parameters) == routineargs.InputTypes(function.Parameters)) {
				if keepsRoutineComment(sourcePlatform) {
					function.Comment = cmp.Or(function.Comment, existing.Comment)
				}
				database.Functions[i] = function
				return true
			}
		}
	}
	return false
}

// routinesOverload reports whether the dialect's server tells two routines of
// one name apart by their argument types. The PostgreSQL family does, and its
// routines are written with a quoted body. MySQL 8.4.11, MariaDB 11.8.9 and
// SQL Server 2022 hold one routine per name, measured where the renderer
// refuses two overloads for them; MariaDB 11.8.9 and 12.3.3 take `CREATE OR
// REPLACE PROCEDURE q(a int)` in the place of `q()`.
func routinesOverload(sourcePlatform string) bool {
	return factsOf(sourcePlatform).routines == descriptor.RoutineQuotedBody
}

// keepsRoutineComment reports whether the dialect's server keeps a routine's
// comment through CREATE OR REPLACE. PostgreSQL replaces the routine in place,
// and its comment stays. The MySQL family's routines are written as compound
// statements, and MariaDB 11.8.9 and 12.3.3 drop the comment: `CREATE OR
// REPLACE FUNCTION f()` after `CREATE FUNCTION f() ... COMMENT 'old'` leaves
// an empty ROUTINE_COMMENT.
func keepsRoutineComment(sourcePlatform string) bool {
	return factsOf(sourcePlatform).routines != descriptor.RoutineCompoundStatement
}

// replaceTrigger puts trigger in the place of the trigger of its name on its
// table, for a CREATE OR REPLACE TRIGGER, and reports whether there was one.
func replaceTrigger(declared []*schemamodel.Database, trigger schemamodel.Trigger, sourcePlatform string) bool {
	for _, database := range declared {
		for i := range database.Triggers {
			existing := database.Triggers[i]
			if existing.Name == trigger.Name &&
				unqualified(existing.Table, sourcePlatform) == unqualified(trigger.Table, sourcePlatform) {
				trigger.Comment = cmp.Or(trigger.Comment, existing.Comment)
				database.Triggers[i] = trigger
				return true
			}
		}
	}
	return false
}
