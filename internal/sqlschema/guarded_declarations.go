package sqlschema

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/routineargs"
)

// A CREATE statement with a guard may name an object that already exists, and
// the guard says what the server does then. IF NOT EXISTS skips the whole
// statement, so the first declaration stands with nothing of the second: not
// its columns, keys or indexes. OR REPLACE replaces the earlier definition with
// its own. Measured on PostgreSQL 15.19 for a table, a sequence, a view, a
// function and a trigger. The reader follows the server, so a database built
// from a file and the file itself compare equal.
//
// The earlier declaration is looked for in the statements of this read and, for
// IF NOT EXISTS, in the earlier files of a schema directory as well. A later
// file's OR REPLACE of an object an earlier file declared is left to the
// directory's merge.

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
func appendFunction(database *schemamodel.Database, node *ast.CreateFunctionNode, sourcePlatform string) {
	function := toFunction(node, sourcePlatform)
	if !node.Replace || !replaceFunction(database, function, sourcePlatform) {
		database.Functions = append(database.Functions, function)
	}
}

// appendTrigger records the trigger node declares, in the place of the one it
// replaces.
func appendTrigger(database *schemamodel.Database, node *ast.CreateTriggerNode, sourcePlatform string) {
	trigger := toTrigger(node, sourcePlatform)
	if !node.Replace || !replaceTrigger(database, trigger, sourcePlatform) {
		database.Triggers = append(database.Triggers, trigger)
	}
}

// skipsCreateTable reports whether the server skips node: a CREATE TABLE IF NOT
// EXISTS whose name reaches a table already declared.
func skipsCreateTable(database *schemamodel.Database, document *Document, node *ast.CreateTableNode, sourcePlatform string) bool {
	if !node.IfNotExists {
		return false
	}
	declared := []*schemamodel.Database{database}
	if document.base != nil {
		declared = append(declared, document.base)
	}
	return resolveTable(declared, ToTable(node, sourcePlatform).QualifiedName(), sourcePlatform) != nil
}

// skipsCreateSequence reports whether the server skips a CREATE SEQUENCE IF NOT
// EXISTS of sequence: one declared under the same name in the same schema.
func skipsCreateSequence(database *schemamodel.Database, document *Document, node *ast.CreateSequenceNode, sequence schemamodel.Sequence, sourcePlatform string) bool {
	if !node.IfNotExists {
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

// replaceView puts view in the place of the view of its name this read
// declared, for a CREATE OR REPLACE VIEW, and reports whether there was one.
func replaceView(database *schemamodel.Database, view schemamodel.View, sourcePlatform string) bool {
	for i := range database.Views {
		if unqualified(database.Views[i].Name, sourcePlatform) == unqualified(view.Name, sourcePlatform) {
			database.Views[i] = view
			return true
		}
	}
	return false
}

// replaceFunction puts function in the place of the routine it replaces, for a
// CREATE OR REPLACE FUNCTION or PROCEDURE, and reports whether there was one.
// A routine is found by its kind, its name and its argument types, as the
// server finds it: an overload with other arguments is another routine.
func replaceFunction(database *schemamodel.Database, function schemamodel.Function, sourcePlatform string) bool {
	for i := range database.Functions {
		declared := database.Functions[i]
		if declared.IsProcedure() == function.IsProcedure() &&
			unqualified(declared.Name, sourcePlatform) == unqualified(function.Name, sourcePlatform) &&
			routineargs.InputTypes(declared.Parameters) == routineargs.InputTypes(function.Parameters) {
			database.Functions[i] = function
			return true
		}
	}
	return false
}

// replaceTrigger puts trigger in the place of the trigger of its name on its
// table, for a CREATE OR REPLACE TRIGGER, and reports whether there was one.
func replaceTrigger(database *schemamodel.Database, trigger schemamodel.Trigger, sourcePlatform string) bool {
	for i := range database.Triggers {
		declared := database.Triggers[i]
		if declared.Name == trigger.Name &&
			unqualified(declared.Table, sourcePlatform) == unqualified(trigger.Table, sourcePlatform) {
			database.Triggers[i] = trigger
			return true
		}
	}
	return false
}
