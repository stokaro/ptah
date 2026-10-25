package postgres

import (
	"slices"
	"strings"

	"ptah.run/core/difftypes"
	"ptah.run/core/schemaext"
	"ptah.run/internal/deporder"
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
// type, or its SQL-standard body reads it.
func removedRoutineNames(diff *difftypes.SchemaDiff, relation string) bool {
	return removedRoutineMatches(diff, func(routine difftypes.RoutineChange) bool {
		return deporder.ReferencesIdentifier(routine.Parameters, unqualified(relation)) ||
			deporder.ReferencesIdentifier(routine.Returns, unqualified(relation)) ||
			sqlStandardBodyReads(routine.Body, relation)
	})
}

// removedRoutineReadsColumns reports whether a routine the plan removes reads
// the columns of table: its SQL-standard body names the table. A row type in
// the parameters or the result does not hold a column back.
func removedRoutineReadsColumns(diff *difftypes.SchemaDiff, table string) bool {
	return removedRoutineMatches(diff, func(routine difftypes.RoutineChange) bool {
		return sqlStandardBodyReads(routine.Body, table)
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
// `BEGIN ATOMIC ... END` or `RETURN expression`, that names relation. The
// server parses such a body when the routine is created and records what it
// reads; a body in quotes is a string it records nothing for.
func sqlStandardBodyReads(body, relation string) bool {
	head := strings.ToUpper(strings.Join(strings.Fields(body), " "))
	if !strings.HasPrefix(head, "BEGIN ATOMIC") && !strings.HasPrefix(head, "RETURN ") {
		return false
	}
	return deporder.ReferencesIdentifier(body, unqualified(relation))
}

// removedTableReads reports whether a captured object of a table the plan
// removes, other than the tables skip names, reads relation.
func removedTableReads(diff *difftypes.SchemaDiff, relation string, skip map[string]bool) bool {
	for _, removal := range diff.TablesRemoved {
		if skip[removal.Name] || removal.Name == relation {
			continue
		}
		if capturedReads(removal.Current.OwnedObjects, relation) {
			return true
		}
	}
	return false
}

// capturedReads reports whether one of objects reads relation.
func capturedReads(objects schemaext.Objects, relation string) bool {
	all, err := objects.All()
	if err != nil {
		// An unreadable capture cannot say it reads nothing, so the drop keeps
		// CASCADE.
		return true
	}
	for _, object := range all {
		if reader, ok := object.Value.(schemaext.RelationReader); ok && reader.ReadsRelation(relation) {
			return true
		}
	}
	return false
}

// removalOutlivesDrop reports whether an object the plan removes in a later
// step depends on relation when the relation is dropped: a removed routine
// that names it, or a captured object of a removed table that reads it.
func removalOutlivesDrop(diff *difftypes.SchemaDiff, relation string) bool {
	return removedRoutineNames(diff, relation) || removedTableReads(diff, relation, nil)
}

// removesFeatureState reports whether a database-scope feature change may drop
// an object, which the removal window does only after the tables and columns
// that object may read. A change whose owner does not say is counted as one.
func removesFeatureState(diff *difftypes.SchemaDiff) bool {
	for _, change := range diff.FeatureChanges {
		if _, says := change.Value.(schemaext.StateRemoval); !says || schemaext.RemovesState(change.Value) {
			return true
		}
	}
	return false
}

// unqualified is relation without its schema, the spelling a routine's
// parameters, result and body use for it most of the time.
func unqualified(relation string) string {
	if ref, ok := tableref.Parse(relation); ok {
		return ref.Name
	}
	return relation
}
