// Package managedrows decides which columns of a declared row set are read
// back from the database.
//
// Both stages that reconcile declared rows ask the same question before they
// read: which of the columns this declaration names does the live table
// actually have. The plan `ptah schema apply` prepares asks it against a table
// the same plan may be about to widen; the body `ptah migrations data` renders
// asks it against the table as it stands. One answer, because a column the
// database does not have is a column no engine will return -- PostgreSQL
// answers 42703 and the whole reconciliation stops (stokaro/ptah#3260).
package managedrows

import (
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
)

// Columns returns every column a declaration names: its key columns and the
// columns its rows carry, sorted so two runs over one declaration read the same
// projection.
func Columns(rows []map[string]any, keys []string) []string {
	named := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		named[key] = struct{}{}
	}
	for _, row := range rows {
		for column := range row {
			named[column] = struct{}{}
		}
	}

	columns := make([]string, 0, len(named))
	for column := range named {
		columns = append(columns, column)
	}
	slices.Sort(columns)
	return columns
}

// ProjectOntoLive drops the columns the live table does not carry, so the read
// asks only for what the database can return. A nil live table is a caller with
// no introspected schema, and the full projection goes through.
//
// A column is matched under names, the connection's rules, and keeps the
// spelling the declaration gave it. Oracle folds the bare name the renderer
// wrote, so a declared `code` is CODE in its catalog: matched exactly, every
// column is dropped and there is nothing left to read.
//
// The dropped column is still compared: the diff reads every column the
// declaration names, and one missing from the live row reads as absent, which
// is what a column the table has not gained yet is. So a declaration that adds
// a column reports the rows that will need its value beside the structural
// finding that reports the column, on every dialect. Reading the column instead
// is what one dialect does with a quoted name it cannot resolve: SQLite returns
// the name as a string literal, and every row then differs from a value nothing
// in the database holds.
func ProjectOntoLive(columns []string, liveTable *catalog.Table, names identifier.Semantics) []string {
	if liveTable == nil {
		return columns
	}
	live := make(map[string]struct{}, len(liveTable.Columns))
	for _, column := range liveTable.Columns {
		live[names.ColumnIdentityKey(column.Name)] = struct{}{}
	}

	projected := make([]string, 0, len(columns))
	for _, column := range columns {
		if _, ok := live[names.ColumnIdentityKey(column)]; ok {
			projected = append(projected, column)
		}
	}
	return projected
}

// LiveTable finds the declaration's table in an introspected catalog.
//
// A declaration that names no schema matches the table of that name whatever
// schema the reader put it in: the reader blanks the connection's own schema,
// and a declaration written without one means "wherever this connection looks".
//
// Names are compared under names, the connection's rules. Oracle folds the bare
// name the renderer wrote, so a declared ora_flags is ORA_FLAGS in its catalog;
// compared exactly, the table is never found, its rows are never read, and
// every apply plans the same INSERTs into a table that already holds them.
func LiveTable(current *catalog.Database, schema, table string, names identifier.Semantics) *catalog.Table {
	if current == nil {
		return nil
	}
	for index := range current.Tables {
		candidate := &current.Tables[index]
		if names.TableIdentityKey(candidate.Name) != names.TableIdentityKey(table) {
			continue
		}
		if schema != "" && candidate.Schema != "" &&
			names.TableIdentityKey(candidate.Schema) != names.TableIdentityKey(schema) {
			continue
		}
		return candidate
	}
	return nil
}
