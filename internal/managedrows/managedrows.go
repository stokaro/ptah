// Package managedrows holds the one comparison of a declared row set against
// the live table.
//
// Two command trees reconcile declared rows. `ptah schema plan` and `ptah
// schema apply` reach them through internal/atlasschema; `ptah migrations
// data`, and the row report of `ptah schema drift` and `ptah schema compare`,
// reach them through internal/datamigrate. Each has to decide which live table
// a declaration names, which of that table's columns it may read, and what the
// difference between the declaration and the rows is. Written twice, those
// decisions agreed when the second copy was written and stopped agreeing when
// the first was extended: the plan found an Oracle table the migration body
// reported as missing, and neither end's tests could see it
// (stokaro/ptah#3276, stokaro/ptah#3321).
//
// [Compare] is that decision, made once. What a caller does with the diff stays
// its own: the plan renders statements and rates them, the migration body
// renders a reversible pair and refuses what it cannot write back, the drift
// report counts.
package managedrows

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
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
	live := liveColumnKeys(*liveTable, names)

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
// Names are compared under names, the connection's rules. Oracle folds the bare
// name the renderer wrote, so a declared ora_flags is ORA_FLAGS in its catalog;
// compared exactly, the table is never found, its rows are never read, and
// every apply plans the same INSERTs into a table that already holds them
// (stokaro/ptah#3321).
//
// A declared schema has to match, either the introspected schema or a blank one
// when the declaration names the connection's default schema, because readers
// blank the schema of default-schema tables (PostgreSQL public, SQLite main).
// An omitted declared schema matches a uniquely named table, and otherwise the
// one in the default schema; a bare name two other schemas share resolves to
// neither, which is what the reference-data page promises.
func LiveTable(current *catalog.Database, schema, table string, names identifier.Semantics) *catalog.Table {
	if current == nil {
		return nil
	}
	var candidates []*catalog.Table
	for index := range current.Tables {
		candidate := &current.Tables[index]
		if names.TableIdentityKey(candidate.Name) == names.TableIdentityKey(table) {
			candidates = append(candidates, candidate)
		}
	}

	if declared := strings.TrimSpace(schema); declared != "" {
		for _, candidate := range candidates {
			if inDeclaredSchema(*candidate, declared, names) {
				return candidate
			}
		}
		return nil
	}

	if len(candidates) == 1 {
		return candidates[0]
	}
	for _, candidate := range candidates {
		if inDefaultSchema(*candidate, names) {
			return candidate
		}
	}
	return nil
}

// inDeclaredSchema answers whether candidate sits in the schema a declaration
// named. A blank introspected schema is the default schema the reader left out,
// so it answers the declaration that names that default and no other.
func inDeclaredSchema(candidate catalog.Table, declared string, names identifier.Semantics) bool {
	introspected := strings.TrimSpace(candidate.Schema)
	if introspected == "" {
		return names.TableIdentityKey(declared) == names.TableIdentityKey(names.DefaultSchema)
	}
	return names.TableIdentityKey(introspected) == names.TableIdentityKey(declared)
}

// inDefaultSchema answers whether candidate sits in the connection's default
// schema, blank included for the reader that leaves it out.
func inDefaultSchema(candidate catalog.Table, names identifier.Semantics) bool {
	introspected := strings.TrimSpace(candidate.Schema)
	return introspected == "" ||
		names.TableIdentityKey(introspected) == names.TableIdentityKey(names.DefaultSchema)
}

// liveColumnKeys indexes a live table's columns by their comparison key under
// the connection's rules, which is the one place a column name is matched.
func liveColumnKeys(liveTable catalog.Table, names identifier.Semantics) map[string]struct{} {
	keys := make(map[string]struct{}, len(liveTable.Columns))
	for _, column := range liveTable.Columns {
		keys[names.ColumnIdentityKey(column.Name)] = struct{}{}
	}
	return keys
}

// projectRows returns the declared rows carrying only the given columns, so the
// comparison runs over the same column set on both sides. It copies rather than
// deleting in place because the rows belong to the caller's declaration.
func projectRows(rows []map[string]any, columns []string) []map[string]any {
	keep := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		keep[column] = struct{}{}
	}
	projected := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		narrowed := make(map[string]any, len(row))
		for column, value := range row {
			if _, ok := keep[column]; ok {
				narrowed[column] = value
			}
		}
		projected = append(projected, narrowed)
	}
	return projected
}

// refuseUndeclaredColumns refuses to render a migration for a table that does
// not carry every declared column.
//
// It is the narrowing rule read from the write side, which is why it reads
// narrowed rather than asking the catalog a second question: the columns
// [ProjectOntoLive] dropped are exactly the ones a report leaves out of its
// comparison, and two recognitions of one set is how the two ends came apart.
//
// A count can leave the column out and still answer. A migration body cannot
// write a column the table does not have, and the reader quotes the names it is
// given, so a database that cannot resolve one either refuses the read or
// answers with something invented: SQLite returns the quoted name as a string
// literal, which would put that literal into the rollback as the value the row
// is restored to.
func refuseUndeclaredColumns(qualified string, columns, narrowed []string) error {
	if len(narrowed) == len(columns) {
		return nil
	}
	kept := make(map[string]struct{}, len(narrowed))
	for _, column := range narrowed {
		kept[column] = struct{}{}
	}
	var missing []string
	for _, column := range columns {
		if _, ok := kept[column]; !ok {
			missing = append(missing, column)
		}
	}
	slices.Sort(missing)
	return fmt.Errorf(
		"managed table %q does not have declared column(s) %s; migrate the schema first or remove the column(s) from the row data",
		qualified, quoteAll(missing))
}

// insertableColumns returns the sorted set of columns to read and re-insert for
// an empty-desired full delete: every column the database will accept in an
// explicit INSERT, so the reversible down restores complete rows.
//
// Two column classes are dropped because the database rejects or ignores an
// explicit value for them:
//   - generated/computed columns (GeneratedKind set) are excluded and recompute
//     from the re-inserted base columns on rollback;
//   - identity columns that reject explicit inserts (SQL Server IDENTITY,
//     PostgreSQL and Oracle GENERATED ALWAYS AS IDENTITY — see
//     [rejectsExplicitInsert])
//     cannot be restored to their original value, so rather than emit a migration
//     whose down fails at apply time (or silently re-inserts a different value),
//     the whole empty-desired case is refused, naming the offending column(s).
//
// Auto-increment/serial columns that DO accept explicit inserts (MySQL
// AUTO_INCREMENT, SQLite AUTOINCREMENT, PostgreSQL SERIAL, and PostgreSQL and
// Oracle GENERATED BY DEFAULT AS IDENTITY) are kept, so re-inserting them
// preserves the original identity values. Every key column must survive both
// filters, since a key drives the DELETE predicate and the rollback INSERT; a
// key that is generated, absent, or reject-on-insert is an error.
//
// A key is matched under names, the connection's rules, so a lower-case
// declaration of a table Oracle folded finds its key rather than reporting the
// table has none (stokaro/ptah#3321). The column it matched is then read under
// the spelling the declaration gave it, which is the rule [ProjectOntoLive]
// follows for the columns a row carries: the comparison indexes each live row
// by the key columns the declaration names, so a row read back as CODE holds no
// value under the declared `code` and reads as a row that lost its key.
func insertableColumns(
	dialect string,
	names identifier.Semantics,
	qualified string,
	liveTable catalog.Table,
	keys []string,
) ([]string, error) {
	declared := make(map[string]string, len(keys))
	for _, key := range keys {
		declared[names.ColumnIdentityKey(key)] = key
	}
	present := make(map[string]struct{}, len(liveTable.Columns))
	columns := make([]string, 0, len(liveTable.Columns))
	var rejected []string
	for _, column := range liveTable.Columns {
		if column.GeneratedKind != "" {
			continue
		}
		if rejectsExplicitInsert(dialect, column) {
			rejected = append(rejected, column.Name)
			continue
		}
		identity := names.ColumnIdentityKey(column.Name)
		present[identity] = struct{}{}
		if spelling, isKey := declared[identity]; isKey {
			columns = append(columns, spelling)
			continue
		}
		columns = append(columns, column.Name)
	}

	if len(rejected) > 0 {
		slices.Sort(rejected)
		return nil, fmt.Errorf(
			"cannot generate a reversible full delete for table %q: column(s) %s reject explicit inserts (identity/auto-generated) and cannot be restored on rollback; keep at least one desired row or remove the annotation",
			qualified, quoteAll(rejected))
	}

	for _, key := range keys {
		if _, ok := present[names.ColumnIdentityKey(key)]; !ok {
			return nil, fmt.Errorf(
				"key column %q of managed table %q is not a writable, non-generated column; a reversible full delete needs every key column",
				key, qualified)
		}
	}

	slices.Sort(columns)
	return columns, nil
}

// rejectsExplicitInsert reports whether the database rejects an explicit value
// for column in an INSERT, so re-inserting it on rollback would fail at apply
// time. It is true for SQL Server IDENTITY columns and for PostgreSQL and
// Oracle GENERATED ALWAYS AS IDENTITY columns, and deliberately false for
// auto-increment/serial columns that accept explicit inserts (MySQL
// AUTO_INCREMENT, SQLite AUTOINCREMENT, PostgreSQL SERIAL, and GENERATED BY
// DEFAULT AS IDENTITY on PostgreSQL and Oracle), so those round-trip with their
// original values preserved.
func rejectsExplicitInsert(dialect string, column catalog.Column) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.SQLServer:
		// The SQL Server reader sets IsAutoIncrement only for IDENTITY columns,
		// which reject explicit inserts unless SET IDENTITY_INSERT is toggled on.
		return column.IsAutoIncrement
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
		return strings.EqualFold(column.IdentityGeneration, "ALWAYS")
	case platform.Oracle:
		// IsAutoIncrement is not enough on Oracle: it is set for BY DEFAULT
		// identities too, and those accept an explicit value. GENERATED ALWAYS
		// refuses one with ORA-32795, so the generation mode decides.
		return strings.EqualFold(column.IdentityGeneration, "ALWAYS")
	default:
		// MySQL/MariaDB AUTO_INCREMENT, SQLite AUTOINCREMENT, and ClickHouse accept
		// explicit inserts, so re-inserting preserves the original value.
		return false
	}
}

// quoteAll renders names as a comma-separated list of double-quoted identifiers
// for an error message (for example `"a", "b"`).
func quoteAll(names []string) string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = fmt.Sprintf("%q", name)
	}
	return strings.Join(quoted, ", ")
}
