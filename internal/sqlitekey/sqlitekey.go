// Package sqlitekey answers whether SQLite enforces NOT NULL on a primary key
// column. Every other engine Ptah renders for decides that from the dialect
// alone; SQLite decides it from the table's shape.
//
// Measured with `pragma table_info` on SQLite 3.51.0, and confirmed against the
// pinned Atlas community v1.3.0 binary, which reports the same nullability for
// each shape when it reads the same DDL through a dev database:
//
//	CREATE TABLE t (id TEXT PRIMARY KEY, ...)                    notnull=0
//	CREATE TABLE t (id INTEGER PRIMARY KEY, ...)                 notnull=0
//	CREATE TABLE t (a TEXT, b TEXT, PRIMARY KEY (a, b))          notnull=0, 0
//	CREATE TABLE t (id TEXT PRIMARY KEY, ...) WITHOUT ROWID      notnull=1
//	CREATE TABLE t (id INTEGER PRIMARY KEY, ...) WITHOUT ROWID   notnull=1
//	CREATE TABLE t (id TEXT PRIMARY KEY, ...) STRICT             notnull=1
//	CREATE TABLE t (id INT PRIMARY KEY, ...) STRICT              notnull=1
//	CREATE TABLE t (a TEXT, b TEXT, PRIMARY KEY (a, b)) STRICT   notnull=1, 1
//	CREATE TABLE t (id INTEGER PRIMARY KEY, ...) STRICT          notnull=0
//
// The rule behind those rows: a primary key column is NOT NULL unless it is the
// rowid alias, and the historical leniency that lets a key column hold NULL at
// all survives only on an ordinary rowid table that is not STRICT. The last row
// is not an exception to that rule but an instance of it -- a STRICT table still
// has a rowid, and `id INTEGER PRIMARY KEY` is still its alias, so an explicit
// `INSERT INTO t (id) VALUES (NULL)` is accepted there and assigns a rowid.
//
// Treating every SQLite table as nullable-key (stokaro/ptah#1235) makes a STRICT
// or WITHOUT ROWID table drift forever against the DDL that created it: the
// catalog answers NOT NULL, the desired model answers nullable, and every plan
// is another full table rebuild. Treating every SQLite table as NOT NULL-key is
// the defect #1235 closed. Both halves need the table.
//
// One shape SQLite distinguishes is deliberately not modeled here: a DESC key
// ordering defeats the rowid alias, and this package does not ask, because
// Ptah's SQLite renderer drops DESC from a PRIMARY KEY. See [isRowidAlias].
package sqlitekey

import (
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
)

// ImpliesNotNull reports whether SQLite makes field NOT NULL because table's
// primary key covers it.
//
// keyColumns is every column that key covers, which [KeyColumns] derives. It is
// a parameter rather than something recomputed here so a caller that already
// knows the key -- because it just resolved embedded fields, say -- does not
// have to spell the table's key twice.
func ImpliesNotNull(table goschema.Table, keyColumns []string, field goschema.Field) bool {
	if !coversColumn(keyColumns, field.Name) {
		return false
	}
	if table.WithoutRowID {
		return true
	}
	if !table.Strict {
		return false
	}
	return !isRowidAlias(keyColumns, field)
}

// KeyColumns returns every column table's primary key covers, reading the
// table-level key where the table declares one and the columns that declare
// `primary` themselves otherwise. It returns nil for a table with no key.
func KeyColumns(table goschema.Table, fields []goschema.Field) []string {
	if columns := tableLevelKeyColumns(table); len(columns) > 0 {
		return columns
	}
	var columns []string
	for _, field := range fields {
		if field.Primary {
			columns = append(columns, field.Name)
		}
	}
	return columns
}

func tableLevelKeyColumns(table goschema.Table) []string {
	var columns []string
	for _, part := range table.PrimaryKeyParts {
		if name := strings.TrimSpace(part.Name); name != "" {
			columns = append(columns, name)
		}
	}
	if len(columns) > 0 {
		return columns
	}
	for _, name := range table.PrimaryKey {
		if trimmed := strings.TrimSpace(name); trimmed != "" {
			columns = append(columns, trimmed)
		}
	}
	return columns
}

// isRowidAlias reports whether field is the table's rowid under another name,
// which is the one key column SQLite still lets hold NULL in a STRICT table. The
// alias exists only for a single-column key of declared type INTEGER on a table
// that has a rowid.
//
// A DESC key ordering defeats the alias in SQLite -- measured, a STRICT
// `id INTEGER PRIMARY KEY DESC` reports notnull=1 where the same key without
// DESC reports 0 -- and this function deliberately does not ask, because Ptah's
// SQLite renderer does not write DESC into a PRIMARY KEY at all. Asking would
// describe a table Ptah never builds: measured, `PRIMARY KEY (id DESC)` in a
// STRICT source is applied as `PRIMARY KEY ("id")`, whose catalog answer is 0,
// so a model that answered NOT NULL for it would plan a rebuild on every run and
// never converge. The DESC that goes missing is its own defect, in the renderer,
// and it has to be fixed there rather than modeled around here.
func isRowidAlias(keyColumns []string, field goschema.Field) bool {
	if len(keyColumns) != 1 {
		return false
	}
	return rendersAsSQLiteInteger(field.Type)
}

// rendersAsSQLiteInteger reports whether the SQLite renderer writes rawType as
// exactly `INTEGER`, which is what makes a single-column key the rowid alias.
//
// The rest of the integer family is not a rowid alias even though SQLite gives
// it INTEGER affinity, because the alias is keyed on the declared type name:
// measured, `CREATE TABLE t (id INT PRIMARY KEY, a TEXT) STRICT` reports
// notnull=1 where the same table spelled INTEGER reports 0.
//
// The list mirrors mapColumnType in core/renderer/internal/dialects/sqlite.
// sqlitekey_internal_test.go pins the correspondence by rendering each type
// through the public renderer, so a change there reddens here.
func rendersAsSQLiteInteger(rawType string) bool {
	switch baseTypeName(rawType) {
	case "INTEGER", "BOOLEAN", "BOOL", "SERIAL", "BIGSERIAL", "SMALLSERIAL", "AUTO_INCREMENT":
		return true
	}
	return false
}

func baseTypeName(rawType string) string {
	base := strings.ToUpper(strings.TrimSpace(rawType))
	if idx := strings.Index(base, "("); idx >= 0 {
		base = strings.TrimSpace(base[:idx])
	}
	return base
}

// coversColumn compares column names the way SQLite does, case-insensitively.
func coversColumn(keyColumns []string, column string) bool {
	return slices.ContainsFunc(keyColumns, func(name string) bool {
		return strings.EqualFold(strings.TrimSpace(name), strings.TrimSpace(column))
	})
}
