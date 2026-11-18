//go:build integration

package integration_test

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// sqliteColumnCollationRow is one column: what the database holds before the
// apply, what the schema file declares, and the collation the column compares
// with after the apply, as sqliteComparedCollation reads it.
type sqliteColumnCollationRow struct {
	name     string
	before   string
	declared string
	want     string
}

// sqliteColumnCollationRows covers a collation added, dropped, changed, kept
// in another case, BINARY restated, and a quoted name. Each name is a plain
// column name, since sqliteComparedCollation writes it bare.
var sqliteColumnCollationRows = []sqliteColumnCollationRow{
	{name: "added", before: "TEXT", declared: "TEXT COLLATE NOCASE", want: "NOCASE"},
	{name: "dropped", before: "TEXT COLLATE NOCASE", declared: "TEXT", want: "BINARY"},
	{name: "changed", before: "TEXT COLLATE NOCASE", declared: "TEXT COLLATE RTRIM", want: "RTRIM"},
	{name: "recased", before: "TEXT COLLATE nocase", declared: "TEXT COLLATE NOCASE", want: "NOCASE"},
	{name: "restated", before: "TEXT", declared: "TEXT COLLATE BINARY", want: "BINARY"},
	{name: "quoted", before: "TEXT", declared: `TEXT COLLATE "NoCase"`, want: "NOCASE"},
}

// sqliteColumnCollationTable is CREATE TABLE table with one column per row,
// each defined by define.
func sqliteColumnCollationTable(table string, define func(sqliteColumnCollationRow) string) string {
	columns := []string{"id INTEGER PRIMARY KEY"}
	for _, row := range sqliteColumnCollationRows {
		columns = append(columns, row.name+" "+define(row))
	}
	return fmt.Sprintf("CREATE TABLE %s (\n  %s\n);\n", table, strings.Join(columns, ",\n  "))
}

// sqliteComparedCollation reads which built-in collation each column of the
// row with id 1 compares with, from what SQLite answers: every column holds
// 'Ab ', which equals 'ab ' under NOCASE alone and 'Ab' under RTRIM alone.
func sqliteComparedCollation(c *qt.C, db *sql.DB, table string) map[string]string {
	c.Helper()
	collations := make(map[string]string, len(sqliteColumnCollationRows))
	for _, row := range sqliteColumnCollationRows {
		var folds, trims bool
		err := db.QueryRowContext(c.Context(), fmt.Sprintf(
			"SELECT %[1]s = 'ab ', %[1]s = 'Ab' FROM %[2]s WHERE id = 1", row.name, table)).Scan(&folds, &trims)
		c.Assert(err, qt.IsNil)
		collations[row.name] = map[[2]bool]string{
			{false, false}: "BINARY", {true, false}: "NOCASE", {false, true}: "RTRIM",
		}[[2]bool{folds, trims}]
	}
	return collations
}

// sqliteCollatedRow inserts the row sqliteComparedCollation reads into table.
func sqliteCollatedRow(table string) string {
	values := []string{"1"}
	for range sqliteColumnCollationRows {
		values = append(values, "'Ab '")
	}
	return fmt.Sprintf("INSERT INTO %s VALUES (%s);\n", table, strings.Join(values, ", "))
}

// TestSchemaApplyWritesASQLiteColumnCollationE2E applies a schema file whose
// columns name collations, creating one table and rebuilding another, and asks
// SQLite which collation each column compares with. The rebuild keeps the row
// it copies, and the next plan is empty.
func TestSchemaApplyWritesASQLiteColumnCollationE2E(t *testing.T) {
	c := qt.New(t)
	db, url := sqliteCollationDatabase(c, c.TempDir(),
		sqliteColumnCollationTable("altered", func(row sqliteColumnCollationRow) string { return row.before })+
			sqliteCollatedRow("altered"))
	declared := func(row sqliteColumnCollationRow) string { return row.declared }
	file := writeDeclaredSchemaFile(c, "schema.sql",
		sqliteColumnCollationTable("altered", declared)+sqliteColumnCollationTable("created", declared))
	want := make(map[string]string, len(sqliteColumnCollationRows))
	for _, row := range sqliteColumnCollationRows {
		want[row.name] = row.want
	}
	args := []string{"schema", "apply", "--db-url", url, "--schema-file", file}

	applied := runPtahIn(c, append(args, "--auto-approve")...)

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	_, err := db.ExecContext(c.Context(), sqliteCollatedRow("created"))
	c.Assert(err, qt.IsNil)
	c.Assert(sqliteComparedCollation(c, db, "created"), qt.DeepEquals, want)
	c.Assert(sqliteComparedCollation(c, db, "altered"), qt.DeepEquals, want)

	settled := runPtahIn(c, append(args, "--dry-run")...)

	c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
	c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
}

// TestSchemaApplyFindsASQLiteCollationInAnyCaseE2E plans nothing for a file
// that differs from the database only in how it spells a collation: another
// case, a quoted name, and BINARY, the collation a column without one compares
// with. SQLite finds each one as the same collation, and a rebuild would copy
// every row of the table for no change.
func TestSchemaApplyFindsASQLiteCollationInAnyCaseE2E(t *testing.T) {
	c := qt.New(t)
	_, url := sqliteCollationDatabase(c, c.TempDir(),
		"CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT COLLATE nocase, b TEXT COLLATE RTRIM, c TEXT);\n")
	file := writeDeclaredSchemaFile(c, "schema.sql",
		`CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT COLLATE NOCASE, b TEXT COLLATE "rtrim", c TEXT COLLATE Binary);`+"\n")

	planned := runPtahIn(c, "schema", "apply", "--db-url", url, "--schema-file", file, "--dry-run")

	c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
	c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
}
