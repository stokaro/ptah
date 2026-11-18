//go:build integration

package integration_test

import (
	"cmp"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// mysqlColumnCollationRow is one column: its definition before the apply,
// the definition the schema file declares, and the collation the column holds
// after the apply. An empty want is a column whose collation is not compared
// and keeps the one the server gave it before the apply, which MySQL 8.4 and
// MariaDB name differently for a character set's default, or none.
type mysqlColumnCollationRow struct {
	name     string
	before   string
	declared string
	want     string
}

// mysqlColumnCollationRows covers a collation added, changed, restated as the
// table's default, written in another case and quoted, and columns that
// declare none and take a collation other than the table's: from a character
// set of their own, from the table's character set named on the column, and
// from NCHAR. The alias rows name utf8, which both servers store as utf8mb3,
// and the counted row a collation both servers accept on an int and drop.
var mysqlColumnCollationRows = []mysqlColumnCollationRow{
	{name: "added", before: "varchar(20)", declared: "varchar(20) COLLATE utf8mb4_bin", want: "utf8mb4_bin"},
	{name: "changed", before: "varchar(20) COLLATE utf8mb4_bin", declared: "varchar(20) COLLATE utf8mb4_general_ci", want: "utf8mb4_general_ci"},
	{name: "restated", before: "varchar(20)", declared: "varchar(20) COLLATE utf8mb4_unicode_ci", want: "utf8mb4_unicode_ci"},
	{name: "recased", before: "varchar(20) COLLATE utf8mb4_bin", declared: "varchar(20) COLLATE UTF8MB4_BIN", want: "utf8mb4_bin"},
	{name: "quoted", before: "varchar(20)", declared: "varchar(20) COLLATE 'utf8mb4_bin'", want: "utf8mb4_bin"},
	{name: "own_charset", before: "varchar(20) CHARACTER SET latin1", declared: "varchar(20) CHARACTER SET latin1"},
	{name: "table_charset", before: "varchar(20) CHARACTER SET utf8mb4", declared: "varchar(20) CHARACTER SET utf8mb4"},
	{name: "national", before: "nchar(3)", declared: "nchar(3)"},
	{name: "alias", before: "varchar(20)", declared: "varchar(20) COLLATE utf8_general_ci", want: "utf8mb3_general_ci"},
	{name: "alias_charset", before: "varchar(20)", declared: "varchar(20) CHARACTER SET utf8 COLLATE utf8_bin", want: "utf8mb3_bin"},
	{name: "alias_charset_only", before: "varchar(20) CHARACTER SET utf8", declared: "varchar(20) CHARACTER SET utf8"},
	{name: "counted", before: "int", declared: "int COLLATE utf8mb4_bin"},
}

// mariaDBColumnCollationRows are the names only MariaDB takes: a collation
// without its character set, and DEFAULT, which selects the default
// collation of the column's character set. MySQL 8.4 refuses both.
var mariaDBColumnCollationRows = []mysqlColumnCollationRow{
	{name: "without_charset", before: "varchar(20)", declared: "varchar(20) COLLATE uca1400_ai_ci", want: "utf8mb4_uca1400_ai_ci"},
	{name: "charset_default", before: "varchar(20) COLLATE DEFAULT", declared: "varchar(20) COLLATE DEFAULT"},
}

// mysqlCollationEngines are the servers a column collation is applied to,
// each with the rows it takes.
var mysqlCollationEngines = []struct {
	name   string
	engine dbtarget.Engine
	rows   []mysqlColumnCollationRow
}{
	{name: "MySQL", engine: dbtarget.MySQLAdmin, rows: mysqlColumnCollationRows},
	{name: "MariaDB", engine: dbtarget.MariaDBAdmin, rows: slices.Concat(mysqlColumnCollationRows, mariaDBColumnCollationRows)},
}

// mysqlColumnCollationTable is CREATE TABLE table with one column per row,
// each defined by define, in a table whose default collation is
// utf8mb4_unicode_ci on both servers.
func mysqlColumnCollationTable(table string, rows []mysqlColumnCollationRow, define func(mysqlColumnCollationRow) string) string {
	columns := []string{"id int PRIMARY KEY"}
	for _, row := range rows {
		columns = append(columns, row.name+" "+define(row))
	}
	return fmt.Sprintf("CREATE TABLE %s (\n  %s\n) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;\n",
		table, strings.Join(columns, ",\n  "))
}

// mysqlColumnCollations reads the collation each column of table but id
// holds, empty for a column that holds none.
func mysqlColumnCollations(c *qt.C, scratch mysqlScratch, database, table string) map[string]string {
	c.Helper()
	rows, err := scratch.admin.QueryContext(c.Context(), `
		SELECT COLUMN_NAME, COALESCE(COLLATION_NAME, '') FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME <> 'id'`, database, table)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	collations := make(map[string]string)
	for rows.Next() {
		var column, collation string
		c.Assert(rows.Scan(&column, &collation), qt.IsNil)
		collations[column] = collation
	}
	c.Assert(rows.Err(), qt.IsNil)
	return collations
}

// TestSchemaApplyWritesAMySQLColumnCollationE2E applies a schema file whose
// columns declare collations, to a table it alters and a table it creates,
// and reads back the collation each column holds. A declared collation is
// compared with the one the column holds, so a column that held another one
// is changed, and one that restates its table's default is left alone. A
// column that declares none keeps the collation the server gave it, and the
// next plan is empty: the server derives that collation from a character set,
// so a comparison against the table's default would restate the column on
// every run. A name the server stores under another name, such as
// utf8_general_ci stored as utf8mb3_general_ci, converges too.
func TestSchemaApplyWritesAMySQLColumnCollationE2E(t *testing.T) {
	for _, engine := range mysqlCollationEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			database, url := scratch.builtFrom(c,
				mysqlColumnCollationTable("altered", engine.rows, func(row mysqlColumnCollationRow) string { return row.before }))
			before := mysqlColumnCollations(c, scratch, database, "altered")
			declared := func(row mysqlColumnCollationRow) string { return row.declared }
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(
				mysqlColumnCollationTable("altered", engine.rows, declared)+
					mysqlColumnCollationTable("created", engine.rows, declared)), 0o600), qt.IsNil)
			want := make(map[string]string, len(engine.rows))
			for _, row := range engine.rows {
				want[row.name] = cmp.Or(row.want, before[row.name])
			}

			applied := runPtahNative(c, "schema", "apply", "--db-url", url, "--schema-file", schema, "--auto-approve")

			c.Assert(mysqlColumnCollations(c, scratch, database, "altered"), qt.DeepEquals, want, qt.Commentf("%s", applied))
			c.Assert(mysqlColumnCollations(c, scratch, database, "created"), qt.DeepEquals, want, qt.Commentf("%s", applied))

			again := runPtahNative(c, "schema", "apply", "--db-url", url, "--schema-file", schema, "--dry-run")

			c.Assert(again, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}
