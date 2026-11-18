//go:build integration

package integration_test

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// columnCollationSchema is the schema the column collation tests work in. It
// holds a collation of its own, so a column can name a collation by schema.
const columnCollationSchema = "coll_app"

// columnCollationRow is one column: what the database holds before the apply,
// empty for a column of the table the apply creates, what the schema file
// declares, and the type and collation pg_attribute holds after the apply.
type columnCollationRow struct {
	name     string
	before   string
	declared string
	want     string
}

// columnCollationRows covers a collation set, removed, changed, kept,
// restated as the type's own, named by schema with a mixed-case name, written
// unquoted, and kept through a type change.
var columnCollationRows = []columnCollationRow{
	{name: "set", before: "text", declared: `text COLLATE "C"`, want: "text pg_catalog.C"},
	{name: "reset", before: `text COLLATE "C"`, declared: "text", want: "text pg_catalog.default"},
	{name: "changed", before: `text COLLATE "C"`, declared: `text COLLATE "POSIX"`, want: "text pg_catalog.POSIX"},
	{name: "kept", before: `text COLLATE "C"`, declared: `text COLLATE "C"`, want: "text pg_catalog.C"},
	{name: "restated", before: "text", declared: `text COLLATE "default"`, want: "text pg_catalog.default"},
	{name: "qualified", before: "varchar(20)", declared: `varchar(20) COLLATE Coll_App."MixedCase"`,
		want: "character varying(20) coll_app.MixedCase"},
	{name: "folded", before: "text", declared: "text COLLATE Ucs_Basic", want: "text pg_catalog.ucs_basic"},
	{name: "retyped", before: `varchar(20) COLLATE "POSIX"`, declared: `varchar(40) COLLATE "POSIX"`,
		want: "character varying(40) pg_catalog.POSIX"},
}

// columnCollationTable is CREATE TABLE schema.table with one column per row,
// each defined by define.
func columnCollationTable(table string, define func(columnCollationRow) string) string {
	columns := []string{"id integer PRIMARY KEY"}
	for _, row := range columnCollationRows {
		columns = append(columns, row.name+" "+define(row))
	}
	return fmt.Sprintf("CREATE TABLE %s.%s (\n  %s\n);\n", columnCollationSchema, table, strings.Join(columns, ",\n  "))
}

// storedColumnCollations reads each collatable column of schema.table back
// from pg_attribute as its type and the schema and name of its collation.
func storedColumnCollations(c *qt.C, database, table string) map[string]string {
	c.Helper()
	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, database))
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(c.Context(), `
		SELECT a.attname, format_type(a.atttypid, a.atttypmod) || ' ' || n.nspname || '.' || co.collname
		FROM pg_attribute a
		JOIN pg_collation co ON co.oid = a.attcollation
		JOIN pg_namespace n ON n.oid = co.collnamespace
		WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped`,
		columnCollationSchema+"."+table)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	stored := make(map[string]string)
	for rows.Next() {
		var name, collation string
		c.Assert(rows.Scan(&name, &collation), qt.IsNil)
		stored[name] = collation
	}
	c.Assert(rows.Err(), qt.IsNil)
	return stored
}

// TestSchemaApplyWritesAPostgreSQLColumnCollationE2E applies a schema file
// whose columns name collations, creating one table and altering another,
// and reads each column's collation back from pg_attribute. The next plan is
// empty: the collation the server holds is the one the file declares.
func TestSchemaApplyWritesAPostgreSQLColumnCollationE2E(t *testing.T) {
	c := qt.New(t)
	target := newPolicyClauseDatabase(c, dbtarget.PostgreSQL)
	execPostgresFamilySQL(c, c.Context(), target, fmt.Sprintf(
		`CREATE SCHEMA %[1]s; CREATE COLLATION %[1]s."MixedCase" FROM "C"; %[2]s`,
		columnCollationSchema, columnCollationTable("altered", func(row columnCollationRow) string { return row.before })))
	declared := func(row columnCollationRow) string { return row.declared }
	file := writeDeclaredSchemaFile(c, "schema.sql",
		columnCollationTable("altered", declared)+columnCollationTable("created", declared))
	want := make(map[string]string, len(columnCollationRows))
	for _, row := range columnCollationRows {
		want[row.name] = row.want
	}
	args := []string{"schema", "apply", "--db-url", target, "--schemas", columnCollationSchema, "--schema-file", file}

	applied := runPtahIn(c, append(args, "--auto-approve")...)

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(storedColumnCollations(c, target, "created"), qt.DeepEquals, want)
	c.Assert(storedColumnCollations(c, target, "altered"), qt.DeepEquals, want)

	settled := runPtahIn(c, append(args, "--dry-run")...)

	c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
	c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
}

// TestSchemaApplyChangesTheCollationOfAColumnAViewReadsE2E changes the
// collation of a column a view and a stored generated column read. PostgreSQL
// changes it with ALTER COLUMN ... TYPE and refuses that while either reads
// the column, so the plan takes both out and puts them back. The column holds
// the declared collation after the apply, the view and the generated column
// are back, and the next plan is empty.
func TestSchemaApplyChangesTheCollationOfAColumnAViewReadsE2E(t *testing.T) {
	c := qt.New(t)
	target := newPolicyClauseDatabase(c, dbtarget.PostgreSQL)
	execPostgresFamilySQL(c, c.Context(), target, fmt.Sprintf(`CREATE SCHEMA %[1]s;
		CREATE TABLE %[1]s.t (id integer PRIMARY KEY, a text, g text GENERATED ALWAYS AS (upper(a)) STORED);
		CREATE VIEW %[1]s.v AS SELECT id, a FROM %[1]s.t;`, columnCollationSchema))
	file := writeDeclaredSchemaFile(c, "schema.sql", fmt.Sprintf(`
CREATE TABLE %[1]s.t (id integer PRIMARY KEY, a text COLLATE "C", g text GENERATED ALWAYS AS (upper(a)) STORED);
CREATE VIEW %[1]s.v AS SELECT id, a FROM %[1]s.t;
`, columnCollationSchema))
	args := []string{"schema", "apply", "--db-url", target, "--schemas", columnCollationSchema, "--schema-file", file}

	applied := runPtahIn(c, append(args, "--auto-approve")...)

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(storedColumnCollations(c, target, "t"), qt.DeepEquals, map[string]string{
		"a": "text pg_catalog.C",
		"g": "text pg_catalog.default",
	})
	c.Assert(storedColumnCollations(c, target, "v"), qt.DeepEquals, map[string]string{"a": "text pg_catalog.C"})

	settled := runPtahIn(c, append(args, "--dry-run")...)

	c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
	c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
}
