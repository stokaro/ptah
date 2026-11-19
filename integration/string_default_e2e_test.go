//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// Each column holds a string default whose characters spell the expression
// the other schema declares, so a comparison that takes the quotes off the
// string finds the two schemas equal.
const (
	stringDefaultSchema = "CREATE TABLE notes (\n" +
		"  id int PRIMARY KEY,\n" +
		"  a varchar(30) DEFAULT '1 + 2',\n" +
		"  b varchar(30) DEFAULT '(1 + 2)',\n" +
		"  n varchar(30) DEFAULT 'now()'\n" +
		");\n"
	mySQLExpressionDefaultSchema = "CREATE TABLE notes (\n" +
		"  id int PRIMARY KEY,\n" +
		"  a varchar(30) DEFAULT (1 + 2),\n" +
		"  b varchar(30) DEFAULT (1 + 2),\n" +
		"  n varchar(30) DEFAULT (now())\n" +
		");\n"
	postgresExpressionDefaultSchema = "CREATE TABLE notes (\n" +
		"  id int PRIMARY KEY,\n" +
		"  a varchar(30) DEFAULT (1 + 2),\n" +
		"  b varchar(30) DEFAULT (1 + 2),\n" +
		"  n varchar(30) DEFAULT now()\n" +
		");\n"
)

// TestApplyTellsAStringDefaultFromAnExpressionMySQLFamilyE2E applies one
// schema and then the other through `ptah schema apply`, in both directions.
// The second schema changes every default, a third apply plans nothing, and a
// row that takes the defaults reads back what the second schema declares: 3
// where the expression is, the characters where the string is.
func TestApplyTellsAStringDefaultFromAnExpressionMySQLFamilyE2E(t *testing.T) {
	rows := []struct {
		name          string
		engine        dbtarget.Engine
		first, second string
		want          string
	}{
		{name: "mysql, string to expression", engine: dbtarget.MySQLAdmin, first: stringDefaultSchema, second: mySQLExpressionDefaultSchema, want: "3,3,1"},
		{name: "mysql, expression to string", engine: dbtarget.MySQLAdmin, first: mySQLExpressionDefaultSchema, second: stringDefaultSchema, want: "1 + 2,(1 + 2),0"},
		{name: "mariadb, string to expression", engine: dbtarget.MariaDBAdmin, first: stringDefaultSchema, second: mySQLExpressionDefaultSchema, want: "3,3,1"},
		{name: "mariadb, expression to string", engine: dbtarget.MariaDBAdmin, first: mySQLExpressionDefaultSchema, second: stringDefaultSchema, want: "1 + 2,(1 + 2),0"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, row.engine)
			name, _ := scratch.database(c, "string_default")
			database := replaceMySQLDatabaseName(c, scratch.adminURL, name)

			got := applyBothDefaultSchemas(c, database, row.first, row.second,
				"SELECT CONCAT_WS(',', a, b, n <> 'now()') FROM notes")

			c.Assert(got, qt.Equals, row.want)
		})
	}
}

// TestApplyTellsAStringDefaultFromAnExpressionPostgreSQLE2E is the same
// measurement on PostgreSQL, which reports the string as '1 + 2'::character
// varying.
func TestApplyTellsAStringDefaultFromAnExpressionPostgreSQLE2E(t *testing.T) {
	rows := []struct {
		name          string
		first, second string
		want          string
	}{
		{name: "string to expression", first: stringDefaultSchema, second: postgresExpressionDefaultSchema, want: "3,3,true"},
		{name: "expression to string", first: postgresExpressionDefaultSchema, second: stringDefaultSchema, want: "1 + 2,(1 + 2),false"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := postgresScratchDevURL(c, "string_default")

			got := applyBothDefaultSchemas(c, database, row.first, row.second,
				"SELECT concat_ws(',', a, b, (n <> 'now()')::text) FROM notes")

			c.Assert(got, qt.Equals, row.want)
		})
	}
}

// applyBothDefaultSchemas applies first and then second to database, asserts
// that a further apply of second plans nothing, inserts a row that takes every
// default, and returns what query reads from it.
func applyBothDefaultSchemas(c *qt.C, database, first, second, query string) string {
	c.Helper()
	dir := c.TempDir()
	firstFile := filepath.Join(dir, "first.sql")
	secondFile := filepath.Join(dir, "second.sql")
	c.Assert(os.WriteFile(firstFile, []byte(first), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(secondFile, []byte(second), 0o600), qt.IsNil)

	created := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", firstFile, "--auto-approve")
	changed := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", secondFile, "--auto-approve")
	again := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", secondFile, "--auto-approve")

	c.Assert(created.ExitCode, qt.Equals, 0, historyComment(created))
	c.Assert(changed.ExitCode, qt.Equals, 0, historyComment(changed))
	c.Assert(changed.Stdout, qt.Not(qt.Equals), "Schema is synced, no changes to be made.\n")
	c.Assert(again.ExitCode, qt.Equals, 0, historyComment(again))
	c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
	conn, err := dbschema.ConnectToDatabase(c.Context(), database)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), "INSERT INTO notes (id) VALUES (1)")
	c.Assert(err, qt.IsNil)
	var got string
	c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&got), qt.IsNil)
	return got
}
