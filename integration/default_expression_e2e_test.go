//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
)

// These tests apply a schema file whose column defaults are expressions
// through `ptah schema apply`, insert a row that takes every default, and read
// it back: the server decides what each default evaluates to. A second apply
// plans nothing, which says the comparison reads the default the server
// reports as the one the file declares.
//
// internal/parser/default_operator_test.go holds where each default ends, and
// migration/schemadiff/internal/compare holds the comparison, without a
// server.

// operatorDefaultSchema declares a default with each kind of operator, each
// followed by the clause that ends it.
const operatorDefaultSchema = "CREATE TABLE public.tallies (\n" +
	"  id int PRIMARY KEY,\n" +
	"  n int DEFAULT 5 - 2 NOT NULL,\n" +
	"  s text DEFAULT 'a' || 'b',\n" +
	"  m int DEFAULT 7 % 4 CHECK (m >= 0),\n" +
	"  p int DEFAULT 2 * 3 + 1,\n" +
	"  e timestamptz DEFAULT now() - interval '1 day'\n" +
	");\n"

// operatorDefaultReadBack answers the row's defaults, whether e is in the
// past, and whether n is NOT NULL and m carries its CHECK.
const operatorDefaultReadBack = "SELECT format('%s,%s,%s,%s,%s,%s,%s', n, s, m, p, e < now(), " +
	"(SELECT attnotnull FROM pg_attribute WHERE attrelid = 'public.tallies'::regclass AND attname = 'n'), " +
	"(SELECT count(*) FROM pg_constraint WHERE conrelid = 'public.tallies'::regclass AND contype = 'c')) " +
	"FROM public.tallies"

// TestApplyReadsAnOperatorDefaultE2E applies the operator defaults to
// PostgreSQL, which evaluates them.
func TestApplyReadsAnOperatorDefaultE2E(t *testing.T) {
	c := qt.New(t)
	fixture := includeChildFixture{database: postgresScratchDevURL(c, "ptah_default_operator")}
	fixture.schema = filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(fixture.schema, []byte(operatorDefaultSchema), 0o600), qt.IsNil)

	applied := fixture.apply(c)
	again := fixture.apply(c)

	c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
	c.Assert(again.ExitCode, qt.Equals, 0, historyComment(again))
	c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
	execPostgresFamilySQL(c, c.Context(), fixture.database, "INSERT INTO public.tallies (id) VALUES (1);\n")
	c.Assert(fixture.read(c, operatorDefaultReadBack), qt.Equals, "3,ab,3,7,t,t,1")
}

// parenthesizedDefaultSchema writes each expression default in the
// parentheses SQLite requires around one. SQLite reports each without them.
const parenthesizedDefaultSchema = "CREATE TABLE tallies (\n" +
	"  id integer PRIMARY KEY,\n" +
	"  d text DEFAULT (datetime('now')) NOT NULL,\n" +
	"  n integer DEFAULT (5 - 2) NOT NULL,\n" +
	"  s text DEFAULT ('a' || 'b'),\n" +
	"  z integer DEFAULT ((0))\n" +
	");\n"

// TestApplyConvergesOnAParenthesizedDefaultE2E applies the parenthesized
// defaults to SQLite. The second apply plans nothing: compared with the
// parentheses, every apply rebuilt the table.
func TestApplyConvergesOnAParenthesizedDefaultE2E(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	database := atlasurl.SQLiteURLFromPath(filepath.Join(dir, "app.db"))
	schema := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(parenthesizedDefaultSchema), 0o600), qt.IsNil)
	apply := []string{"schema", "apply", "--db-url", database, "--schema-file", schema, "--auto-approve"}

	applied := runPtahIn(c, apply...)
	again := runPtahIn(c, apply...)

	c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
	c.Assert(again.ExitCode, qt.Equals, 0, historyComment(again))
	c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
	conn, err := dbschema.ConnectToDatabase(c.Context(), database)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), "INSERT INTO tallies (id) VALUES (1)")
	c.Assert(err, qt.IsNil)
	var got string
	err = conn.QueryRowContext(c.Context(), "SELECT (d > '2000') || ',' || n || ',' || s || ',' || z FROM tallies").Scan(&got)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "1,3,ab,0")
}
