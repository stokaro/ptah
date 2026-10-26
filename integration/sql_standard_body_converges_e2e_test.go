//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// sqlStandardBodySchema declares a routine with a SQL-standard body, which
// PostgreSQL keeps parsed and prints back in its own spelling:
// `SELECT count(*) FROM v` reads back as `SELECT count(*) AS count FROM v`.
const sqlStandardBodySchema = `CREATE TABLE t (id bigint PRIMARY KEY, a int);
CREATE VIEW v AS SELECT id, a FROM t;
CREATE FUNCTION count_v() RETURNS bigint LANGUAGE sql STABLE BEGIN ATOMIC SELECT count(*) FROM v; END;
`

// sqlBodySchemaFile writes text to a schema file in a test's own directory
// and answers its path.
func sqlBodySchemaFile(c *qt.C, text string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(text), 0o600), qt.IsNil)
	return path
}

// TestSchemaApplyConvergesOnASQLStandardBodyLive applies routines with
// SQL-standard bodies and compares again: the comparison puts each declared
// body through the server and compares its stored form, so the second
// comparison finds nothing to do (stokaro/ptah#5021). The server qualifies a
// parameter with the routine's own name, `f.x`, which the probe copy takes.
func TestSchemaApplyConvergesOnASQLStandardBodyLive(t *testing.T) {
	tests := []struct {
		name   string
		schema string
	}{
		{name: "a function that reads a view", schema: sqlStandardBodySchema},
		{name: "a function that uses a parameter", schema: `CREATE TABLE t (id bigint PRIMARY KEY, a int);
CREATE FUNCTION f(x int) RETURNS bigint LANGUAGE sql STABLE BEGIN ATOMIC SELECT count(*) FROM t WHERE a > x; END;
`},
		{name: "two overloads that use their parameter", schema: `CREATE TABLE t (id bigint PRIMARY KEY, a int);
CREATE FUNCTION f(x int) RETURNS bigint LANGUAGE sql STABLE BEGIN ATOMIC SELECT count(*) FROM t WHERE a > x; END;
CREATE FUNCTION f(x text) RETURNS bigint LANGUAGE sql STABLE BEGIN ATOMIC SELECT count(*) FROM t WHERE a::text = x; END;
`},
		{name: "two functions with one body", schema: `CREATE TABLE t (id bigint PRIMARY KEY, a int);
CREATE FUNCTION f(x int) RETURNS bigint LANGUAGE sql STABLE BEGIN ATOMIC SELECT count(*) FROM t WHERE a > x; END;
CREATE FUNCTION g(x int) RETURNS bigint LANGUAGE sql STABLE BEGIN ATOMIC SELECT count(*) FROM t WHERE a > x; END;
`},
		{name: "a function in another schema that uses a parameter", schema: `CREATE SCHEMA other;
CREATE TABLE t (id bigint PRIMARY KEY, a int);
CREATE FUNCTION other.cnt(x int) RETURNS bigint LANGUAGE sql STABLE BEGIN ATOMIC SELECT count(*) FROM t WHERE a > x; END;
`},
		{name: "a procedure without arguments", schema: `CREATE TABLE t (id bigint PRIMARY KEY, a int);
CREATE PROCEDURE p() LANGUAGE sql BEGIN ATOMIC INSERT INTO t VALUES (1, 1); END;
`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _ := scratchReplayDatabase(c)
			schema := sqlBodySchemaFile(c, test.schema)

			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")
			again := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

			c.Assert(again, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyReplacesAChangedSQLStandardBodyLive changes what a routine
// with a SQL-standard body reads, applies the plan and compares again. A body
// that reads a table the plan creates refuses the probe copy, and a second
// copy without the body still answers for the arguments, so the plan replaces
// the routine in place rather than dropping it from under the view that calls
// it. A body that selects every column of a table that gains one is not
// answered from today's columns, so the plan replaces the routine to cover the
// new column. Either way the second comparison finds nothing to do.
func TestSchemaApplyReplacesAChangedSQLStandardBodyLive(t *testing.T) {
	tests := []struct {
		name     string
		before   string
		after    string
		replaces string
	}{
		{name: "a body that reads a table the plan creates",
			before: `CREATE TABLE t (id bigint PRIMARY KEY, a int);
CREATE FUNCTION cnt(b text DEFAULT 'X', n numeric(10,2) = 1.5) RETURNS bigint LANGUAGE sql STABLE BEGIN ATOMIC SELECT count(*) FROM t; END;
CREATE VIEW uses_cnt AS SELECT cnt() AS c;
`,
			after: `CREATE TABLE t (id bigint PRIMARY KEY, a int);
CREATE TABLE u (id bigint PRIMARY KEY);
CREATE FUNCTION cnt(b text DEFAULT 'X', n numeric(10,2) = 1.5) RETURNS bigint LANGUAGE sql STABLE BEGIN ATOMIC SELECT count(*) FROM u; END;
CREATE VIEW uses_cnt AS SELECT cnt() AS c;
`,
			replaces: `CREATE OR REPLACE FUNCTION "cnt"`},
		{name: "a body that selects every column of a table that gains one",
			before: `CREATE TABLE t (id bigint PRIMARY KEY, a int);
CREATE FUNCTION rows_t() RETURNS json LANGUAGE sql STABLE BEGIN ATOMIC SELECT json_agg(s) FROM (SELECT * FROM t) s; END;
`,
			after: `CREATE TABLE t (id bigint PRIMARY KEY, a int, b text DEFAULT 'new');
CREATE FUNCTION rows_t() RETURNS json LANGUAGE sql STABLE BEGIN ATOMIC SELECT json_agg(s) FROM (SELECT * FROM t) s; END;
`,
			replaces: `CREATE OR REPLACE FUNCTION "rows_t"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _ := scratchReplayDatabase(c)
			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", sqlBodySchemaFile(c, test.before), "--auto-approve")
			after := sqlBodySchemaFile(c, test.after)

			plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", after, "--dry-run")
			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", after, "--auto-approve")
			again := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", after, "--dry-run")

			c.Assert(plan, qt.Contains, test.replaces)
			c.Assert(plan, qt.Not(qt.Contains), "DROP FUNCTION")
			c.Assert(again, qt.Contains, "Schema is synced")
		})
	}
}

// TestCompatMigrateDiffWritesNoSecondFileForABeginAtomicBodyE2E diffs a
// declaration with a BEGIN ATOMIC routine twice into one golang-migrate
// directory: the second run replays the first file and finds the dev database
// in sync with the declaration, so it writes no file (stokaro/ptah#5021).
func TestCompatMigrateDiffWritesNoSecondFileForABeginAtomicBodyE2E(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)
	dev := pinnedDevURL(c, postgresScratchDevURL(c, "ptah_sqlbody_dev"), "public")
	work := c.TempDir()
	dir := filepath.Join(work, "migrations")
	schema := sqlBodySchemaFile(c, sqlStandardBodySchema)
	diff := func(name string) (string, error) {
		return runCompatVerb("migrate", "diff", name,
			"--dir", "file://"+filepath.ToSlash(dir)+"?format=golang-migrate",
			"--to", "file://"+filepath.ToSlash(schema),
			"--dev-url", dev)
	}
	first, err := diff("s1")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", first))

	second, err := diff("s2")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", second))
	c.Assert(second, qt.Contains, "The migration directory is synced with the desired state")
	ups, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(ups, qt.HasLen, 1)
}
