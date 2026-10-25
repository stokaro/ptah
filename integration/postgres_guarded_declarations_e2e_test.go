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

// guardedDeclarations each declare an object twice, the second time with a
// guard. PostgreSQL 15.19 runs each file: IF NOT EXISTS skips the second
// statement whole, and OR REPLACE replaces the first definition with the
// second. A database built from the file is what the file declares, so the
// reader has to read the file the way the server ran it.
var guardedDeclarations = []struct {
	name string
	sql  string
}{
	{
		name: "a table declared again with more columns",
		sql:  "CREATE TABLE t (a int);\nCREATE TABLE IF NOT EXISTS t (a int, b int);",
	},
	{
		name: "a table declared again with a key and an index",
		sql: "CREATE TABLE p (id int PRIMARY KEY);\nCREATE TABLE t (a int);\n" +
			"CREATE TABLE IF NOT EXISTS t (a int UNIQUE, b int REFERENCES p (id), CHECK (a > 0));",
	},
	{
		name: "an index declared again on another column",
		sql:  "CREATE TABLE t (a int, b int);\nCREATE INDEX ix ON t (a);\nCREATE INDEX IF NOT EXISTS ix ON t (b);",
	},
	{
		name: "a sequence declared again with another start",
		sql:  "CREATE SEQUENCE s START 5;\nCREATE SEQUENCE IF NOT EXISTS s START 10;",
	},
	{
		name: "a schema declared again",
		sql:  "CREATE SCHEMA app;\nCREATE SCHEMA IF NOT EXISTS app;\nCREATE TABLE app.t (a int);",
	},
	{
		name: "a view replaced",
		sql:  "CREATE TABLE t (a int, b int);\nCREATE VIEW v AS SELECT a FROM t;\nCREATE OR REPLACE VIEW v AS SELECT b AS a FROM t;",
	},
	{
		name: "a function replaced",
		sql: "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;\n" +
			"CREATE OR REPLACE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 2 $$;",
	},
	{
		name: "a function replaced beside an overload it leaves",
		sql: "CREATE FUNCTION f(a int) RETURNS int LANGUAGE sql AS $$ SELECT a $$;\n" +
			"CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;\n" +
			"CREATE OR REPLACE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 2 $$;",
	},
	{
		name: "a trigger replaced",
		sql: "CREATE TABLE t (a int);\n" +
			"CREATE FUNCTION trg() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;\n" +
			"CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION trg();\n" +
			"CREATE OR REPLACE TRIGGER tr BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION trg();",
	},
}

// TestPostgresGuardedDeclarationsDiffEmptyE2E builds a database from each file
// and diffs it with the file: nothing changes, because the reader skips what
// the server skipped and keeps what the server replaced the first definition
// with.
func TestPostgresGuardedDeclarationsDiffEmptyE2E(t *testing.T) {
	for _, test := range guardedDeclarations {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(test.sql+"\n"), 0o600), qt.IsNil)

			code, stdout, stderr := runAsOneRun(c, nativeTree, "schema", "diff",
				"--from", databaseBuiltFrom(c, test.sql), "--to", "file://"+filepath.ToSlash(schema))

			c.Assert(code, qt.Equals, 0, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(stdout, qt.Equals, "Schemas are synced, no changes to be made.\n")
		})
	}
}

// TestPostgresGuardedDeclarationsReplaySyncedE2E is the same files on
// ptah-compat: a migration directory holding the file, replayed on a dev
// database, is synced with the file. The test server is declared disposable,
// since the replay refuses a routine it cannot confine to the dev database
// otherwise.
func TestPostgresGuardedDeclarationsReplaySyncedE2E(t *testing.T) {
	for _, test := range guardedDeclarations {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)
			dir, schema := writeRewrittenMigrationProject(c, test.sql, test.sql)
			dev, _ := scratchReplayDatabase(c)

			code, stdout, stderr := runAsOneRun(c, compatTree, "migrate", "diff", "--dir", "file://"+filepath.ToSlash(dir),
				"--to", "file://"+filepath.ToSlash(schema), "--dev-url", dev)

			c.Assert(code, qt.Equals, 0, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(stdout, qt.Equals, "The migration directory is synced with the desired state, no changes to be made\n")
		})
	}
}
