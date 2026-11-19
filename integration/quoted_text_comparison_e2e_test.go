//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// The comparison folds whitespace and case outside a string and keeps a
// string as written, so it has to know where each string ends. That is the
// dialect's rule. On PostgreSQL and SQLite a backslash is an ordinary
// character, outside an E'...' string on PostgreSQL, so 'p\' is a whole
// string. On MySQL and MariaDB a backslash escapes the next character, and
// MySQL's information_schema prints each quote of a CHECK clause escaped.
//
// Each changed row holds a live object and a declaration whose strings
// differ, so the two give different values, and `ptah schema apply
// --dry-run` has to plan the change. Read with the wrong rule, a backslash
// hides where a string ends, and the spaces inside the next string are folded
// as if they were SQL. Each unchanged row holds the same declaration on both
// sides, and the run has to plan nothing.

// quotedTextRow is a live schema, a desired one, and the statement the plan
// carries, empty for a row that plans nothing.
type quotedTextRow struct {
	name    string
	live    string
	desired string
	planned string
}

// quotedTextPostgresServers are the PostgreSQL release lines the rows run on.
var quotedTextPostgresServers = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "postgres-18", engine: dbtarget.PostgreSQL},
	{name: "postgres-16", engine: dbtarget.PostgreSQL16},
}

// quotedTextPostgresFunction is a trigger function the trigger rows execute.
const quotedTextPostgresFunction = `CREATE TABLE t (id int PRIMARY KEY, t text); ` +
	`CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$; `

// quotedTextPostgresChanged are pairs whose strings differ on PostgreSQL.
var quotedTextPostgresChanged = []quotedTextRow{
	{
		name:    "whitespace in a view literal",
		live:    `CREATE TABLE t (id int PRIMARY KEY, t text); CREATE VIEW v AS SELECT t || ' a  b' AS x FROM t;`,
		desired: `CREATE TABLE t (id int PRIMARY KEY, t text); CREATE VIEW v AS SELECT t || ' a b' AS x FROM t;`,
		planned: `CREATE VIEW "public"."v" AS` + "\n" + `SELECT t || ' a b' AS x FROM t;`,
	},
	{
		name:    "whitespace in a view literal after a backslash",
		live:    `CREATE TABLE t (id int PRIMARY KEY, t text); CREATE VIEW v AS SELECT t || 'p\' || ' a  b' AS x FROM t;`,
		desired: `CREATE TABLE t (id int PRIMARY KEY, t text); CREATE VIEW v AS SELECT t || 'p\' || ' a b' AS x FROM t;`,
		planned: `CREATE VIEW "public"."v" AS` + "\n" + `SELECT t || 'p\' || ' a b' AS x FROM t;`,
	},
	{
		name:    "whitespace in a view literal after an escape string",
		live:    `CREATE TABLE t (id int PRIMARY KEY, t text); CREATE VIEW v AS SELECT t || E'p\'' || ' a  b' AS x FROM t;`,
		desired: `CREATE TABLE t (id int PRIMARY KEY, t text); CREATE VIEW v AS SELECT t || E'p\'' || ' a b' AS x FROM t;`,
		planned: `CREATE OR REPLACE VIEW "public"."v" AS` + "\n" + `SELECT t || E'p\'' || ' a b' AS x FROM t;`,
	},
	{
		name:    "an alias spelled inside a view literal",
		live:    `CREATE TABLE t (id int PRIMARY KEY, t text); CREATE VIEW v AS SELECT t || 'a as a' AS x FROM t;`,
		desired: `CREATE TABLE t (id int PRIMARY KEY, t text); CREATE VIEW v AS SELECT t || 'a' AS x FROM t;`,
		planned: `CREATE VIEW "public"."v" AS` + "\n" + `SELECT t || 'a' AS x FROM t;`,
	},
	{
		name:    "a backslash at the end of a named CHECK literal",
		live:    `CREATE TABLE t (id int PRIMARY KEY, t text, CONSTRAINT c CHECK (t <> 'p\'));`,
		desired: `CREATE TABLE t (id int PRIMARY KEY, t text, CONSTRAINT c CHECK (t <> 'p'));`,
		planned: `ADD CONSTRAINT "c" CHECK (t <> 'p');`,
	},
	{
		name:    "a backslash at the end of an unnamed CHECK literal",
		live:    `CREATE TABLE t (id int PRIMARY KEY, t text CHECK (t <> 'p\'));`,
		desired: `CREATE TABLE t (id int PRIMARY KEY, t text CHECK (t <> 'p'));`,
		planned: `ADD CONSTRAINT "t_t_check" CHECK (t <> 'p');`,
	},
	{
		name:    "a backslash at the end of a partial index predicate",
		live:    `CREATE TABLE t (id int PRIMARY KEY, t text); CREATE INDEX i ON t (id) WHERE t <> 'p\';`,
		desired: `CREATE TABLE t (id int PRIMARY KEY, t text); CREATE INDEX i ON t (id) WHERE t <> 'p';`,
		planned: `CREATE INDEX "i" ON "public"."t" ("id") WHERE t <> 'p';`,
	},
	{
		name:    "a backslash at the end of a trigger condition",
		live:    quotedTextPostgresFunction + `CREATE TRIGGER tr BEFORE UPDATE ON t FOR EACH ROW WHEN (NEW.t <> 'p\') EXECUTE FUNCTION f();`,
		desired: quotedTextPostgresFunction + `CREATE TRIGGER tr BEFORE UPDATE ON t FOR EACH ROW WHEN (NEW.t <> 'p') EXECUTE FUNCTION f();`,
		planned: `CREATE OR REPLACE TRIGGER "tr" BEFORE UPDATE ON "public"."t" FOR EACH ROW WHEN (NEW.t <> 'p') EXECUTE FUNCTION "public"."f"();`,
	},
	{
		name:    "whitespace in a function body literal after a backslash",
		live:    `CREATE TABLE t (id int PRIMARY KEY); CREATE FUNCTION f(t text) RETURNS text LANGUAGE sql AS $$SELECT t || 'x\' || ' a  b'$$;`,
		desired: `CREATE TABLE t (id int PRIMARY KEY); CREATE FUNCTION f(t text) RETURNS text LANGUAGE sql AS $$SELECT t || 'x\' || ' a b'$$;`,
		planned: `CREATE OR REPLACE FUNCTION "public"."f"(t text) RETURNS text AS $$` + "\n" + `SELECT t || 'x\' || ' a b'` + "\n$$\n",
	},
}

// quotedTextPostgresUnchanged are declarations PostgreSQL stores in a form
// the comparison has to read as the same.
var quotedTextPostgresUnchanged = []quotedTextRow{
	{
		name: "a view literal after a backslash",
		live: `CREATE TABLE t (id int PRIMARY KEY, t text); CREATE VIEW v AS SELECT t || 'p\' || ' a  b' AS x FROM t;`,
	},
	{
		name: "a view literal after an escape string",
		live: `CREATE TABLE t (id int PRIMARY KEY, t text); CREATE VIEW v AS SELECT t || E'p\\' || ' a  b' AS x FROM t;`,
	},
	{
		name: "a named CHECK literal after a backslash",
		live: `CREATE TABLE t (id int PRIMARY KEY, t text, CONSTRAINT c CHECK (t <> 'p\' || ' a  b'));`,
	},
	{
		name: "a named CHECK escape string",
		live: `CREATE TABLE t (id int PRIMARY KEY, t text, CONSTRAINT c CHECK (t <> E'a\'b  c'));`,
	},
	{
		name: "an unnamed CHECK literal ending in a backslash",
		live: `CREATE TABLE t (id int PRIMARY KEY, t text CHECK (t <> 'p\'));`,
	},
	{
		name: "a partial index predicate ending in a backslash",
		live: `CREATE TABLE t (id int PRIMARY KEY, t text); CREATE INDEX i ON t (id) WHERE t <> 'p\';`,
	},
	{
		name: "a trigger condition ending in a backslash",
		live: quotedTextPostgresFunction + `CREATE TRIGGER tr BEFORE UPDATE ON t FOR EACH ROW WHEN (NEW.t <> 'p\') EXECUTE FUNCTION f();`,
	},
}

// quotedTextSchemaFile writes desired to a schema file and returns its path.
func quotedTextSchemaFile(c *qt.C, desired string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(desired), 0o600), qt.IsNil)
	return path
}

// quotedTextDryRun is `ptah schema apply --dry-run` of desired against the
// database at url.
func quotedTextDryRun(c *qt.C, url, desired string) clirun.Result {
	c.Helper()
	return runPtahIn(c, "schema", "apply", "--db-url", url, "--schema-file", quotedTextSchemaFile(c, desired), "--dry-run")
}

// quotedTextPostgresDatabase is a database on engine's server holding live.
func quotedTextPostgresDatabase(c *qt.C, engine dbtarget.Engine, live string) string {
	c.Helper()
	url := postgresScratchURLOn(c, engine, "ptah_quoted_text")
	execPostgresFamilySQL(c, c.Context(), url, live)
	return url
}

// TestQuotedTextPlansAChangeOnPostgreSQLE2E holds every changed row to a
// planned change on each release line.
func TestQuotedTextPlansAChangeOnPostgreSQLE2E(t *testing.T) {
	for _, row := range quotedTextPostgresChanged {
		for _, server := range quotedTextPostgresServers {
			t.Run(row.name+"/"+server.name, func(t *testing.T) {
				c := qt.New(t)
				url := quotedTextPostgresDatabase(c, server.engine, row.live)

				planned := quotedTextDryRun(c, url, row.desired)

				c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
				c.Assert(planned.Stdout, qt.Contains, row.planned)
			})
		}
	}
}

// TestQuotedTextPlansNothingOnPostgreSQLE2E holds every unchanged row to no
// change on each release line.
func TestQuotedTextPlansNothingOnPostgreSQLE2E(t *testing.T) {
	for _, row := range quotedTextPostgresUnchanged {
		for _, server := range quotedTextPostgresServers {
			t.Run(row.name+"/"+server.name, func(t *testing.T) {
				c := qt.New(t)
				url := quotedTextPostgresDatabase(c, server.engine, row.live)

				planned := quotedTextDryRun(c, url, row.live)

				c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
				c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			})
		}
	}
}

// quotedTextMySQLServers are the MySQL-family servers the rows run on.
var quotedTextMySQLServers = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "mysql", engine: dbtarget.MySQLAdmin},
	{name: "mariadb", engine: dbtarget.MariaDBAdmin},
}

// quotedTextMySQLChanged are pairs whose strings differ on MySQL and
// MariaDB. The view rows name no definer in the plan: it is the account the
// run connects with.
var quotedTextMySQLChanged = []quotedTextRow{
	{
		name:    "a CHECK literal",
		live:    `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (c <> 'abc'));`,
		desired: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (c <> 'abd'));`,
		planned: `CHECK (c <> 'abd');`,
	},
	{
		name:    "whitespace in a view literal",
		live:    `CREATE TABLE t (id int PRIMARY KEY, c varchar(20)); CREATE VIEW v AS SELECT concat(c, ' a  b') AS x FROM t;`,
		desired: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20)); CREATE VIEW v AS SELECT concat(c, ' a b') AS x FROM t;`,
		planned: "VIEW `v` AS\nSELECT concat(c, ' a b') AS x FROM t;",
	},
	{
		name:    "whitespace in a view literal after an escaped quote",
		live:    `CREATE TABLE t (id int PRIMARY KEY, c varchar(20)); CREATE VIEW v AS SELECT concat(c, 'it\'s', ' a  b') AS x FROM t;`,
		desired: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20)); CREATE VIEW v AS SELECT concat(c, 'it\'s', ' a b') AS x FROM t;`,
		planned: "VIEW `v` AS\nSELECT concat(c, 'it\\'s', ' a b') AS x FROM t;",
	},
}

// quotedTextMySQLUnchanged are declarations the MySQL family stores in a
// form the comparison has to read as the same. MySQL's information_schema
// prints the first as (`c` <> _utf8mb4\'abc\'), with each quote escaped.
var quotedTextMySQLUnchanged = []quotedTextRow{
	{
		name: "a CHECK literal",
		live: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (c <> 'abc'));`,
	},
	{
		name: "a view literal holding an escaped quote",
		live: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20)); CREATE VIEW v AS SELECT concat(c, 'it\'s  x') AS x FROM t;`,
	},
}

// TestQuotedTextPlansAChangeOnTheMySQLFamilyE2E holds every changed row to a
// planned change on MySQL and MariaDB.
func TestQuotedTextPlansAChangeOnTheMySQLFamilyE2E(t *testing.T) {
	for _, row := range quotedTextMySQLChanged {
		for _, server := range quotedTextMySQLServers {
			t.Run(row.name+"/"+server.name, func(t *testing.T) {
				c := qt.New(t)
				_, url := newMySQLFamilyScratch(c, server.engine).builtFrom(c, row.live)

				planned := quotedTextDryRun(c, url, row.desired)

				c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
				c.Assert(planned.Stdout, qt.Contains, row.planned)
			})
		}
	}
}

// TestQuotedTextPlansNothingOnTheMySQLFamilyE2E holds every unchanged row to
// no change on MySQL and MariaDB.
func TestQuotedTextPlansNothingOnTheMySQLFamilyE2E(t *testing.T) {
	for _, row := range quotedTextMySQLUnchanged {
		for _, server := range quotedTextMySQLServers {
			t.Run(row.name+"/"+server.name, func(t *testing.T) {
				c := qt.New(t)
				_, url := newMySQLFamilyScratch(c, server.engine).builtFrom(c, row.live)

				planned := quotedTextDryRun(c, url, row.live)

				c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
				c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			})
		}
	}
}

// quotedTextSQLiteTables are the tables the SQLite rows declare beside the
// object under test.
const quotedTextSQLiteTables = `CREATE TABLE t (id INTEGER PRIMARY KEY, t TEXT); CREATE TABLE log (m TEXT); `

// quotedTextSQLiteChanged are pairs whose strings differ on SQLite.
var quotedTextSQLiteChanged = []quotedTextRow{
	{
		name:    "whitespace in a trigger body literal",
		live:    quotedTextSQLiteTables + `CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO log (m) VALUES (NEW.t || ' a  b'); END;`,
		desired: quotedTextSQLiteTables + `CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO log (m) VALUES (NEW.t || ' a b'); END;`,
		planned: `CREATE TRIGGER "tr" AFTER INSERT ON "t" FOR EACH ROW BEGIN INSERT INTO log (m) VALUES (NEW.t || ' a b'); END;`,
	},
	{
		name:    "whitespace in a trigger body literal after a backslash",
		live:    quotedTextSQLiteTables + `CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO log (m) VALUES (NEW.t || 'x\' || ' a  b'); END;`,
		desired: quotedTextSQLiteTables + `CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO log (m) VALUES (NEW.t || 'x\' || ' a b'); END;`,
		planned: `CREATE TRIGGER "tr" AFTER INSERT ON "t" FOR EACH ROW BEGIN INSERT INTO log (m) VALUES (NEW.t || 'x\' || ' a b'); END;`,
	},
}

// TestQuotedTextPlansAChangeOnSQLiteE2E holds every changed row to a planned
// change on SQLite.
func TestQuotedTextPlansAChangeOnSQLiteE2E(t *testing.T) {
	for _, row := range quotedTextSQLiteChanged {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(c.TempDir(), "app.db")
			sqliteExec(c, path, row.live)

			planned := quotedTextDryRun(c, "sqlite://"+filepath.ToSlash(path), row.desired)

			c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
			c.Assert(planned.Stdout, qt.Contains, row.planned)
		})
	}
}

// TestQuotedTextPlansNothingOnSQLiteE2E holds a trigger whose body ends a
// string in a backslash to no change on SQLite.
func TestQuotedTextPlansNothingOnSQLiteE2E(t *testing.T) {
	c := qt.New(t)
	live := quotedTextSQLiteTables + `CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO log (m) VALUES (NEW.t || 'x\' || ' a  b'); END;`
	path := filepath.Join(c.TempDir(), "app.db")
	sqliteExec(c, path, live)

	planned := quotedTextDryRun(c, "sqlite://"+filepath.ToSlash(path), live)

	c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
	c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
}
