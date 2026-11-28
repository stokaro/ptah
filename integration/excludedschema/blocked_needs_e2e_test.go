//go:build integration

package excludedschema_test

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A table whose drop the selection blocks stays whole, so what it needs stays
// too: the table its foreign key references, the database that table is in,
// and the function its trigger runs. The diff withholds each drop and warns
// about it, and the statements it prints run on the server
// (stokaro/ptah#6847). Each run declares v with a new column, so the diff
// holds a statement to replay, and leaves the rest undeclared.

// statementRunner is what a test runs statements on: an *sql.DB or a
// *dbschema.DatabaseConnection.
type statementRunner interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// replay runs each statement of a printed diff on db.
func replay(c *qt.C, db statementRunner, printed string) {
	c.Helper()
	statements := slices.DeleteFunc(strings.Split(printed, ";\n"), func(statement string) bool {
		return strings.TrimSpace(statement) == ""
	})
	c.Assert(statements, qt.Not(qt.HasLen), 0)
	for _, statement := range statements {
		_, err := db.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// scratchPostgresSchema is an empty schema in the PostgreSQL test database,
// dropped with what it holds when the test ends.
func scratchPostgresSchema(c *qt.C, db statementRunner) string {
	c.Helper()
	schema := fmt.Sprintf("ptah_blocked_needs_%d", time.Now().UnixNano())
	_, err := db.ExecContext(c.Context(), "CREATE SCHEMA "+schema)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, err := db.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		c.Check(err, qt.IsNil)
	})
	return schema
}

// countOf runs a COUNT query on db.
func countOf(c *qt.C, db *sql.DB, query string, args ...any) int {
	c.Helper()
	var count int
	c.Assert(db.QueryRowContext(c.Context(), query, args...).Scan(&count), qt.IsNil)
	return count
}

func TestMySQLFamilyBlockedTableKeepsTheTableItReferencesE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			admin, database, _ := emptyDatabase(c, engine.admin)
			for _, statement := range []string{
				"CREATE TABLE `%s`.t (id INT NOT NULL PRIMARY KEY)",
				"CREATE TABLE `%[1]s`.u (id INT NOT NULL PRIMARY KEY, c INT, t_id INT, CONSTRAINT fk_ut FOREIGN KEY (t_id) REFERENCES `%[1]s`.t (id))",
				"CREATE TABLE `%s`.v (id INT NOT NULL PRIMARY KEY)",
				"INSERT INTO `%s`.t VALUES (1)",
				"INSERT INTO `%s`.u VALUES (1, 42, 1)",
			} {
				_, err := admin.ExecContext(c.Context(), fmt.Sprintf(statement, database))
				c.Assert(err, qt.IsNil)
			}
			single, err := atlasurl.WithDatabaseName(dbtarget.URL(c, engine.admin), database)
			c.Assert(err, qt.IsNil)
			root := c.TempDir()
			file := writeFile(c, root, "schema.sql", database, "CREATE TABLE v (id INT NOT NULL PRIMARY KEY, w INT);\n")

			result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: root},
				"schema", "diff", "--from", single, "--to", file, "--exclude", "u.c")

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
			c.Assert(result.Stderr, qt.Contains, `warning: cannot drop table "t": table "u" needs it`)
			c.Assert(result.Stdout, qt.Not(qt.Contains), "DROP")
			conn, err := dbschema.ConnectToDatabase(c.Context(), single)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
			replay(c, conn, result.Stdout)
			c.Assert(countOf(c, admin, "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'v' AND COLUMN_NAME = 'w'",
				database), qt.Equals, 1)
			c.Assert(countOf(c, admin, "SELECT COUNT(*) FROM `"+database+"`.t"), qt.Equals, 1)
		})
	}
}

// On a whole server, the table u references is in another database, which the
// desired state does not declare: dropping that database drops the table, and
// the server refuses it with Error 3730.
func TestMySQLFamilyBlockedTableKeepsTheDatabaseOfATableItReferencesE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			// other is created first so that its drop runs last, after the
			// drop of kept removes the key into it.
			_, other, _ := emptyDatabase(c, engine.admin)
			admin, kept, server := emptyDatabase(c, engine.admin)
			for _, statement := range []string{
				"CREATE TABLE `" + other + "`.t (id INT NOT NULL PRIMARY KEY)",
				"CREATE TABLE `" + kept + "`.u (id INT NOT NULL PRIMARY KEY, c INT, t_id INT, " +
					"CONSTRAINT fk_ut FOREIGN KEY (t_id) REFERENCES `" + other + "`.t (id))",
				"CREATE TABLE `" + kept + "`.v (id INT NOT NULL PRIMARY KEY)",
				"INSERT INTO `" + other + "`.t VALUES (1)",
				"INSERT INTO `" + kept + "`.u VALUES (1, 42, 1)",
			} {
				_, err := admin.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil)
			}
			root := c.TempDir()
			file := writeFile(c, root, "schema.sql", kept,
				"CREATE DATABASE `<database>`;\nCREATE TABLE `<database>`.v (id INT NOT NULL PRIMARY KEY, w INT);\n")

			result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: root},
				"schema", "diff", "--from", server, "--to", file, "--schemas", kept+","+other, "--exclude", kept+".u.c")

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
			c.Assert(result.Stderr, qt.Contains, fmt.Sprintf(`warning: cannot drop database %q: table "%s.u" needs it`, other, kept))
			c.Assert(result.Stdout, qt.Not(qt.Contains), "DROP")
			replay(c, admin, result.Stdout)
			c.Assert(countOf(c, admin, "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'v' AND COLUMN_NAME = 'w'",
				kept), qt.Equals, 1)
			c.Assert(countOf(c, admin, "SELECT COUNT(*) FROM `"+other+"`.t"), qt.Equals, 1)
		})
	}
}

// On PostgreSQL the trigger of u runs a function in the schema the run reads,
// and the diff withholds its drop beside the drop of t.
func TestPostgreSQLBlockedTableKeepsWhatItNeedsE2E(t *testing.T) {
	c := qt.New(t)
	db, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(db) })
	schema := scratchPostgresSchema(c, db)
	for _, statement := range []string{
		"CREATE TABLE %s.t (id int PRIMARY KEY)",
		"CREATE TABLE %[1]s.u (id int PRIMARY KEY, c int, t_id int CONSTRAINT fk_ut REFERENCES %[1]s.t (id))",
		"CREATE TABLE %s.v (id int PRIMARY KEY)",
		"CREATE FUNCTION %s.trgfn() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$",
		"CREATE TRIGGER trg BEFORE INSERT ON %[1]s.u FOR EACH ROW EXECUTE FUNCTION %[1]s.trgfn()",
		"INSERT INTO %s.t VALUES (1)",
		"INSERT INTO %s.u VALUES (1, 42, 1)",
	} {
		_, err := db.ExecContext(c.Context(), fmt.Sprintf(statement, schema))
		c.Assert(err, qt.IsNil)
	}
	root := c.TempDir()
	file := writeFile(c, root, "schema.sql", schema,
		"CREATE SCHEMA <database>;\nCREATE TABLE <database>.v (id int PRIMARY KEY, w int);\n")

	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: root}, "schema", "diff",
		"--from", dbtarget.URL(c, dbtarget.PostgreSQL), "--to", file, "--schemas", schema, "--exclude", schema+".u.c")

	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
	c.Assert(result.Stderr, qt.Contains, fmt.Sprintf(`warning: cannot drop table "%[1]s.t": table "%[1]s.u" needs it`, schema))
	c.Assert(result.Stderr, qt.Contains, fmt.Sprintf(`warning: cannot drop function "%[1]s.trgfn": table "%[1]s.u" needs it`, schema))
	c.Assert(result.Stdout, qt.Not(qt.Contains), "DROP")
	replay(c, db, result.Stdout)
	c.Assert(rowValue(c, db, "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = '"+schema+
		"' AND table_name = 'v' AND column_name = 'w'"), qt.Equals, 1)
	c.Assert(rowValue(c, db, "SELECT COUNT(*) FROM "+schema+".t"), qt.Equals, 1)
	c.Assert(rowValue(c, db, "SELECT COUNT(*) FROM pg_proc WHERE proname = 'trgfn' AND pronamespace = '"+schema+"'::regnamespace"), qt.Equals, 1)
}
