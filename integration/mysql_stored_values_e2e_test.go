//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A MySQL-family session whose sql_mode names no strict mode lets an ALTER
// TABLE rewrite the rows a column change no longer fits, and the statement
// succeeds: a removed ENUM member becomes '', a removed SET member leaves the
// set, and a narrowed VARCHAR cuts its strings. These runs drive the shipped
// binaries against such a session and read the rows back.
//
// Each session takes the loose mode from its address, as every session of a
// server whose global mode it is would. Setting the global mode would reach
// every other test that shares the server.

// storedValueEngines are the MySQL-family servers a loose session is opened
// on.
var storedValueEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "mysql", engine: dbtarget.MySQLAdmin},
	{name: "mariadb", engine: dbtarget.MariaDBAdmin},
}

// looseSQLMode is a sql_mode that names no strict mode, as the expression the
// driver sets a session's mode to.
const looseSQLMode = "'NO_ENGINE_SUBSTITUTION'"

// storedValuesRead reads back each row of table c and the type of its column
// n.
const storedValuesRead = "SELECT CONCAT_WS(';', (SELECT GROUP_CONCAT(CONCAT(id, '=', QUOTE(n)) ORDER BY id) FROM c), " +
	"(SELECT COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'c' AND COLUMN_NAME = 'n'))"

// storedValuesRefusal is what a refused run says about the change.
const storedValuesRefusal = `column "n": the change would rewrite stored values: `

// looseStoredValuesTarget creates a database on engine's server, runs live
// on it as the server receives it, and returns an address whose sessions are
// loose, after checking that the mode reached a session.
func looseStoredValuesTarget(c *qt.C, engine dbtarget.Engine, live string) string {
	c.Helper()
	_, built := newMySQLFamilyScratch(c, engine).builtFrom(c, live)
	target := withSQLMode(c, built, looseSQLMode)
	c.Assert(sessionSQLMode(c, target), qt.Equals, "NO_ENGINE_SUBSTITUTION")
	return target
}

// readStoredValues answers [storedValuesRead] on a session of database.
func readStoredValues(c *qt.C, database string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), database)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var got string
	c.Assert(conn.QueryRowContext(c.Context(), storedValuesRead).Scan(&got), qt.IsNil)
	return got
}

// execStoredValues runs statement on a session of database.
func execStoredValues(c *qt.C, database, statement string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), database)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), statement)
	c.Assert(err, qt.IsNil)
}

// writeStoredValuesSchema writes text to a schema file of its own and returns
// its path.
func writeStoredValuesSchema(c *qt.C, text string) string {
	c.Helper()
	file := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(file, []byte(text), 0o600), qt.IsNil)
	return file
}

// storedValueRewrites are changes to column n that a row of table c does not
// fit.
var storedValueRewrites = []struct {
	name, live, desired, want string
}{
	{
		name:    "an ENUM member a row holds",
		live:    "CREATE TABLE c (id INT PRIMARY KEY, n ENUM('happy','sad','ok')); INSERT INTO c VALUES (1, 'ok'), (2, 'happy');",
		desired: "CREATE TABLE c (id INT PRIMARY KEY, n ENUM('happy','sad'));\n",
		want:    "1='ok',2='happy';enum('happy','sad','ok')",
	},
	{
		name:    "a SET member a row holds",
		live:    "CREATE TABLE c (id INT PRIMARY KEY, n SET('a','b','c')); INSERT INTO c VALUES (1, 'a,c'), (2, 'b');",
		desired: "CREATE TABLE c (id INT PRIMARY KEY, n SET('a','b'));\n",
		want:    "1='a,c',2='b';set('a','b','c')",
	},
	{
		name:    "a VARCHAR narrower than a row's string",
		live:    "CREATE TABLE c (id INT PRIMARY KEY, n VARCHAR(10)); INSERT INTO c VALUES (1, 'abcdefghij'), (2, 'ab');",
		desired: "CREATE TABLE c (id INT PRIMARY KEY, n VARCHAR(3));\n",
		want:    "1='abcdefghij',2='ab';varchar(10)",
	},
}

// TestSchemaApplyRefusesToRewriteStoredValuesOnALooseSessionE2E applies each
// change with `ptah schema apply`. The plan runs on a strict session, so the
// server refuses the statement, the run names the column and exits 2, and
// every row and the column's type read back as they were.
func TestSchemaApplyRefusesToRewriteStoredValuesOnALooseSessionE2E(t *testing.T) {
	for _, engine := range storedValueEngines {
		for _, change := range storedValueRewrites {
			t.Run(engine.name+"/"+change.name, func(t *testing.T) {
				c := qt.New(t)
				target := looseStoredValuesTarget(c, engine.engine, change.live)

				applied := runPtahIn(c, "schema", "apply", "--db-url", target,
					"--schema-file", writeStoredValuesSchema(c, change.desired), "--auto-approve")

				c.Assert(applied.ExitCode, qt.Equals, 2, historyComment(applied))
				c.Assert(applied.Stderr, qt.Contains, storedValuesRefusal)
				c.Assert(readStoredValues(c, target), qt.Equals, change.want)
			})
		}
	}
}

// TestCompatSchemaApplyRefusesToRewriteStoredValuesOnALooseSessionE2E is the
// same refusal through `ptah-compat schema apply`, which runs the plan through
// the same path, and exits 1 as the community binary exits on an error.
func TestCompatSchemaApplyRefusesToRewriteStoredValuesOnALooseSessionE2E(t *testing.T) {
	for _, engine := range storedValueEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			change := storedValueRewrites[0]
			target := looseStoredValuesTarget(c, engine.engine, change.live)
			_, dev := newMySQLFamilyScratch(c, engine.engine).database(c, "stored_values_dev")

			applied := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()},
				"schema", "apply", "--url", target, "--to", "file://"+filepath.ToSlash(writeStoredValuesSchema(c, change.desired)),
				"--dev-url", dev, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 1, historyComment(applied))
			c.Assert(applied.Stderr, qt.Contains, storedValuesRefusal)
			c.Assert(readStoredValues(c, target), qt.Equals, change.want)
		})
	}
}

// TestSchemaApplyChangesAColumnEveryStoredValueFitsOnALooseSessionE2E is the control
// for the refusals above: where every row fits the new definition, the same
// change applies on the same loose session, and the rows read back as they
// were under the new type. The strict session refuses what would rewrite a
// row, not the change.
func TestSchemaApplyChangesAColumnEveryStoredValueFitsOnALooseSessionE2E(t *testing.T) {
	changes := []struct {
		name, live, desired, want string
	}{
		{
			name:    "an ENUM member no row holds",
			live:    "CREATE TABLE c (id INT PRIMARY KEY, n ENUM('happy','sad','ok')); INSERT INTO c VALUES (2, 'happy');",
			desired: storedValueRewrites[0].desired,
			want:    "2='happy';enum('happy','sad')",
		},
		{
			name:    "a SET member no row holds",
			live:    "CREATE TABLE c (id INT PRIMARY KEY, n SET('a','b','c')); INSERT INTO c VALUES (1, 'a,b'), (2, 'b');",
			desired: storedValueRewrites[1].desired,
			want:    "1='a,b',2='b';set('a','b')",
		},
		{
			name:    "a VARCHAR every row's string fits",
			live:    "CREATE TABLE c (id INT PRIMARY KEY, n VARCHAR(10)); INSERT INTO c VALUES (1, 'abc'), (2, 'ab');",
			desired: storedValueRewrites[2].desired,
			want:    "1='abc',2='ab';varchar(3)",
		},
	}
	for _, engine := range storedValueEngines {
		for _, change := range changes {
			t.Run(engine.name+"/"+change.name, func(t *testing.T) {
				c := qt.New(t)
				target := looseStoredValuesTarget(c, engine.engine, change.live)

				applied := runPtahIn(c, "schema", "apply", "--db-url", target,
					"--schema-file", writeStoredValuesSchema(c, change.desired), "--auto-approve")

				c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
				c.Assert(readStoredValues(c, target), qt.Equals, change.want)
			})
		}
	}
}

// writeStoredValuesMigrations writes a directory whose version 1 creates
// table c and whose version 2 adds the ENUM member 'ok' to column n, and
// returns it.
func writeStoredValuesMigrations(c *qt.C) string {
	c.Helper()
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	files := map[string]string{
		"0000000000001_create_c.up.sql":   "CREATE TABLE c (id INT PRIMARY KEY, n ENUM('happy','sad'));\n",
		"0000000000001_create_c.down.sql": "DROP TABLE c;\n",
		"0000000000002_add_ok.up.sql":     "ALTER TABLE c MODIFY COLUMN n ENUM('happy','sad','ok');\n",
		"0000000000002_add_ok.down.sql":   "ALTER TABLE c MODIFY COLUMN n ENUM('happy','sad');\n",
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	return dir
}

// TestMigrationsDownPlanRefusesToRewriteStoredValuesOnALooseSessionE2E rolls
// version 2 back with `ptah migrations down --plan`, which derives the
// rollback from the database rather than running the down file. The derived
// statement is Ptah's, so it runs on a strict session as a `schema apply` plan
// does, and a row that holds 'ok' keeps it. The up files ran on the loose
// session as written.
func TestMigrationsDownPlanRefusesToRewriteStoredValuesOnALooseSessionE2E(t *testing.T) {
	for _, engine := range storedValueEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := looseStoredValuesTarget(c, engine.engine, "SELECT 1;")
			_, dev := newMySQLFamilyScratch(c, engine.engine).database(c, "stored_values_shadow")
			migrations := writeStoredValuesMigrations(c)
			up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", migrations)
			c.Assert(up.ExitCode, qt.Equals, 0, historyComment(up))
			execStoredValues(c, target, "INSERT INTO c VALUES (1, 'ok'), (2, 'happy')")

			down := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Env: environmentWithoutPtahVariables(), Timeout: 2 * time.Minute},
				"migrations", "down", "--db-url", target, "--migrations-dir", migrations,
				"--target", "1", "--plan", "--shadow-db", dev, "--confirm")

			c.Assert(down.ExitCode, qt.Equals, 2, historyComment(down))
			c.Assert(down.Stderr, qt.Contains, storedValuesRefusal)
			c.Assert(readStoredValues(c, target), qt.Equals, "1='ok',2='happy';enum('happy','sad','ok')")
		})
	}
}
