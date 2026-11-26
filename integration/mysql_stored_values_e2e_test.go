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
const storedValuesRefusal = `column "n": the server would change a value to fit its column: `

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

// readStoredScalar answers query, which reads one value, on a session of
// database.
func readStoredScalar(c *qt.C, database, query string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), database)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var got string
	c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&got), qt.IsNil)
	return got
}

// storedBodyTable is the table the trigger of [storedBodyChanges] is defined
// on, and storedTriggerMode reads the sql_mode the trigger kept.
const (
	storedBodyTable   = "CREATE TABLE t (id INT PRIMARY KEY, code VARCHAR(5));\n"
	storedTriggerBody = "CREATE TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET NEW.code = CONCAT(NEW.code, '-x');\n"
	storedTriggerMode = "SELECT SQL_MODE FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = DATABASE() AND TRIGGER_NAME = 't_bi'"
)

// storedBodyChanges are objects the server keeps a session's sql_mode with,
// added to table t: the schema that adds one, the query that reads the mode
// it kept, and what column code holds after the application's loose session
// inserts 'abcd'.
var storedBodyChanges = []struct {
	name, desired, mode, code string
}{
	{
		name:    "a trigger",
		desired: storedBodyTable + storedTriggerBody,
		mode:    storedTriggerMode,
		code:    "abcd-",
	},
	{
		name:    "a procedure",
		desired: storedBodyTable + "CREATE PROCEDURE p_one() BEGIN SELECT 1; END;\n",
		mode:    "SELECT SQL_MODE FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_NAME = 'p_one'",
		code:    "abcd",
	},
}

// TestSchemaApplyGivesAStoredBodyTheSessionsModeOnALooseSessionE2E adds each
// object with `ptah schema apply`. The plan runs on a strict session, and the
// statement that defines the object runs under the mode the session had, so
// the object keeps that mode, as it would without the strict session. The
// application's loose session then inserts a string the trigger makes longer
// than the column, and the row stores it cut, rather than the insert failing
// under a strict mode the trigger kept.
func TestSchemaApplyGivesAStoredBodyTheSessionsModeOnALooseSessionE2E(t *testing.T) {
	for _, engine := range storedValueEngines {
		for _, change := range storedBodyChanges {
			t.Run(engine.name+"/"+change.name, func(t *testing.T) {
				c := qt.New(t)
				target := looseStoredValuesTarget(c, engine.engine, storedBodyTable)

				applied := runPtahIn(c, "schema", "apply", "--db-url", target,
					"--schema-file", writeStoredValuesSchema(c, change.desired), "--auto-approve")

				c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
				c.Assert(readStoredScalar(c, target, change.mode), qt.Equals, "NO_ENGINE_SUBSTITUTION")
				execStoredValues(c, target, "INSERT INTO t VALUES (1, 'abcd')")
				c.Assert(readStoredScalar(c, target, "SELECT code FROM t WHERE id = 1"), qt.Equals, change.code)
			})
		}
	}
}

// writeStoredTriggerMigrations writes a directory whose version 1 creates
// table t with trigger t_bi and whose version 2 drops the trigger, and
// returns it. A trigger is a writer Ptah cannot check before it runs, so the
// files run with --tx-mode none, and dropping it is destructive, so they run
// with --allow-destructive.
func writeStoredTriggerMigrations(c *qt.C) string {
	c.Helper()
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	files := map[string]string{
		"0000000000001_create_t.up.sql":       storedBodyTable + storedTriggerBody,
		"0000000000001_create_t.down.sql":     "DROP TABLE t;\n",
		"0000000000002_drop_trigger.up.sql":   "DROP TRIGGER t_bi;\n",
		"0000000000002_drop_trigger.down.sql": storedTriggerBody,
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	return dir
}

// TestMigrationsDownPlanGivesARecreatedTriggerTheSessionsModeE2E rolls
// version 2 back with `ptah migrations down --plan`, which derives a CREATE
// TRIGGER from the database rather than running the down file. The derived
// statement runs on the strict session under the mode the session had, so
// the trigger keeps that mode, and the application's loose session inserts
// through it as it did before version 2. The history defines a trigger, which
// the replay guard refuses on a shadow database the run does not own, so the
// rollback passes --trust-replay-sql, as the operator who wrote it would.
func TestMigrationsDownPlanGivesARecreatedTriggerTheSessionsModeE2E(t *testing.T) {
	for _, engine := range storedValueEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := looseStoredValuesTarget(c, engine.engine, "SELECT 1;")
			_, dev := newMySQLFamilyScratch(c, engine.engine).database(c, "stored_trigger_shadow")
			migrations := writeStoredTriggerMigrations(c)
			up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", migrations,
				"--tx-mode", "none", "--allow-destructive")
			c.Assert(up.ExitCode, qt.Equals, 0, historyComment(up))

			down := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Env: environmentWithoutPtahVariables(), Timeout: 2 * time.Minute},
				"migrations", "down", "--db-url", target, "--migrations-dir", migrations,
				"--target", "1", "--plan", "--shadow-db", dev, "--trust-replay-sql", "--confirm")

			c.Assert(down.ExitCode, qt.Equals, 0, historyComment(down))
			c.Assert(readStoredScalar(c, target, storedTriggerMode), qt.Equals, "NO_ENGINE_SUBSTITUTION")
			execStoredValues(c, target, "INSERT INTO t VALUES (1, 'abcd')")
			c.Assert(readStoredScalar(c, target, "SELECT code FROM t WHERE id = 1"), qt.Equals, "abcd-")
		})
	}
}

// storedValuesTestEntities writes the Go schema `ptah schema test` provisions,
// table c with a VARCHAR(3) column n, and returns its directory.
func storedValuesTestEntities(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	content := `package models

//ptah:schema:table name="c"
type C struct {
	//ptah:schema:field name="id" type="INT" primary="true"
	ID int64

	//ptah:schema:field name="n" type="VARCHAR(3)"
	N string
}
`
	c.Assert(os.WriteFile(filepath.Join(dir, "entity.go"), []byte(content), 0o600), qt.IsNil)
	return dir
}

// storedValuesTestCase writes a schema test case that widens column n, stores
// value, and applies the desired schema again, which narrows the column back,
// and returns its directory.
func storedValuesTestCase(c *qt.C, value string) string {
	c.Helper()
	dir := c.TempDir()
	content := `cases:
  - name: narrow the column again
    steps:
      - name: widen the column
        exec: ALTER TABLE c MODIFY n VARCHAR(10)
      - name: store a value
        exec: INSERT INTO c VALUES (1, '` + value + `')
      - name: apply desired schema
        apply_schema: true
`
	c.Assert(os.WriteFile(filepath.Join(dir, "cases.yaml"), []byte(content), 0o600), qt.IsNil)
	return dir
}

// TestSchemaTestRefusesToRewriteStoredValuesOnALooseSessionE2E runs a case
// whose apply_schema step narrows a column below a string a row holds. The
// step runs Ptah's plan on a strict session, as `schema apply` does, so the
// server refuses the statement, the step fails naming the column, and the row
// keeps its string.
func TestSchemaTestRefusesToRewriteStoredValuesOnALooseSessionE2E(t *testing.T) {
	for _, engine := range storedValueEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := looseStoredValuesTarget(c, engine.engine, "SELECT 1;")

			tested := runPtahIn(c, "schema", "test", "--dir", storedValuesTestCase(c, "abcdefghij"),
				"--root-dir", storedValuesTestEntities(c), "--db-url", target)

			c.Assert(tested.ExitCode, qt.Equals, 1, historyComment(tested))
			c.Assert(tested.Stdout+tested.Stderr, qt.Contains, `step "apply desired schema" — apply_schema failed: `+storedValuesRefusal)
			c.Assert(readStoredScalar(c, target, "SELECT n FROM c WHERE id = 1"), qt.Equals, "abcdefghij")
		})
	}
}

// TestSchemaTestNarrowsAColumnEveryStoredValueFitsOnALooseSessionE2E is the
// control: where the row fits the narrower column, the same step applies on
// the same loose session and the case passes.
func TestSchemaTestNarrowsAColumnEveryStoredValueFitsOnALooseSessionE2E(t *testing.T) {
	for _, engine := range storedValueEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := looseStoredValuesTarget(c, engine.engine, "SELECT 1;")

			tested := runPtahIn(c, "schema", "test", "--dir", storedValuesTestCase(c, "ab"),
				"--root-dir", storedValuesTestEntities(c), "--db-url", target)

			c.Assert(tested.ExitCode, qt.Equals, 0, historyComment(tested))
			c.Assert(readStoredScalar(c, target,
				"SELECT CONCAT(n, ';', COLUMN_TYPE) FROM c, information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'c' AND COLUMN_NAME = 'n'"),
				qt.Equals, "ab;varchar(3)")
		})
	}
}
