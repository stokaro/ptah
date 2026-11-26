//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// `ptah migrations down --plan` applies the rollback it derives on a session
// that refuses a statement it cannot read, and reads the whole plan before the
// first statement is sent. The target's session has NO_BACKSLASH_ESCAPES,
// which names no strict mode, so it reads every statement it runs. The plan
// creates pf_a first, and MySQL runs DDL outside a transaction, so pf_a on the
// target would mean a statement was sent.
//
// The replay guard on the shadow database reads each statement of the history
// by every rule a session can read it by. A trigger keeps its body as written,
// and the rollback recreates it as SHOW CREATE TRIGGER prints it, so a trigger
// the target's session reads differently is one the guard refuses in the
// history before its first statement, trusted or not. The second test pins
// that.
//
// A view is where the two readings part. Its body is one string holding a
// doubled quote, the same string by every rule, and the server prints the body
// back with the quote escaped by a backslash, MySQL and MariaDB alike. The
// CREATE VIEW the plan derives from the shadow database holds 'a\', so on the
// target's session the string ends there, and the executable comment after it
// holds another comment, which the session cannot read.

// storedValuesViewMigrations are a directory whose version 1 creates pf_a and
// the view, and whose version 2 drops them.
var storedValuesViewMigrations = map[string]string{
	"0000000000001_init.up.sql": "-- +ptah no_transaction\n" +
		"CREATE TABLE pf_a (id INT);\n" +
		"CREATE VIEW pf_v AS SELECT 'a'' /*!99999 /* */ */ ' AS code;\n",
	"0000000000001_init.down.sql": "DROP VIEW pf_v;\nDROP TABLE pf_a;\n",
	"0000000000002_drop.up.sql":   "-- +ptah no_transaction\nDROP VIEW pf_v;\nDROP TABLE pf_a;\n",
	"0000000000002_drop.down.sql": "CREATE TABLE pf_a (id INT);\n",
}

// storedValuesTriggerMigrations are a directory whose version 1 creates pf_a,
// pf_t and the trigger, and whose version 2 drops pf_a and the trigger. The
// trigger's body is one string on the server that runs the migration, and by
// the target's rule the executable comment in it is outside the string.
var storedValuesTriggerMigrations = map[string]string{
	"0000000000001_init.up.sql": "-- +ptah no_transaction\n" +
		"CREATE TABLE pf_a (id INT);\n" +
		"CREATE TABLE pf_t (code VARCHAR(40));\n" +
		"CREATE TRIGGER pf_bi BEFORE INSERT ON pf_t FOR EACH ROW SET NEW.code = 'a\\' /*!99999 /* */ */ ';\n",
	"0000000000001_init.down.sql": "DROP TRIGGER pf_bi;\nDROP TABLE pf_t;\nDROP TABLE pf_a;\n",
	"0000000000002_drop.up.sql":   "DROP TRIGGER pf_bi;\nDROP TABLE pf_a;\n",
	"0000000000002_drop.down.sql": "CREATE TABLE pf_a (id INT);\n",
}

// storedValuesDownRun applies migrations to a target on engine and rolls it
// back to version 1 with a plan derived on a shadow database, on a target
// session with NO_BACKSLASH_ESCAPES. extra follows the rollback's arguments.
// It returns the rollback and the target's tables after it.
func storedValuesDownRun(c *qt.C, engine dbtarget.Engine, migrations map[string]string, extra ...string) (clirun.Result, string) {
	c.Helper()
	scratch := newMySQLFamilyScratch(c, engine)
	target, targetURL := scratch.database(c, "pfd_target")
	_, devURL := scratch.database(c, "pfd_dev")
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	for name, body := range migrations {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	run := clirun.Options{Dir: c.TempDir(), Env: environmentWithoutPtahVariables(), Timeout: 2 * time.Minute}
	up := clirun.Run(c, clirun.Ptah, run, "migrations", "up", "--db-url", targetURL, "--migrations-dir", dir, "--allow-destructive")
	c.Assert(up.ExitCode, qt.Equals, 0, historyComment(up))

	args := append([]string{"migrations", "down",
		"--db-url", withURLParameter(targetURL, "sql_mode", "%27NO_BACKSLASH_ESCAPES%27"), "--migrations-dir", dir,
		"--target", "1", "--plan", "--shadow-db", devURL, "--confirm"}, extra...)
	down := clirun.Run(c, clirun.Ptah, run, args...)
	return down, scratch.tablesOf(c, target)
}

func TestMigrationsDownPlanRefusesAnUnreadableRollbackBeforeSendingE2E(t *testing.T) {
	for _, engine := range mysqlCommentEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)

			down, tables := storedValuesDownRun(c, engine.engine, storedValuesViewMigrations)

			c.Assert(down.ExitCode, qt.Not(qt.Equals), 0, historyComment(down))
			c.Assert(tables, qt.Equals, "schema_migrations,schema_migrations_log")
			c.Assert(down.Stderr, qt.Contains, "apply derived rollback statements: statement 2 of 2: "+
				"read whether the statement defines an object that keeps the session's settings")
			c.Assert(down.Stderr, qt.Contains, "it holds another comment")
		})
	}
}

func TestMigrationsDownPlanRefusesAnExecutableCommentInTheReplayE2E(t *testing.T) {
	for _, engine := range mysqlCommentEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)

			down, tables := storedValuesDownRun(c, engine.engine, storedValuesTriggerMigrations, "--trust-replay-sql")

			c.Assert(down.ExitCode, qt.Not(qt.Equals), 0, historyComment(down))
			c.Assert(tables, qt.Equals, "pf_t,schema_migrations,schema_migrations_log")
			c.Assert(down.Stderr, qt.Contains, "failed to validate migration SQL in 0000000000001_init.up.sql: ")
			c.Assert(down.Stderr, qt.Contains, " migration replay rejects executable comment "+
				"because its effects cannot be confined to the disposable database realm")
		})
	}
}
