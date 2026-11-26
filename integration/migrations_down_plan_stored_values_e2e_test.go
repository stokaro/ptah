//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// `ptah migrations down --plan` applies the rollback it derives on a session
// that refuses a statement it cannot read, and reads the whole plan before
// the first statement is sent. The trigger version 1 defines is one string on
// the server that runs the migration. The target's session has
// NO_BACKSLASH_ESCAPES, which names no strict mode, so it reads every
// statement it runs: there the string ends at its second quote, and the
// executable comment after it holds another comment, which the session cannot
// read. The plan creates pf_a first, and MySQL runs DDL outside a
// transaction, so without the read pf_a is on the target after the refusal.
//
// The rollback recreates the trigger with the body the shadow database
// stored, as SHOW CREATE TRIGGER prints it. information_schema reports that
// body rewritten: MariaDB doubles the escaped quote, which is one string under
// either rule, so without SHOW CREATE the MariaDB row sends both statements.

// storedValuesDownMigrations are a directory whose version 1 creates pf_a,
// pf_t and the trigger, and whose version 2 drops pf_a and the trigger.
var storedValuesDownMigrations = map[string]string{
	"0000000000001_init.up.sql": "-- +ptah no_transaction\n" +
		"CREATE TABLE pf_a (id INT);\n" +
		"CREATE TABLE pf_t (code VARCHAR(40));\n" +
		"CREATE TRIGGER pf_bi BEFORE INSERT ON pf_t FOR EACH ROW SET NEW.code = 'a\\' /*!99999 /* */ */ ';\n",
	"0000000000001_init.down.sql": "DROP TRIGGER pf_bi;\nDROP TABLE pf_t;\nDROP TABLE pf_a;\n",
	"0000000000002_drop.up.sql":   "DROP TRIGGER pf_bi;\nDROP TABLE pf_a;\n",
	"0000000000002_drop.down.sql": "CREATE TABLE pf_a (id INT);\n",
}

func TestMigrationsDownPlanRefusesAnUnreadableRollbackBeforeSendingE2E(t *testing.T) {
	for _, engine := range mysqlCommentEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			target, targetURL := scratch.database(c, "pfd_target")
			_, devURL := scratch.database(c, "pfd_dev")
			dir := filepath.Join(c.TempDir(), "migrations")
			c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
			for name, body := range storedValuesDownMigrations {
				c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
			}
			run := clirun.Options{Dir: c.TempDir(), Env: environmentWithoutPtahVariables(), Timeout: 2 * time.Minute}
			up := clirun.Run(c, clirun.Ptah, run, "migrations", "up", "--db-url", targetURL, "--migrations-dir", dir, "--allow-destructive")
			c.Assert(up.ExitCode, qt.Equals, 0, historyComment(up))
			c.Assert(scratch.tablesOf(c, target), qt.Equals, "pf_t,schema_migrations,schema_migrations_log")

			down := clirun.Run(c, clirun.Ptah, run, "migrations", "down",
				"--db-url", withURLParameter(targetURL, "sql_mode", "%27NO_BACKSLASH_ESCAPES%27"), "--migrations-dir", dir,
				"--target", "1", "--plan", "--shadow-db", devURL, "--confirm")

			c.Assert(down.ExitCode, qt.Not(qt.Equals), 0, historyComment(down))
			c.Assert(scratch.tablesOf(c, target), qt.Equals, "pf_t,schema_migrations,schema_migrations_log")
			c.Assert(down.Stderr, qt.Contains, "apply derived rollback statements: statement 2 of 2: "+
				"read whether the statement defines an object that keeps the session's settings")
			c.Assert(down.Stderr, qt.Contains, "it holds another comment")
		})
	}
}
