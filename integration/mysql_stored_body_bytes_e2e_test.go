//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// information_schema reports a trigger's or a routine's body with each string
// literal rewritten: on MySQL 'a\'b' reads back as 'a'b', and on MariaDB
// 'c\\d' reads back as 'c\d'. Ptah reads such a body from SHOW CREATE, so a
// body compared with its declaration matches, and a body created again from
// the read is the body that was written.

// escapedBodySchema declares triggers and a function whose bodies hold what
// information_schema rewrites: escapes in a string, a backquote in a quoted
// name, and the character set introducer of a hex literal.
const escapedBodySchema = "CREATE TABLE t (id int PRIMARY KEY, code varchar(100), `a``b` int);\n" +
	"CREATE TRIGGER trg_escapes BEFORE INSERT ON t FOR EACH ROW SET NEW.code = CONCAT('a\\'b', \"c\\\"d\", 'e\\\\f', 'g\\qh');\n" +
	"CREATE TRIGGER trg_quoted_name BEFORE UPDATE ON t FOR EACH ROW SET NEW.`a``b` = 1;\n" +
	"CREATE TABLE t2 (id int PRIMARY KEY, c varchar(10));\n" +
	"CREATE TRIGGER trg_latin1 BEFORE INSERT ON t2 FOR EACH ROW SET NEW.c = _latin1 0xE9;\n" +
	"CREATE FUNCTION f_escapes() RETURNS varchar(100) DETERMINISTIC RETURN 'a\\'b\\\\c';\n"

// TestMigrationsGenerateMatchesAnEscapedBodyE2E generates a migration from
// the schema for an empty database, runs it, and generates again: the bodies
// read back are the declared ones, so nothing is planned.
func TestMigrationsGenerateMatchesAnEscapedBodyE2E(t *testing.T) {
	for _, engine := range mysqlCommentEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			_, url := scratch.database(c, "escaped_body")
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(escapedBodySchema), 0o600), qt.IsNil)
			migrations := filepath.Join(c.TempDir(), "migrations")

			runPtahNative(c, "migrations", "generate", "--db-url", url, "--schema-file", schema,
				"--migrations-dir", migrations, "--name", "init")
			out, err := runPtahNativeWithError("migrations", "up", "--db-url", url, "--migrations-dir", migrations)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			again, err := runPtahNativeWithError("migrations", "generate", "--db-url", url, "--schema-file", schema,
				"--migrations-dir", migrations, "--name", "again")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", again))
			c.Assert(again, qt.Contains, "Schema is synced")
		})
	}
}

// escapedBodyBody is the body version 1 of [escapedBodyDownMigrations] gives
// trg_escapes, and escapedBodyValue the value it writes.
const (
	escapedBodyBody  = `SET NEW.code = CONCAT('a\'b', 'c\\d', 'e\qf', "g\"h", _latin1 0xE9)`
	escapedBodyValue = `a'bc\deqfg"hé`
)

// escapedBodyDownMigrations are a directory whose version 1 creates t and
// trg_escapes and whose version 2 drops the trigger. Version 2 has no down
// file of its own to recreate it, so a rollback derives it from version 1.
var escapedBodyDownMigrations = map[string]string{
	"0000000000001_init.up.sql": "-- +ptah no_transaction\nCREATE TABLE t (code VARCHAR(100));\n" +
		"CREATE TRIGGER trg_escapes BEFORE INSERT ON t FOR EACH ROW " + escapedBodyBody + ";\n",
	"0000000000001_init.down.sql": "DROP TRIGGER trg_escapes;\nDROP TABLE t;\n",
	"0000000000002_drop.up.sql":   "DROP TRIGGER trg_escapes;\n",
	"0000000000002_drop.down.sql": "SELECT 1;\n",
}

// TestMigrationsDownPlanRecreatesAnEscapedBodyE2E rolls version 2 back with
// a rollback `ptah migrations down --plan` derives from the shadow replay of
// version 1. The trigger it recreates has the body version 1 wrote, and
// writes the value that body computes.
func TestMigrationsDownPlanRecreatesAnEscapedBodyE2E(t *testing.T) {
	for _, engine := range mysqlCommentEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			target, targetURL := scratch.database(c, "escd_target")
			_, devURL := scratch.database(c, "escd_dev")
			dir := filepath.Join(c.TempDir(), "migrations")
			c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
			for name, body := range escapedBodyDownMigrations {
				c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
			}
			run := clirun.Options{Dir: c.TempDir(), Env: environmentWithoutPtahVariables(), Timeout: 2 * time.Minute}
			up := clirun.Run(c, clirun.Ptah, run, "migrations", "up", "--db-url", targetURL, "--migrations-dir", dir, "--allow-destructive")
			c.Assert(up.ExitCode, qt.Equals, 0, historyComment(up))

			// The history creates a trigger, which the replay guard refuses on
			// a shadow database the run does not own unless the SQL is
			// trusted. The test is about the body the plan recreates.
			down := clirun.Run(c, clirun.Ptah, run, "migrations", "down", "--db-url", targetURL, "--migrations-dir", dir,
				"--target", "1", "--plan", "--shadow-db", devURL, "--trust-replay-sql", "--confirm")

			c.Assert(down.ExitCode, qt.Equals, 0, historyComment(down))
			c.Assert(showCreateTrigger(c, scratch.admin, target, "trg_escapes"), qt.Matches,
				`(?s).* FOR EACH ROW `+regexp.QuoteMeta(escapedBodyBody))
			c.Assert(firedCode(c, scratch.admin, target), qt.Equals, escapedBodyValue)
		})
	}
}

// TestDbReadReadsEveryBodyOnAConnectionLimitedAccountE2E reads a database of
// 40 triggers as an account limited to one or two sessions at once. The read
// succeeds and prints every trigger with its own body.
func TestDbReadReadsEveryBodyOnAConnectionLimitedAccountE2E(t *testing.T) {
	for _, engine := range mysqlCommentEngines {
		for _, limit := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s/%d", engine.name, limit), func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine.engine)
				database, url := scratch.database(c, "limited_read")
				want := fortyTriggers(c, scratch, database)
				user, password := limitedMySQLUser(c, scratch, limit)

				out, err := runPtahNativeWithError("db", "read", "--db-url", replaceMySQLCredentials(c, url, user, password))

				c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
				for _, statement := range want {
					c.Assert(out, qt.Contains, statement)
				}
			})
		}
	}
}

// TestSchemaDiffComparesTwoDatabasesOnAConnectionLimitedAccountE2E compares
// two databases of 40 triggers each as an account limited to two sessions at
// once. The diff reads --from on a pool of its own and keeps it open while
// --to connects, so a --from read that left its pool more sessions than one
// would have the --to side refused. The diff succeeds and finds them equal.
func TestSchemaDiffComparesTwoDatabasesOnAConnectionLimitedAccountE2E(t *testing.T) {
	for _, engine := range mysqlCommentEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			from, fromURL := scratch.database(c, "limited_from")
			to, toURL := scratch.database(c, "limited_to")
			fortyTriggers(c, scratch, from)
			fortyTriggers(c, scratch, to)
			user, password := limitedMySQLUser(c, scratch, 2)

			out, err := runPtahNativeWithError("schema", "diff",
				"--from", replaceMySQLCredentials(c, fromURL, user, password),
				"--to", replaceMySQLCredentials(c, toURL, user, password),
			)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "Schemas are synced, no changes to be made.")
		})
	}
}

// fortyTriggers creates 40 triggers in database, two on each table, each with
// a body of its own, and returns the statement db read prints for each.
func fortyTriggers(c *qt.C, scratch mysqlScratch, database string) []string {
	c.Helper()
	var want []string
	for index := range 40 {
		table, trigger := fmt.Sprintf("t_%02d", index/2), fmt.Sprintf("tr_%02d", index)
		body := fmt.Sprintf(`SET NEW.code = 'body \\%d'`, index)
		statements := []string{
			"CREATE TABLE IF NOT EXISTS `" + database + "`." + table + " (code varchar(50))",
			"CREATE TRIGGER `" + database + "`." + trigger + " BEFORE INSERT ON `" + database + "`." + table + " FOR EACH ROW " + body,
		}
		for _, statement := range statements {
			_, err := scratch.admin.ExecContext(c.Context(), statement)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
		}
		want = append(want, "CREATE TRIGGER `"+trigger+"` BEFORE INSERT ON `"+table+"` FOR EACH ROW "+body+";")
	}
	return want
}

// limitedMySQLUser creates an account that may hold limit sessions at once and
// read everything, drops it when the test ends, and returns its name and
// password.
func limitedMySQLUser(c *qt.C, scratch mysqlScratch, limit int) (user, password string) {
	c.Helper()
	user, password = fmt.Sprintf("ptah_lim_%d_%d", limit, time.Now().UnixNano()%1_000_000_000), "limited-Account-pw"
	for _, statement := range []string{
		fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY '%s' WITH MAX_USER_CONNECTIONS %d", user, password, limit),
		fmt.Sprintf("GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%'", user),
	} {
		_, err := scratch.admin.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	c.Cleanup(func() { dropMySQLUser(c, context.Background(), scratch.admin, user) })
	return user, password
}

// showCreateTrigger returns the statement SHOW CREATE TRIGGER prints for
// trigger in database.
func showCreateTrigger(c *qt.C, admin *sql.DB, database, trigger string) string {
	c.Helper()
	var name, mode, statement string
	var rest [4]sql.NullString
	err := admin.QueryRowContext(c.Context(), "SHOW CREATE TRIGGER `"+database+"`.`"+trigger+"`").
		Scan(&name, &mode, &statement, &rest[0], &rest[1], &rest[2], &rest[3])
	c.Assert(err, qt.IsNil)
	return statement
}

// firedCode inserts a row into t of database and returns the code the
// trigger wrote.
func firedCode(c *qt.C, admin *sql.DB, database string) string {
	c.Helper()
	// #nosec G202 -- database is a name the scratch helper generated.
	_, err := admin.ExecContext(c.Context(), "INSERT INTO `"+database+"`.t (code) VALUES ('')")
	c.Assert(err, qt.IsNil)
	var code string
	c.Assert(admin.QueryRowContext(c.Context(), "SELECT code FROM `"+database+"`.t").Scan(&code), qt.IsNil)
	return code
}
