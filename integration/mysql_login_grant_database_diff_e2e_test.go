//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// TestSchemaApplyBetweenMySQLFamilyDatabasesLeavesALoginsGrantsE2E compares a
// database whose tables a login holds privileges on with a database holding
// the same tables and no grant. No read declares a login, so the other side
// lacking its grants says nothing about the login: the diff is synced, the
// apply leaves every grant the login holds, and a description of the first
// database, native or through the compatibility inspect, prints no GRANT to
// it and a note that counts the four grants it leaves out. Without the rule the diff plans a REVOKE of each privilege, and
// the apply takes the login's access away.
func TestSchemaApplyBetweenMySQLFamilyDatabasesLeavesALoginsGrantsE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			const tables = "CREATE TABLE t1 (id INT PRIMARY KEY, v INT);\n"
			held, from := scratch.builtAs(c, "login_from", tables)
			_, to := scratch.builtAs(c, "login_to", tables)
			user := fmt.Sprintf("ptah_ld_%d", time.Now().UnixNano()%1_000_000_000)
			account := "'" + user + "'@'%'"
			c.Cleanup(func() {
				_, err := scratch.admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+account)
				c.Check(err, qt.IsNil)
			})
			for _, statement := range []string{
				"CREATE USER " + account,
				"GRANT SELECT, INSERT ON `" + held + "`.* TO " + account,
				"GRANT SELECT ON `" + held + "`.t1 TO " + account,
				"GRANT UPDATE (v) ON `" + held + "`.t1 TO " + account,
			} {
				_, err := scratch.admin.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil)
			}
			before := grantRows(c, scratch.admin, "SHOW GRANTS FOR "+account)
			options := clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute}

			diff := clirun.Run(c, clirun.Ptah, options, "schema", "diff", "--from", from, "--to", to)
			applied := clirun.Run(c, clirun.Ptah, options, "schema", "apply", "--db-url", from, "--to", to, "--auto-approve")
			read := clirun.Run(c, clirun.Ptah, options, "db", "read", "--db-url", from)
			inspect := clirun.Run(c, clirun.Compat, options, "schema", "inspect", "--url", from)

			c.Assert(diff.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", diff.Stdout, diff.Stderr))
			c.Assert(diff.Stdout, qt.Not(qt.Contains), "REVOKE")
			c.Assert(diff.Stdout, qt.Contains, "Schemas are synced")
			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Not(qt.Contains), "REVOKE")
			c.Assert(grantRows(c, scratch.admin, "SHOW GRANTS FOR "+account), qt.DeepEquals, before)
			c.Assert(read.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", read.Stdout, read.Stderr))
			c.Assert(read.Stdout, qt.Not(qt.Contains), user)
			c.Assert(read.Stderr, qt.Contains,
				"note: grants to logins are not captured, since no statement here creates a login: 4 grants to "+user+"\n")
			c.Assert(inspect.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", inspect.Stdout, inspect.Stderr))
			c.Assert(inspect.Stdout, qt.Not(qt.Contains), user)
			c.Assert(inspect.Stderr, qt.Contains,
				"note: grants to logins are not captured, since no statement here creates a login: 4 grants to "+user+"\n")
		})
	}
}

// TestMigrationsCheckpointLeavesALoginsGrantOutWithANoteE2E replays a history
// that creates a login and grants it SELECT on a table, on a shadow database
// of another server. The checkpoint declares no login, so it carries no GRANT
// to it and names no shadow database, and stderr counts the grant it leaves
// out. Without the note the checkpoint reads as a history that granted
// nothing.
func TestMigrationsCheckpointLeavesALoginsGrantOutWithANoteE2E(t *testing.T) {
	for _, engine := range mysqlDevServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.dev)
			shadowName, shadow := scratch.database(c, "ckp_login")
			user := fmt.Sprintf("ptah_cl_%d", time.Now().UnixNano()%1_000_000_000)
			c.Cleanup(func() {
				_, err := scratch.admin.ExecContext(context.Background(), "DROP USER IF EXISTS '"+user+"'@'%'")
				c.Check(err, qt.IsNil)
			})
			dir := c.TempDir()
			for name, body := range map[string]string{
				"0000000001_base.up.sql": "CREATE TABLE t (id int PRIMARY KEY);\n" +
					"CREATE USER '" + user + "'@'%' ACCOUNT LOCK;\nGRANT SELECT ON t TO '" + user + "'@'%';\n",
				"0000000001_base.down.sql": "DROP USER '" + user + "'@'%';\nDROP TABLE t;\n",
			} {
				c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
			}
			options := clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute, Env: environmentWithoutPtahVariables()}

			got := clirun.Run(c, clirun.Ptah, options, "migrations", "checkpoint", "--migrations-dir", dir,
				"--shadow-db", shadow, "--trust-replay-sql", "--dry-run")

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Contains, "CREATE TABLE `t`")
			c.Assert(got.Stdout, qt.Not(qt.Contains), user)
			c.Assert(got.Stdout, qt.Not(qt.Contains), shadowName)
			c.Assert(got.Stderr, qt.Contains,
				"note: grants to logins are not captured, since no statement here creates a login: 1 grant to "+user+"\n")
		})
	}
}
