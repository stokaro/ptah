//go:build integration

package integration_test

import (
	"context"
	"fmt"
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
// database prints no GRANT to it. Without the rule the diff plans a REVOKE of
// each privilege, and the apply takes the login's access away.
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

			c.Assert(diff.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", diff.Stdout, diff.Stderr))
			c.Assert(diff.Stdout, qt.Not(qt.Contains), "REVOKE")
			c.Assert(diff.Stdout, qt.Contains, "Schemas are synced")
			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Not(qt.Contains), "REVOKE")
			c.Assert(grantRows(c, scratch.admin, "SHOW GRANTS FOR "+account), qt.DeepEquals, before)
			c.Assert(read.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", read.Stdout, read.Stderr))
			c.Assert(read.Stdout, qt.Not(qt.Contains), user)
		})
	}
}
