//go:build integration

package integration_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// A reader that sees mysql.user but lacks SELECT on mysql.* gets no rows for
// another role from the privilege views. Its declared revoke must still reach
// the server, which refuses a table revoke of access granted on a database.
func TestSchemaApplyMySQLFamilyHiddenGrantsE2E(t *testing.T) {
	const schema = "CREATE TABLE t1 (id INT PRIMARY KEY);\n"
	const desired = schema + "REVOKE SELECT ON t1 FROM %[1]s;\n"
	tests := []struct {
		name      string
		cut       int
		wildcard  string
		grantRead []string
	}{
		{name: "database grant"},
		{name: "pattern grant", cut: 4, wildcard: "%"},
		{name: "database grant with separate table privileges", grantRead: []string{
			"SELECT ON mysql.db", "SELECT ON mysql.tables_priv", "SELECT ON mysql.columns_priv",
		}},
		{name: "pattern grant with separate table privileges", cut: 4, wildcard: "%", grantRead: []string{
			"SELECT ON mysql.db", "SELECT ON mysql.tables_priv", "SELECT ON mysql.columns_priv",
		}},
	}
	for _, engine := range mysqlServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newColumnGrantRunFrom(c, engine.admin, schema, nil)
				pattern := run.database[:len(run.database)-test.cut] + test.wildcard
				grantOnPattern(c, run, pattern)
				catalogGrants := append(grantVisibilityCatalogGrants(engine.admin), test.grantRead...)
				account := routineAccount(c, c.Context(), run.scratch.admin, engine.admin,
					run.database, "ALL PRIVILEGES", " WITH GRANT OPTION", catalogGrants...)
				run.url = replaceMySQLCredentials(c, run.url, account, "Rt-pass-9")

				planned := run.apply(c, desired, "--dry-run")

				c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
				c.Assert(planned.Stdout, qt.Contains, "REVOKE SELECT ON")
				c.Assert(planned.Stdout, qt.Not(qt.Contains), "Schema is synced")
				applied := run.apply(c, desired, "--auto-approve")
				c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(applied.Stderr, qt.Contains, "1147")
				c.Assert(showGrants(c, run), qt.Contains, "GRANT SELECT ON `"+pattern+"`.* TO `"+run.role+"`")
				_, err := run.member(c).ExecContext(c.Context(), "SELECT id FROM t1")
				c.Assert(err, qt.IsNil)

				// Opening the whole mysql schema makes the covering grant
				// visible, so the same declaration is refused before execution.
				_, err = run.scratch.admin.ExecContext(c.Context(), fmt.Sprintf("GRANT SELECT ON mysql.* TO '%s'@'%%'", account))
				c.Assert(err, qt.IsNil)
				visible := run.apply(c, desired, "--dry-run")
				c.Assert(visible.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", visible.Stdout, visible.Stderr))
				c.Assert(visible.Stderr, qt.Contains, "which covers table t1")
				c.Assert(visible.Stderr, qt.Contains, "so a revoke on the table cannot take it")
			})
		}
	}
}

// A table grant hidden by the same catalog filtering is removable. The
// statement must reach the server and remove the member's access.
func TestSchemaApplyMySQLFamilyHiddenTableGrantE2E(t *testing.T) {
	const schema = "CREATE TABLE t1 (id INT PRIMARY KEY);\n"
	const desired = schema + "REVOKE SELECT ON t1 FROM %[1]s;\n"
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newColumnGrantRunFrom(c, engine.admin, schema,
				[]string{"GRANT SELECT, INSERT ON `%[1]s`.t1 TO %[2]s"})
			account := routineAccount(c, c.Context(), run.scratch.admin, engine.admin,
				run.database, "ALL PRIVILEGES", " WITH GRANT OPTION", grantVisibilityCatalogGrants(engine.admin)...)
			run.url = replaceMySQLCredentials(c, run.url, account, "Rt-pass-9")
			member := run.member(c)
			_, err := member.ExecContext(c.Context(), "SELECT id FROM t1")
			c.Assert(err, qt.IsNil)

			applied := run.apply(c, desired, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Contains, "REVOKE SELECT ON")
			c.Assert(run.privileges(c), qt.DeepEquals, []string{"table t1 INSERT NO"})
			_, err = member.ExecContext(c.Context(), "SELECT id FROM t1")
			c.Assert(err, qt.ErrorMatches, `(?s).*command denied to user.*`)
		})
	}
}

// grantVisibilityCatalogGrants lets the reader list roles and memberships,
// without the database-wide privilege the information_schema views require.
func grantVisibilityCatalogGrants(engine dbtarget.Engine) []string {
	return map[dbtarget.Engine][]string{
		dbtarget.MySQLAdmin: {
			"SELECT ON mysql.user", "SELECT ON mysql.role_edges", "SELECT ON mysql.default_roles",
		},
		dbtarget.MariaDBAdmin: {"SELECT ON mysql.user", "SELECT ON mysql.roles_mapping"},
	}[engine]
}
