//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A migration adds only the privilege a login lacks. Rolling it back keeps
// every privilege the login held before it, including the grant option and a
// column grant. A second account of the same name on another host is separate.
func TestMigrationsMySQLFamilyKeepsPreexistingLoginGrantsE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			database, address := scratch.builtAs(c, "login_grants", "CREATE TABLE items (id INT PRIMARY KEY, value INT);\n")
			user := fmt.Sprintf("ptah_lg_%d", time.Now().UnixNano()%1_000_000_000)
			account := "'" + user + "'@'%'"
			other := "'" + user + "'@'localhost'"
			c.Cleanup(func() {
				_, err := scratch.admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+account+", "+other)
				c.Check(err, qt.IsNil)
			})
			for _, statement := range []string{
				"CREATE USER " + account + ", " + other,
				"GRANT SELECT ON `" + database + "`.items TO " + account + " WITH GRANT OPTION",
				"GRANT UPDATE (value) ON `" + database + "`.items TO " + account,
				"GRANT DELETE ON `" + database + "`.items TO " + other,
			} {
				_, err := scratch.admin.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil)
			}
			held := grantRows(c, scratch.admin, "SHOW GRANTS FOR "+account)
			otherHeld := grantRows(c, scratch.admin, "SHOW GRANTS FOR "+other)
			file := writeDeclaredSchemaFile(c, "login.sql", "CREATE TABLE items (id INT PRIMARY KEY, value INT);\n"+
				"GRANT SELECT ON items TO "+user+" WITH GRANT OPTION;\n"+
				"GRANT UPDATE (value) ON items TO "+user+" WITH GRANT OPTION;\n"+
				"GRANT INSERT ON items TO "+user+" WITH GRANT OPTION;\n")
			dir := c.TempDir()
			options := clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute}

			// Reading a login's grants does not make its privileges managed:
			// a declaration that names no login leaves every grant in place.
			objectsOnly := writeDeclaredSchemaFile(c, "objects.sql", "CREATE TABLE items (id INT PRIMARY KEY, value INT);\n")
			unchanged := clirun.Run(c, clirun.Ptah, options, "schema", "apply", "--db-url", address,
				"--schema-file", objectsOnly, "--dry-run")
			c.Assert(unchanged.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", unchanged.Stdout, unchanged.Stderr))
			c.Assert(unchanged.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			c.Assert(unchanged.Stdout, qt.Not(qt.Contains), "REVOKE ")
			c.Assert(unchanged.Stdout, qt.Not(qt.Contains), "GRANT ")
			c.Assert(grantRows(c, scratch.admin, "SHOW GRANTS FOR "+account), qt.DeepEquals, held)
			c.Assert(grantRows(c, scratch.admin, "SHOW GRANTS FOR "+other), qt.DeepEquals, otherHeld)

			generated := clirun.Run(c, clirun.Ptah, options, "migrations", "generate", "--db-url", address,
				"--schema-file", file, "--migrations-dir", dir, "--name", "login_access")

			c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
			up := generatedStatements(c, dir, "login_access", "up")
			down := generatedStatements(c, dir, "login_access", "down")
			c.Assert(up, qt.Contains, "GRANT INSERT")
			c.Assert(up, qt.Not(qt.Contains), "GRANT SELECT")
			c.Assert(up, qt.Not(qt.Contains), "GRANT UPDATE")
			c.Assert(down, qt.Contains, "REVOKE INSERT")
			c.Assert(down, qt.Not(qt.Contains), "REVOKE SELECT")
			c.Assert(down, qt.Not(qt.Contains), "REVOKE UPDATE")
			applied := clirun.Run(c, clirun.Ptah, options, "migrations", "up", "--db-url", address, "--migrations-dir", dir)
			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(strings.Join(grantRows(c, scratch.admin, "SHOW GRANTS FOR "+account), "\n"), qt.Contains, "INSERT")
			settled := clirun.Run(c, clirun.Ptah, options, "schema", "apply", "--db-url", address, "--schema-file", file, "--dry-run")
			c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
			c.Assert(settled.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			rolledBack := clirun.Run(c, clirun.Ptah, options, "migrations", "down", "--db-url", address,
				"--migrations-dir", dir, "--limit", "1", "--confirm")
			c.Assert(rolledBack.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", rolledBack.Stdout, rolledBack.Stderr))
			c.Assert(grantRows(c, scratch.admin, "SHOW GRANTS FOR "+account), qt.DeepEquals, held)
			c.Assert(grantRows(c, scratch.admin, "SHOW GRANTS FOR "+other), qt.DeepEquals, otherHeld)
		})
	}
}

// A MariaDB role and a login can share a name. A bare declaration grants to
// the role, so the login's SELECT cannot satisfy that declaration.
func TestSchemaApplyMariaDBLoginGrantDoesNotCoverASameNamedRoleE2E(t *testing.T) {
	c := qt.New(t)
	run := newColumnGrantRunFrom(c, dbtarget.MariaDBAdmin, "CREATE TABLE t1 (id INT PRIMARY KEY);\n",
		[]string{"GRANT INSERT ON `%[1]s`.t1 TO %[2]s"})
	account := "'" + run.role + "'@'%'"
	c.Cleanup(func() {
		_, err := run.scratch.admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+account)
		c.Check(err, qt.IsNil)
	})
	_, err := run.scratch.admin.ExecContext(c.Context(), "CREATE USER "+account)
	c.Assert(err, qt.IsNil)
	// #nosec G202 -- identifiers cannot be bound; both names use fixed fixture prefixes and numeric timestamps, and the host is constant.
	_, err = run.scratch.admin.ExecContext(c.Context(), "GRANT SELECT ON `"+run.database+"`.t1 TO "+account)
	c.Assert(err, qt.IsNil)
	held := grantRows(c, run.scratch.admin, "SHOW GRANTS FOR "+account)

	applied := run.apply(c, "CREATE TABLE t1 (id INT PRIMARY KEY);\nCREATE ROLE %[1]s;\n"+
		"GRANT SELECT, INSERT ON t1 TO %[1]s;\n", "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stdout, qt.Contains, "GRANT SELECT")
	c.Assert(grantRows(c, run.scratch.admin, "SHOW GRANTS FOR "+account), qt.DeepEquals, held)
	c.Assert(strings.Join(grantRows(c, run.scratch.admin, "SHOW GRANTS FOR "+run.role), "\n"), qt.Contains, "SELECT, INSERT")
}
