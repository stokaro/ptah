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
	"ptah.run/internal/dbtarget"
)

// A MySQL-family role Ptah created compares equal to its declaration on the
// next run, and a role declared not to inherit is refused before anything is
// created (stokaro/ptah#3890).
//
// A role here always passes on the privileges of the roles granted to it, and
// there is no NOINHERIT. Every schema source declares a role as inheriting when
// the author says nothing, so the read has to say the same: a read that
// reported false would stop the second command after `schema apply` on an
// ALTER ROLE this family does not have, for a role Ptah had just created.

// roleInheritEngines are the two engines of the family, reached with an account
// that may create a database and a role.
var roleInheritEngines = []struct {
	name   string
	engine dbtarget.Engine
	scheme string
}{
	{name: "mysql", engine: dbtarget.MySQLAdmin, scheme: "mysql"},
	{name: "mariadb", engine: dbtarget.MariaDBAdmin, scheme: "mariadb"},
}

// roleInheritTarget creates an empty database and names a role nothing holds
// yet. It drops both when the test ends, because a role is server-wide and
// would outlive the database.
func roleInheritTarget(c *qt.C, engine dbtarget.Engine, scheme string) (server mysqlFamilyServer, target, role string) {
	c.Helper()
	server = newMySQLFamilyServer(c, engine)
	database := server.database(c, "role_inherit")
	role = fmt.Sprintf("ptah_inherit_%d", time.Now().UnixNano()%1_000_000_000)
	c.Cleanup(func() {
		_, err := server.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS `"+role+"`")
		c.Check(err, qt.IsNil)
	})
	return server, server.url(scheme, database), role
}

// roleExists reports whether the server holds a principal of that name.
func roleExists(c *qt.C, server mysqlFamilyServer, role string) bool {
	c.Helper()
	var count int
	err := server.admin.QueryRowContext(c.Context(),
		"SELECT COUNT(*) FROM mysql.user WHERE user = ?", role).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count > 0
}

// writeRoleSources writes the role and a table in each form a declaration can
// take, into a fresh working directory: role.sql, and the package under
// entities/. inherit is the annotation's attribute, empty for none; a SQL
// CREATE ROLE here takes a name and nothing else, so the file cannot say it.
func writeRoleSources(c *qt.C, role, inherit string) string {
	c.Helper()
	workDir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(workDir, "role.sql"), []byte(
		"CREATE ROLE "+role+";\nCREATE TABLE orders (id bigint PRIMARY KEY);\n"), 0o600), qt.IsNil)

	entities := filepath.Join(workDir, "entities")
	c.Assert(os.MkdirAll(entities, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(entities, "access.go"), []byte(`package entities

//ptah:schema:role name="`+role+`"`+inherit+`
//ptah:schema:table name="orders"
type Order struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
}
`), 0o600), qt.IsNil)
	return workDir
}

// TestSchemaCompareConvergesOnAnAppliedMySQLFamilyRoleE2E applies a declared
// role and runs the three commands that plan from a comparison against the
// result. Each must find nothing to do; a read of false stops all three with
// "declares an altered attribute".
//
// The role is created by the apply rather than by the fixture, so the test also
// holds the reader to what Ptah itself writes.
func TestSchemaCompareConvergesOnAnAppliedMySQLFamilyRoleE2E(t *testing.T) {
	sources := []struct {
		name   string
		source []string
	}{
		{name: "sql schema file", source: []string{"--schema-file", "role.sql"}},
		{name: "go annotations", source: []string{"--root-dir", "entities"}},
	}

	for _, engine := range roleInheritEngines {
		for _, source := range sources {
			t.Run(engine.name+", "+source.name, func(t *testing.T) {
				c := qt.New(t)
				server, target, role := roleInheritTarget(c, engine.engine, engine.scheme)
				workDir := writeRoleSources(c, role, "")
				run := func(args ...string) clirun.Result {
					return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: workDir},
						append(append(args, "--db-url", target), source.source...)...)
				}

				applied := run("schema", "apply", "--auto-approve")

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(applied.Stdout, qt.Contains, "CREATE ROLE IF NOT EXISTS `"+role+"`;")
				c.Assert(roleExists(c, server, role), qt.IsTrue)

				compared := run("schema", "compare", "--exit-code")

				c.Assert(compared.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", compared.Stdout, compared.Stderr))
				c.Assert(compared.Stdout, qt.Contains, "No schema differences detected.\n")
				c.Assert(compared.Stderr, qt.Equals, "")

				reapplied := run("schema", "apply", "--auto-approve")

				c.Assert(reapplied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", reapplied.Stdout, reapplied.Stderr))
				c.Assert(reapplied.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")

				generated := run("migrations", "generate", "--migrations-dir", "migrations")

				c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
				c.Assert(generated.Stdout, qt.Contains, "no migration files generated")
			})
		}
	}
}

// TestSchemaApplyRefusesAMySQLFamilyRoleDeclaredNotToInheritE2E holds a
// declared inherit="false" to a refusal that names it, on the first command
// and before the server changes.
//
// Accepting it would create a role that inherits, and every later comparison
// would find the difference. The check that the role is absent afterwards is
// what separates a refusal from a warning printed beside a CREATE ROLE that ran.
func TestSchemaApplyRefusesAMySQLFamilyRoleDeclaredNotToInheritE2E(t *testing.T) {
	for _, engine := range roleInheritEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server, target, role := roleInheritTarget(c, engine.engine, engine.scheme)
			workDir := writeRoleSources(c, role, ` inherit="false"`)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: workDir},
				"schema", "apply", "--db-url", target, "--root-dir", "entities", "--auto-approve")

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, `unsupported feature: `+engine.name+`: role "`+role+`" declares inherit=false, `+
				`which a role cannot have here: a role always passes on the privileges of the roles granted to it, `+
				`and CREATE ROLE has no NOINHERIT`)
			c.Assert(roleExists(c, server, role), qt.IsFalse)
		})
	}
}
