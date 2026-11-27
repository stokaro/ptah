//go:build integration

package integration_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/dbschema"
)

// inheritedGrantSchema is the database the MariaDB inherited-grant tests
// read: t1 with a column the role it inherits from may insert into, and t2.
const inheritedGrantSchema = "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50));\nCREATE TABLE t2 (id INT PRIMARY KEY);\n"

// inheritedGrantHeld makes the run's role a member of {role}_p, which holds a
// grant on the database, on a pattern that reaches it, on a table and on a
// column, and gives the role DELETE on t2 of its own, in the words of
// [roleCoverNames].
var inheritedGrantHeld = []string{
	"CREATE ROLE {role}_p",
	"GRANT SELECT, EVENT ON `{db}`.* TO {role}_p",
	"GRANT INSERT ON `{prefix}%`.* TO {role}_p",
	"GRANT UPDATE ON `{db}`.t1 TO {role}_p",
	"GRANT REFERENCES (name) ON `{db}`.t1 TO {role}_p",
	"GRANT {role}_p TO {role}",
	"GRANT DELETE ON `{db}`.t2 TO {role}",
}

// inheritedGrantGhosts leave the run's role the grant option alone on its
// database and on t1: a privilege granted WITH GRANT OPTION and revoked keeps
// a row that SHOW GRANTS and the privilege views list as USAGE, grantable.
var inheritedGrantGhosts = []string{
	"GRANT INSERT ON `{db}`.* TO {role} WITH GRANT OPTION",
	"REVOKE INSERT ON `{db}`.* FROM {role}",
	"GRANT SELECT ON `{db}`.t1 TO {role} WITH GRANT OPTION",
	"REVOKE SELECT ON `{db}`.t1 FROM {role}",
}

// TestMariaDBInheritedGrantsReadE2E reads the grants of a MariaDB role that
// holds another. MariaDB's privilege views list what the role inherits as its
// own: measured on MariaDB 11.8.9 and 12.3.3, SCHEMA_PRIVILEGES lists the
// other role's SELECT on the database for it too, while SHOW GRANTS FOR the
// role lists only the membership and its own DELETE, and REVOKE SELECT ON
// db.* FROM it fails with Error 1141. The read describes each role with its
// own grants, the ones SHOW GRANTS lists for it, a row holding the grant
// option alone among them.
func TestMariaDBInheritedGrantsReadE2E(t *testing.T) {
	for _, row := range mariaDBPublicEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			run := newColumnGrantRunFrom(c, row.engine, inheritedGrantSchema, nil)
			dropRoleCoverRoles(c, run)
			holdRoleCoverGrants(c, run, append(slices.Clone(inheritedGrantHeld), inheritedGrantGhosts...))
			conn, err := dbschema.ConnectToDatabase(c.Context(), run.url)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)

			read, err := conn.Reader().ReadSchemaContext(c.Context())

			c.Assert(err, qt.IsNil)
			parent := run.role + "_p"
			c.Assert(grantsOf(read.Grants, run.role), qt.DeepEquals, []catalog.Grant{
				{Role: run.role, Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: run.database, WithOption: true},
				{Role: run.role, Privilege: "USAGE", ObjectType: "TABLE", Schema: run.database, ObjectName: "t1", WithOption: true},
				{Role: run.role, Privilege: "DELETE", ObjectType: "TABLE", Schema: run.database, ObjectName: "t2"},
			})
			c.Assert(grantsOf(read.Grants, parent), qt.DeepEquals, []catalog.Grant{
				{Role: parent, Privilege: "EVENT", ObjectType: "SCHEMA", ObjectName: run.database},
				{Role: parent, Privilege: "SELECT", ObjectType: "SCHEMA", ObjectName: run.database},
				{Role: parent, Privilege: "UPDATE", ObjectType: "TABLE", Schema: run.database, ObjectName: "t1"},
				{Role: parent, Privilege: "REFERENCES", ObjectType: "TABLE", Schema: run.database, ObjectName: "t1", Column: "name"},
			})
			c.Assert(slices.ContainsFunc(read.WideGrants, func(grant catalog.WideGrant) bool { return grant.Role == run.role }), qt.IsFalse)
			c.Assert(slices.ContainsFunc(read.WideGrants, func(grant catalog.WideGrant) bool { return grant.Role == parent }), qt.IsTrue)
			c.Assert(ownGrantLines(c, run), qt.DeepEquals, []string{
				"GRANT DELETE ON `" + run.database + "`.`t2` TO `" + run.role + "`",
			})
		})
	}
}

// TestMariaDBInheritedGrantDeclaredE2E declares for a MariaDB role a grant
// another role holds and passes on to it, UPDATE on t1. The role does not hold
// it itself, so the plan grants it, and SHOW GRANTS then lists it for the
// role; the next plan finds nothing to change.
func TestMariaDBInheritedGrantDeclaredE2E(t *testing.T) {
	const declared = inheritedGrantSchema + "GRANT UPDATE ON t1 TO %[1]s;\nGRANT DELETE ON t2 TO %[1]s;\n"
	for _, row := range mariaDBPublicEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			run := newColumnGrantRunFrom(c, row.engine, inheritedGrantSchema, nil)
			dropRoleCoverRoles(c, run)
			holdRoleCoverGrants(c, run, inheritedGrantHeld)

			applied := run.apply(c, declared, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(ownGrantLines(c, run), qt.DeepEquals, []string{
				"GRANT DELETE ON `" + run.database + "`.`t2` TO `" + run.role + "`",
				"GRANT UPDATE ON `" + run.database + "`.`t1` TO `" + run.role + "`",
			})
			again := run.apply(c, declared, "--dry-run")
			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
		})
	}
}

// TestMariaDBInheritedGrantRevokedE2E declares a revoke from a MariaDB role
// of a privilege it holds only through another role's grant on the table. The
// revoke cannot take it, and the server refuses it with Error 1147, so the
// apply is refused before it runs, naming the other role.
func TestMariaDBInheritedGrantRevokedE2E(t *testing.T) {
	const declared = inheritedGrantSchema + "GRANT DELETE ON t2 TO %[1]s;\nREVOKE UPDATE ON t1 FROM %[1]s;\n"
	for _, row := range mariaDBPublicEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			run := newColumnGrantRunFrom(c, row.engine, inheritedGrantSchema, nil)
			dropRoleCoverRoles(c, run)
			holdRoleCoverGrants(c, run, inheritedGrantHeld)

			applied := run.apply(c, declared, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stderr, qt.Contains, roleCoverNames(run,
				"{role}_p holds UPDATE ON t1, which gives role \"{role}\" the privilege on table t1, so a revoke from \"{role}\" cannot take it"))
			c.Assert(ownGrantLines(c, run), qt.DeepEquals, []string{
				"GRANT DELETE ON `" + run.database + "`.`t2` TO `" + run.role + "`",
			})
		})
	}
}

// grantsOf is the grants of role, in the order the read lists them.
func grantsOf(grants []catalog.Grant, role string) []catalog.Grant {
	return slices.DeleteFunc(slices.Clone(grants), func(grant catalog.Grant) bool { return grant.Role != role })
}

// ownGrantLines is what SHOW GRANTS lists for the run's role as granted to it
// on its database, sorted: the lines that name the database and end with the
// role. MariaDB lists a held role's grants under that role's name, so they
// are not among them.
func ownGrantLines(c *qt.C, run columnGrantRun) []string {
	c.Helper()
	lines := slices.DeleteFunc(strings.Split(showGrants(c, run), "\n"), func(line string) bool {
		return !strings.Contains(line, "`"+run.database+"`") || !strings.HasSuffix(line, " TO `"+run.role+"`")
	})
	slices.Sort(lines)
	return lines
}
