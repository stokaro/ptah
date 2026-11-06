//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// TestSchemaDiffReportsAGrantOnlyOneSideHoldsE2E compares two databases that differ
// by one grant, in both directions. Both are read in full, so the grant is a
// difference whoever holds it, and the two directions have to agree that one
// exists. Each object carries a grant both sides hold, so the extra grant is what
// sets the two apart and not the first grant on the object.
//
// The role of the first row is named by nothing on the --to side, and PUBLIC is a
// role no side can name. A comparison that reads --to as a declaration plans no
// REVOKE for them and answers `Schemas are synced`, as a superuser.
func TestSchemaDiffReportsAGrantOnlyOneSideHoldsE2E(t *testing.T) {
	const shared = `
			CREATE TABLE t (id int PRIMARY KEY, a int);
			CREATE VIEW v AS SELECT id FROM t;
			CREATE SEQUENCE s;
			CREATE SCHEMA app;
			CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT 1';
			REVOKE EXECUTE ON FUNCTION f() FROM PUBLIC;
			GRANT SELECT ON t TO {grantee}_shared;
			GRANT SELECT ON v TO {grantee}_shared;
			GRANT USAGE ON SEQUENCE s TO {grantee}_shared;
			GRANT USAGE ON SCHEMA app TO {grantee}_shared;
			GRANT EXECUTE ON FUNCTION f() TO {grantee}_shared;`
	tests := []struct {
		name   string
		extra  string
		revoke string
		grant  string
	}{
		{
			name:   "a table grant to a role the other side does not name",
			extra:  `GRANT INSERT ON t TO {grantee};`,
			revoke: `REVOKE INSERT ON TABLE "t" FROM GRANTEE;`,
			grant:  `GRANT INSERT ON TABLE "t" TO GRANTEE;`,
		},
		{
			name:   "a table grant to PUBLIC",
			extra:  `GRANT SELECT ON t TO PUBLIC;`,
			revoke: `REVOKE SELECT ON TABLE "t" FROM PUBLIC;`,
			grant:  `GRANT SELECT ON TABLE "t" TO PUBLIC;`,
		},
		{
			name:   "a view grant",
			extra:  `GRANT SELECT ON v TO {grantee};`,
			revoke: `REVOKE SELECT ON TABLE "v" FROM GRANTEE;`,
			grant:  `GRANT SELECT ON TABLE "v" TO GRANTEE;`,
		},
		{
			name:   "a column grant",
			extra:  `GRANT UPDATE (a) ON t TO {grantee};`,
			revoke: `REVOKE UPDATE ("a") ON TABLE "t" FROM GRANTEE;`,
			grant:  `GRANT UPDATE ("a") ON TABLE "t" TO GRANTEE;`,
		},
		{
			name:   "a sequence grant",
			extra:  `GRANT SELECT ON SEQUENCE s TO {grantee};`,
			revoke: `REVOKE SELECT ON SEQUENCE "s" FROM GRANTEE;`,
			grant:  `GRANT SELECT ON SEQUENCE "s" TO GRANTEE;`,
		},
		{
			name:   "a schema grant",
			extra:  `GRANT CREATE ON SCHEMA app TO {grantee};`,
			revoke: `REVOKE CREATE ON SCHEMA "app" FROM GRANTEE;`,
			grant:  `GRANT CREATE ON SCHEMA "app" TO GRANTEE;`,
		},
		{
			name:   "a routine grant",
			extra:  `GRANT EXECUTE ON FUNCTION f() TO {grantee};`,
			revoke: `REVOKE EXECUTE ON FUNCTION "f"() FROM GRANTEE;`,
			grant:  `GRANT EXECUTE ON FUNCTION "f"() TO GRANTEE;`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			roles := newConnectingRoles(c)
			sharedRole := roles.grantee + "_shared"
			admin, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.PostgreSQL))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() {
				_, dropErr := admin.ExecContext(context.Background(), `DROP ROLE IF EXISTS `+sharedRole)
				c.Check(dropErr, qt.IsNil)
				dbschema.CloseAndWarn(admin)
			})
			_, err = admin.ExecContext(c.Context(), `CREATE ROLE `+sharedRole+` NOLOGIN`)
			c.Assert(err, qt.IsNil)
			left := scratchOwnedDatabase(c, roles, shared+test.extra)
			right := scratchOwnedDatabase(c, roles, shared)
			revoke := strings.ReplaceAll(test.revoke, "GRANTEE", fmt.Sprintf("%q", roles.grantee))
			grant := strings.ReplaceAll(test.grant, "GRANTEE", fmt.Sprintf("%q", roles.grantee))

			leftToRight := runPtahNative(c, "schema", "diff", "--from", left, "--to", right)
			rightToLeft := runPtahNative(c, "schema", "diff", "--from", right, "--to", left)

			c.Assert(strings.TrimSpace(leftToRight), qt.Equals, revoke)
			c.Assert(strings.TrimSpace(rightToLeft), qt.Equals, grant)
		})
	}
}

// TestSchemaDiffToAFileLeavesARoleItDoesNotNameE2E is the other side of the rule
// above: a file that says nothing about a role makes no claim about what the role
// may do, so its grant stays out of the plan.
func TestSchemaDiffToAFileLeavesARoleItDoesNotNameE2E(t *testing.T) {
	c := qt.New(t)
	roles := newConnectingRoles(c)
	const table = `CREATE TABLE t (id integer PRIMARY KEY);`
	database := scratchOwnedDatabase(c, roles, table+` GRANT SELECT ON t TO {grantee};`)
	declaration := writeDeclaration(c, table)

	out := runPtahNative(c, "schema", "diff", "--from", database, "--to", "file://"+declaration)

	c.Assert(out, qt.Contains, "Schemas are synced")
}

// TestSchemaDiffLeavesWhatTheOtherSideHoldsByDefaultE2E is a privilege a database
// holds with no grant saying so. PostgreSQL stores no ACL until an object's first
// GRANT or REVOKE, and that first statement writes the defaults out. Revoking them to
// match would take them from a side that holds them: the only difference in each row
// is the grant to the grantee.
func TestSchemaDiffLeavesWhatTheOtherSideHoldsByDefaultE2E(t *testing.T) {
	t.Run("PUBLIC's EXECUTE on a routine", func(t *testing.T) {
		c := qt.New(t)
		roles := newConnectingRoles(c)
		const function = `CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT 1';`
		left := scratchOwnedDatabase(c, roles, function+` GRANT EXECUTE ON FUNCTION f() TO {grantee};`)
		right := scratchOwnedDatabase(c, roles, function)

		out := runPtahNative(c, "schema", "diff", "--from", left, "--to", right)

		c.Assert(strings.TrimSpace(out), qt.Equals, fmt.Sprintf(`REVOKE EXECUTE ON FUNCTION "f"() FROM %q;`, roles.grantee))
	})
	t.Run("the privileges of a table's owner", func(t *testing.T) {
		c := qt.New(t)
		roles := newConnectingRoles(c)
		const table = `CREATE TABLE t (id int PRIMARY KEY);`
		left := scratchOwnedDatabase(c, roles, table+` GRANT SELECT ON t TO {grantee};`)
		// Another role owns the table on the other side, so --to names neither the
		// grantee nor this side's owner.
		right := scratchOwnedDatabase(c, newConnectingRoles(c), table)

		out := runPtahNative(c, "schema", "diff", "--from", left, "--to", right)

		c.Assert(out, qt.Contains, fmt.Sprintf(`REVOKE SELECT ON TABLE "t" FROM %q;`, roles.grantee))
		c.Assert(out, qt.Not(qt.Contains), fmt.Sprintf(`ON TABLE "t" FROM %q`, roles.owner))
	})
}
