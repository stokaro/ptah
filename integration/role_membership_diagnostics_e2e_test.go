//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// The tests below cover a role whose declaration states no member_of while the
// server holds a membership of it in another declared role. Ptah leaves that
// membership to the server, so deleting member_of from a role revokes nothing.
// schema apply and schema compare write an UNMANAGED line beside their result,
// which is how a reader who deleted the key learns the plan kept the
// membership. An empty member_of list revokes it.

// membershipDiagnosticsSchema declares the fixture's two roles and table t.
// MEMBEROF is replaced with the owner role's member_of, or with nothing.
const membershipDiagnosticsSchema = `roles:
  OWNER: {login: false MEMBEROF}
  GRANTEE: {login: false}
tables:
  t:
    columns:
      id: {type: INTEGER, primary: true}
`

// writeMembershipSchema writes the YAML schema with the owner's member_of
// spelled as memberOf, which is empty or starts with a comma.
func writeMembershipSchema(c *qt.C, roles connectingRoles, memberOf string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.yaml")
	body := strings.NewReplacer("MEMBEROF", memberOf, "OWNER", roles.owner, "GRANTEE", roles.grantee).
		Replace(membershipDiagnosticsSchema)
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return path
}

// membershipDatabase is a scratch database holding table t, with the owner
// role a member of the grantee role. Memberships are server-wide, so every
// database of the server reads the edge.
func membershipDatabase(c *qt.C, roles connectingRoles) string {
	c.Helper()
	database := ownedPublicDatabase(c, roles, `CREATE TABLE t (id int PRIMARY KEY)`)
	execPostgresFamilySQL(c, c.Context(), database, fmt.Sprintf(`GRANT %s TO %s`, roles.grantee, roles.owner))
	return database
}

// heldMemberships counts the memberships of member in group the server holds.
func heldMemberships(c *qt.C, database, group, member string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), database)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	c.Assert(conn.QueryRowContext(c.Context(), `
		SELECT count(*) FROM pg_auth_members m
		JOIN pg_roles g ON g.oid = m.roleid
		JOIN pg_roles r ON r.oid = m.member
		WHERE g.rolname = $1 AND r.rolname = $2`, group, member).Scan(&count), qt.IsNil)
	return count
}

// TestSchemaApplyReportsAMembershipTheFileLeavesToTheServerE2E is the owner
// role without member_of. The plan revokes nothing and is synced, both commands
// name the membership in an UNMANAGED line, and the server still holds it after
// an apply.
func TestSchemaApplyReportsAMembershipTheFileLeavesToTheServerE2E(t *testing.T) {
	c := qt.New(t)
	roles := newConnectingRoles(c)
	database := membershipDatabase(c, roles)
	file := writeMembershipSchema(c, roles, "")
	line := fmt.Sprintf(`UNMANAGED: role %q is a member of the declared role %q, and its declaration states no member_of, `+
		`so the plan keeps the membership; state member_of on %q to manage its memberships, or an empty list to revoke them all`,
		roles.owner, roles.grantee, roles.owner)

	planned := runPtahNative(c, "schema", "apply", "--db-url", database, "--schema-file", file, "--dry-run")
	compared := runPtahNative(c, "schema", "compare", "--db-url", database, "--schema-file", file)
	runPtahNative(c, "schema", "apply", "--db-url", database, "--schema-file", file, "--auto-approve")

	c.Assert(planned, qt.Contains, line)
	c.Assert(planned, qt.Contains, "Schema is synced")
	c.Assert(planned, qt.Not(qt.Contains), "REVOKE")
	c.Assert(compared, qt.Contains, line)
	c.Assert(heldMemberships(c, database, roles.grantee, roles.owner), qt.Equals, 1)
}

// TestSchemaApplyRevokesAMembershipAnEmptyMemberOfLeavesOutE2E is the control:
// the same owner role states an empty member_of, so the plan revokes the
// membership, reports nothing unmanaged, and the server no longer holds it.
func TestSchemaApplyRevokesAMembershipAnEmptyMemberOfLeavesOutE2E(t *testing.T) {
	c := qt.New(t)
	roles := newConnectingRoles(c)
	database := membershipDatabase(c, roles)
	file := writeMembershipSchema(c, roles, ", member_of: []")

	planned := runPtahNative(c, "schema", "apply", "--db-url", database, "--schema-file", file, "--dry-run")
	runPtahNative(c, "schema", "apply", "--db-url", database, "--schema-file", file, "--auto-approve")

	c.Assert(planned, qt.Contains, "REVOKE")
	c.Assert(planned, qt.Not(qt.Contains), "UNMANAGED")
	c.Assert(heldMemberships(c, database, roles.grantee, roles.owner), qt.Equals, 0)
}
