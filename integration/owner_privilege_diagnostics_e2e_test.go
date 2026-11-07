//go:build integration

package integration_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
)

// The tests below drive stokaro/ptah#5765. An owner that revoked privileges from
// itself on a table a schema file declares holds less than PostgreSQL gives an
// owner by default. The file does not manage those privileges, so Ptah plans
// nothing for them, and reports what the owner lacks instead: schema apply and
// schema compare write an UNMANAGED line beside their result, and schema
// security reports OWN02. A privilege the file names is reconciled as it says,
// and is not reported.

// ownerDiagnosticsTable is the table the owner creates, granted to the grantee so
// the ACL is written out.
const ownerDiagnosticsTable = `CREATE TABLE t (id int PRIMARY KEY);GRANT SELECT ON t TO {grantee};`

// ownerSchema is the SQL fixture of the owner tests: both roles, table t and the
// grantee's SELECT.
func ownerSchema() string { return ownerFixtureFiles[0].body }

// writeOwnerSchema writes a SQL schema with the fixture's roles named.
func writeOwnerSchema(c *qt.C, roles connectingRoles, body string) string {
	c.Helper()
	return ownerFixtureFile{extension: "sql", body: body}.write(c, roles, "")
}

// grantsToOwner matches a planned GRANT to the fixture's owner.
func grantsToOwner(roles connectingRoles) string {
	return `(?s).*GRANT [^;]* TO "?` + regexp.QuoteMeta(roles.owner) + `"?.*`
}

// TestSchemaApplyReportsAnOwnersUnmanagedPrivilegesE2E is an owner missing
// INSERT and UPDATE, which the file does not name. Nothing grants them back,
// whether or not the file declares the owner role, and whatever the owner reaches
// through PUBLIC or through a role it belongs to: the read reports the owner's
// direct ACL entry, and those paths are somebody else's grants.
func TestSchemaApplyReportsAnOwnersUnmanagedPrivilegesE2E(t *testing.T) {
	tests := []struct {
		name, state, schema string
	}{
		{name: "the file declares the owner role", state: `REVOKE INSERT, UPDATE ON t FROM {owner};`, schema: ownerSchema()},
		{name: "the file does not declare the owner role", state: `REVOKE INSERT, UPDATE ON t FROM {owner};`,
			schema: "CREATE ROLE GRANTEE NOLOGIN;\nCREATE TABLE t (id int PRIMARY KEY);\nGRANT SELECT ON t TO GRANTEE;\n"},
		{name: "PUBLIC holds the privileges", state: `REVOKE INSERT, UPDATE ON t FROM {owner};GRANT INSERT, UPDATE ON t TO PUBLIC;`,
			schema: ownerSchema()},
		{name: "a role the owner belongs to holds the privileges",
			state:  `REVOKE INSERT, UPDATE ON t FROM {owner};GRANT INSERT, UPDATE ON t TO {grantee};RESET ROLE;GRANT {grantee} TO {owner};SET ROLE {owner};`,
			schema: ownerSchema()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			roles := newConnectingRoles(c)
			database := ownedPublicDatabase(c, roles, ownerDiagnosticsTable+test.state)
			file := writeOwnerSchema(c, roles, test.schema)
			line := "UNMANAGED: table t: owner " + roles.owner + " lacks INSERT, UPDATE, which PostgreSQL gives an owner by default; " +
				"the schema does not manage them, so nothing is planned"

			planned := runPtahNative(c, "schema", "apply", "--db-url", database, "--schema-file", file, "--dry-run")
			compared := runPtahNative(c, "schema", "compare", "--db-url", database, "--schema-file", file)
			security := runPtahNative(c, "schema", "security", "--db-url", database, "--fail-on", "none")

			c.Assert(planned, qt.Contains, line)
			c.Assert(planned, qt.Not(qt.Matches), grantsToOwner(roles))
			c.Assert(compared, qt.Contains, line)
			c.Assert(security, qt.Contains, "owner "+roles.owner+" of table t lacks INSERT, UPDATE, which PostgreSQL gives an owner by default")
			c.Assert(ownerTableACL(c, database, roles), qt.Matches, `\{owner=rdDxtm/owner,.*`)
		})
	}
}

// TestSchemaApplyReportsNothingAboutAnOwnerThatHoldsWhatTheFileSaysE2E is an owner
// with nothing to report: a table it never revoked anything on, and privileges a
// manual GRANT gave back. The plan is synced and neither command names the owner.
func TestSchemaApplyReportsNothingAboutAnOwnerThatHoldsWhatTheFileSaysE2E(t *testing.T) {
	tests := []struct {
		name, state, schema string
	}{
		{name: "a new table", schema: ownerSchema()},
		{name: "privileges a manual GRANT gave back", state: `REVOKE INSERT, UPDATE ON t FROM {owner};GRANT INSERT, UPDATE ON t TO {owner};`,
			schema: ownerSchema()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			roles := newConnectingRoles(c)
			database := ownedPublicDatabase(c, roles, ownerDiagnosticsTable+test.state)
			file := writeOwnerSchema(c, roles, test.schema)

			planned := runPtahNative(c, "schema", "apply", "--db-url", database, "--schema-file", file, "--dry-run")
			security := runPtahNative(c, "schema", "security", "--db-url", database, "--fail-on", "none")

			c.Assert(planned, qt.Contains, "Schema is synced")
			c.Assert(planned, qt.Not(qt.Contains), "UNMANAGED")
			c.Assert(security, qt.Not(qt.Contains), "OWN02")
		})
	}
}

// TestSchemaApplyGivesAnOwnerBackAPrivilegeTheFileGrantsE2E is the privilege a file
// does manage: a declared GRANT to the owner restores what it revoked, read back
// from the server, and the next plan is synced with nothing to report.
func TestSchemaApplyGivesAnOwnerBackAPrivilegeTheFileGrantsE2E(t *testing.T) {
	c := qt.New(t)
	roles := newConnectingRoles(c)
	database := ownedPublicDatabase(c, roles, ownerDiagnosticsTable+`REVOKE INSERT ON t FROM {owner};`)
	file := writeOwnerSchema(c, roles, ownerSchema()+"GRANT INSERT ON t TO OWNER;\n")

	runPtahNative(c, "schema", "apply", "--db-url", database, "--schema-file", file, "--auto-approve")
	again := runPtahNative(c, "schema", "apply", "--db-url", database, "--schema-file", file, "--dry-run")

	c.Assert(ownerTableACL(c, database, roles), qt.Equals, `{owner=arwdDxtm/owner,grantee=r/owner}`)
	c.Assert(again, qt.Contains, "Schema is synced")
	c.Assert(again, qt.Not(qt.Contains), "UNMANAGED")
}

// TestSchemaApplyRespectsAPrivilegeTheFileRevokesFromTheOwnerE2E is a revoke the
// file declares, which the owner made too. The privilege is managed, so the plan
// is synced and reports nothing unmanaged. schema security reads the database
// alone and cannot tell a revoke a file declares from an accident, so OWN02
// still names it, with the suggestion to leave a deliberate revoke in place.
func TestSchemaApplyRespectsAPrivilegeTheFileRevokesFromTheOwnerE2E(t *testing.T) {
	c := qt.New(t)
	roles := newConnectingRoles(c)
	database := ownedPublicDatabase(c, roles, ownerDiagnosticsTable+`REVOKE INSERT ON t FROM {owner};`)
	file := writeOwnerSchema(c, roles, ownerSchema()+"REVOKE INSERT ON t FROM OWNER;\n")

	planned := runPtahNative(c, "schema", "apply", "--db-url", database, "--schema-file", file, "--dry-run")
	security := runPtahNative(c, "schema", "security", "--db-url", database, "--fail-on", "none")

	c.Assert(planned, qt.Contains, "Schema is synced")
	c.Assert(planned, qt.Not(qt.Contains), "UNMANAGED")
	c.Assert(security, qt.Contains, "owner "+roles.owner+" of table t lacks INSERT")
}
