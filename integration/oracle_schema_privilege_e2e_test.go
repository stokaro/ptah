//go:build integration

package integration_test

import (
	"database/sql"
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A declared revoke on a table that a grant wider than the table keeps is
// refused on Oracle, for a table the plan creates and for one that exists.
//
// On Oracle 23 the role holds SELECT ANY TABLE ON SCHEMA of the account.
// Measured on Oracle 23.26.3 with the read of DBA_SCHEMA_PRIVS taken out: the
// plan for a created table has only CREATE TABLE, the plan for an existing
// table revokes the table grant, the next plan reads as synced, and a member
// of the role reads both tables. The owner's REVOKE of a privilege the role
// holds only through the schema fails with ORA-01927.
//
// Oracle 21 has no privilege on a schema and no DBA_SCHEMA_PRIVS, and the
// reader does not ask for the view there. The role holds SELECT ANY TABLE
// instead: the read of the grants wider than a table is whole and holds no
// grant on a schema, and the revoke is refused naming SELECT ANY TABLE. Asked
// anyway, the view answers ORA-00942 and the read is partial.
func TestSchemaApplyOracleRevokeUnderASchemaPrivilegeE2E(t *testing.T) {
	const tables = "CREATE TABLE t1 (id NUMBER(10) PRIMARY KEY, name VARCHAR2(50));\nCREATE TABLE t3 (id NUMBER(10) PRIMARY KEY);\n"
	tests := []struct {
		name     string
		declared string
		table    string
	}{
		{name: "a table the plan creates", declared: tables + "REVOKE SELECT ON t3 FROM {role};\n", table: "t3"},
		{name: "a table that exists", declared: tables + "REVOKE SELECT ON t1 FROM {role};\n", table: "t1"},
		{name: "ALL on a table that exists", declared: tables + "REVOKE ALL ON t1 FROM {role};\n", table: "t1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			server, account := newOracleRevokeAllServer(c, "GRANT SELECT ON t1 TO {role}")
			admin := oracleSchemaPrivilegeAdmin(c)

			assertOracleWideGrantRefusesTheRevoke(c, admin, server, account, test.declared, test.table)
		})
	}
}

// A grant wider than a table that keeps no privilege a revoke on a table of
// this schema names leaves the revoke to the plan: the plan revokes the table
// grant, the next plan is empty, and a member of the role is refused the
// table. On Oracle 23 the role holds SELECT ANY TABLE ON SCHEMA of another
// account. On Oracle 21, which has no privilege on a schema, it holds EXECUTE
// ANY PROCEDURE, a system privilege that names no table privilege.
//
// On Oracle 23 this pins the reader's `WHERE p.schema = :1`: the read holds no
// grant on another schema, so the test passes with the schema check in
// grantcover.Find taken out. That check is pinned by the unit row
// TestFind_HappyPath/a_grant_on_every_table_of_another_schema.
func TestSchemaApplyOracleRevokeUnderAPrivilegeOnAnotherSchemaE2E(t *testing.T) {
	c := qt.New(t)
	server, account := newOracleRevokeAllServer(c, "GRANT SELECT ON t1 TO {role}")
	admin := oracleSchemaPrivilegeAdmin(c)

	assertOracleUnrelatedWideGrantLeavesTheRevoke(c, admin, server, account)
}

// assertOracleWideGrantRefusesTheRevoke dispatches on what the target's preset
// says, so the branch lives here rather than in the test body.
func assertOracleWideGrantRefusesTheRevoke(
	c *qt.C, admin *dbschema.DatabaseConnection, server revokeAllServer, account, declared, table string,
) {
	c.Helper()
	if admin.Info().Capabilities.Has(capability.CatalogSchemaPrivileges) {
		assertOracleSchemaPrivilegeRefusesTheRevoke(c, admin, server, account, declared, table)
		return
	}
	assertOracleSystemPrivilegeRefusesTheRevoke(c, admin, server, declared, table)
}

// assertOracleSchemaPrivilegeRefusesTheRevoke is the Oracle 23 half: the read
// reports the privilege on the account's schema, and the revoke is refused
// naming it.
func assertOracleSchemaPrivilegeRefusesTheRevoke(
	c *qt.C, admin *dbschema.DatabaseConnection, server revokeAllServer, account, declared, table string,
) {
	c.Helper()
	grant := "SELECT ANY TABLE ON SCHEMA " + account
	execOracle(c.Context(), c, admin, "GRANT "+grant+" TO "+server.role)

	read := readOracleSchemaAs(c, server.url)
	applied := server.apply(c, declared, "--auto-approve")

	c.Assert(read.WideGrantsPartial, qt.IsFalse)
	c.Assert(wideGrantsOf(read, server.role), qt.DeepEquals, []catalog.WideGrant{
		{Role: server.role, Privilege: "SELECT", Grant: grant, Schema: account},
	})
	assertOracleRevokeRefused(c, server, applied, table, grant)
	c.Assert(oracleStrings(c, server.catalog, oracleSchemaPrivilegesQuery, server.role), qt.DeepEquals,
		[]string{"SELECT ANY TABLE ON " + account})
}

// assertOracleSystemPrivilegeRefusesTheRevoke is the Oracle 21 half: the read
// of the grants wider than a table is whole and holds no grant on a schema,
// and the revoke is refused naming SELECT ANY TABLE.
func assertOracleSystemPrivilegeRefusesTheRevoke(
	c *qt.C, admin *dbschema.DatabaseConnection, server revokeAllServer, declared, table string,
) {
	c.Helper()
	const grant = "SELECT ANY TABLE"
	execOracle(c.Context(), c, admin, "GRANT "+grant+" TO "+server.role)

	read := readOracleSchemaAs(c, server.url)
	applied := server.apply(c, declared, "--auto-approve")

	c.Assert(read.WideGrants, qt.IsNotNil)
	c.Assert(read.WideGrantsPartial, qt.IsFalse)
	c.Assert(slices.ContainsFunc(read.WideGrants, func(held catalog.WideGrant) bool { return held.Schema != "" }), qt.IsFalse)
	c.Assert(wideGrantsOf(read, server.role), qt.DeepEquals, []catalog.WideGrant{
		{Role: server.role, Privilege: "SELECT", Grant: grant},
	})
	assertOracleRevokeRefused(c, server, applied, table, grant)
	c.Assert(oracleStrings(c, server.catalog, oracleSystemPrivilegesQuery, server.role), qt.DeepEquals, []string{grant})
}

// assertOracleRevokeRefused asserts that applied refused the revoke on table
// naming grant, and that the server holds what it held before: the role's
// SELECT on t1, and no t3.
func assertOracleRevokeRefused(c *qt.C, server revokeAllServer, applied clirun.Result, table, grant string) {
	c.Helper()
	c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stderr, qt.Matches, `(?s).*role "`+server.role+`" holds `+regexp.QuoteMeta(grant)+
		`, which covers table `+table+`, so a revoke on the table cannot take it: `+
		`revoke `+regexp.QuoteMeta(grant)+` from "`+server.role+`" instead.*`)
	c.Assert(server.privileges(c), qt.DeepEquals, []string{"SELECT"})
	c.Assert(oracleTablesOf(c, server.catalog), qt.DeepEquals, []string{"T1"})
}

// assertOracleUnrelatedWideGrantLeavesTheRevoke dispatches on what the
// target's preset says, so the branch lives here rather than in the test body.
func assertOracleUnrelatedWideGrantLeavesTheRevoke(
	c *qt.C, admin *dbschema.DatabaseConnection, server revokeAllServer, account string,
) {
	c.Helper()
	if admin.Info().Capabilities.Has(capability.CatalogSchemaPrivileges) {
		other := strings.Replace(account, "PTAH_RA_", "PTAH_RA_M_", 1)
		assertOracleRevokePlannedUnder(c, admin, server, account,
			"SELECT ANY TABLE ON SCHEMA "+other, oracleSchemaPrivilegesQuery, "SELECT ANY TABLE ON "+other)
		return
	}
	assertOracleRevokePlannedUnder(c, admin, server, account,
		"EXECUTE ANY PROCEDURE", oracleSystemPrivilegesQuery, "EXECUTE ANY PROCEDURE")
}

// assertOracleRevokePlannedUnder gives the role grant, applies a revoke of
// SELECT on t1, and asserts that the plan took the table grant and left grant,
// which heldQuery reports as held.
func assertOracleRevokePlannedUnder(
	c *qt.C, admin *dbschema.DatabaseConnection, server revokeAllServer, account, grant, heldQuery, held string,
) {
	c.Helper()
	const declared = "CREATE TABLE t1 (id NUMBER(10) PRIMARY KEY, name VARCHAR2(50));\nREVOKE SELECT ON t1 FROM {role};\n"
	execOracle(c.Context(), c, admin, "GRANT "+grant+" TO "+server.role)

	applied := server.apply(c, declared, "--auto-approve")
	again := server.apply(c, declared, "--dry-run")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
	c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
	c.Assert(server.privileges(c), qt.IsNil)
	c.Assert(oracleStrings(c, server.catalog, heldQuery, server.role), qt.DeepEquals, []string{held})
	_, err := server.member.ExecContext(c.Context(), strings.ReplaceAll("SELECT id FROM {account}.t1", "{account}", account))
	c.Assert(err, qt.ErrorMatches, `(?s).*ORA-00942.*`)
}

const (
	// oracleSchemaPrivilegesQuery lists the system privileges granted to :1 on
	// a schema, as "<privilege> ON <schema>".
	oracleSchemaPrivilegesQuery = "SELECT privilege || ' ON ' || schema FROM dba_schema_privs WHERE grantee = :1 ORDER BY privilege, schema"
	// oracleSystemPrivilegesQuery lists the system privileges granted to :1.
	oracleSystemPrivilegesQuery = "SELECT privilege FROM dba_sys_privs WHERE grantee = :1 ORDER BY privilege"
)

// readOracleSchemaAs reads the schema the way the CLI does, connected as the
// account address names.
func readOracleSchemaAs(c *qt.C, address string) *catalog.Database {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), address)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	read, err := conn.Reader().ReadSchemaContext(c.Context())
	c.Assert(err, qt.IsNil)
	return read
}

// wideGrantsOf is the grants wider than a table that read reports role holds.
func wideGrantsOf(read *catalog.Database, role string) []catalog.WideGrant {
	return slices.DeleteFunc(slices.Clone(read.WideGrants), func(held catalog.WideGrant) bool { return held.Role != role })
}

// oracleSchemaPrivilegeAdmin connects as the administrative account, which may
// grant a system privilege on another account's schema.
func oracleSchemaPrivilegeAdmin(c *qt.C) *dbschema.DatabaseConnection {
	c.Helper()
	admin, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.OracleAdmin))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })
	return admin
}

// oracleTablesOf is the tables the connected account owns, in order, less
// the lock table Ptah creates before it reads.
func oracleTablesOf(c *qt.C, owner *sql.DB) []string {
	c.Helper()
	return oracleStrings(c, owner, "SELECT table_name FROM user_tables WHERE table_name <> 'PTAH_LOCKS' ORDER BY table_name")
}

// oracleStrings runs query and returns its one column, one row each.
func oracleStrings(c *qt.C, db *sql.DB, query string, args ...any) []string {
	c.Helper()
	rows, err := db.QueryContext(c.Context(), query, args...)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var got []string
	for rows.Next() {
		var value string
		c.Assert(rows.Scan(&value), qt.IsNil)
		got = append(got, value)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return got
}
