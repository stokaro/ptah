//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// TestMySQLMandatoryRolesReachE2E names a role in MySQL's mandatory_roles.
// The server grants it to every account, and every account may make it
// active: a login granted nothing reads the tables it reads under SET ROLE
// ALL, measured on MySQL 8.4.13 and 26.10.0 and here by a session. `schema
// security` reports that reach for PUBLIC, every account, through the
// mandatory role, counts it for every other role, and does not report the
// mandatory role as a role nobody holds.
//
// mandatory_roles is global, so the test does not run in parallel: the
// contour runs one package at a time, and a test that is not parallel runs
// alone in its package.
func TestMySQLMandatoryRolesReachE2E(t *testing.T) {
	c := qt.New(t)
	server := newMySQLFamilyServer(c, dbtarget.MySQLAdmin)
	database := server.database(c, "mandatory")
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano()%1_000_000_000_000)
	mandatory, other := granteeIdentifier("mand"+suffix), granteeIdentifier("mother"+suffix)
	c.Cleanup(func() {
		_, err := server.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+mandatory+", "+other)
		c.Check(err, qt.IsNil)
	})
	for _, statement := range []string{
		"CREATE TABLE " + granteeIdentifier(database) + ".a (id int PRIMARY KEY)",
		"CREATE TABLE " + granteeIdentifier(database) + ".b (id int PRIMARY KEY)",
		"CREATE TABLE " + granteeIdentifier(database) + ".c (id int PRIMARY KEY)",
		"CREATE ROLE " + mandatory + ", " + other,
		"GRANT SELECT ON " + granteeIdentifier(database) + ".a TO " + mandatory,
		"GRANT SELECT ON " + granteeIdentifier(database) + ".b TO " + mandatory,
		"GRANT SELECT ON " + granteeIdentifier(database) + ".c TO " + other,
	} {
		_, err := server.admin.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	withMandatoryRoles(c, server, "mand"+suffix)

	lines := publicFindingLines(c, server.url("mysql", database), suffix)

	everyAccount := mandatorySessionReads(c, server, database, "mpub"+suffix, nil)
	holdingOther := mandatorySessionReads(c, server, database, "moth"+suffix, []string{other})
	c.Assert(lines, qt.DeepEquals, []string{
		"ROL04 info mother",
		fmt.Sprintf("roles/read-reach warning PUBLIC %d/3 through=[mand]", everyAccount),
		fmt.Sprintf("roles/read-reach error mother %d/3 through=[]", holdingOther),
	})
}

// TestMySQLRoleNamedPublicBesideMandatoryRolesE2E creates a MySQL role named
// PUBLIC, which MySQL allows, beside a mandatory role. The security analysis
// reads that name as every account, and the role's memberships are printed,
// so the read does not record the mandatory role as held by it: `db read`
// prints no GRANT of the mandatory role, and the access is left unmeasured.
func TestMySQLRoleNamedPublicBesideMandatoryRolesE2E(t *testing.T) {
	c := qt.New(t)
	server := newMySQLFamilyServer(c, dbtarget.MySQLAdmin)
	database := server.database(c, "mandatory_public")
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano()%1_000_000_000_000)
	mandatory := granteeIdentifier("mand" + suffix)
	c.Cleanup(func() {
		_, err := server.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+mandatory+", PUBLIC")
		c.Check(err, qt.IsNil)
	})
	for _, statement := range []string{
		"CREATE TABLE " + granteeIdentifier(database) + ".a (id int PRIMARY KEY)",
		"CREATE ROLE " + mandatory + ", PUBLIC",
		"GRANT SELECT ON " + granteeIdentifier(database) + ".a TO " + mandatory,
	} {
		_, err := server.admin.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	withMandatoryRoles(c, server, "mand"+suffix)
	target := server.url("mysql", database)
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	read, err := conn.Reader().ReadSchemaContext(c.Context())
	printed, printErr := runPtahNativeStdout("db", "read", "--db-url", target)

	c.Assert(err, qt.IsNil)
	c.Assert(read.WideAccess, qt.IsNil)
	c.Assert(slices.DeleteFunc(read.RoleMemberships, func(membership catalog.RoleMembership) bool {
		return membership.Member != "PUBLIC"
	}), qt.HasLen, 0)
	c.Assert(printErr, qt.IsNil, qt.Commentf("%s", printed))
	c.Assert(printed, qt.Contains, "CREATE ROLE IF NOT EXISTS `PUBLIC`")
	c.Assert(printed, qt.Not(qt.Contains), "TO `PUBLIC`")
}

// withMandatoryRoles sets mandatory_roles on the server, and sets it back
// when the test ends. Register the cleanup that drops a mandatory role before
// calling it: the server refuses to drop a mandatory role (Error 3628), and
// cleanups run last registered first.
func withMandatoryRoles(c *qt.C, server mysqlFamilyServer, value string) {
	c.Helper()
	var was string
	c.Assert(server.admin.QueryRowContext(c.Context(), "SELECT @@GLOBAL.mandatory_roles").Scan(&was), qt.IsNil)
	c.Cleanup(func() {
		_, err := server.admin.ExecContext(context.Background(), "SET GLOBAL mandatory_roles = ?", was)
		c.Check(err, qt.IsNil)
	})
	_, err := server.admin.ExecContext(c.Context(), "SET GLOBAL mandatory_roles = ?", value)
	c.Assert(err, qt.IsNil)
}

// mandatorySessionReads is how many of the tables a, b and c of database a
// session of a new login reads with every role it may use active, the login
// granted roles and nothing else. A statement the server refuses for want of
// a privilege counts as not read, and any other failure fails the test. The
// login is dropped when the test ends.
func mandatorySessionReads(c *qt.C, server mysqlFamilyServer, database, login string, roles []string) int {
	c.Helper()
	account := granteeIdentifier(login) + "@'%'"
	c.Cleanup(func() {
		_, err := server.admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+account)
		c.Check(err, qt.IsNil)
	})
	statements := []string{"CREATE USER " + account + " IDENTIFIED BY 'mandatory_login_pw'"}
	for _, role := range roles {
		statements = append(statements, "GRANT "+role+" TO "+account)
	}
	for _, statement := range statements {
		_, err := server.admin.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	config := server.config.Clone()
	config.User, config.Passwd, config.DBName = login, "mandatory_login_pw", ""
	db, err := sql.Open("mysql", config.FormatDSN())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	session, err := db.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(session.Close(), qt.IsNil) })
	_, err = session.ExecContext(c.Context(), "SET ROLE ALL")
	c.Assert(err, qt.IsNil)
	reads := 0
	reached := map[bool]int{true: 1}
	for _, table := range []string{"a", "b", "c"} {
		_, err := session.ExecContext(c.Context(), readProbe(database, table))
		c.Assert(err == nil || refusedForPrivilege(err), qt.IsTrue, qt.Commentf("%s reading %s: %v", login, table, err))
		reads += reached[err == nil]
	}
	return reads
}
