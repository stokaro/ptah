//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
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
// The read takes the setting from @@GLOBAL.mandatory_roles, which reads the
// same under any session collation: a connection that sets
// default_collation_for_utf8mb4 to utf8mb4_general_ci reads the same reach,
// where a join of information_schema.APPLICABLE_ROLES against the grant tables
// fails with Error 1267, measured on MySQL 8.4.13 and 26.10.0.
//
// mandatory_roles is global, so the test does not run in parallel: the
// contour runs one package at a time, and a test that is not parallel runs
// alone in its package.
func TestMySQLMandatoryRolesReachE2E(t *testing.T) {
	for _, session := range []struct{ name, query string }{
		{name: "the default collation"},
		{name: "utf8mb4_general_ci", query: "?default_collation_for_utf8mb4=utf8mb4_general_ci"},
	} {
		t.Run(session.name, func(t *testing.T) {
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

			lines := publicFindingLines(c, server.url("mysql", database)+session.query, suffix)

			everyAccount := loginReads(c, server, database, "mpub"+suffix, nil)
			holdingOther := loginReads(c, server, database, "moth"+suffix, []string{other})
			c.Assert(lines, qt.DeepEquals, []string{
				"ROL04 info mother",
				fmt.Sprintf("roles/read-reach warning PUBLIC %d/3 through=[mand]", everyAccount),
				fmt.Sprintf("roles/read-reach error mother %d/3 through=[]", holdingOther),
			})
		})
	}
}

// TestMySQLMandatoryRoleInAnotherHostCaseReachE2E names two roles in
// mandatory_roles: one on host %, and the role 'mhc'@'localhost' spelled with
// its host in upper case. The server makes `mhc`@`LOCALHOST` active, which
// holds nothing: a login granted nothing reads only the table the first role
// reads, and a login granted a third role reads what that role and the first
// one read. `schema security` measures every role with that one table for
// PUBLIC, which is under the threshold, and reports the third role.
func TestMySQLMandatoryRoleInAnotherHostCaseReachE2E(t *testing.T) {
	c := qt.New(t)
	server := newMySQLFamilyServer(c, dbtarget.MySQLAdmin)
	database := server.database(c, "mandatory_host_case")
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano()%1_000_000_000_000)
	mandatory, local, other := granteeIdentifier("mand7"+suffix), granteeIdentifier("mhc"+suffix)+"@'localhost'", granteeIdentifier("mo7"+suffix)
	c.Cleanup(func() {
		_, err := server.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+mandatory+", "+local+", "+other)
		c.Check(err, qt.IsNil)
	})
	for _, statement := range []string{
		"CREATE TABLE " + granteeIdentifier(database) + ".a (id int PRIMARY KEY)",
		"CREATE TABLE " + granteeIdentifier(database) + ".b (id int PRIMARY KEY)",
		"CREATE TABLE " + granteeIdentifier(database) + ".c (id int PRIMARY KEY)",
		"CREATE ROLE " + mandatory + ", " + local + ", " + other,
		"GRANT SELECT ON " + granteeIdentifier(database) + ".a TO " + local,
		"GRANT SELECT ON " + granteeIdentifier(database) + ".b TO " + local,
		"GRANT SELECT ON " + granteeIdentifier(database) + ".c TO " + mandatory,
		"GRANT SELECT ON " + granteeIdentifier(database) + ".a TO " + other,
		"GRANT SELECT ON " + granteeIdentifier(database) + ".b TO " + other,
	} {
		_, err := server.admin.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	withMandatoryRoles(c, server, "mand7"+suffix+", `mhc"+suffix+"`@LOCALHOST")

	lines := publicFindingLines(c, server.url("mysql", database), suffix)

	c.Assert(loginReads(c, server, database, "m7pub"+suffix, nil), qt.Equals, 1)
	holdingOther := loginReads(c, server, database, "m7oth"+suffix, []string{other})
	c.Assert(lines, qt.DeepEquals, []string{
		"ROL04 info mo7",
		fmt.Sprintf("roles/read-reach error mo7 %d/3 through=[]", holdingOther),
	})
}

// TestMySQLMandatoryRoleDatabaseGrantReachE2E names a role holding SELECT on
// a whole database in mandatory_roles, beside a role holding two of its three
// tables. A login granted only that other role reads every table under SET
// ROLE ALL, so the analysis counts the mandatory role's database row for
// every role: PUBLIC is reported reaching every table through it, and the
// other role, which reaches no more, is not reported on its own. Without the
// row it would be reported reaching two tables of three, over the 50% the
// warning starts at.
func TestMySQLMandatoryRoleDatabaseGrantReachE2E(t *testing.T) {
	c := qt.New(t)
	server := newMySQLFamilyServer(c, dbtarget.MySQLAdmin)
	database := server.database(c, "mandatory_db")
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano()%1_000_000_000_000)
	mandatory, other := granteeIdentifier("mand6"+suffix), granteeIdentifier("other6"+suffix)
	c.Cleanup(func() {
		_, err := server.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+mandatory+", "+other)
		c.Check(err, qt.IsNil)
	})
	for _, statement := range []string{
		"CREATE TABLE " + granteeIdentifier(database) + ".a (id int PRIMARY KEY)",
		"CREATE TABLE " + granteeIdentifier(database) + ".b (id int PRIMARY KEY)",
		"CREATE TABLE " + granteeIdentifier(database) + ".c (id int PRIMARY KEY)",
		"CREATE ROLE " + mandatory + ", " + other,
		"GRANT SELECT ON " + granteeIdentifier(database) + ".* TO " + mandatory,
		"GRANT SELECT ON " + granteeIdentifier(database) + ".a TO " + other,
		"GRANT SELECT ON " + granteeIdentifier(database) + ".b TO " + other,
	} {
		_, err := server.admin.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	withMandatoryRoles(c, server, "mand6"+suffix)

	lines := publicFindingLines(c, server.url("mysql", database), suffix)

	c.Assert(loginReads(c, server, database, "m6oth"+suffix, []string{other}), qt.Equals, 3)
	c.Assert(lines, qt.DeepEquals, []string{
		"ROL04 info other6",
		"roles/read-reach error PUBLIC 3/3 through=[mand6]",
	})
}

// TestMySQLRoleNamedPublicBesideMandatoryRolesE2E creates a MySQL role named
// PUBLIC, which MySQL allows, beside a mandatory role. The role's memberships
// are printed, so the read does not record the mandatory role as held by
// it: `db read` prints no GRANT of the mandatory role, and the read says every
// account holds roles it does not state, which leaves every role's reach
// unmeasured.
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
	c.Assert(read.PublicIsRole, qt.IsTrue)
	c.Assert(read.EveryAccountHoldsUnread, qt.IsTrue)
	c.Assert(slices.DeleteFunc(read.RoleMemberships, func(membership catalog.RoleMembership) bool {
		return membership.Member != "PUBLIC"
	}), qt.HasLen, 0)
	c.Assert(printErr, qt.IsNil, qt.Commentf("%s", printed))
	c.Assert(printed, qt.Contains, "CREATE ROLE IF NOT EXISTS `PUBLIC`")
	c.Assert(printed, qt.Not(qt.Contains), "TO `PUBLIC`")
}

// TestMySQLMandatoryRoleInDoubleQuotesHoldingACommaE2E names the role
// 'mq7,x'@'%', which reads two tables of three, in mandatory_roles between
// double quotes. The release lines read the setting differently: measured
// with a login granted nothing, MySQL 26.10.0 makes the role active and the
// login reads what it reads, and MySQL 8.4.13 makes no role active. The read
// follows the line it runs on, so the reach `schema security` reports for
// PUBLIC is what a login granted nothing reads there.
func TestMySQLMandatoryRoleInDoubleQuotesHoldingACommaE2E(t *testing.T) {
	c := qt.New(t)
	server := newMySQLFamilyServer(c, dbtarget.MySQLAdmin)
	database := server.database(c, "mandatory_double_quotes")
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano()%1_000_000_000_000)
	role := granteeIdentifier("mq7,x" + suffix)
	c.Cleanup(func() {
		_, err := server.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
		c.Check(err, qt.IsNil)
	})
	for _, statement := range []string{
		"CREATE TABLE " + granteeIdentifier(database) + ".a (id int PRIMARY KEY)",
		"CREATE TABLE " + granteeIdentifier(database) + ".b (id int PRIMARY KEY)",
		"CREATE TABLE " + granteeIdentifier(database) + ".c (id int PRIMARY KEY)",
		"CREATE ROLE " + role,
		"GRANT SELECT ON " + granteeIdentifier(database) + ".a TO " + role,
		"GRANT SELECT ON " + granteeIdentifier(database) + ".b TO " + role,
	} {
		_, err := server.admin.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	withMandatoryRoles(c, server, `"mq7,x`+suffix+`"`)

	lines := publicFindingLines(c, server.url("mysql", database), suffix)

	reads := loginReads(c, server, database, "mq7pub"+suffix, nil)
	c.Assert(slices.DeleteFunc(lines, func(line string) bool { return !strings.Contains(line, " PUBLIC ") }), qt.DeepEquals, map[int][]string{
		0: {},
		2: {"roles/read-reach warning PUBLIC 2/3 through=[mq7,x]"},
	}[reads], qt.Commentf("a login granted nothing reads %d of the tables", reads))
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

// loginReads is how many of the tables a, b and c of database a session of
// a new login reads with every role it may use active, the login granted each
// of grants, a role or a privilege on an object, and nothing else. A
// statement the server refuses for want of a privilege counts as not read,
// and any other failure fails the test. The login is dropped when the test
// ends.
func loginReads(c *qt.C, server mysqlFamilyServer, database, login string, grants []string) int {
	c.Helper()
	account := granteeIdentifier(login) + "@'%'"
	c.Cleanup(func() {
		_, err := server.admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+account)
		c.Check(err, qt.IsNil)
	})
	statements := []string{"CREATE USER " + account + " IDENTIFIED BY 'mandatory_login_pw'"}
	for _, granted := range grants {
		statements = append(statements, "GRANT "+granted+" TO "+account)
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
