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

// partialRevokeTables are the tables a, b and c the partial revoke tests
// create in their database and declare in their schema.
var partialRevokeTables = []string{"a", "b", "c"}

// TestMySQLPartialRevokeReachE2E takes a database from a role's grant on every
// database with a partial revoke. Under partial_revokes the global grant stays
// in mysql.user and the revoke is a restriction beside it: measured on MySQL
// 8.4.13 and 26.10.0 and here by a session, a role holding SELECT and INSERT
// on every database with SELECT revoked from the database inserts into each of
// its tables and reads none, and so does its member. `schema security`
// reports the write reach of both and no read reach, and the read reach of a
// role whose grant no revoke narrows.
//
// partial_revokes is global, so the test does not run in parallel, as
// [TestMySQLMandatoryRolesReachE2E] says.
func TestMySQLPartialRevokeReachE2E(t *testing.T) {
	c := qt.New(t)
	server := newMySQLFamilyServer(c, dbtarget.MySQLAdmin)
	withPartialRevokes(c, server)
	database := server.database(c, "partial")
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano()%1_000_000_000_000)
	revoked, member, whole := granteeIdentifier("prv"+suffix), granteeIdentifier("prm"+suffix), granteeIdentifier("pru"+suffix)
	c.Cleanup(func() {
		_, err := server.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+revoked+", "+member+", "+whole)
		c.Check(err, qt.IsNil)
	})
	partialRevokeExec(c, server, append(partialRevokeTableStatements(database),
		"CREATE ROLE "+revoked+", "+member+", "+whole,
		"GRANT SELECT, INSERT ON *.* TO "+revoked,
		"REVOKE SELECT ON "+granteeIdentifier(database)+".* FROM "+revoked,
		"GRANT "+revoked+" TO "+member,
		"GRANT SELECT ON *.* TO "+whole,
	))

	lines := publicFindingLines(c, server.url("mysql", database), suffix)

	c.Assert(loginReads(c, server, database, "prl"+suffix, []string{revoked}), qt.Equals, 0)
	c.Assert(loginReads(c, server, database, "pml"+suffix, []string{member}), qt.Equals, 0)
	c.Assert(loginReads(c, server, database, "pul"+suffix, []string{whole}), qt.Equals, 3)
	c.Assert(lines, qt.DeepEquals, []string{
		"ROL04 info prm",
		"ROL04 info pru",
		"roles/read-reach error pru 3/3 through=[]",
		"roles/write-reach error prm 3/3 through=[prv]",
		"roles/write-reach error prv 3/3 through=[]",
	})
}

// TestSchemaApplyMySQLRevokeUnderAPartiallyRevokedGrantE2E revokes SELECT on
// a table from a role whose grant on every database a partial revoke takes
// from the table's database. The global grant no longer reaches the table, so
// the revoke takes the role's grant on it: measured on MySQL 8.4.13 and
// 26.10.0, the REVOKE succeeds and a session with the role active is refused
// the table. The apply is not refused as one the global grant undoes.
func TestSchemaApplyMySQLRevokeUnderAPartiallyRevokedGrantE2E(t *testing.T) {
	c := qt.New(t)
	server := newMySQLFamilyServer(c, dbtarget.MySQLAdmin)
	withPartialRevokes(c, server)
	database := server.database(c, "partial_cover")
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano()%1_000_000_000_000)
	role := granteeIdentifier("prc" + suffix)
	c.Cleanup(func() {
		_, err := server.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
		c.Check(err, qt.IsNil)
	})
	partialRevokeExec(c, server, append(partialRevokeTableStatements(database),
		"CREATE ROLE "+role,
		"GRANT SELECT ON *.* TO "+role,
		"REVOKE SELECT ON "+granteeIdentifier(database)+".* FROM "+role,
		"GRANT SELECT ON "+granteeIdentifier(database)+".a TO "+role,
	))
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE a (id int PRIMARY KEY);\nCREATE TABLE b (id int PRIMARY KEY);\n"+
		"CREATE TABLE c (id int PRIMARY KEY);\nREVOKE SELECT ON a FROM "+role+";\n"), 0o600), qt.IsNil)

	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
		"schema", "apply", "--db-url", server.url("mysql", database), "--schema-file", schema, "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(loginReads(c, server, database, "pcl"+suffix, []string{role}), qt.Equals, 0)
}

// withPartialRevokes turns MySQL's partial_revokes on, and sets it back when
// the test ends. Register the cleanup that drops the roles holding a partial
// revoke after calling it: the server refuses to turn the setting off while
// one exists (Error 3896), and cleanups run last registered first.
func withPartialRevokes(c *qt.C, server mysqlFamilyServer) {
	c.Helper()
	var was int
	c.Assert(server.admin.QueryRowContext(c.Context(), "SELECT @@GLOBAL.partial_revokes").Scan(&was), qt.IsNil)
	c.Cleanup(func() {
		_, err := server.admin.ExecContext(context.Background(), "SET GLOBAL partial_revokes = ?", was)
		c.Check(err, qt.IsNil)
	})
	_, err := server.admin.ExecContext(c.Context(), "SET GLOBAL partial_revokes = ON")
	c.Assert(err, qt.IsNil)
}

// partialRevokeTableStatements create [partialRevokeTables] in database.
func partialRevokeTableStatements(database string) []string {
	statements := make([]string, 0, len(partialRevokeTables))
	for _, table := range partialRevokeTables {
		statements = append(statements, "CREATE TABLE "+granteeIdentifier(database)+"."+table+" (id int PRIMARY KEY)")
	}
	return statements
}

// partialRevokeExec runs statements on the server as its administrator.
func partialRevokeExec(c *qt.C, server mysqlFamilyServer, statements []string) {
	c.Helper()
	for _, statement := range statements {
		_, err := server.admin.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}
