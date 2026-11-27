//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// TestMySQLRoleRowInAnotherCaseUnderPartialRevokesReachE2E grants a role
// SELECT on its database spelled in upper case. Under partial_revokes a
// role's row on a name holding `_` reaches a database it equals without case,
// while an account's row does not: measured on MySQL 8.4.13 and 26.10.0 with
// lower_case_table_names=0, and here by a session of each. The database the
// test creates holds `_`, as every database [mysqlFamilyServer.database] names
// does. `schema security` reports the role's read reach.
func TestMySQLRoleRowInAnotherCaseUnderPartialRevokesReachE2E(t *testing.T) {
	c := qt.New(t)
	server := newMySQLFamilyServer(c, dbtarget.MySQLAdmin)
	withPartialRevokes(c, server)
	database := server.database(c, "case_reach")
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano()%1_000_000_000_000)
	role := granteeIdentifier("pwc" + suffix)
	c.Cleanup(func() {
		_, err := server.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
		c.Check(err, qt.IsNil)
	})
	upper := "SELECT ON " + granteeIdentifier(strings.ToUpper(database)) + ".*"
	partialRevokeExec(c, server, append(partialRevokeTableStatements(database), "CREATE ROLE "+role, "GRANT "+upper+" TO "+role))

	lines := publicFindingLines(c, server.url("mysql", database), suffix)

	c.Assert(loginReads(c, server, database, "pwl"+suffix, []string{role}), qt.Equals, 3)
	c.Assert(loginReads(c, server, database, "pwd"+suffix, []string{upper}), qt.Equals, 0)
	c.Assert(lines, qt.DeepEquals, []string{
		"ROL04 info pwc",
		"roles/read-reach error pwc 3/3 through=[]",
	})
}

// TestSchemaApplyMySQLRevokeUnderARoleRowInAnotherCaseE2E revokes SELECT on a
// table from a role that also holds SELECT on the table's database spelled in
// upper case. Under partial_revokes that row reaches the database for the
// role, so the revoke takes the grant on the table and the role still reads
// it: measured on MySQL 8.4.13 and 26.10.0. The apply is refused before it
// runs, naming the row.
func TestSchemaApplyMySQLRevokeUnderARoleRowInAnotherCaseE2E(t *testing.T) {
	c := qt.New(t)
	server := newMySQLFamilyServer(c, dbtarget.MySQLAdmin)
	withPartialRevokes(c, server)
	database := server.database(c, "case_cover")
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano()%1_000_000_000_000)
	role := granteeIdentifier("pcc" + suffix)
	c.Cleanup(func() {
		_, err := server.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
		c.Check(err, qt.IsNil)
	})
	upper := granteeIdentifier(strings.ToUpper(database))
	partialRevokeExec(c, server, append(partialRevokeTableStatements(database),
		"CREATE ROLE "+role,
		"GRANT SELECT ON "+upper+".* TO "+role,
		"GRANT SELECT ON "+granteeIdentifier(database)+".a TO "+role,
	))
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE a (id int PRIMARY KEY);\nCREATE TABLE b (id int PRIMARY KEY);\n"+
		"CREATE TABLE c (id int PRIMARY KEY);\nREVOKE SELECT ON a FROM "+role+";\n"), 0o600), qt.IsNil)

	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
		"schema", "apply", "--db-url", server.url("mysql", database), "--schema-file", schema, "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stderr, qt.Contains, "holds SELECT ON "+upper+".*, which covers table a")
	c.Assert(loginReads(c, server, database, "pcl"+suffix, []string{role}), qt.Equals, 3)
}
