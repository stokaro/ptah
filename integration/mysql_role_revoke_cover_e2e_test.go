//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// roleCoverSchema declares the two tables of the database, and revokes from
// the role, %[1]s, the SELECT on t3 it holds.
const roleCoverSchema = "CREATE TABLE t1 (id INT PRIMARY KEY);\nCREATE TABLE t3 (id INT PRIMARY KEY);\nREVOKE SELECT ON t3 FROM %[1]s;\n"

// roleCoverHeld gives the run's role SELECT on t3 of its database, so a
// revoke of it is one the server carries out. Each statement is a format
// whose %[1]s is the database and %[2]s the role.
var roleCoverHeld = []string{"GRANT SELECT ON `%[1]s`.t3 TO %[2]s"}

// roleCoverNames writes the run's names into text: {db} is the database,
// {prefix} the database without its last four bytes, and {role} the run's
// role.
func roleCoverNames(run columnGrantRun, text string) string {
	return strings.NewReplacer("{db}", run.database, "{prefix}", run.database[:len(run.database)-4], "{role}", run.role).Replace(text)
}

// TestSchemaApplyMySQLFamilyRevokeUnderARoleGrantE2E refuses a declared
// revoke on a table that a grant held by a role the revoke's role holds keeps:
// one wider than the table, or one on the table itself. A member holds what its
// roles hold, so the revoke takes nothing:
// measured on MySQL 8.4.13 and here, with parent holding SELECT ON `gc%`.*
// and granted to member, REVOKE SELECT ON gcdb.t1 FROM member succeeds and a
// session with member active still reads t1. The refusal names the role that
// holds the grant and changes nothing on the server.
func TestSchemaApplyMySQLFamilyRevokeUnderARoleGrantE2E(t *testing.T) {
	// The role holding the grant is {role}_p, and each statement and refusal
	// is in the words of [roleCoverNames].
	tests := []struct {
		name    string
		held    []string
		refusal string
	}{
		{
			name: "a pattern grant held by a role the role holds",
			held: []string{"CREATE ROLE {role}_p", "GRANT SELECT ON `{prefix}%`.* TO {role}_p", "GRANT {role}_p TO {role}"},
			refusal: "{role}_p holds SELECT ON `{prefix}%`.*, which gives role \"{role}\" the privilege on table t3, " +
				"so a revoke on the table cannot take it: revoke SELECT ON `{prefix}%`.* from {role}_p instead",
		},
		{
			name: "a grant on every database held by a role the role holds",
			held: []string{"CREATE ROLE {role}_p", "GRANT SELECT ON *.* TO {role}_p", "GRANT {role}_p TO {role}"},
			refusal: "{role}_p holds SELECT ON *.*, which gives role \"{role}\" the privilege on table t3, " +
				"so a revoke on the table cannot take it: revoke SELECT ON *.* from {role}_p instead",
		},
		{
			name: "a grant on the database held through two memberships",
			held: []string{
				"CREATE ROLE {role}_p", "CREATE ROLE {role}_m", "GRANT SELECT ON `{db}`.* TO {role}_p",
				"GRANT {role}_p TO {role}_m", "GRANT {role}_m TO {role}",
			},
			refusal: "{role}_p holds SELECT ON {db}.*, which gives role \"{role}\" the privilege on table t3, " +
				"so a revoke on the table cannot take it: revoke SELECT ON {db}.* from {role}_p instead",
		},
		{
			name: "a grant on the table held by a role the role holds",
			held: []string{"CREATE ROLE {role}_p", "GRANT SELECT ON `{db}`.t3 TO {role}_p", "GRANT {role}_p TO {role}"},
			refusal: "{role}_p holds SELECT ON t3, which gives role \"{role}\" the privilege on table t3, " +
				"so a revoke from \"{role}\" cannot take it: revoke SELECT ON t3 from {role}_p instead, or end the membership that passes it on",
		},
	}
	for _, engine := range mysqlServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newColumnGrantRunFrom(c, engine.admin, "CREATE TABLE t1 (id INT PRIMARY KEY);\nCREATE TABLE t3 (id INT PRIMARY KEY);\n", roleCoverHeld)
				dropRoleCoverRoles(c, run)
				holdRoleCoverGrants(c, run, test.held)

				applied := run.apply(c, roleCoverSchema, "--auto-approve")

				c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(applied.Stderr, qt.Contains, roleCoverNames(run, test.refusal))
				c.Assert(showGrants(c, run), qt.Contains, "GRANT SELECT ON `"+run.database+"`.`t3` TO `"+run.role+"`")
				c.Assert(roleCoverRead(c, run), qt.IsNil)
			})
		}
	}
}

// TestSchemaApplyMySQLFamilyRevokeOutsideARoleGrantE2E is the control: a
// wider grant held by a role that holds the revoke's role keeps nothing for
// it, so the revoke runs, and a session with the role active is refused t3.
func TestSchemaApplyMySQLFamilyRevokeOutsideARoleGrantE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newColumnGrantRunFrom(c, engine.admin, "CREATE TABLE t1 (id INT PRIMARY KEY);\nCREATE TABLE t3 (id INT PRIMARY KEY);\n", roleCoverHeld)
			dropRoleCoverRoles(c, run)
			holdRoleCoverGrants(c, run, []string{"CREATE ROLE {role}_p", "GRANT SELECT ON `{db}`.* TO {role}_p", "GRANT {role} TO {role}_p"})

			applied := run.apply(c, roleCoverSchema, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(showGrants(c, run), qt.Not(qt.Contains), "`"+run.database+"`.`t3`")
			err := roleCoverRead(c, run)
			c.Assert(refusedForPrivilege(err), qt.IsTrue, qt.Commentf("%v", err))
		})
	}
}

// removedMembershipSchema declares t1 and t3, the role {role}_p holding
// SELECT on the whole database, and the role {role} with no memberships,
// revoking from it SELECT on t3, in the words of [roleCoverNames].
const removedMembershipSchema = `tables:
  t1:
    columns:
      id: {type: INT, primary: true}
  t3:
    columns:
      id: {type: INT, primary: true}
roles:
  {role}_p: {}
  {role}:
    member_of: []
grants:
  database:
    role: {role}_p
    privileges: [SELECT]
    on_schema: {db}
revokes:
  table:
    role: {role}
    privileges: [SELECT]
    on_table: t3
`

// removedMembershipRun is a run whose role holds SELECT on t3 and the role
// {role}_p, which holds SELECT on the whole database, and the arguments of a
// `schema apply` of [removedMembershipSchema] to it.
func removedMembershipRun(c *qt.C, engine dbtarget.Engine) (columnGrantRun, []string) {
	c.Helper()
	run := newColumnGrantRunFrom(c, engine, "CREATE TABLE t1 (id INT PRIMARY KEY);\nCREATE TABLE t3 (id INT PRIMARY KEY);\n", roleCoverHeld)
	dropRoleCoverRoles(c, run)
	holdRoleCoverGrants(c, run, []string{"CREATE ROLE {role}_p", "GRANT SELECT ON `{db}`.* TO {role}_p", "GRANT {role}_p TO {role}"})
	schema := filepath.Join(c.TempDir(), "schema.yaml")
	c.Assert(os.WriteFile(schema, []byte(roleCoverNames(run, removedMembershipSchema)), 0o600), qt.IsNil)
	return run, []string{"schema", "apply", "--db-url", run.url, "--schema-file", schema}
}

// TestSchemaApplyMySQLRevokeBesideARemovedMembershipE2E applies
// [removedMembershipSchema] on MySQL, which plans memberships. The plan
// removes the role's membership, so the grant on the database keeps nothing
// for it once the plan runs: the revoke is planned before the membership's
// removal, both run, and a session with the role active is refused t3.
func TestSchemaApplyMySQLRevokeBesideARemovedMembershipE2E(t *testing.T) {
	c := qt.New(t)
	run, args := removedMembershipRun(c, dbtarget.MySQLAdmin)

	planned := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute}, append(args, "--dry-run")...)
	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute}, append(args, "--auto-approve")...)

	c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
	c.Assert(plannedRoleStatements(planned.Stdout), qt.DeepEquals, []string{
		roleCoverNames(run, "REVOKE SELECT ON `{db}`.`t3` FROM `{role}`;"),
		roleCoverNames(run, "REVOKE `{role}_p` FROM `{role}`;"),
	})
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	err := roleCoverRead(c, run)
	c.Assert(refusedForPrivilege(err), qt.IsTrue, qt.Commentf("%v", err))
}

// TestSchemaApplyMariaDBRevokeBesideAnUnplannedMembershipE2E applies
// [removedMembershipSchema] on MariaDB, which plans no membership, so the
// role keeps its membership and the grant on the database keeps SELECT on t3
// for it. The revoke is refused, and a session with the role active still
// reads t3.
func TestSchemaApplyMariaDBRevokeBesideAnUnplannedMembershipE2E(t *testing.T) {
	c := qt.New(t)
	run, args := removedMembershipRun(c, dbtarget.MariaDBAdmin)

	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute}, append(args, "--auto-approve")...)

	c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stderr, qt.Contains, roleCoverNames(run, "{role}_p holds SELECT ON {db}.*, which the schema grants too and which gives role \"{role}\" the privilege on table t3"))
	c.Assert(roleCoverRead(c, run), qt.IsNil)
}

// TestSchemaApplyMySQLRevokeBesideARoleNamedPublicE2E revokes SELECT on t3
// from a role on a MySQL server holding a role named PUBLIC, which MySQL
// allows, that holds SELECT on the whole database. That role is no account's
// unless granted, so it keeps nothing for the revoke's role: the revoke runs,
// and a session with the role active is refused t3.
func TestSchemaApplyMySQLRevokeBesideARoleNamedPublicE2E(t *testing.T) {
	c := qt.New(t)
	run := newColumnGrantRunFrom(c, dbtarget.MySQLAdmin, "CREATE TABLE t1 (id INT PRIMARY KEY);\nCREATE TABLE t3 (id INT PRIMARY KEY);\n", roleCoverHeld)
	c.Cleanup(func() {
		_, err := run.scratch.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS PUBLIC")
		c.Check(err, qt.IsNil)
	})
	holdRoleCoverGrants(c, run, []string{"CREATE ROLE PUBLIC", "GRANT SELECT ON `{db}`.* TO PUBLIC"})

	applied := run.apply(c, roleCoverSchema, "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(showGrants(c, run), qt.Not(qt.Contains), "`"+run.database+"`.`t3`")
	err := roleCoverRead(c, run)
	c.Assert(refusedForPrivilege(err), qt.IsTrue, qt.Commentf("%v", err))
}

// plannedRoleStatements is the REVOKE statements a dry run printed, in order.
func plannedRoleStatements(stdout string) []string {
	var statements []string
	for line := range strings.Lines(stdout) {
		if line = strings.TrimSpace(line); strings.HasPrefix(line, "REVOKE ") {
			statements = append(statements, line)
		}
	}
	return statements
}

// dropRoleCoverRoles drops the roles the run's statements create beside its
// own when the test ends.
func dropRoleCoverRoles(c *qt.C, run columnGrantRun) {
	c.Helper()
	c.Cleanup(func() {
		_, err := run.scratch.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+run.role+"_p, "+run.role+"_m")
		c.Check(err, qt.IsNil)
	})
}

// holdRoleCoverGrants runs statements on the run's server, in the words of
// [roleCoverNames].
func holdRoleCoverGrants(c *qt.C, run columnGrantRun, statements []string) {
	c.Helper()
	for _, statement := range statements {
		statement = roleCoverNames(run, statement)
		_, err := run.scratch.admin.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// roleCoverRead reads t3 in a session of a new user granted the run's role,
// with the role active and no database selected, and returns what the server
// answered. The user is dropped when the test ends.
func roleCoverRead(c *qt.C, run columnGrantRun) error {
	c.Helper()
	user := run.role + "_u"
	c.Cleanup(func() {
		_, err := run.scratch.admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+user)
		c.Check(err, qt.IsNil)
	})
	holdRoleCoverGrants(c, run, []string{"CREATE USER {role}_u IDENTIFIED BY 'ptah_cover_pw'", "GRANT {role} TO {role}_u"})
	config, err := mysqldriver.ParseDSN(run.scratch.adminDSN)
	c.Assert(err, qt.IsNil)
	config.User, config.Passwd, config.DBName = user, "ptah_cover_pw", ""
	db, err := sql.Open("mysql", config.FormatDSN())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	session, err := db.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(session.Close(), qt.IsNil) })
	_, err = session.ExecContext(c.Context(), "SET ROLE "+run.role)
	c.Assert(err, qt.IsNil)
	_, err = session.ExecContext(c.Context(), readProbe(run.database, "t3"))
	return err
}
