//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A declared revoke on a table the plan creates is not planned on MySQL and
// MariaDB, where the new table gives the role nothing to revoke. Planned, the
// revoke fails the apply with Error 1147, measured on MySQL 8.4.13 and MariaDB
// 11.8.9, and every later run fails the same way. The role keeps SELECT on t1,
// which the schema does not mention, and a member is refused t3.
func TestSchemaApplyMySQLFamilyRevokeOnACreatedTableE2E(t *testing.T) {
	const tables = "CREATE TABLE t1 (id INT PRIMARY KEY);\nCREATE TABLE t3 (id INT PRIMARY KEY);\n"
	tests := []struct {
		name     string
		declared string
	}{
		{name: "a role the schema does not create", declared: tables + "REVOKE SELECT ON t3 FROM %[1]s;\n"},
		{name: "a role the schema creates", declared: tables + "CREATE ROLE %[1]s;\nGRANT SELECT ON t1 TO %[1]s;\nREVOKE ALL ON t3 FROM %[1]s;\n"},
	}
	for _, engine := range mysqlServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newColumnGrantRunFrom(c, engine.admin, "CREATE TABLE t1 (id INT PRIMARY KEY);\n",
					[]string{"GRANT SELECT ON `%[1]s`.t1 TO %[2]s"})

				applied := run.apply(c, test.declared, "--auto-approve")

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(run.privileges(c), qt.DeepEquals, []string{"table t1 SELECT NO"})
				again := run.apply(c, test.declared, "--dry-run")

				c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
				c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
				member := run.member(c)
				_, err := member.ExecContext(c.Context(), "SELECT id FROM t1")
				c.Assert(err, qt.IsNil)
				_, err = member.ExecContext(c.Context(), "SELECT id FROM t3")
				c.Assert(err, qt.ErrorMatches, `(?s).*Error 1142.*`)
			})
		}
	}
}

// A declared revoke on a table the plan creates is not planned on Oracle,
// where the new table gives the role nothing to revoke. Planned, the revoke
// fails with ORA-01927 after CREATE TABLE has run, measured on Oracle 23.26.3.
// The role keeps SELECT on t1, which the schema does not mention, and a member
// is refused t3.
func TestSchemaApplyOracleRevokeOnACreatedTableE2E(t *testing.T) {
	const tables = "CREATE TABLE t1 (id NUMBER(10) PRIMARY KEY, name VARCHAR2(50));\nCREATE TABLE t3 (id NUMBER(10) PRIMARY KEY);\n"
	tests := []struct {
		name     string
		declared string
	}{
		{name: "a role the schema does not create", declared: tables + "REVOKE SELECT ON t3 FROM {role};\n"},
		{name: "a role the schema creates", declared: tables + "CREATE ROLE {role};\nGRANT SELECT ON t1 TO {role};\nREVOKE ALL ON t3 FROM {role};\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			server, account := newOracleRevokeAllServer(c, "GRANT SELECT ON t1 TO {role}")

			applied := server.apply(c, test.declared, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(server.privileges(c), qt.DeepEquals, []string{"SELECT"})
			t3 := server
			t3.heldQuery = "SELECT privilege FROM all_tab_privs WHERE grantee = :1 AND table_schema = :2 AND table_name = 'T3' ORDER BY privilege"
			c.Assert(t3.privileges(c), qt.IsNil)
			again := server.apply(c, test.declared, "--dry-run")

			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			_, err := server.member.ExecContext(c.Context(), strings.ReplaceAll("SELECT id FROM {account}.t1", "{account}", account))
			c.Assert(err, qt.IsNil)
			_, err = server.member.ExecContext(c.Context(), strings.ReplaceAll("SELECT id FROM {account}.t3", "{account}", account))
			c.Assert(err, qt.ErrorMatches, `(?s).*ORA-00942.*`)
		})
	}
}

// A declared revoke on a table that a grant on its whole database or on every
// database keeps is refused on MySQL and MariaDB, for a table the plan creates
// and for one that exists. Measured on MySQL 8.4.13 and MariaDB 11.8.9 with
// the refusal taken out: the plan for a created table has only CREATE TABLE,
// the plan for an existing one is empty, the next plan reads as synced, and a
// member of the role reads t3. The server refuses the REVOKE itself with Error
// 1147. The refusal names the grant, the apply changes nothing, and the
// member keeps reading t1 through it.
func TestSchemaApplyMySQLFamilyRevokeUnderAWiderGrantE2E(t *testing.T) {
	tests := []struct {
		name     string
		schema   string
		held     string
		declared string
		want     string
	}{
		{
			name:     "a grant on the database, a table the plan creates",
			schema:   "CREATE TABLE t1 (id INT PRIMARY KEY);\n",
			held:     "GRANT SELECT ON `%[1]s`.* TO %[2]s",
			declared: "CREATE TABLE t1 (id INT PRIMARY KEY);\nCREATE TABLE t3 (id INT PRIMARY KEY);\nREVOKE SELECT ON t3 FROM %[1]s;\n",
			want:     `(?s).*role "%[2]s" holds SELECT ON %[1]s\.\*, which covers table t3, so a revoke on the table cannot take it: revoke SELECT ON %[1]s\.\* from "%[2]s" instead.*`,
		},
		{
			name:     "a grant on the database, a table that exists",
			schema:   "CREATE TABLE t1 (id INT PRIMARY KEY);\nCREATE TABLE t3 (id INT PRIMARY KEY);\n",
			held:     "GRANT SELECT ON `%[1]s`.* TO %[2]s",
			declared: "CREATE TABLE t1 (id INT PRIMARY KEY);\nCREATE TABLE t3 (id INT PRIMARY KEY);\nREVOKE ALL ON t3 FROM %[1]s;\n",
			want:     `(?s).*role "%[2]s" holds SELECT ON %[1]s\.\*, which covers table t3, so a revoke on the table cannot take it.*`,
		},
		{
			name:     "a grant on every database",
			schema:   "CREATE TABLE t1 (id INT PRIMARY KEY);\n",
			held:     "GRANT SELECT ON *.* TO %[2]s",
			declared: "CREATE TABLE t1 (id INT PRIMARY KEY);\nCREATE TABLE t3 (id INT PRIMARY KEY);\nREVOKE SELECT ON t3 FROM %[1]s;\n",
			want:     `(?s).*role "%[2]s" holds SELECT ON \*\.\*, which covers table t3, so a revoke on the table cannot take it: revoke SELECT ON \*\.\* from "%[2]s" instead.*`,
		},
	}
	for _, engine := range mysqlServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newColumnGrantRunFrom(c, engine.admin, test.schema, []string{test.held})

				applied := run.apply(c, test.declared, "--auto-approve")

				c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(applied.Stderr, qt.Matches, fmt.Sprintf(test.want, run.database, run.role))
				c.Assert(run.privileges(c), qt.IsNil)
				_, err := run.member(c).ExecContext(c.Context(), "SELECT id FROM t1")
				c.Assert(err, qt.IsNil)
			})
		}
	}
}

// On MariaDB a grant to PUBLIC on the database gives every role the
// privilege, and a revoke on one of its tables is refused the same way,
// naming PUBLIC. Measured on MariaDB 10.11.19 and 11.8.9: the grant is a row
// of SCHEMA_PRIVILEGES whose grantee is PUBLIC with an empty host, and the
// table REVOKE fails with Error 1147.
func TestSchemaApplyMariaDBRevokeUnderAPublicGrantE2E(t *testing.T) {
	c := qt.New(t)
	run := newColumnGrantRunFrom(c, dbtarget.MariaDBAdmin, "CREATE TABLE t1 (id INT PRIMARY KEY);\n",
		[]string{"GRANT SELECT ON `%[1]s`.* TO PUBLIC"})
	c.Cleanup(func() {
		// Dropping the database leaves the grant to PUBLIC in mysql.db.
		_, err := run.scratch.admin.ExecContext(context.Background(), strings.ReplaceAll("REVOKE SELECT ON `{database}`.* FROM PUBLIC", "{database}", run.database))
		c.Check(err, qt.IsNil)
	})

	applied := run.apply(c, "CREATE TABLE t1 (id INT PRIMARY KEY);\nCREATE TABLE t3 (id INT PRIMARY KEY);\nREVOKE SELECT ON t3 FROM %[1]s;\n",
		"--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stderr, qt.Matches, fmt.Sprintf(
		`(?s).*PUBLIC holds SELECT ON %[1]s\.\*, which gives role "%[2]s" the privilege on table t3, so a revoke on the table cannot take it: `+
			`revoke SELECT ON %[1]s\.\* from PUBLIC instead.*`, run.database, run.role))
}

// A grant on the database that the plan removes keeps nothing: for a role the
// schema creates, the plan revokes the undeclared database grant, plans no
// revoke on the created t3, and the next plan is empty. The role holds nothing
// on the database or its tables.
func TestSchemaApplyMySQLFamilyRevokeUnderARemovedDatabaseGrantE2E(t *testing.T) {
	const declared = "CREATE TABLE t1 (id INT PRIMARY KEY);\nCREATE TABLE t3 (id INT PRIMARY KEY);\nCREATE ROLE %[1]s;\nREVOKE SELECT ON t3 FROM %[1]s;\n"
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newColumnGrantRunFrom(c, engine.admin, "CREATE TABLE t1 (id INT PRIMARY KEY);\n",
				[]string{"GRANT SELECT ON `%[1]s`.* TO %[2]s"})

			applied := run.apply(c, declared, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			again := run.apply(c, declared, "--dry-run")

			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			c.Assert(heldOnDatabase(c, run), qt.IsNil)
			c.Assert(run.privileges(c), qt.IsNil)
		})
	}
}

// heldOnDatabase is what the server holds for the run's role on the whole
// database, in order.
func heldOnDatabase(c *qt.C, run columnGrantRun) []string {
	c.Helper()
	rows, err := run.scratch.admin.QueryContext(c.Context(), `
		SELECT PRIVILEGE_TYPE FROM information_schema.SCHEMA_PRIVILEGES
		WHERE TABLE_SCHEMA = ? AND SUBSTRING_INDEX(GRANTEE, '@', 1) = CONCAT('''', ?, '''')
		ORDER BY PRIVILEGE_TYPE`, run.database, run.role)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var held []string
	for rows.Next() {
		var privilege string
		c.Assert(rows.Scan(&privilege), qt.IsNil)
		held = append(held, privilege)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return held
}

// A declared revoke on a table that SELECT ANY TABLE keeps is refused on
// Oracle, for a table the plan creates and for one that exists. Measured on
// Oracle 23.26.3 with the refusal taken out: the plan for a created table has
// only CREATE TABLE, the next plan reads as synced, and a member of the role
// reads t3. The owner's REVOKE fails with ORA-01927.
func TestSchemaApplyOracleRevokeUnderASystemPrivilegeE2E(t *testing.T) {
	const tables = "CREATE TABLE t1 (id NUMBER(10) PRIMARY KEY, name VARCHAR2(50));\nCREATE TABLE t3 (id NUMBER(10) PRIMARY KEY);\n"
	tests := []struct {
		name     string
		declared string
		table    string
	}{
		{name: "a table the plan creates", declared: tables + "REVOKE SELECT ON t3 FROM {role};\n", table: "t3"},
		{name: "a table that exists", declared: tables + "REVOKE SELECT ON t1 FROM {role};\n", table: "t1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			server, account := newOracleRevokeAllServer(c, "GRANT SELECT ON t1 TO {role}")
			admin, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.OracleAdmin))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(admin) })
			execOracle(c.Context(), c, admin, "GRANT SELECT ANY TABLE TO "+server.role)

			applied := server.apply(c, test.declared, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stderr, qt.Matches, `(?s).*role "`+server.role+`" holds SELECT ANY TABLE, which covers table `+test.table+
				`, so a revoke on the table cannot take it: revoke SELECT ANY TABLE from "`+server.role+`" instead.*`)
			c.Assert(server.privileges(c), qt.DeepEquals, []string{"SELECT"})
			_, err = server.member.ExecContext(c.Context(), strings.ReplaceAll("SELECT id FROM {account}.t1", "{account}", account))
			c.Assert(err, qt.IsNil)
		})
	}
}

// ptah-compat refuses the same revoke on both of its paths: `schema apply`,
// which plans through the connected comparison, and `schema diff`, which
// reaches the comparison through a variant that returns no error and checks
// the declaration itself. Measured with the refusal taken out of each path:
// the run exits 0 and reports no change, and the role keeps reading t3.
func TestSchemaApplyCompatMySQLFamilyRevokeUnderADatabaseGrantE2E(t *testing.T) {
	const declared = "CREATE TABLE t1 (id INT PRIMARY KEY);\nCREATE TABLE t3 (id INT PRIMARY KEY);\nREVOKE SELECT ON t3 FROM %[1]s;\n"
	tests := []struct {
		name string
		args []string
	}{
		{name: "schema apply", args: []string{"schema", "apply", "-u", "{url}", "--to", "file://schema.sql", "--dev-url", "{dev}", "--auto-approve"}},
		{name: "schema diff", args: []string{"schema", "diff", "--from", "{url}", "--to", "file://schema.sql", "--dev-url", "{dev}"}},
	}
	for _, engine := range mysqlServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newColumnGrantRunFrom(c, engine.admin, "CREATE TABLE t1 (id INT PRIMARY KEY);\nCREATE TABLE t3 (id INT PRIMARY KEY);\n",
					[]string{"GRANT SELECT ON `%[1]s`.* TO %[2]s"})
				_, dev := run.scratch.database(c, "dev")
				work := c.TempDir()
				c.Assert(os.WriteFile(filepath.Join(work, "schema.sql"), []byte(fmt.Sprintf(declared, run.role)), 0o600), qt.IsNil)
				places := strings.NewReplacer("{url}", run.url, "{dev}", dev)
				args := make([]string, 0, len(test.args))
				for _, arg := range test.args {
					args = append(args, places.Replace(arg))
				}

				ran := clirun.Run(c, clirun.Compat, clirun.Options{Dir: work}, args...)

				c.Assert(ran.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", ran.Stdout, ran.Stderr))
				c.Assert(ran.Stdout+ran.Stderr, qt.Matches, fmt.Sprintf(
					`(?s).*role "%[2]s" holds SELECT ON %[1]s\.\*, which covers table t3, so a revoke on the table cannot take it.*`,
					run.database, run.role))
				c.Assert(heldOnDatabase(c, run), qt.DeepEquals, []string{"SELECT"})
			})
		}
	}
}

// A schema that revokes the covering grant on the database beside the revoke
// on the table is not refused: the plan revokes the database grant, the table
// revoke needs no statement, and the next plan is empty. The role holds
// nothing on the database or its tables.
func TestSchemaApplyMySQLFamilyRevokeWithItsDatabaseGrantE2E(t *testing.T) {
	const declared = "CREATE TABLE t1 (id INT PRIMARY KEY);\nCREATE TABLE t3 (id INT PRIMARY KEY);\nREVOKE SELECT ON t3 FROM %[1]s;\n"
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newColumnGrantRunFrom(c, engine.admin, "CREATE TABLE t1 (id INT PRIMARY KEY);\nCREATE TABLE t3 (id INT PRIMARY KEY);\n",
				[]string{"GRANT SELECT ON `%[1]s`.* TO %[2]s"})
			revokes := filepath.Join(c.TempDir(), "revokes.yaml")
			c.Assert(os.WriteFile(revokes, fmt.Appendf(nil,
				"revokes:\n  database:\n    role: %s\n    privileges: [SELECT]\n    on_schema: %s\n", run.role, run.database), 0o600), qt.IsNil)

			applied := run.apply(c, declared, "--auto-approve", "--schema-file", revokes)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			again := run.apply(c, declared, "--dry-run", "--schema-file", revokes)

			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			c.Assert(heldOnDatabase(c, run), qt.IsNil)
			c.Assert(run.privileges(c), qt.IsNil)
		})
	}
}
