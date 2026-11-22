//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// grantAllTable is what GRANT ALL on a table grants on each engine, one
// `table t1 <privilege> NO` per privilege, in order.
var grantAllTable = map[string][]string{
	"MySQL": {
		"table t1 ALTER NO", "table t1 CREATE NO", "table t1 CREATE VIEW NO", "table t1 DELETE NO",
		"table t1 DROP NO", "table t1 INDEX NO", "table t1 INSERT NO", "table t1 REFERENCES NO",
		"table t1 SELECT NO", "table t1 SHOW VIEW NO", "table t1 TRIGGER NO", "table t1 UPDATE NO",
	},
	"MariaDB": {
		"table t1 ALTER NO", "table t1 CREATE NO", "table t1 CREATE VIEW NO", "table t1 DELETE HISTORY NO",
		"table t1 DELETE NO", "table t1 DROP NO", "table t1 INDEX NO", "table t1 INSERT NO",
		"table t1 REFERENCES NO", "table t1 SELECT NO", "table t1 SHOW VIEW NO", "table t1 TRIGGER NO",
		"table t1 UPDATE NO",
	},
}

// grantAllDatabaseCount is how many privileges GRANT ALL on a database grants
// on each engine: MariaDB 11.8 adds DELETE HISTORY and SHOW CREATE ROUTINE to
// MySQL's list.
var grantAllDatabaseCount = map[string]int{"MySQL": 18, "MariaDB": 20}

// databasePrivileges is how many privileges the server holds for the role on
// the whole database.
func (r columnGrantRun) databasePrivileges(c *qt.C) int {
	c.Helper()
	var held int
	c.Assert(r.scratch.admin.QueryRowContext(c.Context(), `
		SELECT COUNT(*) FROM information_schema.SCHEMA_PRIVILEGES
		WHERE TABLE_SCHEMA = ? AND SUBSTRING_INDEX(GRANTEE, '@', 1) = CONCAT('''', ?, '''')`,
		r.database, r.role).Scan(&held), qt.IsNil)
	return held
}

// A declared GRANT ALL on a table is held once the server granted it, so the
// next plan is empty and a member keeps every privilege ALL gives. Read as
// PostgreSQL's list, ALL is never held, measured on MySQL 8.4.13 and MariaDB
// 11.8.9: every run plans GRANT ALL again, and for a role the schema creates
// every run after the first also revokes ALTER, CREATE, CREATE VIEW, DROP,
// INDEX and SHOW VIEW, and DELETE HISTORY on MariaDB.
func TestSchemaApplyMySQLFamilyGrantAllConvergesE2E(t *testing.T) {
	tests := []struct {
		name     string
		declared string
	}{
		{name: "a role the schema does not create", declared: revokeAllSchema + "GRANT ALL ON t1 TO %[1]s;\n"},
		{name: "a role the schema creates", declared: revokeAllSchema + "CREATE ROLE %[1]s;\nGRANT ALL ON t1 TO %[1]s;\n"},
	}
	for _, engine := range mysqlServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newColumnGrantRunFrom(c, engine.admin, revokeAllSchema, nil)

				for range 2 {
					applied := run.apply(c, test.declared, "--auto-approve")

					c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
					c.Assert(run.privileges(c), qt.DeepEquals, grantAllTable[engine.name])
				}
				again := run.apply(c, test.declared, "--dry-run")

				c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
				c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
				member := run.member(c)
				for _, statement := range memberOfNothingOnT1 {
					_, err := member.ExecContext(c.Context(), statement)
					c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
				}
			})
		}
	}
}

// A declared GRANT ALL on the database is held once the server granted it, so
// the next plan is empty. Read as PostgreSQL's schema list, ALL is never held,
// measured on MySQL 8.4.13 and MariaDB 11.8.9, and every run plans GRANT ALL
// again.
func TestSchemaApplyMySQLFamilyGrantAllOnDatabaseConvergesE2E(t *testing.T) {
	tests := []struct {
		name     string
		declared string
	}{
		{name: "a role the schema does not create", declared: revokeAllSchema + "-- The grants file gives %[1]s ALL on the database.\n"},
		{name: "a role the schema creates", declared: revokeAllSchema + "CREATE ROLE %[1]s;\n"},
	}
	for _, engine := range mysqlServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newColumnGrantRunFrom(c, engine.admin, revokeAllSchema, nil)
				grants := filepath.Join(c.TempDir(), "grants.yaml")
				c.Assert(os.WriteFile(grants, fmt.Appendf(nil,
					"grants:\n  everything:\n    role: %s\n    privileges: [ALL]\n    on_schema: %s\n", run.role, run.database), 0o600), qt.IsNil)

				for range 2 {
					applied := run.apply(c, test.declared, "--auto-approve", "--schema-file", grants)

					c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
					c.Assert(run.databasePrivileges(c), qt.Equals, grantAllDatabaseCount[engine.name])
				}
				again := run.apply(c, test.declared, "--dry-run", "--schema-file", grants)

				c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
				c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			})
		}
	}
}

// A declared GRANT ALL on a table is held once the server granted it, so the
// next plan is empty. Read as PostgreSQL's list, which asks for TRUNCATE and
// TRIGGER, ALL is never held, measured on SQL Server 2025, and every run plans
// GRANT ALL again.
func TestSchemaApplySQLServerGrantAllConvergesE2E(t *testing.T) {
	const table = "CREATE TABLE t1 (id INT PRIMARY KEY, name NVARCHAR(50));\n"
	tests := []struct {
		name     string
		declared string
	}{
		{name: "a role the schema does not create", declared: table + "GRANT ALL ON t1 TO [{role}];\n"},
		{name: "a role the schema creates", declared: table + "CREATE ROLE [{role}];\nGRANT ALL ON t1 TO [{role}];\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			server := newSQLServerRevokeAllServer(c, "SELECT 1")

			for range 2 {
				applied := server.apply(c, test.declared, "--auto-approve")

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(server.privileges(c), qt.DeepEquals, []string{"DELETE", "INSERT", "REFERENCES", "SELECT", "UPDATE"})
			}
			again := server.apply(c, test.declared, "--dry-run")

			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			for _, statement := range []string{"SELECT id FROM dbo.t1", "INSERT INTO dbo.t1 VALUES (2, 'b')", "UPDATE dbo.t1 SET name = 'c'", "DELETE FROM dbo.t1"} {
				_, err := server.member.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
			}
		})
	}
}

// A declared GRANT ALL on an Oracle table or view is held once the server
// granted it, so the next plan is empty and a member keeps what ALL gives.
// Read as PostgreSQL's list, which asks for TRUNCATE, ALL is never held,
// measured on Oracle 23.26.3: every run plans GRANT ALL again, and a role the
// schema creates loses what PostgreSQL's ALL does not name. A view holds MERGE
// VIEW where a table holds ALTER, and the read calls both TABLE.
func TestSchemaApplyOracleGrantAllConvergesE2E(t *testing.T) {
	const table = "CREATE TABLE t1 (id NUMBER(10) PRIMARY KEY, name VARCHAR2(50));\n"
	const view = "CREATE VIEW v1 AS SELECT id, name FROM t1;\n"
	tests := []struct {
		name     string
		object   string
		declared string
		want     []string
		member   []string
	}{
		{
			name:     "a table, for a role the schema does not create",
			object:   "T1",
			declared: table + "GRANT ALL ON t1 TO {role};\n",
			want: []string{
				"ALTER", "DEBUG", "DELETE", "FLASHBACK", "INSERT", "ON COMMIT REFRESH",
				"QUERY REWRITE", "READ", "SELECT", "UPDATE",
			},
			member: []string{"SELECT id FROM {account}.t1", "INSERT INTO {account}.t1 VALUES (2, 'b')", "UPDATE {account}.t1 SET name = 'c'", "DELETE FROM {account}.t1"},
		},
		{
			name:     "a table, for a role the schema creates",
			object:   "T1",
			declared: table + "CREATE ROLE {role};\nGRANT ALL ON t1 TO {role};\n",
			want: []string{
				"ALTER", "DEBUG", "DELETE", "FLASHBACK", "INSERT", "ON COMMIT REFRESH",
				"QUERY REWRITE", "READ", "SELECT", "UPDATE",
			},
			member: []string{"SELECT id FROM {account}.t1", "INSERT INTO {account}.t1 VALUES (2, 'b')", "UPDATE {account}.t1 SET name = 'c'", "DELETE FROM {account}.t1"},
		},
		{
			name:     "a view, for a role the schema creates",
			object:   "V1",
			declared: table + view + "CREATE ROLE {role};\nGRANT ALL ON v1 TO {role};\n",
			want: []string{
				"DEBUG", "DELETE", "FLASHBACK", "INSERT", "MERGE VIEW", "ON COMMIT REFRESH",
				"QUERY REWRITE", "READ", "SELECT", "UPDATE",
			},
			member: []string{"SELECT id FROM {account}.v1", "UPDATE {account}.v1 SET name = 'c'"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			server, account := newOracleRevokeAllServer(c, "SELECT 1 FROM dual")
			server.heldQuery = "SELECT privilege FROM all_tab_privs WHERE grantee = :1 AND table_schema = :2 AND table_name = :3 ORDER BY privilege"
			server.heldArgs = []any{server.role, account, test.object}

			for range 2 {
				applied := server.apply(c, test.declared, "--auto-approve")

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(server.privileges(c), qt.DeepEquals, test.want)
			}
			again := server.apply(c, test.declared, "--dry-run")

			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			for _, statement := range test.member {
				_, err := server.member.ExecContext(c.Context(), strings.ReplaceAll(statement, "{account}", account))
				c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
			}
		})
	}
}
