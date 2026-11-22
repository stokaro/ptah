//go:build integration

package integration_test

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// sqlServerRoutinePermissionsOf is what the server holds for role on dbo.rt,
// one `<permission> <state>` per row, in order.
func sqlServerRoutinePermissionsOf(c *qt.C, db *sql.DB, role string) []string {
	c.Helper()
	rows, err := db.QueryContext(c.Context(), `
		SELECT pe.permission_name + ' ' + pe.state_desc
		FROM sys.database_permissions AS pe
		JOIN sys.database_principals AS r ON r.principal_id = pe.grantee_principal_id
		WHERE r.name = @p1 AND pe.class = 1 AND pe.major_id = OBJECT_ID(N'dbo.rt')
		ORDER BY 1`, role)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	held := make([]string, 0)
	for rows.Next() {
		var permission string
		c.Assert(rows.Scan(&permission), qt.IsNil)
		held = append(held, strings.TrimSpace(permission))
	}
	c.Assert(rows.Err(), qt.IsNil)
	return held
}

// A declared GRANT ALL on a SQL Server procedure or function is held once the
// server granted it, for a role the schema creates: two applies leave the role
// what ALL gives on the routine, the next plan is empty, and a member runs the
// routine. ALL gives a table-valued function the table's list.
func TestSchemaApplySQLServerGrantAllOnARoutineE2E(t *testing.T) {
	tests := []struct {
		name    string
		routine string
		want    []string
		call    string
	}{
		{
			name:    "a procedure",
			routine: "CREATE PROCEDURE dbo.rt AS BEGIN SELECT 1 AS one; END;\nGO\n",
			want:    []string{"EXECUTE GRANT"},
			call:    "EXEC dbo.rt",
		},
		{
			name:    "a scalar function",
			routine: "CREATE FUNCTION dbo.rt() RETURNS INT AS BEGIN RETURN 1; END;\nGO\n",
			want:    []string{"EXECUTE GRANT", "REFERENCES GRANT"},
			call:    "SELECT dbo.rt()",
		},
		{
			name:    "a table-valued function",
			routine: "CREATE FUNCTION dbo.rt() RETURNS TABLE AS RETURN (SELECT 1 AS one);\nGO\n",
			want:    []string{"DELETE GRANT", "INSERT GRANT", "REFERENCES GRANT", "SELECT GRANT", "UPDATE GRANT"},
			call:    "SELECT one FROM dbo.rt()",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, db := declaredSQLServerDatabase(c)
			file := writeDeclaredSchemaFile(c, "schema.sql", test.routine+"CREATE ROLE rt_runner;\nGRANT ALL ON dbo.rt TO rt_runner;\n")

			for range 2 {
				applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")
				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			}
			settled := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run")

			c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
			c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(sqlServerRoutinePermissionsOf(c, db, "rt_runner"), qt.DeepEquals, test.want)
			declaredSQLServerExec(c, db, "CREATE USER rt_member WITHOUT LOGIN", "ALTER ROLE rt_runner ADD MEMBER rt_member")
			conn, err := db.Conn(c.Context())
			c.Assert(err, qt.IsNil)
			defer func() { c.Assert(conn.Close(), qt.IsNil) }()
			_, err = conn.ExecContext(c.Context(), "EXECUTE AS USER = 'rt_member'; "+test.call+"; REVERT;")
			c.Assert(err, qt.IsNil)
		})
	}
}

// A body change to a SQL Server routine with a declared grant drops the
// routine, creates it again and grants back every permission a role held on
// it: the declared role holds what it held before, and rt_auditor, a role the
// schema does not manage, keeps its permission and the grant option on it.
// The next plan is empty, and a member of the declared role runs the new body.
func TestSchemaApplySQLServerKeepsTheGrantsOnAReplacedRoutineE2E(t *testing.T) {
	tests := []struct {
		name    string
		before  string
		after   string
		grant   string
		want    []string
		auditor string
		call    string
	}{
		{
			name:    "ALL on a procedure",
			before:  "CREATE PROCEDURE dbo.rt AS BEGIN SELECT 1 AS one; END;\nGO\n",
			after:   "CREATE PROCEDURE dbo.rt AS BEGIN SELECT 2 AS one; END;\nGO\n",
			grant:   "GRANT ALL ON dbo.rt TO rt_runner;\n",
			want:    []string{"EXECUTE GRANT"},
			auditor: "EXECUTE",
			call:    "EXEC dbo.rt",
		},
		{
			name:    "ALL on a scalar function",
			before:  "CREATE FUNCTION dbo.rt() RETURNS INT AS BEGIN RETURN 1; END;\nGO\n",
			after:   "CREATE FUNCTION dbo.rt() RETURNS INT AS BEGIN RETURN 2; END;\nGO\n",
			grant:   "GRANT ALL ON dbo.rt TO rt_runner;\n",
			want:    []string{"EXECUTE GRANT", "REFERENCES GRANT"},
			auditor: "EXECUTE",
			call:    "SELECT dbo.rt()",
		},
		{
			name:    "ALL on a table-valued function",
			before:  "CREATE FUNCTION dbo.rt() RETURNS TABLE AS RETURN (SELECT 1 AS one);\nGO\n",
			after:   "CREATE FUNCTION dbo.rt() RETURNS TABLE AS RETURN (SELECT 2 AS one);\nGO\n",
			grant:   "GRANT ALL ON dbo.rt TO rt_runner;\n",
			want:    []string{"DELETE GRANT", "INSERT GRANT", "REFERENCES GRANT", "SELECT GRANT", "UPDATE GRANT"},
			auditor: "SELECT",
			call:    "SELECT one FROM dbo.rt()",
		},
		{
			name:    "EXECUTE on a procedure",
			before:  "CREATE PROCEDURE dbo.rt AS BEGIN SELECT 1 AS one; END;\nGO\n",
			after:   "CREATE PROCEDURE dbo.rt AS BEGIN SELECT 2 AS one; END;\nGO\n",
			grant:   "GRANT EXECUTE ON dbo.rt TO rt_runner;\n",
			want:    []string{"EXECUTE GRANT"},
			auditor: "EXECUTE",
			call:    "EXEC dbo.rt",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, db := declaredSQLServerDatabase(c)
			grants := "CREATE ROLE rt_runner;\n" + test.grant
			created := runPtahIn(c, "schema", "apply", "--db-url", target,
				"--schema-file", writeDeclaredSchemaFile(c, "before.sql", test.before+grants), "--auto-approve")
			c.Assert(created.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", created.Stdout, created.Stderr))
			declaredSQLServerExec(c, db, "CREATE ROLE rt_auditor", "GRANT "+test.auditor+" ON dbo.rt TO rt_auditor WITH GRANT OPTION")
			after := writeDeclaredSchemaFile(c, "after.sql", test.after+grants)

			replaced := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", after, "--auto-approve")
			settled := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", after, "--dry-run")

			c.Assert(replaced.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", replaced.Stdout, replaced.Stderr))
			c.Assert(replaced.Stdout, qt.Contains, "CREATE OR ALTER ")
			c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
			c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(sqlServerRoutinePermissionsOf(c, db, "rt_runner"), qt.DeepEquals, test.want)
			c.Assert(sqlServerRoutinePermissionsOf(c, db, "rt_auditor"), qt.DeepEquals, []string{test.auditor + " GRANT_WITH_GRANT_OPTION"})
			declaredSQLServerExec(c, db, "CREATE USER rt_member WITHOUT LOGIN", "ALTER ROLE rt_runner ADD MEMBER rt_member")
			conn, err := db.Conn(c.Context())
			c.Assert(err, qt.IsNil)
			defer func() { c.Assert(conn.Close(), qt.IsNil) }()
			_, err = conn.ExecContext(c.Context(), "EXECUTE AS USER = 'rt_member'; "+test.call+"; REVERT;")
			c.Assert(err, qt.IsNil)
		})
	}
}

// A plan that replaces a SQL Server routine for a new body and, in the same
// plan, revokes a permission or a grant option on it leaves the revoke in
// effect: the REVOKE runs before the DROP, and the permissions given back
// after the CREATE leave out what it took. The role keeps the rest, and the
// next plan is empty.
func TestSchemaApplySQLServerKeepsARevokeOnAReplacedRoutineE2E(t *testing.T) {
	tests := []struct {
		name   string
		before string
		after  string
		want   []string
	}{
		{
			name:   "a permission",
			before: "GRANT EXECUTE ON dbo.rt TO rt_runner;\nGRANT REFERENCES ON dbo.rt TO rt_runner;\n",
			after:  "GRANT EXECUTE ON dbo.rt TO rt_runner;\n",
			want:   []string{"EXECUTE GRANT"},
		},
		{
			name:   "a grant option",
			before: "GRANT EXECUTE ON dbo.rt TO rt_runner WITH GRANT OPTION;\n",
			after:  "GRANT EXECUTE ON dbo.rt TO rt_runner;\n",
			want:   []string{"EXECUTE GRANT"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, db := declaredSQLServerDatabase(c)
			routine := "CREATE FUNCTION dbo.rt() RETURNS INT AS BEGIN RETURN %d; END;\nGO\nCREATE ROLE rt_runner;\n"
			created := runPtahIn(c, "schema", "apply", "--db-url", target,
				"--schema-file", writeDeclaredSchemaFile(c, "before.sql", fmt.Sprintf(routine, 1)+test.before), "--auto-approve")
			c.Assert(created.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", created.Stdout, created.Stderr))
			after := writeDeclaredSchemaFile(c, "after.sql", fmt.Sprintf(routine, 2)+test.after)

			replaced := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", after, "--auto-approve")
			settled := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", after, "--dry-run")

			c.Assert(replaced.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", replaced.Stdout, replaced.Stderr))
			c.Assert(replaced.Stdout, qt.Contains, "REVOKE ")
			c.Assert(replaced.Stdout, qt.Contains, "CREATE OR ALTER FUNCTION")
			c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
			c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(sqlServerRoutinePermissionsOf(c, db, "rt_runner"), qt.DeepEquals, test.want)
			var two int
			c.Assert(db.QueryRowContext(c.Context(), "SELECT dbo.rt()").Scan(&two), qt.IsNil)
			c.Assert(two, qt.Equals, 2)
		})
	}
}
