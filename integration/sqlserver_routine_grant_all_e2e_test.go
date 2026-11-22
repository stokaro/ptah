//go:build integration

package integration_test

import (
	"database/sql"
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
