//go:build integration

package integration_test

import (
	"database/sql"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// sqlServerColumnBase is the table and the role every declaration here keeps,
// with a user that holds the role and no permission of its own.
const sqlServerColumnBase = "CREATE TABLE dbo.t (id INT PRIMARY KEY, name NVARCHAR(50), secret NVARCHAR(50));\n" +
	"CREATE ROLE cp_reader;\n"

// sqlServerColumnDatabase is a scratch database holding the table and the role
// of [sqlServerColumnBase], a user cp_member in the role, and held.
func sqlServerColumnDatabase(c *qt.C, held ...string) (string, *sql.DB) {
	c.Helper()
	target, db := declaredSQLServerDatabase(c)
	declaredSQLServerExec(c, db, append([]string{
		"CREATE TABLE dbo.t (id INT PRIMARY KEY, name NVARCHAR(50), secret NVARCHAR(50))",
		"INSERT INTO dbo.t VALUES (1, N'a', N's')",
		"CREATE ROLE cp_reader",
		"CREATE USER cp_member WITHOUT LOGIN",
		"ALTER ROLE cp_reader ADD MEMBER cp_member",
	}, held...)...)
	return target, db
}

// sqlServerPermissionsOf reads the permissions the role holds on dbo.t as
// "PERMISSION STATE column", the column empty for the whole table.
func sqlServerPermissionsOf(c *qt.C, db *sql.DB, role string) []string {
	c.Helper()
	rows, err := db.QueryContext(c.Context(), `
		SELECT pe.permission_name + ' ' + pe.state_desc + ' ' + COALESCE(COL_NAME(pe.major_id, NULLIF(pe.minor_id, 0)), '')
		FROM sys.database_permissions AS pe
		JOIN sys.database_principals AS r ON r.principal_id = pe.grantee_principal_id
		WHERE r.name = @p1 AND pe.class = 1 AND pe.major_id = OBJECT_ID(N'dbo.t')
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

// sqlServerPermissionStatements is the GRANT, REVOKE and DENY lines of a
// rendered schema, in order.
func sqlServerPermissionStatements(rendered string) []string {
	statements := make([]string, 0)
	for line := range strings.SplitSeq(rendered, "\n") {
		for _, verb := range []string{"GRANT ", "REVOKE ", "DENY "} {
			if strings.HasPrefix(line, verb) {
				statements = append(statements, line)
			}
		}
	}
	return statements
}

// sqlServerAsMember runs statement as cp_member, which holds cp_reader and no
// permission of its own, and answers what the server says.
func sqlServerAsMember(c *qt.C, db *sql.DB, statement string) error {
	c.Helper()
	conn, err := db.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(conn.Close(), qt.IsNil) }()
	_, err = conn.ExecContext(c.Context(), "EXECUTE AS USER = 'cp_member'; "+statement+"; REVERT;")
	return err
}

// A SQL Server permission on one column is applied as written, read back as
// the column it is on, and settles: the server holds exactly the declared
// rows, a user holding the role reads what they grant and is refused the
// rest, and the next plan is empty.
//
// A permission on the whole table takes the place of the column permissions
// of the same name on SQL Server, and a column revoke under one withholds the
// column, so the rows that change one into the other pass only if the plan
// revokes before it grants. A grantable permission is revoked only after its
// option, which SQL Server answers with Msg 4611 otherwise. A column the
// declaration spells another way than the catalog is one column under the
// database's case-insensitive collation.
//
// A table permission with a column row of another state under it holds for
// the other columns only. Read as the whole table, the declaration of the
// whole table would compare equal and the plan would leave the member without
// the column.
func TestSchemaApplySQLServerColumnPermissionsE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		held     []string
		declared string
		want     []string
		reads    string
		denied   string
		refusal  string
	}{
		{
			name:     "declared on two columns",
			declared: "GRANT SELECT (id, name) ON dbo.t TO cp_reader;\nGRANT UPDATE (name) ON dbo.t TO cp_reader;\n",
			want:     []string{"SELECT GRANT id", "SELECT GRANT name", "UPDATE GRANT name"},
			reads:    "SELECT id, name FROM dbo.t; UPDATE dbo.t SET name = N'b'",
			denied:   "SELECT secret FROM dbo.t",
			refusal:  `(?s).*The SELECT permission was denied on the column 'secret' of the object 't'.*`,
		},
		{
			name:     "a column permission widened to the table",
			held:     []string{"GRANT SELECT (id) ON dbo.t TO cp_reader"},
			declared: "GRANT SELECT ON dbo.t TO cp_reader;\n",
			want:     []string{"SELECT GRANT"},
			reads:    "SELECT id, name, secret FROM dbo.t",
			denied:   "UPDATE dbo.t SET name = N'b'",
			refusal:  `(?s).*The UPDATE permission was denied on the object 't'.*`,
		},
		{
			name:     "a grantable table permission narrowed to a column",
			held:     []string{"GRANT SELECT ON dbo.t TO cp_reader WITH GRANT OPTION"},
			declared: "GRANT SELECT (id) ON dbo.t TO cp_reader;\n",
			want:     []string{"SELECT GRANT id"},
			reads:    "SELECT id FROM dbo.t",
			denied:   "SELECT secret FROM dbo.t",
			refusal:  `(?s).*The SELECT permission was denied on the column 'secret' of the object 't'.*`,
		},
		{
			name:     "a table permission with a column revoked under it",
			held:     []string{"GRANT SELECT ON dbo.t TO cp_reader", "REVOKE SELECT (secret) ON dbo.t FROM cp_reader"},
			declared: "GRANT SELECT ON dbo.t TO cp_reader;\n",
			want:     []string{"SELECT GRANT"},
			reads:    "SELECT id, name, secret FROM dbo.t",
			denied:   "UPDATE dbo.t SET name = N'b'",
			refusal:  `(?s).*The UPDATE permission was denied on the object 't'.*`,
		},
		{
			name:     "a table permission with a column denied under it",
			held:     []string{"GRANT SELECT ON dbo.t TO cp_reader", "DENY SELECT (secret) ON dbo.t TO cp_reader"},
			declared: "GRANT SELECT ON dbo.t TO cp_reader;\n",
			want:     []string{"SELECT GRANT"},
			reads:    "SELECT id, name, secret FROM dbo.t",
			denied:   "UPDATE dbo.t SET name = N'b'",
			refusal:  `(?s).*The UPDATE permission was denied on the object 't'.*`,
		},
		{
			name: "a grantable table permission with the option revoked from a column",
			held: []string{
				"GRANT SELECT ON dbo.t TO cp_reader WITH GRANT OPTION",
				"REVOKE GRANT OPTION FOR SELECT (secret) ON dbo.t FROM cp_reader CASCADE",
			},
			declared: "GRANT SELECT ON dbo.t TO cp_reader WITH GRANT OPTION;\n",
			want:     []string{"SELECT GRANT_WITH_GRANT_OPTION"},
			reads:    "SELECT id, name, secret FROM dbo.t",
			denied:   "UPDATE dbo.t SET name = N'b'",
			refusal:  `(?s).*The UPDATE permission was denied on the object 't'.*`,
		},
		{
			name:     "a column spelled as the catalog does not",
			declared: "GRANT SELECT (NAME) ON dbo.t TO cp_reader;\n",
			want:     []string{"SELECT GRANT name"},
			reads:    "SELECT name FROM dbo.t",
			denied:   "SELECT id FROM dbo.t",
			refusal:  `(?s).*The SELECT permission was denied on the column 'id' of the object 't'.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, db := sqlServerColumnDatabase(c, test.held...)
			file := writeDeclaredSchemaFile(c, "schema.sql", sqlServerColumnBase+test.declared)

			applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(sqlServerPermissionsOf(c, db, "cp_reader"), qt.DeepEquals, test.want)
			c.Assert(sqlServerAsMember(c, db, test.reads), qt.IsNil)
			c.Assert(sqlServerAsMember(c, db, test.denied), qt.ErrorMatches, test.refusal)

			settled := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run")

			c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
			c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// A column list on a permission SQL Server keeps only for the whole object is
// refused by name before anything runs, and the role holds what it held.
func TestSchemaApplySQLServerColumnPermissionsE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	target, db := sqlServerColumnDatabase(c, "GRANT SELECT (id) ON dbo.t TO cp_reader")
	file := writeDeclaredSchemaFile(c, "schema.sql", sqlServerColumnBase+"GRANT INSERT (id) ON dbo.t TO cp_reader;\n")

	applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")

	c.Assert(applied.ExitCode, qt.Not(qt.Equals), 0)
	c.Assert(applied.Stderr, qt.Contains, "GRANT INSERT (id) ON dbo.t: sqlserver grants only SELECT, UPDATE, REFERENCES and UNMASK on columns")
	c.Assert(sqlServerPermissionsOf(c, db, "cp_reader"), qt.DeepEquals, []string{"SELECT GRANT id"})
}

// `ptah db read` prints a column permission on its column and a DENY as a
// DENY, and leaves out the REVOKE row a column revoke leaves under a grant on
// the whole table. That grant prints as the permission on each other column.
// Printed as the whole table, the output would give a role that replays it the
// column the source database keeps from it.
func TestDBReadSQLServerColumnPermissionsE2E(t *testing.T) {
	c := qt.New(t)
	target, _ := sqlServerColumnDatabase(c,
		"GRANT SELECT (id, name) ON dbo.t TO cp_reader",
		"GRANT UPDATE (name) ON dbo.t TO cp_reader WITH GRANT OPTION",
		"DENY SELECT (secret) ON dbo.t TO cp_reader",
		"CREATE ROLE cp_auditor",
		"GRANT SELECT ON dbo.t TO cp_auditor",
		"REVOKE SELECT (secret) ON dbo.t FROM cp_auditor",
	)

	read := runPtahIn(c, "db", "read", "--db-url", target)

	c.Assert(read.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", read.Stdout, read.Stderr))
	c.Assert(sqlServerPermissionStatements(read.Stdout), qt.DeepEquals, []string{
		"GRANT SELECT ([id]) ON [dbo].[t] TO [cp_auditor];",
		"GRANT SELECT ([name]) ON [dbo].[t] TO [cp_auditor];",
		"GRANT SELECT ([id]) ON [dbo].[t] TO [cp_reader];",
		"GRANT SELECT ([name]) ON [dbo].[t] TO [cp_reader];",
		"GRANT UPDATE ([name]) ON [dbo].[t] TO [cp_reader] WITH GRANT OPTION;",
		"DENY SELECT ([secret]) ON [dbo].[t] TO [cp_reader];",
	})
}
