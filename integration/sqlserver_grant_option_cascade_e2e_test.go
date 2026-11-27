//go:build integration

package integration_test

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"
)

// sqlServerGrantOptionTable is the table the SQL Server grant option tests
// declare: the one newSQLServerRevokeAllServer creates.
const sqlServerGrantOptionTable = "CREATE TABLE t1 (id INT PRIMARY KEY, name NVARCHAR(50));\n"

// sqlServerOnwardGrant gives the role SELECT on t1 with the grant option and
// has a member grant SELECT onward, as the role, to the user {role}_v.
const sqlServerOnwardGrant = "GRANT SELECT ON dbo.t1 TO [{role}] WITH GRANT OPTION; " +
	"CREATE USER [{role}_v] WITHOUT LOGIN; " +
	"EXECUTE AS USER = '{user}'; GRANT SELECT ON dbo.t1 TO [{role}_v] AS [{role}]; REVERT"

// onwardGrants is what the user {role}_v holds on t1 and from whom.
func onwardGrants(c *qt.C, server revokeAllServer) []string {
	c.Helper()
	return grantRows(c, server.catalog, `
		SELECT CONCAT(p.permission_name, ' ', p.state, ' from ', USER_NAME(p.grantor_principal_id))
		FROM sys.database_permissions p
		WHERE p.grantee_principal_id = USER_ID(@p1) AND p.class = 1 AND p.major_id = OBJECT_ID('dbo.t1')
		ORDER BY 1`, server.role+"_v")
}

// A revoke of the grant option on SQL Server carries CASCADE, which the server
// requires, and CASCADE takes the grants the grantee made with the option.
// Where the grantee granted SELECT onward to a user the schema does not name,
// the plan is refused and names that user, whether the schema takes the option
// with REVOKE GRANT OPTION FOR or declares a role it creates without it. The
// role keeps the option and the user keeps SELECT, read back from the server.
func TestSchemaApplySQLServerRefusesAGrantOptionRevokeThatCascadesE2E(t *testing.T) {
	for _, test := range []struct {
		name     string
		declared string
	}{
		{
			name: "the option revoked from a role the schema does not create",
			declared: sqlServerGrantOptionTable + "GRANT SELECT ON t1 TO [{role}] WITH GRANT OPTION;\n" +
				"REVOKE GRANT OPTION FOR SELECT ON t1 FROM [{role}];\n",
		},
		{
			name:     "a role the schema creates, declared without the option",
			declared: sqlServerGrantOptionTable + "CREATE ROLE [{role}];\nGRANT SELECT ON t1 TO [{role}];\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			server := newSQLServerRevokeAllServer(c, sqlServerOnwardGrant)
			c.Assert(onwardGrants(c, server), qt.DeepEquals, []string{"SELECT G from " + server.role})

			applied := server.apply(c, test.declared, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stderr, qt.Contains, `sqlserver: cannot revoke the grant option for SELECT on dbo.t1 from "`+
				server.role+`": SQL Server revokes it with CASCADE, which also takes the SELECT that "`+server.role+
				`" granted to "`+server.role+`_v", and the schema does not revoke those grants`)
			c.Assert(server.privileges(c), qt.DeepEquals, []string{"SELECT W"})
			c.Assert(onwardGrants(c, server), qt.DeepEquals, []string{"SELECT G from " + server.role})
		})
	}
}

// The control: a role that granted nothing onward loses the option and keeps
// SELECT, CASCADE takes nothing more, and the next plan is empty.
func TestSchemaApplySQLServerTakesAGrantOptionNobodyGrantedOnwardE2E(t *testing.T) {
	c := qt.New(t)
	server := newSQLServerRevokeAllServer(c, "GRANT SELECT ON dbo.t1 TO [{role}] WITH GRANT OPTION")
	declared := sqlServerGrantOptionTable + "GRANT SELECT ON t1 TO [{role}] WITH GRANT OPTION;\n" +
		"REVOKE GRANT OPTION FOR SELECT ON t1 FROM [{role}];\n"

	applied := server.apply(c, declared, "--auto-approve")
	again := server.apply(c, declared, "--dry-run")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stdout, qt.Contains, "REVOKE GRANT OPTION FOR SELECT ON [dbo].[t1] FROM ["+server.role+"] CASCADE AS [dbo];")
	c.Assert(server.privileges(c), qt.DeepEquals, []string{"SELECT G"})
	c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
	c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
}

// grantRows runs query on db and returns its one column, row by row.
func grantRows(c *qt.C, db *sql.DB, query string, args ...any) []string {
	c.Helper()
	rows, err := db.QueryContext(c.Context(), query, args...)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var values []string
	for rows.Next() {
		var value string
		c.Assert(rows.Scan(&value), qt.IsNil)
		values = append(values, value)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return values
}
