//go:build integration

package integration_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// A revoke removes the row of every grantor on objects, schemas and columns,
// including a grantable row beside a plain one. Taking only the grant option
// keeps every plain row. The next live comparison must find no change.
func TestSchemaApplySQLServerRevokesEveryGrantorE2E(t *testing.T) {
	for _, test := range []struct {
		name, target, privilege, held, declared string
		want                                    []string
	}{
		{
			name: "object from another grantor", target: "dbo.t1", privilege: "SELECT",
			declared: "REVOKE SELECT ON t1 FROM [{role}];",
		},
		{
			name: "object from two grantors", target: "dbo.t1", privilege: "SELECT",
			held: "GRANT SELECT ON dbo.t1 TO [{role}];", declared: "REVOKE SELECT ON t1 FROM [{role}];",
		},
		{
			name: "object with the option from another grantor", target: "dbo.t1", privilege: "SELECT",
			held:     "GRANT SELECT ON dbo.t1 TO [{role}] WITH GRANT OPTION AS [{role}_g];",
			declared: "REVOKE SELECT ON t1 FROM [{role}];",
		},
		{
			name: "object with plain and grantable rows", target: "dbo.t1", privilege: "SELECT",
			held:     "GRANT SELECT ON dbo.t1 TO [{role}]; GRANT SELECT ON dbo.t1 TO [{role}] WITH GRANT OPTION AS [{role}_g];",
			declared: "REVOKE SELECT ON t1 FROM [{role}];",
		},
		{
			name: "schema from another grantor", target: "SCHEMA::dbo", privilege: "SELECT",
			declared: "REVOKE SELECT ON SCHEMA::dbo FROM [{role}];",
		},
		{
			name: "schema from two grantors", target: "SCHEMA::dbo", privilege: "SELECT",
			held: "GRANT SELECT ON SCHEMA::dbo TO [{role}];", declared: "REVOKE SELECT ON SCHEMA::dbo FROM [{role}];",
		},
		{
			name: "schema with plain and grantable rows", target: "SCHEMA::dbo", privilege: "SELECT",
			held:     "GRANT SELECT ON SCHEMA::dbo TO [{role}]; GRANT SELECT ON SCHEMA::dbo TO [{role}] WITH GRANT OPTION AS [{role}_g];",
			declared: "REVOKE SELECT ON SCHEMA::dbo FROM [{role}];",
		},
		{
			name: "column from two grantors", target: "dbo.t1", privilege: "SELECT (id)",
			held: "GRANT SELECT (id) ON dbo.t1 TO [{role}];", declared: "REVOKE SELECT (id) ON t1 FROM [{role}];",
		},
		{
			name: "object option leaves both plain grants", target: "dbo.t1", privilege: "SELECT",
			held:     "GRANT SELECT ON dbo.t1 TO [{role}]; GRANT SELECT ON dbo.t1 TO [{role}] WITH GRANT OPTION AS [{role}_g];",
			declared: "GRANT SELECT ON t1 TO [{role}]; REVOKE GRANT OPTION FOR SELECT ON t1 FROM [{role}];",
			want:     []string{"SELECT G 1:0 from dbo", "SELECT G 1:0 from {role}_g"},
		},
		{
			name: "schema option leaves both plain grants", target: "SCHEMA::dbo", privilege: "SELECT",
			held:     "GRANT SELECT ON SCHEMA::dbo TO [{role}]; GRANT SELECT ON SCHEMA::dbo TO [{role}] WITH GRANT OPTION AS [{role}_g];",
			declared: "GRANT SELECT ON SCHEMA::dbo TO [{role}]; REVOKE GRANT OPTION FOR SELECT ON SCHEMA::dbo FROM [{role}];",
			want:     []string{"SELECT G 3:0 from dbo", "SELECT G 3:0 from {role}_g"},
		},
		{
			name: "column option leaves both plain grants", target: "dbo.t1", privilege: "SELECT (id)",
			held:     "GRANT SELECT (id) ON dbo.t1 TO [{role}]; GRANT SELECT (id) ON dbo.t1 TO [{role}] WITH GRANT OPTION AS [{role}_g];",
			declared: "GRANT SELECT (id) ON t1 TO [{role}]; REVOKE GRANT OPTION FOR SELECT (id) ON t1 FROM [{role}];",
			want:     []string{"SELECT G 1:1 from dbo", "SELECT G 1:1 from {role}_g"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			server := newSQLServerRevokeAllServer(c, "CREATE USER [{role}_g] WITHOUT LOGIN; "+
				"GRANT SELECT ON "+test.target+" TO [{role}_g] WITH GRANT OPTION; "+
				"GRANT "+test.privilege+" ON "+test.target+" TO [{role}] AS [{role}_g]; "+test.held)
			declared := sqlServerGrantOptionTable + test.declared
			want := make([]string, 0, len(test.want))
			for _, row := range test.want {
				want = append(want, strings.ReplaceAll(row, "{role}", server.role))
			}

			applied := server.apply(c, declared, "--auto-approve")
			again := server.apply(c, declared, "--dry-run")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Contains, " AS ["+server.role+"_g]")
			c.Assert(sqlServerGrantorRows(c, server), qt.DeepEquals, want)
			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
		})
	}
}

// sqlServerGrantorRows reads the role's explicit permissions and each grantor
// without collapsing rows that have the same permission and scope.
func sqlServerGrantorRows(c *qt.C, server revokeAllServer) []string {
	c.Helper()
	return append(make([]string, 0), grantRows(c, server.catalog, `
		SELECT CONCAT(p.permission_name, ' ', p.state, ' ', p.class, ':', p.minor_id,
		              ' from ', USER_NAME(p.grantor_principal_id))
		FROM sys.database_permissions p
		WHERE p.grantee_principal_id = USER_ID(@p1) AND p.class IN (1, 3)
		ORDER BY 1`, server.role)...)
}

// A plain row from dbo must not hide the grant option another grantor gave
// the role. Refusal preserves the role's rows and its onward grant.
func TestSchemaApplySQLServerHiddenGrantorCascadeRefusedE2E(t *testing.T) {
	c := qt.New(t)
	server := newSQLServerRevokeAllServer(c,
		"CREATE USER [{role}_g] WITHOUT LOGIN; "+
			"GRANT SELECT ON dbo.t1 TO [{role}_g] WITH GRANT OPTION; "+
			"GRANT SELECT ON dbo.t1 TO [{role}]; "+
			"GRANT SELECT ON dbo.t1 TO [{role}] WITH GRANT OPTION AS [{role}_g]; "+
			"CREATE USER [{role}_v] WITHOUT LOGIN; "+
			"EXECUTE AS USER = '{user}'; GRANT SELECT ON dbo.t1 TO [{role}_v] AS [{role}]; REVERT")
	before := sqlServerGrantorRows(c, server)
	c.Assert(before, qt.DeepEquals, []string{"SELECT G 1:0 from dbo", "SELECT W 1:0 from " + server.role + "_g"})
	c.Assert(onwardGrants(c, server), qt.DeepEquals, []string{"SELECT G from " + server.role})

	applied := server.apply(c, sqlServerGrantOptionTable+"REVOKE SELECT ON t1 FROM [{role}];", "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stderr, qt.Contains, "the schema does not revoke those grants")
	c.Assert(sqlServerGrantorRows(c, server), qt.DeepEquals, before)
	c.Assert(onwardGrants(c, server), qt.DeepEquals, []string{"SELECT G from " + server.role})
}
