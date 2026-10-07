package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
)

// TestRenderSQL_AccessNodes_FailurePath refuses, on every target without the
// key, a node built by hand that declares what the key holds: a group, a
// membership of one role in another, and a grant or revoke on the database.
// The declaration gate refuses the same in a schema; these never reach one.
func TestRenderSQL_AccessNodes_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		node    ast.Node
		wantKey capability.Capability
	}{
		{name: "a group on PostgreSQL", dialect: platform.Postgres, caps: capability.Postgres18(),
			node: ast.NewCreateRole("readers").SetGroup(true), wantKey: capability.GroupPrincipals},
		{name: "a dropped group on ClickHouse", dialect: platform.ClickHouse, caps: capability.ClickHouse24(),
			node: ast.NewDropRole("readers").SetGroup(true), wantKey: capability.GroupPrincipals},
		{name: "a member added on MySQL", dialect: platform.MySQL, caps: capability.MySQL84(),
			node: ast.NewGrantRoleMembership("readers", "app"), wantKey: capability.RoleMembership},
		{name: "a member dropped on SQL Server", dialect: platform.SQLServer, caps: capability.SQLServer2022(),
			node: ast.NewRevokeRoleMembership("readers", "app"), wantKey: capability.RoleMembership},
		{name: "a member added on Oracle", dialect: platform.Oracle, caps: capability.Oracle23(),
			node: ast.NewGrantRoleMembership("readers", "app"), wantKey: capability.RoleMembership},
		{name: "a member added on SQLite", dialect: platform.SQLite, caps: capability.SQLite3(),
			node: ast.NewGrantRoleMembership("readers", "app"), wantKey: capability.RoleMembership},
		{name: "a member added on PostgreSQL", dialect: platform.Postgres, caps: capability.Postgres18(),
			node: ast.NewGrantRoleMembership("readers", "app"), wantKey: capability.RoleMembership},
		{name: "a member added on ClickHouse", dialect: platform.ClickHouse, caps: capability.ClickHouse24(),
			node: ast.NewGrantRoleMembership("readers", "app"), wantKey: capability.RoleMembership},
		{name: "a grant on the database on MySQL", dialect: platform.MySQL, caps: capability.MySQL84(),
			node: ast.NewGrantPrivilege("app", "DATABASE", "", []string{"SELECT"}), wantKey: capability.DatabaseGrants},
		{name: "a revoke on the database on PostgreSQL", dialect: platform.Postgres, caps: capability.Postgres18(),
			node: ast.NewRevokePrivilege("app", "database", "app", []string{"CONNECT"}), wantKey: capability.DatabaseGrants},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := builtin.RenderSQLWithCapabilities(test.dialect, test.caps, test.node)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			var capabilityErr *ptaherr.CapabilityError
			c.Assert(err, qt.ErrorAs, &capabilityErr)
			c.Assert(capabilityErr.Feature, qt.Equals, string(test.wantKey))
			c.Assert(got, qt.Equals, "")
		})
	}
}

// TestRenderSQL_AccessNodes_HappyPath pins that a role and a grant of the
// kinds every target has still render where the keys are absent.
func TestRenderSQL_AccessNodes_HappyPath(t *testing.T) {
	c := qt.New(t)
	got, err := builtin.RenderSQLWithCapabilities(platform.Postgres, capability.Postgres18(),
		ast.NewCreateRole("readers"), ast.NewGrantPrivilege("readers", "SCHEMA", "app", []string{"USAGE"}))
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Contains, `CREATE ROLE "readers"`)
	c.Assert(got, qt.Contains, `GRANT USAGE ON SCHEMA "app" TO "readers"`)
}
