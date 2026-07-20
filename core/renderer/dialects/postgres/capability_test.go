package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/core/ptaherr"
	"github.com/stokaro/ptah/core/renderer/dialects/postgres"
)

func TestPostgreSQLRenderer_NilCapabilitiesAreConservative(t *testing.T) {
	c := qt.New(t)

	renderer := postgres.NewWithCapabilities(nil, platform.CockroachDB)

	idx := ast.NewIndex("idx_users_email", "users", "email")
	idx.Concurrently = true
	sql, err := renderer.Render(idx)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Not(qt.Contains), "CONCURRENTLY")

	xmlTable := ast.NewCreateTable("events").
		AddColumn(ast.NewColumn("payload", "XML"))
	_, err = renderer.Render(xmlTable)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `error rendering column payload: unsupported feature: cockroachdb does not support XML columns; use a platform-specific type override`)
}

func TestPostgreSQLRenderer_SequenceCapability(t *testing.T) {
	t.Run("postgres keeps SERIAL", func(t *testing.T) {
		c := qt.New(t)

		renderer := postgres.NewWithCapabilities(capability.Postgres16(), platform.Postgres)
		table := ast.NewCreateTable("users").
			AddColumn(ast.NewColumn("id", "SERIAL").SetPrimary())

		sql, err := renderer.Render(table)

		c.Assert(err, qt.IsNil)
		c.Assert(legacyPostgresSQL(sql), qt.Contains, "id SERIAL PRIMARY KEY NOT NULL")
	})

	t.Run("cockroach rejects explicit SERIAL", func(t *testing.T) {
		c := qt.New(t)

		renderer := postgres.NewWithCapabilities(capability.CockroachDB23(), platform.CockroachDB)
		table := ast.NewCreateTable("users").
			AddColumn(ast.NewColumn("id", "SERIAL").SetPrimary())

		_, err := renderer.Render(table)

		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(err, qt.ErrorMatches, `error rendering column id: unsupported feature: cockroachdb does not support sequence-backed type SERIAL; use a platform-specific type override`)
	})

	t.Run("spanner rejects auto increment mapping", func(t *testing.T) {
		c := qt.New(t)

		renderer := postgres.NewWithCapabilities(capability.SpannerPostgres(), platform.Spanner)
		table := ast.NewCreateTable("users").
			AddColumn(ast.NewColumn("id", "BIGINT AUTO_INCREMENT").SetPrimary())

		_, err := renderer.Render(table)

		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(err, qt.ErrorMatches, `error rendering column id: unsupported feature: spanner does not support sequence-backed type BIGINT AUTO_INCREMENT; use a platform-specific type override`)
	})
}

func TestPostgreSQLRenderer_RowLevelSecurityCapability(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
	}{
		{
			name: "create policy",
			node: ast.NewCreatePolicy("tenant_policy", "users").
				SetPolicyFor("SELECT").
				SetUsingExpression("tenant_id = current_setting('app.tenant_id')::uuid"),
		},
		{
			name: "drop policy",
			node: ast.NewDropPolicy("tenant_policy", "users").SetIfExists(),
		},
		{
			name: "enable RLS",
			node: ast.NewAlterTableEnableRLS("users"),
		},
		{
			name: "disable RLS",
			node: ast.NewAlterTableDisableRLS("users"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			renderer := postgres.NewWithCapabilities(capability.CockroachDB23(), platform.CockroachDB)
			_, err := renderer.Render(tt.node)

			c.Assert(err, qt.ErrorMatches, `cockroachdb does not support row-level security`)
		})
	}
}

func TestPostgreSQLRenderer_RoleManagementCapability(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
	}{
		{
			name: "create role",
			node: ast.NewCreateRole("app_role"),
		},
		{
			name: "drop role",
			node: ast.NewDropRole("app_role").SetIfExists(),
		},
		{
			name: "alter role",
			node: ast.NewAlterRole("app_role").AddOperation(ast.NewSetLoginOperation(true)),
		},
		{
			name: "grant",
			node: ast.NewGrantPrivilege("app_role", "TABLE", "users", []string{"SELECT"}),
		},
		{
			name: "revoke",
			node: ast.NewRevokePrivilege("app_role", "TABLE", "users", []string{"SELECT"}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			renderer := postgres.NewWithCapabilities(capability.CockroachDB23(), platform.CockroachDB)
			_, err := renderer.Render(tt.node)

			c.Assert(err, qt.ErrorMatches, `cockroachdb does not support role management`)
		})
	}
}
