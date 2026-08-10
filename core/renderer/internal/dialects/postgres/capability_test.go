package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/postgres"
	"go.5x5.cz/ptah/internal/atlasschema"
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

	t.Run("cockroach keeps explicit SERIAL", func(t *testing.T) {
		c := qt.New(t)

		renderer := postgres.NewWithCapabilities(capability.CockroachDB23(), platform.CockroachDB)
		table := ast.NewCreateTable("users").
			AddColumn(ast.NewColumn("id", "SERIAL").SetPrimary())

		sql, err := renderer.Render(table)

		c.Assert(err, qt.IsNil)
		c.Assert(legacyPostgresSQL(sql), qt.Contains, "id SERIAL PRIMARY KEY NOT NULL")
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

// TestPostgreSQLRenderer_RowLevelSecurityCapability and
// TestPostgreSQLRenderer_RoleManagementCapability pin what a refused
// row-level-security or role-management object renders as.
//
// These used to assert an error with nothing rendered. That answer is
// unavailable to the migration planner, which cannot put an error in a plan, so
// the planner compensated by dropping roles, grants and policies from the plan
// before they could reach a visitor — and dropped them without saying so
// (stokaro/ptah#929 items 1 and 4).
//
// The rendered output changed here: no error, one named skip comment per
// object, same as the sequence, view, function and trigger gates beside them.
func TestPostgreSQLRenderer_RowLevelSecurityCapability(t *testing.T) {
	tests := []struct {
		name    string
		node    ast.Node
		skipped string
	}{
		{
			name: "create policy",
			node: ast.NewCreatePolicy("tenant_policy", "users").
				SetPolicyFor("SELECT").
				SetUsingExpression("tenant_id = current_setting('app.tenant_id')::uuid"),
			skipped: "-- SPANNER: policy tenant_policy on users is not supported by this target; skipped.",
		},
		{
			name:    "drop policy",
			node:    ast.NewDropPolicy("tenant_policy", "users").SetIfExists(),
			skipped: "-- SPANNER: policy tenant_policy on users is not supported by this target; skipped.",
		},
		{
			name:    "enable RLS",
			node:    ast.NewAlterTableEnableRLS("users"),
			skipped: "-- SPANNER: row-level security on users is not supported by this target; skipped.",
		},
		{
			name:    "disable RLS",
			node:    ast.NewAlterTableDisableRLS("users"),
			skipped: "-- SPANNER: row-level security on users is not supported by this target; skipped.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			renderer := postgres.NewWithCapabilities(capability.SpannerPostgres(), platform.Spanner)
			sql, err := renderer.Render(tt.node)

			c.Assert(err, qt.IsNil)
			c.Assert(strings.TrimSpace(sql), qt.Equals, tt.skipped)
		})
	}
}

func TestPostgreSQLRenderer_RoleManagementCapability(t *testing.T) {
	tests := []struct {
		name    string
		node    ast.Node
		skipped string
	}{
		{
			name:    "create role",
			node:    ast.NewCreateRole("app_role"),
			skipped: "-- SPANNER: role app_role is not supported by this target; skipped.",
		},
		{
			name:    "drop role",
			node:    ast.NewDropRole("app_role").SetIfExists(),
			skipped: "-- SPANNER: role app_role is not supported by this target; skipped.",
		},
		{
			name:    "alter role",
			node:    ast.NewAlterRole("app_role").AddOperation(ast.NewSetLoginOperation(true)),
			skipped: "-- SPANNER: role app_role is not supported by this target; skipped.",
		},
		{
			name:    "grant",
			node:    ast.NewGrantPrivilege("app_role", "TABLE", "users", []string{"SELECT"}),
			skipped: "-- SPANNER: grant on users to app_role is not supported by this target; skipped.",
		},
		{
			name:    "revoke",
			node:    ast.NewRevokePrivilege("app_role", "TABLE", "users", []string{"SELECT"}),
			skipped: "-- SPANNER: revoke on users from app_role is not supported by this target; skipped.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			renderer := postgres.NewWithCapabilities(capability.SpannerPostgres(), platform.Spanner)
			sql, err := renderer.Render(tt.node)

			c.Assert(err, qt.IsNil)
			c.Assert(strings.TrimSpace(sql), qt.Equals, tt.skipped)
		})
	}
}

func TestPostgreSQLRenderer_RoleManagementValidationPrecedesCapabilityRefusal(t *testing.T) {
	tests := []struct {
		name    string
		node    ast.Node
		wantErr string
	}{
		{
			name:    "grant without privileges",
			node:    ast.NewGrantPrivilege("app_role", "TABLE", "users", nil),
			wantErr: "GRANT requires at least one privilege",
		},
		{
			name:    "grant without role",
			node:    ast.NewGrantPrivilege("", "TABLE", "users", []string{"SELECT"}),
			wantErr: "GRANT requires a role",
		},
		{
			name:    "grant without object",
			node:    ast.NewGrantPrivilege("app_role", "TABLE", "", []string{"SELECT"}),
			wantErr: "GRANT requires an object type and object name",
		},
		{
			name:    "revoke without privileges",
			node:    ast.NewRevokePrivilege("app_role", "TABLE", "users", nil),
			wantErr: "REVOKE requires at least one privilege",
		},
		{
			name:    "revoke without role",
			node:    ast.NewRevokePrivilege("", "TABLE", "users", []string{"SELECT"}),
			wantErr: "REVOKE requires a role",
		},
		{
			name:    "revoke without object",
			node:    ast.NewRevokePrivilege("app_role", "TABLE", "", []string{"SELECT"}),
			wantErr: "REVOKE requires an object type and object name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			renderer := postgres.NewWithCapabilities(capability.SpannerPostgres(), platform.Spanner)
			sql, err := renderer.Render(tt.node)

			c.Assert(err, qt.ErrorMatches, tt.wantErr)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

func TestPostgreSQLRenderer_SkipCommentNamesStayCommentOnly(t *testing.T) {
	c := qt.New(t)

	renderer := postgres.NewWithCapabilities(capability.SpannerPostgres(), platform.Spanner)
	role := ast.NewCreateRole("app_role\nDROP TABLE users")

	sql, err := renderer.Render(role)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "-- SPANNER: role app_role DROP TABLE users is not supported by this target; skipped.")
	c.Assert(sql, qt.Not(qt.Contains), "\nDROP TABLE users")
	c.Assert(atlasschema.SplitApplyStatements(sql, platform.Spanner), qt.HasLen, 0)
}

// noForeignKeys is a PostgreSQL preset with foreign keys switched off.
//
// No shipped PostgreSQL-family preset refuses a foreign key -- Spanner's keeps
// them, deliberately -- so the gate is reachable only through a
// caller-supplied capability set, which is how core/renderer's
// TestGetOrderedCreateStatements_ForeignKeysDisabled_FailurePath reaches it.
// Turning ForeignKeys off requires turning the reference policy off with it,
// or capability.Validate refuses the set.
func noForeignKeys() capability.Capabilities {
	return capability.Postgres17().
		With(capability.ForeignKeysRequireUniqueReference, false).
		With(capability.ForeignKeys, false)
}

// foreignKeyRoutes is the same foreign key arriving at this renderer the three
// ways it can arrive: as a table-level constraint on a CREATE TABLE, as a
// column-level reference on a CREATE TABLE column, and as an
// ALTER TABLE ADD CONSTRAINT.
func foreignKeyRoutes() []struct {
	name string
	node ast.Node
} {
	reference := &ast.ForeignKeyRef{Table: "users", Column: "id", Name: "fk_orders_user"}
	return []struct {
		name string
		node ast.Node
	}{
		{
			name: "table-level constraint on create table",
			node: ast.NewCreateTable("orders").
				AddColumn(ast.NewColumn("user_id", "BIGINT")).
				AddConstraint(ast.NewForeignKeyConstraint("fk_orders_user", []string{"user_id"}, reference)),
		},
		{
			name: "column-level reference on create table",
			node: ast.NewCreateTable("orders").
				AddColumn(ast.NewColumn("user_id", "BIGINT").SetForeignKey("users", "id", "fk_orders_user")),
		},
		{
			name: "alter table add constraint",
			node: &ast.AlterTableNode{
				Name: "orders",
				Operations: []ast.AlterOperation{&ast.AddConstraintOperation{
					Constraint: ast.NewForeignKeyConstraint("fk_orders_user", []string{"user_id"}, reference),
				}},
			},
		},
	}
}

// TestPostgreSQLRenderer_ForeignKeyRefusalIsNamedOnEveryRoute pins that a
// foreign key a target cannot host is NAMED wherever it reaches this renderer,
// and that all three routes say the same sentence.
//
// Only the ALTER route named it. The two CREATE TABLE routes fell through
// renderConstraint's `return "", nil` and were dropped by `if line != ""`, so
// a schema whose author declared referential integrity got a table without it,
// at exit 0, with nothing said -- the same silent-omission shape
// stokaro/ptah#929 is about, surviving inside the renderer this change
// unifies.
//
// The column route was worse than silent: renderColumnForeignKey's empty
// string was appended to the column list unconditionally, so the refused key
// became a blank entry between two commas inside CREATE TABLE (...).
func TestPostgreSQLRenderer_ForeignKeyRefusalIsNamedOnEveryRoute(t *testing.T) {
	for _, tt := range foreignKeyRoutes() {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			renderer := postgres.NewWithCapabilities(noForeignKeys(), platform.Postgres)
			sql, err := renderer.Render(tt.node)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains,
				"-- POSTGRES: foreign key constraint fk_orders_user is not supported by this target; skipped.")
			// The DDL must be absent, not merely accompanied by a comment: a
			// renderer that named the key and then emitted it would satisfy a
			// "names the object" assertion while still handing the target a
			// constraint it cannot accept.
			c.Assert(sql, qt.Not(qt.Contains), "FOREIGN KEY")
			// And nothing may be left where the refused key would have gone.
			// The column route used to append an empty line here, producing
			// `"user_id" BIGINT,` followed by a bare comma.
			c.Assert(sql, qt.Not(qt.Contains), ",\n\n")
			c.Assert(sql, qt.Not(qt.Contains), ",\n)")
		})
	}
}

// TestPostgreSQLRenderer_ForeignKeyIsRenderedWhenTheTargetHostsIt is the
// control for the test above: with the capability on, every one of those three
// routes emits the constraint and writes no skip comment. Without it, a
// renderer that refused foreign keys unconditionally would pass.
func TestPostgreSQLRenderer_ForeignKeyIsRenderedWhenTheTargetHostsIt(t *testing.T) {
	for _, tt := range foreignKeyRoutes() {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			renderer := postgres.NewWithCapabilities(capability.Postgres17(), platform.Postgres)
			sql, err := renderer.Render(tt.node)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, `FOREIGN KEY ("user_id") REFERENCES "users"("id")`)
			c.Assert(sql, qt.Not(qt.Contains), "is not supported by this target")
		})
	}
}

// TestPostgreSQLRenderer_UnnamedForeignKeyRefusalStillNamesTheReference covers
// the constraint the schema author never named. `constraint ""` would tell a
// reader nothing about what was dropped, so the comment falls back to the
// columns and the referenced table.
func TestPostgreSQLRenderer_UnnamedForeignKeyRefusalStillNamesTheReference(t *testing.T) {
	c := qt.New(t)

	renderer := postgres.NewWithCapabilities(noForeignKeys(), platform.Postgres)
	table := ast.NewCreateTable("orders").
		AddColumn(ast.NewColumn("tenant_id", "BIGINT")).
		AddColumn(ast.NewColumn("user_id", "BIGINT")).
		AddConstraint(ast.NewForeignKeyConstraint(
			"",
			[]string{"tenant_id", "user_id"},
			&ast.ForeignKeyRef{Table: "users", Column: "id"},
		))

	sql, err := renderer.Render(table)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains,
		"-- POSTGRES: foreign key constraint on (tenant_id, user_id) references users is not supported by this target; skipped.")
}
