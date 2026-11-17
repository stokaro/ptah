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

// TestRenderSQL_ViewOwner_HappyPath pins the statements that give a view to
// its owner on each target that manages it: a new view is followed by its
// ALTER ... OWNER TO, and a change of owner alone is that statement. The role
// is quoted, so a name in mixed case or with a space is the name the server
// stores.
func TestRenderSQL_ViewOwner_HappyPath(t *testing.T) {
	view := ast.NewCreateView("app.report").SetBody("SELECT 1")
	view.Owner = "Report Owner"
	materialized := ast.NewCreateMaterializedView("totals").SetBody("SELECT 1")
	materialized.Owner = "reporting"
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		node    ast.Node
		want    string
	}{
		{name: "a new view on PostgreSQL", dialect: platform.Postgres, caps: capability.Postgres18(), node: view,
			want: "CREATE VIEW \"app\".\"report\" AS\nSELECT 1\n;\nALTER VIEW \"app\".\"report\" OWNER TO \"Report Owner\";\n"},
		{name: "a new materialized view on PostgreSQL", dialect: platform.Postgres, caps: capability.Postgres18(), node: materialized,
			want: "CREATE MATERIALIZED VIEW \"totals\" AS\nSELECT 1\n;\nALTER MATERIALIZED VIEW \"totals\" OWNER TO \"reporting\";\n"},
		{name: "a change of owner on CockroachDB", dialect: platform.CockroachDB, caps: capability.ForDialect(platform.CockroachDB),
			node: ast.NewAlterOwner("VIEW", "report", "reporting"), want: "ALTER VIEW \"report\" OWNER TO \"reporting\";\n"},
		{name: "a change of owner on YugabyteDB", dialect: platform.YugabyteDB, caps: capability.ForDialect(platform.YugabyteDB),
			node: ast.NewAlterOwner("MATERIALIZED VIEW", "totals", `with"quote`), want: "ALTER MATERIALIZED VIEW \"totals\" OWNER TO \"with\"\"quote\";\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := builtin.RenderSQLWithCapabilities(test.dialect, test.caps, test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestRenderSQL_ViewOwner_UnmanagedFailurePath pins that a target which does
// not manage the owner of a view refuses a view that declares one, and a node
// that changes one, rather than create a view owned by whoever runs the plan.
func TestRenderSQL_ViewOwner_UnmanagedFailurePath(t *testing.T) {
	view := ast.NewCreateView("report").SetBody("SELECT 1")
	view.Owner = "reporting"
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		node    ast.Node
	}{
		{name: "a new view on MySQL", dialect: platform.MySQL, caps: capability.MySQL84(), node: view},
		{name: "a new view on SQLite", dialect: platform.SQLite, caps: capability.SQLite3(), node: view},
		{name: "a change of owner on SQL Server", dialect: platform.SQLServer, caps: capability.SQLServer2022(),
			node: ast.NewAlterOwner("VIEW", "report", "reporting")},
		{name: "a change of owner on ClickHouse", dialect: platform.ClickHouse, caps: capability.ClickHouse24(),
			node: ast.NewAlterOwner("MATERIALIZED VIEW", "report", "reporting")},
		{name: "a change of owner on Oracle", dialect: platform.Oracle, caps: capability.Oracle23(),
			node: ast.NewAlterOwner("VIEW", "report", "reporting")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := builtin.RenderSQLWithCapabilities(test.dialect, test.caps, test.node)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*Ptah does not manage the owner of a (materialized )?view.*`)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// TestRenderSQL_ViewOwner_RoleFailurePath pins that a node built by hand with
// a role no declaration may name is refused at the renderer too, as a
// malformed node.
func TestRenderSQL_ViewOwner_RoleFailurePath(t *testing.T) {
	for _, test := range []struct{ owner, wantErr string }{
		{owner: "", wantErr: `(?s).*view report: invalid owner: the role's name is empty.*`},
		{owner: "PUBLIC", wantErr: `(?s).*view report: invalid owner "PUBLIC": PUBLIC is every role.*`},
		{owner: "current_role", wantErr: `(?s).*view report: invalid owner CURRENT_ROLE: it names the role of the session.*`},
	} {
		t.Run(test.owner, func(t *testing.T) {
			c := qt.New(t)

			got, err := builtin.RenderSQLWithCapabilities(platform.Postgres, capability.Postgres18(), ast.NewAlterOwner("VIEW", "report", test.owner))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, "")
		})
	}
}
