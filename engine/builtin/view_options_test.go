package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
)

// A planner that creates a view or a materialized view the database holds
// again, or replaces a view, carries the options PostgreSQL holds it with, such
// as security_invoker. The PostgreSQL renderer writes them before AS, each
// value as a string literal, which the server stores as written.
func TestRenderSQL_ViewOptions_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		node    ast.Node
		want    string
	}{
		{
			name:    "a view created again",
			dialect: "postgres",
			node: &ast.CreateViewNode{Name: "v", Body: "SELECT id FROM t",
				Options: []string{"security_invoker=true", "security_barrier=on"}},
			want: "CREATE VIEW \"v\" WITH (security_invoker='true', security_barrier='on') AS\nSELECT id FROM t\n;\n",
		},
		{
			name:    "a view replaced in place",
			dialect: "postgres",
			node: &ast.CreateViewNode{Name: "v", Body: "SELECT id FROM t", Replace: true,
				Options: []string{"security_invoker=true"}},
			want: "CREATE OR REPLACE VIEW \"v\" WITH (security_invoker='true') AS\nSELECT id FROM t\n;\n",
		},
		{
			name:    "a materialized view created again",
			dialect: "postgres",
			node: &ast.CreateMaterializedViewNode{Name: "m", Body: "SELECT id FROM t",
				Options: []string{"fillfactor=70", "autovacuum_enabled=false"}},
			want: "CREATE MATERIALIZED VIEW \"m\" WITH (fillfactor='70', autovacuum_enabled='false') AS\nSELECT id FROM t\n;\n",
		},
		{
			name:    "a value with a quote",
			dialect: "postgres",
			node:    &ast.CreateViewNode{Name: "v", Body: "SELECT 1", Options: []string{"check=it's"}},
			want:    "CREATE VIEW \"v\" WITH (check='it''s') AS\nSELECT 1\n;\n",
		},
		{
			name:    "a view without options",
			dialect: "postgres",
			node:    &ast.CreateViewNode{Name: "v", Body: "SELECT id FROM t"},
			want:    "CREATE VIEW \"v\" AS\nSELECT id FROM t\n;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := builtin.RenderSQL(test.dialect, test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}

// Options are PostgreSQL's. Every other renderer refuses a view or a
// materialized view that carries any rather than write it without them, and
// PostgreSQL refuses an option that is not name=value rather than write it
// into the statement.
func TestRenderSQL_ViewOptions_FailurePath(t *testing.T) {
	view := func() ast.Node {
		return &ast.CreateViewNode{Name: "v", Body: "SELECT id FROM t", Options: []string{"security_invoker=true"}}
	}
	materialized := func() ast.Node {
		return &ast.CreateMaterializedViewNode{Name: "m", Body: "SELECT id FROM t", Options: []string{"fillfactor=70"}}
	}
	tests := []struct {
		name    string
		dialect string
		node    ast.Node
		want    error
		message string
	}{
		{name: "mysql", dialect: "mysql", node: view(), want: ptaherr.ErrUnsupportedFeature,
			message: `unsupported feature: mysql: view v carries the PostgreSQL options security_invoker=true, which this target has no clause for`},
		{name: "mariadb", dialect: "mariadb", node: view(), want: ptaherr.ErrUnsupportedFeature,
			message: `unsupported feature: mariadb: view v carries the PostgreSQL options security_invoker=true, which this target has no clause for`},
		{name: "sqlite", dialect: "sqlite", node: view(), want: ptaherr.ErrUnsupportedFeature,
			message: `unsupported feature: sqlite: view v carries the PostgreSQL options security_invoker=true, which this target has no clause for`},
		{name: "sqlserver", dialect: "sqlserver", node: view(), want: ptaherr.ErrUnsupportedFeature,
			message: `unsupported feature: sqlserver: view v carries the PostgreSQL options security_invoker=true, which this target has no clause for`},
		{name: "clickhouse view", dialect: "clickhouse", node: view(), want: ptaherr.ErrUnsupportedFeature,
			message: `unsupported feature: clickhouse: view v carries the PostgreSQL options security_invoker=true, which this target has no clause for`},
		{name: "clickhouse materialized view", dialect: "clickhouse", node: materialized(), want: ptaherr.ErrUnsupportedFeature,
			message: `unsupported feature: clickhouse: materialized view m carries the PostgreSQL options fillfactor=70, which this target has no clause for`},
		{name: "oracle view", dialect: "oracle", node: view(), want: ptaherr.ErrUnsupportedFeature,
			message: `unsupported feature: oracle: view v carries the PostgreSQL options security_invoker=true, which this target has no clause for`},
		{name: "oracle materialized view", dialect: "oracle", node: materialized(), want: ptaherr.ErrUnsupportedFeature,
			message: `unsupported feature: oracle: materialized view m carries the PostgreSQL options fillfactor=70, which this target has no clause for`},
		{name: "ydb", dialect: "ydb", node: view(), want: ptaherr.ErrUnsupportedFeature,
			message: `(?s).*WITH \(security_invoker=true\) on view v.*the options are PostgreSQL's.*`},
		{name: "postgres option without a value", dialect: "postgres",
			node: &ast.CreateViewNode{Name: "v", Body: "SELECT 1", Options: []string{"security_invoker"}}, want: ptaherr.ErrInvalidSchemaDiff,
			message: `invalid schema diff: postgres: view v carries the option "security_invoker", which is not name=value`},
		{name: "postgres option with a quoted name", dialect: "postgres",
			node: &ast.CreateMaterializedViewNode{Name: "m", Body: "SELECT 1", Options: []string{`"x)y"=1`}}, want: ptaherr.ErrInvalidSchemaDiff,
			message: `invalid schema diff: postgres: materialized view m carries the option "\\"x\)y\\"=1", which is not name=value`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := builtin.RenderSQL(test.dialect, test.node)

			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(err, qt.ErrorMatches, test.message)
			c.Assert(sql, qt.Equals, "")
		})
	}
}
