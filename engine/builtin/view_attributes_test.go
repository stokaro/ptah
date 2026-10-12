package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
)

// A view's own WITH clause -- SCHEMABINDING, VIEW_METADATA -- is SQL Server's.
// Every other renderer refuses it by name. A view written without it is not
// the one declared, and the comparison, which compares the attributes, would
// replace it on every run.
func TestRenderSQL_ViewAttributes_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		replace bool
	}{
		{name: "postgres create", dialect: "postgres"},
		{name: "postgres replace", dialect: "postgres", replace: true},
		{name: "cockroachdb", dialect: "cockroachdb"},
		{name: "yugabytedb", dialect: "yugabytedb"},
		{name: "spanner", dialect: "spanner"},
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb replace", dialect: "mariadb", replace: true},
		{name: "sqlite", dialect: "sqlite"},
		{name: "clickhouse", dialect: "clickhouse"},
		{name: "oracle", dialect: "oracle"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			view := &ast.CreateViewNode{
				Name: "v", Body: "SELECT id FROM t", Replace: test.replace,
				Attributes: []string{"SCHEMABINDING", "VIEW_METADATA"},
			}

			sql, err := builtin.RenderSQL(test.dialect, view)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `unsupported feature: `+test.dialect+
				`: view v declares WITH SCHEMABINDING, VIEW_METADATA, which only SQL Server has; `+
				`a view created without it is not the view declared`)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// SQL Server writes the clause, and a view without attributes is written on
// every target that refuses one.
func TestRenderSQL_ViewAttributes_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		view    ast.CreateViewNode
		want    string
	}{
		{
			name:    "SQL Server writes the clause",
			dialect: "sqlserver",
			view:    ast.CreateViewNode{Name: "v", Body: "SELECT id FROM t", Attributes: []string{"SCHEMABINDING"}},
			want:    "CREATE VIEW [v] WITH SCHEMABINDING AS",
		},
		{
			name:    "PostgreSQL writes a view without attributes",
			dialect: "postgres",
			view:    ast.CreateViewNode{Name: "v", Body: "SELECT id FROM t"},
			want:    `CREATE VIEW "v" AS`,
		},
		{
			name:    "MySQL writes a view without attributes",
			dialect: "mysql",
			view:    ast.CreateViewNode{Name: "v", Body: "SELECT id FROM t"},
			want:    "CREATE VIEW `v` AS",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := builtin.RenderSQL(test.dialect, &test.view)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}
