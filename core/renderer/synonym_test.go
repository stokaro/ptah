package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
)

// executableLines returns the rendered lines a server would run, dropping SQL
// line comments. The distinction is the whole point of the tests below: a
// target without synonyms names the object inside a comment, and a comment
// mentioning CREATE SYNONYM is exactly what "not supported" looks like.
func executableLines(rendered string) []string {
	var out []string
	for line := range strings.SplitSeq(rendered, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		out = append(out, trimmed)
	}
	return out
}

func TestRenderSQL_SQLServerEmitsASynonym(t *testing.T) {
	c := qt.New(t)

	rendered, err := renderer.RenderSQL(platform.SQLServer,
		ast.NewCreateSynonym("app.current_orders").SetTarget("sales.orders"))

	c.Assert(err, qt.IsNil)
	c.Assert(executableLines(rendered), qt.DeepEquals,
		[]string{"CREATE SYNONYM [app].[current_orders] FOR [sales].[orders];"})
}

// TestRenderSQL_SQLServerEscapesTheTarget is the reason the target does not go
// through the writer verbatim the way a view's body does. A synonym's target is
// an identifier, and an unescaped one breaks on the first reserved word, space,
// or four-part name pointing at a linked server.
func TestRenderSQL_SQLServerEscapesTheTarget(t *testing.T) {
	for _, tc := range []struct {
		name   string
		target string
		want   string
	}{
		{
			name:   "a two-part local target",
			target: "sales.orders",
			want:   "CREATE SYNONYM [dbo].[alias] FOR [sales].[orders];",
		},
		{
			name:   "a reserved word survives quoting",
			target: "dbo.order",
			want:   "CREATE SYNONYM [dbo].[alias] FOR [dbo].[order];",
		},
		{
			name:   "an unqualified target",
			target: "orders",
			want:   "CREATE SYNONYM [dbo].[alias] FOR [orders];",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			rendered, err := renderer.RenderSQL(platform.SQLServer,
				ast.NewCreateSynonym("dbo.alias").SetTarget(tc.target))

			c.Assert(err, qt.IsNil)
			c.Assert(executableLines(rendered), qt.DeepEquals, []string{tc.want})
		})
	}
}

func TestRenderSQL_SQLServerDropsASynonym(t *testing.T) {
	c := qt.New(t)

	rendered, err := renderer.RenderSQL(platform.SQLServer,
		ast.NewDropSynonym("app.current_orders").SetIfExists())

	c.Assert(err, qt.IsNil)
	c.Assert(executableLines(rendered), qt.DeepEquals,
		[]string{"DROP SYNONYM IF EXISTS [app].[current_orders];"})
}

// TestRenderSQL_OnlySQLServerEmitsASynonym is the grid. Every other target
// names the object and executes nothing: a synonym is a SQL Server construct,
// and a dialect that quietly rendered nothing at all would lose a declared
// object without saying so.
func TestRenderSQL_OnlySQLServerEmitsASynonym(t *testing.T) {
	for _, dialect := range []string{
		platform.Postgres, platform.MySQL, platform.MariaDB, platform.ClickHouse,
		platform.SQLite, platform.SQLServer, platform.CockroachDB, platform.YugabyteDB,
		platform.Spanner,
	} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			rendered, err := renderer.RenderSQL(dialect,
				ast.NewCreateSynonym("app.current_orders").SetTarget("sales.orders"))

			c.Assert(err, qt.IsNil)
			c.Assert(len(executableLines(rendered)) > 0, qt.Equals, dialect == platform.SQLServer,
				qt.Commentf("%s rendered %q", dialect, rendered))
			c.Assert(rendered, qt.Contains, "current_orders",
				qt.Commentf("%s lost the object without naming it", dialect))
		})
	}
}
