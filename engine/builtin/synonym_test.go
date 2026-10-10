package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
	"ptah.run/feature/synonym"
)

func synonymStatement(action synonym.Action, schema, name, target string) *ast.ExtensionStatement {
	return &ast.ExtensionStatement{Payload: &synonym.Operation{Action: action,
		Synonym: synonym.DesiredSynonym{Synonym: synonym.Synonym{Schema: schema, Name: name, Target: target}}}}
}

// TestRenderSQL_RendersSynonymOperations is the grid of the two targets that
// have synonyms. The target is quoted part by part like the alias, because it
// is a name rather than a body: written verbatim it breaks on the first
// reserved word, space, or four-part name pointing at a linked server.
func TestRenderSQL_RendersSynonymOperations(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		node    ast.Node
		want    string
	}{
		{
			name: "a two-part local target", dialect: platform.SQLServer,
			node: synonymStatement(synonym.Create, "app", "current_orders", "sales.orders"),
			want: "CREATE SYNONYM [app].[current_orders] FOR [sales].[orders];\n",
		},
		{
			name: "a reserved word survives quoting", dialect: platform.SQLServer,
			node: synonymStatement(synonym.Create, "dbo", "alias", "dbo.order"),
			want: "CREATE SYNONYM [dbo].[alias] FOR [dbo].[order];\n",
		},
		{
			name: "an unqualified target", dialect: platform.SQLServer,
			node: synonymStatement(synonym.Create, "dbo", "alias", "orders"),
			want: "CREATE SYNONYM [dbo].[alias] FOR [orders];\n",
		},
		{
			name: "a linked server with no database keeps the empty part", dialect: platform.SQLServer,
			node: synonymStatement(synonym.Create, "dbo", "alias", "[remote]..[dbo].[orders]"),
			want: "CREATE SYNONYM [dbo].[alias] FOR [remote]..[dbo].[orders];\n",
		},
		{
			name: "a drop", dialect: platform.SQLServer,
			node: synonymStatement(synonym.Drop, "app", "current_orders", "sales.orders"),
			want: "DROP SYNONYM IF EXISTS [app].[current_orders];\n",
		},
		{
			name: "a retarget drops and creates", dialect: platform.SQLServer,
			node: synonymStatement(synonym.Retarget, "app", "current_orders", "sales.orders_v2"),
			want: "DROP SYNONYM IF EXISTS [app].[current_orders];\nCREATE SYNONYM [app].[current_orders] FOR [sales].[orders_v2];\n",
		},
		{
			name: "an Oracle synonym", dialect: platform.Oracle,
			node: synonymStatement(synonym.Create, "app", "current_orders", "sales.orders"),
			want: "CREATE SYNONYM app.current_orders FOR sales.orders;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			rendered, err := builtin.RenderSQL(test.dialect, test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.Equals, test.want)
		})
	}
}

// TestRenderSQL_RefusesASynonymOperationElsewhere pins the other targets: none
// has synonyms, and a declaration bound to SQL Server and Oracle never reaches
// them, so an operation that does was built by hand.
func TestRenderSQL_RefusesASynonymOperationElsewhere(t *testing.T) {
	for _, dialect := range []string{
		platform.Postgres, platform.MySQL, platform.MariaDB, platform.ClickHouse,
		platform.SQLite, platform.CockroachDB, platform.YugabyteDB, platform.Spanner, platform.YDB,
	} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			rendered, err := builtin.RenderSQL(dialect, synonymStatement(synonym.Create, "app", "current_orders", "sales.orders"))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(rendered, qt.Equals, "")
		})
	}
}
