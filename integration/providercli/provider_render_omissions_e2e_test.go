//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
)

// omittedLength is the omission the render-omissions fixture reports for a
// VARCHAR(255) column of table accounts, whose length its TEXT type does not
// keep.
var omittedLength = renderer.Omission{Dialect: "fixture", Reason: "unsupported", Kind: "column", Name: "accounts.email",
	Property: "type modifier", Detail: "VARCHAR(255): length 255"}

// TestProviderRenderReportsWhatItLeavesOut renders a table whose column
// declares a length the provider's engine does not keep, through a provider
// process, as a schema and as a node. A provider under render.omissions
// renders the column without the length and the host reports the omission in
// the result, as a built-in engine reports one; a provider that predates the
// feature renders the same statement and reports nothing.
func TestProviderRenderReportsWhatItLeavesOut(t *testing.T) {
	const statement = `CREATE TABLE "accounts" ("id" INTEGER NOT NULL PRIMARY KEY, "email" TEXT);`
	tests := []struct {
		name        string
		environment []string
		want        []renderer.Omission
	}{
		{name: "a provider that reports omissions", want: []renderer.Omission{omittedLength}},
		{name: "a provider that predates the feature", environment: []string{"FIXTURE_RENDER_OMISSIONS=off"}},
	}
	for _, test := range tests {
		t.Run(test.name+", schema render", func(t *testing.T) {
			c := qt.New(t)
			_, runtime := yamlProviderEnvironment(c, "render-omissions", test.environment)

			rendered, err := runtime.RenderSchema(c.Context(), renderer.SchemaRequest{Target: "fixture", Capabilities: capability.ForDialect("fixture"),
				Identifiers: identifier.Semantics{DefaultSchema: "main", TableNames: identifier.ComparisonExact, ColumnNames: identifier.ComparisonExact,
					IndexNames: identifier.ComparisonExact, IndexNamespace: identifier.IndexNamespaceTable},
				Schema: &schemamodel.Database{Tables: []schemamodel.Table{{Name: "accounts", StructName: "Accounts"}},
					Fields: []schemamodel.Field{{StructName: "Accounts", Name: "id", Type: "INTEGER", Primary: true},
						{StructName: "Accounts", Name: "email", Type: "VARCHAR(255)", Nullable: true}}}})

			c.Assert(err, qt.IsNil)
			c.Assert(rendered.Statements, qt.DeepEquals, []string{statement})
			c.Assert(rendered.Omissions, qt.DeepEquals, test.want)
		})
		t.Run(test.name+", node render", func(t *testing.T) {
			c := qt.New(t)
			_, runtime := yamlProviderEnvironment(c, "render-omissions", test.environment)
			table := &ast.CreateTableNode{Name: "accounts", Columns: []*ast.ColumnNode{ast.NewColumn("id", "INTEGER").SetPrimary().SetNotNull(),
				{Name: "email", Type: "VARCHAR(255)", Nullable: true}}}

			rendered, err := runtime.Render(c.Context(), renderer.Request{Target: "fixture", Capabilities: capability.ForDialect("fixture"), Nodes: []ast.Node{table}})

			c.Assert(err, qt.IsNil)
			sql, err := rendered.SQL()
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, statement)
			c.Assert(rendered.Omissions, qt.DeepEquals, test.want)
		})
	}
}
