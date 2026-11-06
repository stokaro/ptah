package mssql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/engine/builtin"
)

// TestVisitCreateTable_WritesCommentsAsExtendedProperties pins the statements that
// put a table's comment and a column's in the catalog. T-SQL has no comment clause,
// so each is a call after the table. A table the statement may find keeps what it
// holds, so its calls are guarded.
func TestVisitCreateTable_WritesCommentsAsExtendedProperties(t *testing.T) {
	const onTable = "EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'it''s t', " +
		"@level0type = N'SCHEMA', @level0name = N'app', @level1type = N'TABLE', @level1name = N't';\n"
	const onColumn = "EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'the id', " +
		"@level0type = N'SCHEMA', @level0name = N'app', @level1type = N'TABLE', @level1name = N't', " +
		"@level2type = N'COLUMN', @level2name = N'id';\n"
	const table = "CREATE TABLE [app].[t] (\n  [id] INT\n);\n"
	tests := []struct {
		name string
		node ast.CreateTableNode
		want string
	}{
		{
			name: "no comment",
			node: ast.CreateTableNode{Name: "app.t", Columns: []*ast.ColumnNode{{Name: "id", Type: "INT", Nullable: true}}},
			want: table,
		},
		{
			name: "a comment on the table and on a column",
			node: ast.CreateTableNode{
				Name: "app.t", Comment: "it's t",
				Columns: []*ast.ColumnNode{{Name: "id", Type: "INT", Nullable: true, Comment: "the id"}},
			},
			want: table + onTable + onColumn,
		},
		{
			name: "a table the statement may find",
			node: ast.CreateTableNode{
				Name: "app.t", Comment: "it's t", IfNotExists: true,
				Columns: []*ast.ColumnNode{{Name: "id", Type: "INT", Nullable: true, Comment: "the id"}},
			},
			want: "IF OBJECT_ID('app.t', 'U') IS NULL\n" + table +
				"IF NOT EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 1 AND major_id = OBJECT_ID('app.t') " +
				"AND minor_id = 0 AND name = N'MS_Description')\n" + onTable +
				"IF NOT EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 1 AND major_id = OBJECT_ID('app.t') " +
				"AND minor_id = COLUMNPROPERTY(OBJECT_ID('app.t'), 'id', 'ColumnId') AND name = N'MS_Description')\n" + onColumn,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			node := test.node

			got, err := builtin.RenderSQL(platform.SQLServer, &node)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}
