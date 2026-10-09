package modelast_test

import (
	"context"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/modelast"
)

// inlineIndexDatabase declares one table with a plain and a unique index, and
// an index whose table nothing declares.
func inlineIndexDatabase() schemamodel.Database {
	return schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields: []schemamodel.Field{
			{StructName: "Order", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Order", Name: "customer_id", Type: "BIGINT"},
			{StructName: "Order", Name: "number", Type: "TEXT"},
		},
		Indexes: []schemamodel.Index{
			{StructName: "Order", Name: "idx_orders_customer", Fields: []string{"customer_id"}},
			{StructName: "Order", Name: "uidx_orders_number", Fields: []string{"number"}, Unique: true},
			{Name: "idx_elsewhere", TableName: "invoices", Fields: []string{"total"}},
		},
	}
}

// walkTypes names the node types WalkDatabase visits, in order, and the index
// names the first CREATE TABLE carries inside its column list.
func walkTypes(c *qt.C, database schemamodel.Database, target string) (types, inline []string) {
	var visited []ast.Node
	err := modelast.WalkDatabase(database, target, func(node ast.Node) error {
		visited = append(visited, node)
		types = append(types, fmt.Sprintf("%T", node))
		return nil
	}, modelast.Lowering{Context: context.Background()})
	c.Assert(err, qt.IsNil)
	table, ok := visited[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	inline = make([]string, 0, len(table.Indexes))
	for _, index := range table.Indexes {
		inline = append(inline, index.Name+" on "+index.Table)
	}
	return types, inline
}

// TestWalkDatabase_YDBDeclaresATablesIndexesInItsCreateTable pins the fold for
// the target that needs it. Local YDB 25.1.4.7 to 26.2.1.14 refuse ALTER
// TABLE ... ADD INDEX ... GLOBAL UNIQUE, even on an empty table, so a unique
// index declared on a new table exists only if CREATE TABLE writes it. An
// index on a table the declaration does not have stays a statement of its
// own, for the renderer to answer.
func TestWalkDatabase_YDBDeclaresATablesIndexesInItsCreateTable(t *testing.T) {
	c := qt.New(t)

	types, inline := walkTypes(c, inlineIndexDatabase(), platform.YDB)

	c.Assert(types, qt.DeepEquals, []string{"*ast.CreateTableNode", "*ast.IndexNode"})
	c.Assert(inline, qt.DeepEquals, []string{"idx_orders_customer on orders", "uidx_orders_number on orders"})
}

// TestWalkDatabase_OtherTargetsKeepIndexesStandalone is the control: the fold
// belongs to YDB, and a target that creates indexes on their own still sees
// every index as a statement after its table.
func TestWalkDatabase_OtherTargetsKeepIndexesStandalone(t *testing.T) {
	for _, target := range []string{platform.Postgres, platform.MySQL, platform.SQLite} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)

			types, inline := walkTypes(c, inlineIndexDatabase(), target)

			c.Assert(inline, qt.HasLen, 0)
			c.Assert(types, qt.HasLen, 4)
			c.Assert(types[0], qt.Equals, "*ast.CreateTableNode")
		})
	}
}
