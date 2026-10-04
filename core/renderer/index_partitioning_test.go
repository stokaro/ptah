package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
)

// TestRender_IndexPartitioning_FailurePath refuses an index's partitioning on
// every target without index_partitioning, through the whole-schema render, a
// single index node and a change of partitioning alike: built without it, the
// index would split as the server's defaults say, and nothing would report the
// difference.
func TestRender_IndexPartitioning_FailurePath(t *testing.T) {
	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "int", Primary: true},
			{StructName: "T", Name: "a", Type: "int", Nullable: true},
		},
		Indexes: []schemamodel.Index{{StructName: "T", Name: "k_a", TableName: "t", Fields: []string{"a"},
			Partitioning: &ast.IndexPartitioningSpec{MinPartitions: 3}}},
	}
	tests := []struct {
		dialect string
		caps    capability.Capabilities
	}{
		{dialect: platform.Postgres, caps: capability.Postgres18()},
		{dialect: platform.MySQL, caps: capability.MySQL84()},
		{dialect: platform.SQLite, caps: capability.SQLite3()},
		{dialect: platform.ClickHouse, caps: capability.ClickHouse24()},
		{dialect: platform.YDB, caps: capability.YDB262().With(capability.IndexPartitioning, false)},
	}

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(schema, test.dialect, test.caps)
			c.Assert(err, qt.ErrorMatches, `.*index "k_a" declares its partitioning, which requires target capability index_partitioning, .*`)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.IsNil)

			node := &ast.IndexNode{Name: "k_a", Table: "t", Columns: []string{"a"}, Partitioning: &ast.IndexPartitioningSpec{MinPartitions: 3}}
			sql, err := renderer.RenderSQLWithCapabilities(test.dialect, test.caps, node)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Equals, "")

			change := &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{
				&ast.SetIndexPartitioningOperation{IndexName: "k_a", Partitioning: &ast.IndexPartitioningSpec{MinPartitions: 3}},
			}}
			sql, err = renderer.RenderSQLWithCapabilities(test.dialect, test.caps, change)
			c.Assert(err, qt.ErrorMatches, `.*changing the partitioning of index "k_a", which requires target capability index_partitioning, .*`)
			c.Assert(sql, qt.Equals, "")
		})
	}
}
