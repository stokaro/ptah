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

// TestRender_ColumnFamilies_FailurePath refuses a YDB column family on every
// target without the column_families key, through the whole-schema render, a
// new table's node and a change of one alike: built without it, every column
// would sit in one storage pool, uncompressed, and nothing would report the
// difference.
func TestRender_ColumnFamilies_FailurePath(t *testing.T) {
	families := []ast.YDBColumnFamilySpec{{Name: "cold", Compression: "lz4", Columns: []string{"body"}}}
	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"}, YDBColumnFamilies: families}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "int", Primary: true},
			{StructName: "T", Name: "body", Type: "TEXT", Nullable: true},
		},
	}
	tests := []struct {
		dialect string
		caps    capability.Capabilities
	}{
		{dialect: platform.Postgres, caps: capability.Postgres18()},
		{dialect: platform.CockroachDB, caps: capability.CockroachDB26()},
		{dialect: platform.MySQL, caps: capability.MySQL84()},
		{dialect: platform.SQLite, caps: capability.SQLite3()},
		{dialect: platform.ClickHouse, caps: capability.ClickHouse24()},
		{dialect: platform.SQLServer, caps: capability.SQLServer2022()},
		{dialect: platform.Oracle, caps: capability.Oracle23()},
		{dialect: platform.YDB, caps: capability.YDB262().With(capability.ColumnFamilies, false)},
	}

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(schema, test.dialect, test.caps)
			c.Assert(err, qt.ErrorMatches, `.*table "t" declares column families, which requires target capability column_families, .*`)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.IsNil)

			// The validation a plan runs before it compares: without it a
			// planner that has no such statement leaves a table that exists
			// with every column in one family, and reports the schema synced.
			err = renderer.ValidateSchemaWithCapabilities(schema, test.dialect, test.caps)
			c.Assert(err, qt.ErrorMatches, `.*table "t" declares column families, which requires target capability column_families, .*`)

			node := &ast.CreateTableNode{Name: "t", Columns: []*ast.ColumnNode{
				ast.NewColumn("id", "INTEGER").SetPrimary(), ast.NewColumn("body", "TEXT"),
			}, YDBColumnFamilies: families}
			sql, err := renderer.RenderSQLWithCapabilities(test.dialect, test.caps, node)
			c.Assert(err, qt.ErrorMatches, `.*table "t" declares column families, which requires target capability column_families, .*`)
			c.Assert(sql, qt.Equals, "")

			change := &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{
				&ast.SetYDBColumnFamiliesOperation{Families: families},
			}}
			sql, err = renderer.RenderSQLWithCapabilities(test.dialect, test.caps, change)
			c.Assert(err, qt.ErrorMatches, `.*changing the column families of table "t", which requires target capability column_families, .*`)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestRender_ColumnFamilies_HappyPath writes a declared table's families on
// YDB through the whole-schema render, and a table declaring only the default
// family stating no setting needs no key on any target.
func TestRender_ColumnFamilies_HappyPath(t *testing.T) {
	c := qt.New(t)
	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"},
			YDBColumnFamilies: []ast.YDBColumnFamilySpec{{Name: "default", Compression: "lz4"}}}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "int", Primary: true}},
	}
	plain := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"},
			YDBColumnFamilies: []ast.YDBColumnFamilySpec{{Name: "default"}}}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "int", Primary: true}},
	}

	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(schema, platform.YDB, capability.YDB251())
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"CREATE TABLE `t` (\n    `id` Int32 NOT NULL,\n    PRIMARY KEY (`id`),\n    FAMILY `default` (COMPRESSION = 'lz4')\n);\n",
	})
	_, err = renderer.GetOrderedCreateStatementsWithCapabilities(plain, platform.Postgres, capability.Postgres18())
	c.Assert(err, qt.IsNil)
}
