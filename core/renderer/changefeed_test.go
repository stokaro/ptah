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

// TestRender_Changefeed_FailurePath refuses a changefeed on every target
// without the changefeeds key, through the whole-schema render, a new table's
// node and each change of one alike: built without it, the table would carry
// no stream of its changes, and nothing would report the difference.
func TestRender_Changefeed_FailurePath(t *testing.T) {
	feed := ast.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}
	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"},
			Changefeeds: []ast.ChangefeedSpec{feed}}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "int", Primary: true}},
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
		{dialect: platform.YDB, caps: capability.YDB262().With(capability.Changefeeds, false)},
	}
	operations := []ast.AlterOperation{
		&ast.AddChangefeedOperation{Changefeed: feed},
		&ast.DropChangefeedOperation{Name: "updates"},
		&ast.AlterChangefeedTopicOperation{Changefeed: feed, Previous: feed},
	}

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(schema, test.dialect, test.caps)
			c.Assert(err, qt.ErrorMatches, `.*table "t" declares changefeed "updates", which requires target capability changefeeds, .*`)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.IsNil)

			node := &ast.CreateTableNode{Name: "t", Columns: []*ast.ColumnNode{ast.NewColumn("id", "INTEGER").SetPrimary()},
				Changefeeds: []ast.ChangefeedSpec{feed}}
			sql, err := renderer.RenderSQLWithCapabilities(test.dialect, test.caps, node)
			c.Assert(err, qt.ErrorMatches, `.*table "t" declares changefeed "updates", which requires target capability changefeeds, .*`)
			c.Assert(sql, qt.Equals, "")

			for _, operation := range operations {
				change := &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{operation}}
				sql, err = renderer.RenderSQLWithCapabilities(test.dialect, test.caps, change)
				c.Assert(err, qt.ErrorMatches, `.*changing the changefeeds of table "t", which requires target capability changefeeds, .*`)
				c.Assert(sql, qt.Equals, "")
			}
		})
	}
}

// TestRender_Changefeed_HappyPath writes a declared table's changefeed after
// its CREATE TABLE on YDB, through the whole-schema render.
func TestRender_Changefeed_HappyPath(t *testing.T) {
	c := qt.New(t)
	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"},
			Changefeeds: []ast.ChangefeedSpec{{Name: "updates", Mode: "UPDATES", Format: "JSON"}}}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "int", Primary: true}},
	}

	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(schema, platform.YDB, capability.YDB251())

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"CREATE TABLE `t` (\n    `id` Int32 NOT NULL,\n    PRIMARY KEY (`id`)\n);\n" +
			"ALTER TABLE `t` ADD CHANGEFEED `updates` WITH (MODE = 'UPDATES', FORMAT = 'JSON');\n",
	})
}
