package builtin_test

import (
	"fmt"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/engine/builtin"
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
		dialect      string
		caps         capability.Capabilities
		wantOpErrors []string
	}{
		{dialect: platform.Postgres, caps: capability.Postgres18(), wantOpErrors: unsupportedChangefeedErrors(platform.Postgres)},
		{dialect: platform.CockroachDB, caps: capability.CockroachDB26(), wantOpErrors: unsupportedChangefeedErrors(platform.CockroachDB)},
		{dialect: platform.MySQL, caps: capability.MySQL84(), wantOpErrors: unsupportedChangefeedErrors(platform.MySQL)},
		{dialect: platform.SQLite, caps: capability.SQLite3(), wantOpErrors: unsupportedChangefeedErrors(platform.SQLite)},
		{dialect: platform.ClickHouse, caps: capability.ClickHouse24(), wantOpErrors: unsupportedChangefeedErrors(platform.ClickHouse)},
		{dialect: platform.YDB, caps: capability.YDB262().With(capability.Changefeeds, false), wantOpErrors: []string{
			`changing the changefeeds of table "t", which requires target capability changefeeds, unavailable on this ydb target`,
			`dropping changefeed "updates" of table "t", which requires target capability changefeeds, unavailable on this ydb target`,
			`changing the changefeeds of table "t", which requires target capability changefeeds, unavailable on this ydb target`,
		}},
	}
	operations := []ast.AlterOperation{&ast.ExtensionAlterOperation{Payload: &ydbast.AddChangefeed{Changefeed: feed}}, &ast.ExtensionAlterOperation{Payload: &ydbast.DropChangefeed{Name: "updates"}}, &ast.ExtensionAlterOperation{Payload: &ydbast.AlterChangefeedTopic{Changefeed: feed, Previous: feed}}}

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(schema, test.dialect, test.caps)
			c.Assert(err, qt.ErrorMatches, `.*table "t" declares changefeed "updates", which requires target capability changefeeds, .*`)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.IsNil)

			node := &ast.CreateTableNode{Name: "t", Columns: []*ast.ColumnNode{ast.NewColumn("id", "INTEGER").SetPrimary()},
				Changefeeds: []ast.ChangefeedSpec{feed}}
			sql, err := builtin.RenderSQLWithCapabilities(test.dialect, test.caps, node)
			c.Assert(err, qt.ErrorMatches, `.*table "t" declares changefeed "updates", which requires target capability changefeeds, .*`)
			c.Assert(sql, qt.Equals, "")

			for i, operation := range operations {
				change := &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{operation}}
				sql, err = builtin.RenderSQLWithCapabilities(test.dialect, test.caps, change)
				c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantOpErrors[i]))
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
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

	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(schema, platform.YDB, capability.YDB251())

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"CREATE TABLE `t` (\n    `id` Int32 NOT NULL,\n    PRIMARY KEY (`id`)\n);\n" +
			"ALTER TABLE `t` ADD CHANGEFEED `updates` WITH (MODE = 'UPDATES', FORMAT = 'JSON');\n",
	})
}

func unsupportedChangefeedErrors(dialect string) []string {
	var messages []string
	for _, kind := range []schemaext.Kind{ydbast.AddChangefeedKind, ydbast.DropChangefeedKind, ydbast.AlterChangefeedTopicKind} {
		messages = append(messages, fmt.Sprintf("target %q does not support extension %q in role %q", dialect, kind, ast.AlterExtension))
	}
	return messages
}
