package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer/internal/dialects/ydb"
)

// Full-text settings reach both inline indexes and indexes added later.
func TestRender_FullText(t *testing.T) {
	for _, test := range []struct {
		name string
		node ast.Node
		want string
	}{
		{name: "added relevance index", node: &ast.IndexNode{Name: "ft", Table: "docs", Type: "fulltext_relevance", Columns: []string{"body"}, StorageParams: map[string]string{"tokenizer": "standard", "use_filter_lowercase": "true"}},
			want: "ALTER TABLE `docs` ADD INDEX `ft` GLOBAL USING fulltext_relevance ON (`body`) WITH (tokenizer=standard, use_filter_lowercase=true);\n"},
		{name: "inline plain index", node: fullTextTable(&ast.IndexNode{Name: "ft", Type: "fulltext_plain", Columns: []string{"body"}, StorageParams: map[string]string{"tokenizer": "keyword"}}, ast.NewColumn("body", "TEXT")),
			want: "CREATE TABLE `t` (\n    `id` Uint64 NOT NULL,\n    `body` Utf8,\n    PRIMARY KEY (`id`),\n    INDEX `ft` GLOBAL USING fulltext_plain ON (`body`) WITH (tokenizer=keyword)\n);\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(capability.YDB262().With(capability.FullTextIndexes, true)).Render(test.node)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestRender_FullTextWithoutCapability(t *testing.T) {
	c := qt.New(t)
	_, err := ydb.NewWithCapabilities(capability.YDB262().With(capability.FullTextIndexes, false)).Render(&ast.IndexNode{Name: "ft", Table: "docs", Type: "fulltext_plain", Columns: []string{"body"}, StorageParams: map[string]string{"tokenizer": "standard"}})
	c.Assert(err, qt.ErrorMatches, `(?s).*full_text_indexes.*`)
}

// fullTextTable gives the index the Uint64 primary key YDB requires.
func fullTextTable(index *ast.IndexNode, column *ast.ColumnNode) *ast.CreateTableNode {
	table := ast.NewCreateTable("t")
	table.AddColumn(ast.NewColumn("id", "Uint64").SetPrimary())
	table.AddColumn(column)
	table.AddIndex(index)
	return table
}

// Restrictions measured on 26.2 are refused before sending a CREATE TABLE.
func TestRender_FullTextTableShape(t *testing.T) {
	for _, test := range []struct {
		name, keyType string
		want          string
	}{
		{name: "signed key", keyType: "BIGINT", want: `.*requires a Uint64 primary key column.*`},
		{name: "string key", keyType: "TEXT", want: `.*requires a Uint64 primary key column.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			table := fullTextTable(&ast.IndexNode{Name: "ft", Type: "fulltext_plain", Columns: []string{"body"}, StorageParams: map[string]string{"tokenizer": "standard"}}, ast.NewColumn("body", "TEXT"))
			table.Columns[0].Type = test.keyType
			_, err := ydb.NewWithCapabilities(capability.YDB262().With(capability.FullTextIndexes, true)).Render(table)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}
