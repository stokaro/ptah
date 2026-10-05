package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer/internal/dialects/ydb"
)

func TestRender_ColumnTable(t *testing.T) {
	c := qt.New(t)
	node := ast.NewCreateTable("events")
	node.AddColumn(ast.NewColumn("id", "Uint64").SetPrimary())
	node.AddColumn(ast.NewColumn("body", "Utf8"))
	node.YDBColumnTable = &ast.YDBColumnTableSpec{HashColumns: []string{"id"}, Partitions: 1}
	node.AddIndex(&ast.IndexNode{Name: "bf", Type: "bloom_filter", Columns: []string{"body"}})
	got, err := ydb.NewWithCapabilities(capability.YDB262()).Render(node)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "CREATE TABLE `events` (\n    `id` Uint64 NOT NULL,\n    `body` Utf8,\n    PRIMARY KEY (`id`),\n    INDEX `bf` LOCAL USING bloom_filter ON (`body`) WITH (false_positive_probability = 0.1)\n) PARTITION BY HASH (`id`) WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1);\n")
}

func TestRender_ColumnTableRejectsGlobalIndex(t *testing.T) {
	c := qt.New(t)
	node := ast.NewCreateTable("events")
	node.AddColumn(ast.NewColumn("id", "Uint64").SetPrimary())
	node.AddColumn(ast.NewColumn("body", "Utf8"))
	node.YDBColumnTable = &ast.YDBColumnTableSpec{}
	node.AddIndex(&ast.IndexNode{Name: "global", Columns: []string{"body"}})
	_, err := ydb.NewWithCapabilities(capability.YDB262()).Render(node)
	c.Assert(err, qt.ErrorMatches, `.*column tables require LOCAL indexes.*`)
}
