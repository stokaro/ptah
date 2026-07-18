package mariadb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/renderer"
)

func TestMariaDBRenderer_JSONColumnUsesLongtextCheck(t *testing.T) {
	c := qt.New(t)

	table := ast.NewCreateTable("users").
		AddColumn(ast.NewColumn("id", "int").SetNotNull()).
		AddColumn(ast.NewColumn("name", "json").SetNotNull())

	sql, err := renderer.RenderSQL("mariadb", table)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "`name` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL CHECK (json_valid(`name`))")
}

func TestMariaDBRenderer_JSONColumnPreservesExplicitCharsetCollate(t *testing.T) {
	c := qt.New(t)

	table := ast.NewCreateTable("users").
		AddColumn(ast.NewColumn("payload", "json").
			SetCharset("utf8").
			SetCollate("utf8_bin"))

	sql, err := renderer.RenderSQL("mariadb", table)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "`payload` longtext CHARACTER SET utf8 COLLATE utf8_bin CHECK (json_valid(`payload`))")
}

func TestMariaDBRenderer_ColumnOnUpdateExpression(t *testing.T) {
	c := qt.New(t)

	table := ast.NewCreateTable("users").
		AddColumn(ast.NewColumn("updated_at", "datetime(6)").
			SetNotNull().
			SetDefaultExpression("CURRENT_TIMESTAMP(6)").
			SetUpdateExpression("CURRENT_TIMESTAMP(6)"))

	sql, err := renderer.RenderSQL("mariadb", table)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "`updated_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)")
}

func TestMariaDBRenderer_EscapesReservedIdentifiers(t *testing.T) {
	c := qt.New(t)

	table := ast.NewCreateTable("user").
		AddColumn(ast.NewColumn("order", "int").SetNotNull()).
		AddColumn(ast.NewColumn("key", "varchar(32)")).
		AddConstraint(&ast.ConstraintNode{
			Type:    ast.UniqueConstraint,
			Name:    "user_order_key",
			Columns: []string{"order", "key"},
		})
	index := ast.NewIndex("idx_user_order", "user", "order")

	sql, err := renderer.RenderSQL("mariadb", table, index)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "CREATE TABLE `user` (")
	c.Assert(sql, qt.Contains, "`order` int NOT NULL")
	c.Assert(sql, qt.Contains, "`key` varchar(32)")
	c.Assert(sql, qt.Contains, "CONSTRAINT `user_order_key` UNIQUE (`order`, `key`)")
	c.Assert(sql, qt.Contains, "CREATE INDEX `idx_user_order` ON `user` (`order`);")
}

func TestMariaDBRenderer_EscapesEmbeddedBackticks(t *testing.T) {
	c := qt.New(t)

	sql, err := renderer.RenderSQL("mariadb",
		ast.NewCreateTable("tenant`data").
			AddColumn(ast.NewColumn("order`key", "int")),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "CREATE TABLE `tenant``data` (")
	c.Assert(sql, qt.Contains, "`order``key` int")
}
