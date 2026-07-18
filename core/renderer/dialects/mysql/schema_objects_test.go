package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
)

func TestMySQLRenderer_ViewsAndTriggers(t *testing.T) {
	c := qt.New(t)

	sql := renderMySQL(t,
		ast.NewCreateView("active_users").
			SetReplace().
			SetBody("SELECT id FROM users WHERE deleted_at IS NULL"),
		ast.NewCreateTrigger("set_updated_at", "users").
			SetTiming("BEFORE").
			SetEvent("UPDATE").
			SetBody("SET NEW.updated_at = NOW()").
			SetReplace(),
		ast.NewCreateMaterializedView("user_stats").
			SetBody("SELECT id, COUNT(*) FROM users GROUP BY id"),
	)

	c.Assert(sql, qt.Contains, "CREATE OR REPLACE VIEW `active_users` AS")
	c.Assert(sql, qt.Contains, "DROP TRIGGER IF EXISTS `set_updated_at`;")
	c.Assert(sql, qt.Contains, "CREATE TRIGGER `set_updated_at` BEFORE UPDATE ON `users` FOR EACH ROW SET NEW.updated_at = NOW();")
	c.Assert(sql, qt.Contains, "-- MYSQL does not support CREATE MATERIALIZED VIEW user_stats")
}

func TestMySQLRenderer_ConstraintColumnParts(t *testing.T) {
	c := qt.New(t)

	table := ast.NewCreateTable("t1").
		AddColumn(ast.NewColumn("`id`", "tinytext").SetNotNull()).
		AddConstraint(&ast.ConstraintNode{
			Type:    ast.PrimaryKeyConstraint,
			Columns: []string{"`id`"},
			ColumnParts: []ast.ConstraintColumn{{
				Name:   "`id`",
				Prefix: "7",
				Desc:   true,
			}},
		})

	sql := renderMySQL(t, table)

	c.Assert(sql, qt.Contains, "PRIMARY KEY (`id` (7) DESC)")
}

func TestMySQLRenderer_EmptyLiteralDefault(t *testing.T) {
	c := qt.New(t)

	table := ast.NewCreateTable("users").
		AddColumn(ast.NewColumn("name", "varchar(255)").SetNotNull().SetDefault(""))

	sql := renderMySQL(t, table)

	c.Assert(sql, qt.Contains, "`name` varchar(255) NOT NULL DEFAULT ''")
}

func TestMySQLRenderer_ColumnCharsetCollate(t *testing.T) {
	c := qt.New(t)

	table := ast.NewCreateTable("users").
		AddColumn(ast.NewColumn("name", "varchar(255)").
			SetCharset("hebrew").
			SetCollate("hebrew_general_ci").
			SetNotNull())

	sql := renderMySQL(t, table)

	c.Assert(sql, qt.Contains, "`name` varchar(255) CHARACTER SET hebrew COLLATE hebrew_general_ci NOT NULL")
}

func TestMySQLRenderer_JSONColumnRemainsNativeJSON(t *testing.T) {
	c := qt.New(t)

	table := ast.NewCreateTable("users").
		AddColumn(ast.NewColumn("payload", "json").SetNotNull())

	sql := renderMySQL(t, table)

	c.Assert(sql, qt.Contains, "`payload` json NOT NULL")
	c.Assert(sql, qt.Not(qt.Contains), "json_valid")
}

func TestMySQLRenderer_IndexParts(t *testing.T) {
	c := qt.New(t)

	sql := renderMySQL(t,
		ast.NewIndex("idx_users_rank", "users", "rank").
			SetParts([]ast.IndexPart{{Name: "rank", Desc: true}}),
		ast.NewIndex("idx_users_name", "users", "name").
			SetParts([]ast.IndexPart{{Name: "name", Prefix: "64"}}),
		ast.NewIndex("idx_users_lower_name", "users", "lower(name)").
			SetParts([]ast.IndexPart{{Expr: "lower(name)"}}),
	)

	c.Assert(sql, qt.Contains, "CREATE INDEX `idx_users_rank` ON `users` (`rank` DESC);")
	c.Assert(sql, qt.Contains, "CREATE INDEX `idx_users_name` ON `users` (`name` (64));")
	c.Assert(sql, qt.Contains, "CREATE INDEX `idx_users_lower_name` ON `users` ((lower(name)));")
}

func TestMySQLRenderer_FulltextIndexParser(t *testing.T) {
	c := qt.New(t)

	index := ast.NewIndex("idx_users_bio", "users", "bio")
	index.Type = "FULLTEXT"
	index.Parser = "ngram"

	sql := renderMySQL(t, index)

	c.Assert(sql, qt.Contains, "CREATE FULLTEXT INDEX `idx_users_bio` ON `users` (`bio`) /*!50100 WITH PARSER `ngram` */;")
}

func TestMySQLRenderer_EscapesReservedIdentifiers(t *testing.T) {
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

	sql := renderMySQL(t, table, index)

	c.Assert(sql, qt.Contains, "CREATE TABLE `user` (")
	c.Assert(sql, qt.Contains, "`order` int NOT NULL")
	c.Assert(sql, qt.Contains, "`key` varchar(32)")
	c.Assert(sql, qt.Contains, "CONSTRAINT `user_order_key` UNIQUE (`order`, `key`)")
	c.Assert(sql, qt.Contains, "CREATE INDEX `idx_user_order` ON `user` (`order`);")
}

func TestMySQLRenderer_EscapesEmbeddedBackticks(t *testing.T) {
	c := qt.New(t)

	sql := renderMySQL(t,
		ast.NewCreateTable("tenant`data").
			AddColumn(ast.NewColumn("order`key", "int")),
	)

	c.Assert(sql, qt.Contains, "CREATE TABLE `tenant``data` (")
	c.Assert(sql, qt.Contains, "`order``key` int")
}
