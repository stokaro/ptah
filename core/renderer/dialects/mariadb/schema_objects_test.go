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
	c.Assert(sql, qt.Contains, "name longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL CHECK (json_valid(name))")
}

func TestMariaDBRenderer_JSONColumnPreservesExplicitCharsetCollate(t *testing.T) {
	c := qt.New(t)

	table := ast.NewCreateTable("users").
		AddColumn(ast.NewColumn("payload", "json").
			SetCharset("utf8").
			SetCollate("utf8_bin"))

	sql, err := renderer.RenderSQL("mariadb", table)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "payload longtext CHARACTER SET utf8 COLLATE utf8_bin CHECK (json_valid(payload))")
}
