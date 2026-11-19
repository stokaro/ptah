package mysqllike_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
)

// A backslash inside a literal default, a column comment or a table comment is
// doubled. Under MySQL's and MariaDB's default sql_mode a
// backslash in a string escapes the character after it: written once, `a\b`
// stores a and a backspace, and `c\d` stores cd.
func TestVisitNode_CreateTableDoublesABackslashInAString(t *testing.T) {
	c := qt.New(t)
	column := ast.NewColumn("a", "VARCHAR(30)")
	column.SetDefault(`a\b`)
	column.Comment = `c\d`
	quoted := ast.NewColumn("q", "VARCHAR(30)")
	quoted.SetDefault(`it\'s`)
	node := &ast.CreateTableNode{Name: "notes", Columns: []*ast.ColumnNode{column, quoted}, Comment: `r\s`}

	got := renderNode(c, node)

	c.Assert(got, qt.Contains, "`a` VARCHAR(30) COMMENT 'c\\\\d' DEFAULT 'a\\\\b'")
	c.Assert(got, qt.Contains, "DEFAULT 'it\\\\''s'")
	c.Assert(got, qt.Contains, "COMMENT='r\\\\s'")
}

// A literal the declaration already wrote in quotes is SQL, and keeps its
// spelling: a schema file that writes 'a\\b' meant the three characters.
func TestVisitNode_CreateTableKeepsAQuotedDefault(t *testing.T) {
	c := qt.New(t)
	column := ast.NewColumn("a", "VARCHAR(30)")
	column.SetDefault(`'a\\b'`)

	got := renderNode(c, &ast.CreateTableNode{Name: "notes", Columns: []*ast.ColumnNode{column}})

	c.Assert(got, qt.Contains, "`a` VARCHAR(30) DEFAULT 'a\\\\b'")
}

// A literal NULL on a column that stores text is the string, which the
// comparison reads it as; on any other column it stays SQL NULL.
func TestVisitNode_CreateTableQuotesALiteralNullOnText(t *testing.T) {
	tests := []struct {
		name, columnType string
		want             string
	}{
		{name: "a varchar", columnType: "VARCHAR(8)", want: "`c` VARCHAR(8) DEFAULT 'NULL'"},
		{name: "an enum", columnType: "ENUM('NULL','x')", want: "DEFAULT 'NULL'"},
		{name: "an integer", columnType: "INT", want: "`c` INT DEFAULT NULL"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			column := ast.NewColumn("c", test.columnType)
			column.SetDefault("NULL")

			got := renderNode(c, &ast.CreateTableNode{Name: "notes", Columns: []*ast.ColumnNode{column}})

			c.Assert(got, qt.Contains, test.want)
		})
	}
}
