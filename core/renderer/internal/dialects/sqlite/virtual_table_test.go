package sqlite_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/renderer"
)

// TestRenderCreateVirtualTable pins the statement a virtual table renders as.
//
// A SQLite virtual table is not a CREATE TABLE with an extra keyword. It has no
// column list of its own, and the module declaration after USING is the whole
// definition, so rendering the columns SQLite happens to report produces a
// statement that never created the object: a plain table named `docs` is not a
// full-text index. See stokaro/ptah#1028.
func TestRenderCreateVirtualTable(t *testing.T) {
	tests := []struct {
		name    string
		build   func() *ast.CreateTableNode
		wantSQL string
	}{
		{
			name: "a module with arguments",
			build: func() *ast.CreateTableNode {
				table := ast.NewCreateTable("docs")
				table.SetOption(ast.SQLiteVirtualModuleOption, "fts5")
				table.SetOption(ast.SQLiteVirtualArgumentsOption, "title, body")
				return table
			},
			wantSQL: "CREATE VIRTUAL TABLE \"docs\" USING fts5(title, body);\n",
		},
		{
			name: "module owned surrounding whitespace is preserved",
			build: func() *ast.CreateTableNode {
				table := ast.NewCreateTable("docs")
				table.SetOption(ast.SQLiteVirtualModuleOption, "fts5")
				table.SetOption(ast.SQLiteVirtualArgumentsOption, " body ")
				return table
			},
			wantSQL: "CREATE VIRTUAL TABLE \"docs\" USING fts5( body );\n",
		},
		{
			// Verbatim: the comma inside the quoted identifier, the spaces
			// around `=`, and the single-quoted option value are the module's
			// to interpret, not the renderer's to normalize.
			name: "arguments carrying quotes, commas and an option value",
			build: func() *ast.CreateTableNode {
				table := ast.NewCreateTable("docs")
				table.SetOption(ast.SQLiteVirtualModuleOption, "fts5")
				table.SetOption(ast.SQLiteVirtualArgumentsOption, `"col,two", tokenize = 'porter unicode61'`)
				return table
			},
			wantSQL: "CREATE VIRTUAL TABLE \"docs\" USING fts5(\"col,two\", tokenize = 'porter unicode61');\n",
		},
		{
			name: "a module with no arguments",
			build: func() *ast.CreateTableNode {
				table := ast.NewCreateTable("pages")
				table.SetOption(ast.SQLiteVirtualModuleOption, "dbstat")
				return table
			},
			wantSQL: "CREATE VIRTUAL TABLE \"pages\" USING dbstat;\n",
		},
		{
			name: "a schema-qualified virtual table",
			build: func() *ast.CreateTableNode {
				table := ast.NewCreateTable("aux.docs")
				table.SetOption(ast.SQLiteVirtualModuleOption, "fts5")
				table.SetOption(ast.SQLiteVirtualArgumentsOption, "body")
				return table
			},
			wantSQL: "CREATE VIRTUAL TABLE \"aux\".\"docs\" USING fts5(body);\n",
		},
		{
			name: "a guard the caller asked for",
			build: func() *ast.CreateTableNode {
				table := ast.NewCreateTable("docs").SetIfNotExists()
				table.SetOption(ast.SQLiteVirtualModuleOption, "fts5")
				table.SetOption(ast.SQLiteVirtualArgumentsOption, "body")
				return table
			},
			wantSQL: "CREATE VIRTUAL TABLE IF NOT EXISTS \"docs\" USING fts5(body);\n",
		},
		{
			// A module name that is not a bare identifier still has to name the
			// same module after the round trip.
			name: "a module name needing quotes",
			build: func() *ast.CreateTableNode {
				table := ast.NewCreateTable("docs")
				table.SetOption(ast.SQLiteVirtualModuleOption, "my module")
				table.SetOption(ast.SQLiteVirtualArgumentsOption, "body")
				return table
			},
			wantSQL: "CREATE VIRTUAL TABLE \"docs\" USING \"my module\"(body);\n",
		},
		{
			// SQLite parses an unquoted keyword as syntax, not as the registered
			// module identifier. Keep the catalog spelling while quoting it.
			name: "a module name that is a SQLite keyword",
			build: func() *ast.CreateTableNode {
				table := ast.NewCreateTable("docs")
				table.SetOption(ast.SQLiteVirtualModuleOption, "select")
				table.SetOption(ast.SQLiteVirtualArgumentsOption, "body")
				return table
			},
			wantSQL: "CREATE VIRTUAL TABLE \"docs\" USING \"select\"(body);\n",
		},
		{
			// The non-interference control: without the module option nothing
			// about ordinary CREATE TABLE rendering changes.
			name: "an ordinary table is untouched",
			build: func() *ast.CreateTableNode {
				return ast.NewCreateTable("users").
					AddColumn(ast.NewColumn("id", "INTEGER").SetPrimary())
			},
			wantSQL: "CREATE TABLE \"users\" (\n  \"id\" INTEGER PRIMARY KEY\n);\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL("sqlite", tt.build())

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
		})
	}
}
