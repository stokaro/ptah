package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/sqlite/sqlitetable"
	"ptah.run/internal/parser"
)

// sqliteOwners are the SQLite owner's SQL extensions, which read CREATE
// VIRTUAL TABLE.
func sqliteOwners() sqlext.Set {
	return must.Must(sqlext.NewSet(sqlitetable.SQL()...))
}

// TestParseCreateVirtualTable_KeepsTheModuleAndItsArgumentsVerbatim pins that
// Ptah can read the statement it writes, through the SQLite owner, in a
// SQLite document and in one that names no target.
//
// The reader emits CREATE VIRTUAL TABLE and the SQLite renderer writes it, but
// the SQL parser refused it -- so `ptah db read` produced a file `ptah schema
// diff --from file://...` rejected with "unsupported CREATE target: VIRTUAL".
// Every other part of the pipeline was already in place: the AST carries the
// declaration as the SQLite owner's facet and both conversions existed.
//
// The arguments are compared verbatim rather than re-rendered from a parsed
// structure. They are not a column list -- fts5 takes tokenizer settings,
// quoted values, and commas and parentheses inside quotes -- and SQLite stores
// and compares the text as written.
func TestParseCreateVirtualTable_KeepsTheModuleAndItsArgumentsVerbatim(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		wantTable     string
		wantModule    string
		wantArguments string
	}{
		{
			name:          "an fts5 index with a tokenizer",
			sql:           `CREATE VIRTUAL TABLE docs USING fts5(title, body, tokenize = 'porter unicode61');`,
			wantTable:     "docs",
			wantModule:    "fts5",
			wantArguments: `title, body, tokenize = 'porter unicode61'`,
		},
		{
			name:          "a comma inside a quoted argument is not a separator",
			sql:           `CREATE VIRTUAL TABLE docs USING fts5(body, tokenize = "unicode61 remove_diacritics 2, keep");`,
			wantTable:     "docs",
			wantModule:    "fts5",
			wantArguments: `body, tokenize = "unicode61 remove_diacritics 2, keep"`,
		},
		{
			name:          "parentheses nest",
			sql:           `CREATE VIRTUAL TABLE t USING rtree(id, minX, maxX, +extra INTEGER DEFAULT (1));`,
			wantTable:     "t",
			wantModule:    "rtree",
			wantArguments: `id, minX, maxX, +extra INTEGER DEFAULT (1)`,
		},
		{
			// The quotes stay, which is what the ordinary CREATE TABLE path
			// does with the same name -- measured, both spellings answer
			// `"my docs"`. Stripping them here would make a virtual table the
			// one kind whose name is normalized differently.
			name:          "a quoted table name is carried as written",
			sql:           `CREATE VIRTUAL TABLE "my docs" USING fts5(a);`,
			wantTable:     `"my docs"`,
			wantModule:    "fts5",
			wantArguments: "a",
		},
		{
			name:       "a module may take no arguments",
			sql:        `CREATE VIRTUAL TABLE t USING some_module;`,
			wantTable:  "t",
			wantModule: "some_module",
		},
		{
			name:          "IF NOT EXISTS is accepted",
			sql:           `CREATE VIRTUAL TABLE IF NOT EXISTS docs USING fts5(a, b);`,
			wantTable:     "docs",
			wantModule:    "fts5",
			wantArguments: "a, b",
		},
	}

	for _, dialect := range []string{platform.SQLite, ""} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				statements, err := parser.NewParser(test.sql, parser.WithDialect(dialect), parser.WithSQL(sqliteOwners())).Parse()

				c.Assert(err, qt.IsNil)
				c.Assert(statements.Statements, qt.HasLen, 1)
				table, ok := statements.Statements[0].(*ast.CreateTableNode)
				c.Assert(ok, qt.IsTrue, qt.Commentf("got %T", statements.Statements[0]))
				c.Assert(table.Name, qt.Equals, test.wantTable)
				declared, err := sqlitetable.VirtualDeclaration(table.Facets)
				c.Assert(err, qt.IsNil)
				c.Assert(declared, qt.DeepEquals, &sqlitetable.DesiredVirtual{Virtual: sqlitetable.Virtual{Module: test.wantModule, Arguments: test.wantArguments}})
				// A virtual table has no parsed column list: the arguments are
				// the module's business, and inventing columns from them would
				// plan statements SQLite refuses on a virtual table.
				c.Assert(table.Columns, qt.HasLen, 0)
			})
		}
	}
}

// TestParseCreateVirtualTable_IfNotExists keeps the guard on the table the
// statement declares.
func TestParseCreateVirtualTable_IfNotExists(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser("CREATE VIRTUAL TABLE IF NOT EXISTS docs USING fts5(a);\nCREATE VIRTUAL TABLE more USING fts5(b);",
		parser.WithDialect(platform.SQLite), parser.WithSQL(sqliteOwners())).Parse()

	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 2)
	c.Assert(statements.Statements[0].(*ast.CreateTableNode).IfNotExists, qt.IsTrue)
	c.Assert(statements.Statements[1].(*ast.CreateTableNode).IfNotExists, qt.IsFalse)
}

// TestParseCreateVirtualTable_IsTheSQLiteOwners refuses the statement where
// the SQLite owner does not read it: a parse that selects no owner, and a
// document of another target. The shared grammar has no virtual table.
func TestParseCreateVirtualTable_IsTheSQLiteOwners(t *testing.T) {
	for _, test := range []struct {
		name    string
		options []parser.Option
	}{
		{name: "no owner", options: []parser.Option{parser.WithDialect(platform.SQLite)}},
		{name: "another target", options: []parser.Option{parser.WithDialect(platform.Postgres), parser.WithSQL(sqliteOwners())}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser("CREATE VIRTUAL TABLE docs USING fts5(a);", test.options...).Parse()

			c.Assert(err, qt.ErrorMatches, `unsupported CREATE target: VIRTUAL at position 7`)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestParseCreateVirtualTable_RefusesAnUnfinishedStatement keeps the argument
// scan from swallowing the rest of a file when a parenthesis is never closed.
func TestParseCreateVirtualTable_RefusesAnUnfinishedStatement(t *testing.T) {
	c := qt.New(t)

	_, err := parser.NewParser(`CREATE VIRTUAL TABLE docs USING fts5(title, body`, parser.WithDialect(platform.SQLite), parser.WithSQL(sqliteOwners())).Parse()

	c.Assert(err, qt.ErrorMatches, `unterminated virtual table arguments at position 37`)
}
