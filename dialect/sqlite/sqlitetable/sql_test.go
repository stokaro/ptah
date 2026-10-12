package sqlitetable_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/sqlite/sqlitetable"
	"ptah.run/internal/parser"
)

// sqliteOwners is the SQLite owner's SQL contribution, frozen.
func sqliteOwners() sqlext.Set {
	return must.Must(sqlext.NewSet(sqlitetable.SQL()...))
}

// parseSQLite parses sql as a SQLite document read with the SQLite owner.
func parseSQLite(sql string) (*ast.StatementList, error) {
	return parser.NewParser(sql, parser.WithDialect(platform.SQLite), parser.WithSQL(sqliteOwners())).Parse()
}

// A virtual table's name and module are carried as written, parts joined by
// the dots that touch them, quotes and brackets kept, and its arguments
// verbatim between the outer parentheses, without the space around them. The
// statement is lexed as SQLite lexes it, where a backslash escapes nothing,
// so the parenthesis in "b)" is inside the string.
func TestSQL_ReadsNamesAsWritten(t *testing.T) {
	tests := []struct {
		sql           string
		wantTable     string
		wantModule    string
		wantArguments string
	}{
		{sql: `CREATE VIRTUAL TABLE main.docs USING fts5(a);`, wantTable: "main.docs", wantModule: "fts5", wantArguments: "a"},
		{sql: `CREATE VIRTUAL TABLE "main"."my docs" USING "my module"(a);`, wantTable: `"main"."my docs"`, wantModule: `"my module"`,
			wantArguments: "a"},
		{sql: `create virtual table [my]]docs] using fts5;`, wantTable: "[my]]docs]", wantModule: "fts5"},
		{sql: "CREATE /* note */ VIRTUAL\n  TABLE -- why\n docs USING fts5 ( a,\n b ) ;", wantTable: "docs", wantModule: "fts5",
			wantArguments: "a,\n b"},
		{sql: `CREATE VIRTUAL TABLE docs USING m("a\", "b)");`, wantTable: "docs", wantModule: "m", wantArguments: `"a\", "b)"`},
	}
	for _, test := range tests {
		t.Run(test.sql, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parseSQLite(test.sql)

			c.Assert(err, qt.IsNil)
			table := statements.Statements[0].(*ast.CreateTableNode)
			c.Assert(table.Name, qt.Equals, test.wantTable)
			declared, err := sqlitetable.VirtualDeclaration(table.Facets)
			c.Assert(err, qt.IsNil)
			c.Assert(declared.Module, qt.Equals, test.wantModule)
			c.Assert(declared.Arguments, qt.Equals, test.wantArguments)
		})
	}
}

// A malformed statement is refused at its position in the document, a
// second statement included, and nothing may follow the declaration.
func TestSQL_RefusesAMalformedStatement(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{name: "no module", sql: "SELECT 1;\nCREATE VIRTUAL TABLE docs USING;",
			wantErr: `expected virtual table module: expected identifier, got EOF at position 41`},
		{name: "no USING", sql: "CREATE VIRTUAL TABLE docs fts5(a);",
			wantErr: `expected 'USING', got 'fts5' at position 26`},
		{name: "a broken guard", sql: "CREATE VIRTUAL TABLE IF EXISTS docs USING fts5;",
			wantErr: `expected NOT after IF: expected 'NOT', got 'EXISTS' at position 24`},
		{name: "a guard cut short", sql: "CREATE VIRTUAL TABLE IF NOT docs USING fts5;",
			wantErr: `expected EXISTS after IF NOT: expected 'EXISTS', got 'docs' at position 28`},
		{name: "no name", sql: "CREATE VIRTUAL TABLE USING fts5;",
			wantErr: `expected 'USING', got 'fts5' at position 27`},
		{name: "a dot with no part after it", sql: "CREATE VIRTUAL TABLE main. USING fts5;",
			wantErr: `expected 'USING', got 'fts5' at position 33`},
		{name: "a string for a name", sql: "CREATE VIRTUAL TABLE 'docs' USING fts5;",
			wantErr: `expected virtual table name: expected identifier, got String at position 21`},
		{name: "an unclosed bracket", sql: "CREATE VIRTUAL TABLE [docs USING fts5",
			wantErr: `expected virtual table name: unterminated bracketed identifier at position 21`},
		{name: "unclosed arguments", sql: "SELECT 1;\nCREATE VIRTUAL TABLE docs USING fts5(a, (b)",
			wantErr: `unterminated virtual table arguments at position 47`},
		{name: "words after the arguments", sql: "CREATE VIRTUAL TABLE docs USING fts5(a) WITHOUT ROWID;",
			wantErr: `unexpected "WITHOUT" after the virtual table declaration at position 40`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parseSQLite(test.sql)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// A SQLite document claims to describe every virtual table, so a live one it
// leaves out is one it asks to drop, unless its header declines the whole
// kind; a header that names one virtual table is refused. A document that
// names no target reads the syntax and claims nothing.
func TestSQL_Coverage(t *testing.T) {
	set := sqliteOwners()
	tests := []struct {
		name   string
		header []coverage.Object
		want   schemaext.KnowledgeState
	}{
		{name: "no header", want: schemaext.Complete},
		{name: "declined", header: []coverage.Object{{Kind: "virtual_table"}}, want: schemaext.Uninspected},
		{name: "declined in another case", header: []coverage.Object{{Kind: "VIRTUAL_TABLE"}}, want: schemaext.Uninspected},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			claim, claimed, err := set.Coverage(platform.SQLite, sqlext.CoverageRequest{Limits: test.header})

			c.Assert(err, qt.IsNil)
			c.Assert(claimed, qt.IsTrue)
			c.Assert(claim.Lookup(sqlitetable.VirtualKind, objectidentity.ID{}).State, qt.Equals, test.want)
		})
	}
}

func TestSQL_Coverage_FailurePath(t *testing.T) {
	c := qt.New(t)

	claim, claimed, err := sqliteOwners().Coverage(platform.SQLite, sqlext.CoverageRequest{Limits: []coverage.Object{{Kind: "virtual_table", Name: "docs"}}})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `coverage of ptah.run/sqlite: .*ptah:not-described virtual_table names "docs"; the directive takes no name in a SQLite document`)
	c.Assert(claimed, qt.IsFalse)
	c.Assert(claim.IsZero(), qt.IsTrue)
}

// The header kind is the SQLite owner's on a SQLite document alone, and a
// document that names no target makes no claim about virtual tables.
func TestSQL_ReadsItsHeaderOnSQLiteAlone(t *testing.T) {
	c := qt.New(t)
	set := sqliteOwners()

	_, claimed, err := set.Coverage("", sqlext.CoverageRequest{})

	c.Assert(err, qt.IsNil)
	c.Assert(claimed, qt.IsFalse)
	c.Assert(set.ReadsHeader(platform.SQLite, sqlitetable.VirtualDirective), qt.IsTrue)
	c.Assert(set.ReadsHeader("", sqlitetable.VirtualDirective), qt.IsFalse)
	c.Assert(set.ReadsHeader(platform.Postgres, sqlitetable.VirtualDirective), qt.IsFalse)
}
