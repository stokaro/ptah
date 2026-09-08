package parser_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// indexKeywordDocument declares a table whose second column is named after one
// of MySQL's table-level index keywords.
func indexKeywordDocument(column string) string {
	return fmt.Sprintf("CREATE TABLE t (\n  a INTEGER NOT NULL,\n  %s TEXT NOT NULL\n);", column)
}

// mysqlInlineKeyDocument declares the same table with a MySQL table-body index,
// which is the reading these keywords must keep where the dialect writes them.
const mysqlInlineKeyDocument = "CREATE TABLE t (\n  a INT NOT NULL,\n  b INT NOT NULL,\n  KEY idx_ab (a, b)\n);"

// parsedColumnNames returns the column names of every CREATE TABLE in a
// document, so a test can say what the parser made of a table element without
// reaching into the node type itself.
func parsedColumnNames(c *qt.C, sql string, opts ...parser.Option) []string {
	c.Helper()
	statements, err := parser.NewParser(sql, opts...).Parse()
	c.Assert(err, qt.IsNil)
	names := make([]string, 0)
	for _, statement := range statements.Statements {
		table, ok := statement.(*ast.CreateTableNode)
		if !ok {
			continue
		}
		for _, column := range table.Columns {
			names = append(names, column.Name)
		}
	}
	return names
}

// parsedIndexNames returns the names of the indexes a CREATE TABLE body
// declared, which is where a MySQL `KEY name (cols)` element lands.
func parsedIndexNames(c *qt.C, sql string, opts ...parser.Option) []string {
	c.Helper()
	statements, err := parser.NewParser(sql, opts...).Parse()
	c.Assert(err, qt.IsNil)
	names := make([]string, 0)
	for _, statement := range statements.Statements {
		table, ok := statement.(*ast.CreateTableNode)
		if !ok {
			continue
		}
		for _, index := range table.Indexes {
			names = append(names, index.Name)
		}
		for _, constraint := range table.Constraints {
			names = append(names, constraint.Name)
		}
	}
	return names
}

// TestIndexKeywordIsAColumnName_HappyPath holds the parser to what the engines
// accept.
//
// PostgreSQL and SQLite declare no index inside CREATE TABLE, so a table element
// opening with one of MySQL's index keywords there can only be a column, and
// neither engine reserves the name. Measured on PostgreSQL 17 and SQLite 3.51.
// SQLite reserves `index` alone, so it is absent from the SQLite rows rather
// than asserted to parse.
//
// The rows carry the dialect and the column name because the assertion is the
// same for every one of them: the table has two columns and the second is the
// one the document named (stokaro/ptah#3089).
func TestIndexKeywordIsAColumnName_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		column  string
	}{
		{name: "postgres key", dialect: platform.Postgres, column: "key"},
		{name: "postgres index", dialect: platform.Postgres, column: "index"},
		{name: "postgres spatial", dialect: platform.Postgres, column: "spatial"},
		{name: "postgres fulltext", dialect: platform.Postgres, column: "fulltext"},
		{name: "sqlite key", dialect: platform.SQLite, column: "key"},
		{name: "sqlite spatial", dialect: platform.SQLite, column: "spatial"},
		{name: "sqlite fulltext", dialect: platform.SQLite, column: "fulltext"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := parsedColumnNames(c, indexKeywordDocument(test.column), parser.WithDialect(test.dialect))
			c.Assert(got, qt.DeepEquals, []string{"a", test.column})
		})
	}
}

// TestIndexKeywordStaysAnIndex_HappyPath is the control the change would be
// meaningless without.
//
// MySQL 8 refuses all four words as column names and reads `KEY idx_ab (a, b)`
// in the same position as an index, so the MySQL family keeps the keyword. So
// does the dialect-less best-effort mode, which has no family to ask: turning an
// unrecognized KEY into a column there would drop an index without a word, the
// defect stokaro/ptah#2778 fixed on the ALTER path.
func TestIndexKeywordStaysAnIndex_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		opts []parser.Option
	}{
		{name: "mysql", opts: []parser.Option{parser.WithDialect(platform.MySQL)}},
		{name: "mariadb", opts: []parser.Option{parser.WithDialect(platform.MariaDB)}},
		{name: "no dialect", opts: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(parsedColumnNames(c, mysqlInlineKeyDocument, test.opts...), qt.DeepEquals, []string{"a", "b"})
			c.Assert(parsedIndexNames(c, mysqlInlineKeyDocument, test.opts...), qt.DeepEquals, []string{"idx_ab"})
		})
	}
}

// TestReservedTableElementKeywordsAreNeverColumns_HappyPath pins the half of the
// rule that asks no dialect: a word reserved wherever Ptah renders opens a
// constraint on every dialect, including the two this change taught to read a
// column name.
func TestReservedTableElementKeywordsAreNeverColumns_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		element string
	}{
		{name: "postgres unique", dialect: platform.Postgres, element: "UNIQUE (a)"},
		{name: "postgres primary key", dialect: platform.Postgres, element: "PRIMARY KEY (a)"},
		{name: "postgres check", dialect: platform.Postgres, element: "CHECK (a > 0)"},
		{name: "sqlite unique", dialect: platform.SQLite, element: "UNIQUE (a)"},
		{name: "sqlite primary key", dialect: platform.SQLite, element: "PRIMARY KEY (a)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql := fmt.Sprintf("CREATE TABLE t (\n  a INTEGER NOT NULL,\n  %s\n);", test.element)
			c.Assert(parsedColumnNames(c, sql, parser.WithDialect(test.dialect)), qt.DeepEquals, []string{"a"})
		})
	}
}

// TestIndexKeywordColumnNameIsRefusedWithItsReason_FailurePath covers the
// dialects that reserve the word.
//
// Refusing is right there -- MySQL 8 rejects `key TEXT NOT NULL` too, and SQLite
// 3.51 rejects `index TEXT NOT NULL` -- so what the fix owed was the reason. The
// reader used to get `expected Operator, got Identifier at position 45` and
// nothing else (stokaro/ptah#3089).
func TestIndexKeywordColumnNameIsRefusedWithItsReason_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		column  string
		wantErr string
	}{
		{
			name:    "mysql key",
			dialect: platform.MySQL,
			column:  "key",
			wantErr: `(?s)a table element opening with KEY at position \d+ declares a table-level ` +
				`index on mysql rather than a column named after the word.*`,
		},
		{
			name:    "sqlite index",
			dialect: platform.SQLite,
			column:  "index",
			wantErr: `(?s)a table element opening with INDEX at position \d+ declares a table-level ` +
				`index on sqlite rather than a column named after the word.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(
				indexKeywordDocument(test.column),
				parser.WithDialect(test.dialect),
			).Parse()
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestGenuineIndexFailureKeepsItsOwnReason_FailurePath is the control that keeps
// the sentence above from being pasted over every failure in an index element.
//
// A document declaring `KEY idx_a (a) WITH PARSER ngram` did mean an index, and
// telling its author it should have been a column would describe their intent
// wrongly. Only the element whose column list never opened is a candidate.
func TestGenuineIndexFailureKeepsItsOwnReason_FailurePath(t *testing.T) {
	c := qt.New(t)
	sql := "CREATE TABLE t (\n  a INT NOT NULL,\n  KEY idx_a (a) WITH PARSER ngram\n);"

	statements, err := parser.NewParser(sql, parser.WithDialect(platform.MySQL)).Parse()

	c.Assert(err, qt.ErrorMatches,
		`WITH PARSER belongs to a FULLTEXT index, and this one is an ordinary index`)
	c.Assert(statements, qt.IsNil)
}
