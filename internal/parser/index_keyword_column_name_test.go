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
	return keywordColumnDocument(column, "TEXT")
}

// keywordColumnDocument declares the same table with the second column's type
// chosen by the caller, because a type carrying parentheses gives the element
// the shape of an index over a column list.
func keywordColumnDocument(column, columnType string) string {
	return fmt.Sprintf("CREATE TABLE t (\n  a INTEGER NOT NULL,\n  %s %s NOT NULL\n);", column, columnType)
}

// inlineIndexDocument declares a table with two columns followed by one more
// table element.
func inlineIndexDocument(element string) string {
	return fmt.Sprintf("CREATE TABLE t (\n  a INT NOT NULL,\n  b INT NOT NULL,\n  %s\n);", element)
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
// An engine with no table element opening with one of MySQL's index keywords
// can only mean a column there, and each row is a column name its engine
// accepts: measured on PostgreSQL, SQLite, CockroachDB, Spanner, ClickHouse,
// Oracle and SQL Server. YugabyteDB takes PostgreSQL's grammar and was not
// measured. A word an engine reserves is absent from its rows rather than
// asserted to parse.
//
// Read as an index instead, `key TEXT NOT NULL` is refused at the type while
// the engine creates the column (stokaro/ptah#3089, stokaro/ptah#3299).
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
		{name: "yugabytedb key", dialect: platform.YugabyteDB, column: "key"},
		{name: "yugabytedb index", dialect: platform.YugabyteDB, column: "index"},
		{name: "yugabytedb spatial", dialect: platform.YugabyteDB, column: "spatial"},
		{name: "yugabytedb fulltext", dialect: platform.YugabyteDB, column: "fulltext"},
		{name: "cockroachdb key", dialect: platform.CockroachDB, column: "key"},
		{name: "cockroachdb index", dialect: platform.CockroachDB, column: "index"},
		{name: "cockroachdb spatial", dialect: platform.CockroachDB, column: "spatial"},
		{name: "cockroachdb fulltext", dialect: platform.CockroachDB, column: "fulltext"},
		{name: "spanner key", dialect: platform.Spanner, column: "key"},
		{name: "spanner index", dialect: platform.Spanner, column: "index"},
		{name: "spanner spatial", dialect: platform.Spanner, column: "spatial"},
		{name: "spanner fulltext", dialect: platform.Spanner, column: "fulltext"},
		{name: "clickhouse key", dialect: platform.ClickHouse, column: "key"},
		{name: "clickhouse spatial", dialect: platform.ClickHouse, column: "spatial"},
		{name: "clickhouse fulltext", dialect: platform.ClickHouse, column: "fulltext"},
		{name: "oracle key", dialect: platform.Oracle, column: "key"},
		{name: "oracle spatial", dialect: platform.Oracle, column: "spatial"},
		{name: "oracle fulltext", dialect: platform.Oracle, column: "fulltext"},
		{name: "sqlserver spatial", dialect: platform.SQLServer, column: "spatial"},
		{name: "sqlserver fulltext", dialect: platform.SQLServer, column: "fulltext"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := parsedColumnNames(c, indexKeywordDocument(test.column), parser.WithDialect(test.dialect))
			c.Assert(got, qt.DeepEquals, []string{"a", test.column})
		})
	}
}

// TestIndexKeywordColumnWithParenthesizedTypeKeepsTheColumn_HappyPath covers
// the silent half of stokaro/ptah#3299.
//
// A column type carrying parentheses gives `spatial NVARCHAR(32)` the shape of
// MySQL's `SPATIAL name (cols)`. Read as an index, the element declares one
// named after the type over a column named `32`, the parse returns no error,
// and the column is gone. Every row is a statement its engine accepts as a
// column -- SQL Server returns both columns from SELECT * -- so the table has
// two columns and no index.
func TestIndexKeywordColumnWithParenthesizedTypeKeepsTheColumn_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		dialect    string
		column     string
		columnType string
	}{
		{name: "sqlserver spatial", dialect: platform.SQLServer, column: "spatial", columnType: "NVARCHAR(32)"},
		{name: "sqlserver fulltext", dialect: platform.SQLServer, column: "fulltext", columnType: "NVARCHAR(32)"},
		{name: "oracle key", dialect: platform.Oracle, column: "key", columnType: "VARCHAR2(32)"},
		{name: "oracle spatial", dialect: platform.Oracle, column: "spatial", columnType: "VARCHAR2(32)"},
		{name: "oracle fulltext", dialect: platform.Oracle, column: "fulltext", columnType: "VARCHAR2(32)"},
		{name: "cockroachdb key", dialect: platform.CockroachDB, column: "key", columnType: "VARCHAR(32)"},
		{name: "spanner index", dialect: platform.Spanner, column: "index", columnType: "VARCHAR(32)"},
		{name: "clickhouse spatial", dialect: platform.ClickHouse, column: "spatial", columnType: "FixedString(32)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql := keywordColumnDocument(test.column, test.columnType)
			c.Assert(parsedColumnNames(c, sql, parser.WithDialect(test.dialect)), qt.DeepEquals, []string{"a", test.column})
			c.Assert(parsedIndexNames(c, sql, parser.WithDialect(test.dialect)), qt.HasLen, 0)
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

// TestInlineIndexElementStaysAnIndex_HappyPath is the control for the two
// engines outside the MySQL family that do declare an index inside CREATE TABLE.
//
// CockroachDB accepts `INDEX idx_b (b)` and `INDEX (b)`, and SQL Server accepts
// `INDEX idx_b (b)`, each as an index. On CockroachDB the word also names a
// column, so what makes it an index is the column list that follows it,
// directly or after a name, whatever comes between the tokens.
func TestInlineIndexElementStaysAnIndex_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		element     string
		wantIndexes []string
	}{
		{name: "cockroachdb named", dialect: platform.CockroachDB, element: "INDEX idx_b (b)", wantIndexes: []string{"idx_b"}},
		{name: "cockroachdb unnamed", dialect: platform.CockroachDB, element: "INDEX (b)", wantIndexes: []string{""}},
		{name: "cockroachdb comment after the name", dialect: platform.CockroachDB, element: "INDEX idx_b /* by b */ (b)", wantIndexes: []string{"idx_b"}},
		{name: "sqlserver named", dialect: platform.SQLServer, element: "INDEX idx_b (b)", wantIndexes: []string{"idx_b"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql := inlineIndexDocument(test.element)
			c.Assert(parsedColumnNames(c, sql, parser.WithDialect(test.dialect)), qt.DeepEquals, []string{"a", "b"})
			c.Assert(parsedIndexNames(c, sql, parser.WithDialect(test.dialect)), qt.DeepEquals, test.wantIndexes)
		})
	}
}

// TestReservedTableElementKeywordsAreNeverColumns_HappyPath pins the half of the
// rule that asks no dialect: a word reserved wherever Ptah renders opens a
// constraint on every dialect, including the ones that read an index keyword
// as a column name.
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
		{name: "cockroachdb primary key", dialect: platform.CockroachDB, element: "PRIMARY KEY (a)"},
		{name: "oracle unique", dialect: platform.Oracle, element: "UNIQUE (a)"},
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
// Refusing is right there -- MySQL 8 rejects `key TEXT NOT NULL`, SQLite 3.51
// rejects `index TEXT NOT NULL`, SQL Server rejects `key` and Oracle rejects
// `index` -- so what is owed is the reason. `expected Operator, got Identifier
// at position 45` and nothing else leaves the reader with none
// (stokaro/ptah#3089). The SQL Server and Oracle rows pin that the reserved
// word keeps refusing on dialects that read the other keywords as columns.
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
		{
			name:    "sqlserver key",
			dialect: platform.SQLServer,
			column:  "key",
			wantErr: `(?s)a table element opening with KEY at position \d+ .*`,
		},
		{
			name:    "oracle index",
			dialect: platform.Oracle,
			column:  "index",
			wantErr: `(?s)a table element opening with INDEX at position \d+ .*`,
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

// TestCockroachDBQuotedIndexNameIsReadAsAnIndex_FailurePath pins which reading
// a CockroachDB INDEX element with a double-quoted name gets.
//
// A quoted name followed by a column list opens the list the same way a bare
// name does, so `INDEX "idx_b" (b)` is read as an index. The table-constraint
// reader takes no quoted index name, so the document is refused, and the
// refusal names the index reading. Read as a column instead, the same document
// fails at the quoted name as if it were a column type, which describes an
// element the author never wrote.
func TestCockroachDBQuotedIndexNameIsReadAsAnIndex_FailurePath(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser(
		inlineIndexDocument(`INDEX "idx_b" (b)`),
		parser.WithDialect(platform.CockroachDB),
	).Parse()

	c.Assert(err, qt.ErrorMatches,
		`(?s)a table element opening with INDEX at position \d+ declares a table-level index on cockroachdb .*`)
	c.Assert(statements, qt.IsNil)
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
