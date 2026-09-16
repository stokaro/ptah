package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// TestIndexColumnListNumber_FailurePath pins the refusal that keeps a column
// from disappearing into an index nobody wrote.
//
// The lexer emits a number as an identifier, so an index's column list took
// `32` for a column name. On every dialect that reads KEY, SPATIAL, FULLTEXT or
// INDEX as an index, a type carrying a length then had the shape of
// `KEY name (columns)`: `key varchar(32)` parsed as an index named `varchar`
// over a column named `32`, the author's column was gone from the model, and
// nothing was reported. MySQL and MariaDB answer the same DDL with Error 1064.
//
// The refusal names the keyword, because that is what the author has to change.
func TestIndexColumnListNumber_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		document string
		want     string
	}{
		{
			name:     "mysql reads key as an index",
			dialect:  platform.MySQL,
			document: "CREATE TABLE t (id INT PRIMARY KEY, key varchar(32));",
			want:     `(?s).*a table element opening with KEY .*a column list names columns, and "32" .*is a number.*`,
		},
		{
			name:     "mysql reads spatial as an index",
			dialect:  platform.MySQL,
			document: "CREATE TABLE t (id INT PRIMARY KEY, spatial varchar(32));",
			want:     `(?s).*a table element opening with SPATIAL .*is a number.*`,
		},
		{
			name:     "mariadb reads fulltext as an index",
			dialect:  platform.MariaDB,
			document: "CREATE TABLE t (id INT PRIMARY KEY, fulltext varchar(32));",
			want:     `(?s).*a table element opening with FULLTEXT .*is a number.*`,
		},
		{
			name:     "sqlserver reserves key",
			dialect:  platform.SQLServer,
			document: "CREATE TABLE t (id int PRIMARY KEY, key nvarchar(32));",
			want:     `(?s).*a table element opening with KEY .*is a number.*`,
		},
		{
			name:     "oracle reserves index",
			dialect:  platform.Oracle,
			document: "CREATE TABLE t (id NUMBER PRIMARY KEY, index VARCHAR2(32));",
			want:     `(?s).*a table element opening with INDEX .*is a number.*`,
		},
		{
			name:     "a primary key over a number is refused too",
			dialect:  platform.MySQL,
			document: "CREATE TABLE t (id INT, PRIMARY KEY (32));",
			want:     `(?s).*a column list names columns, and "32" .*is a number.*`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := parser.NewParser(tt.document, parser.WithDialect(tt.dialect)).Parse()
			c.Assert(err, qt.ErrorMatches, tt.want)
		})
	}
}

// TestIndexColumnListNumber_HappyPath pins what the rule must not reach.
//
// A prefix length is a number and is read after the column name, so it is not a
// column. A quoted name keeps its quotes in the token, so a column genuinely
// called `32` still parses. And a dialect that reads the keyword as a column
// name never gets here at all.
func TestIndexColumnListNumber_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		document string
		columns  []string
	}{
		{
			name:     "an index with a prefix length",
			dialect:  platform.MySQL,
			document: "CREATE TABLE t (id INT PRIMARY KEY, name varchar(32), KEY idx_name (name(10)));",
			columns:  []string{"id", "name"},
		},
		{
			name:     "an ordinary index",
			dialect:  platform.MySQL,
			document: "CREATE TABLE t (id INT PRIMARY KEY, name varchar(32), KEY idx_name (name));",
			columns:  []string{"id", "name"},
		},
		{
			// The quotes travel in the name here and the renderer re-emits
			// them, which is what it did before this rule and after it. What
			// the row is for is the rule's reach: a quoted part keeps its
			// quotes in the token, so it is not a bare number and is not
			// refused.
			name:     "a quoted column that is a number",
			dialect:  platform.MySQL,
			document: "CREATE TABLE t (id INT PRIMARY KEY, `32` varchar(32), KEY idx_32 (`32`));",
			columns:  []string{"id", "`32`"},
		},
		{
			name:     "postgres reads the word as a column",
			dialect:  platform.Postgres,
			document: "CREATE TABLE t (id integer PRIMARY KEY, key varchar(32));",
			columns:  []string{"id", "key"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(parsedColumnNames(c, tt.document, parser.WithDialect(tt.dialect)), qt.DeepEquals, tt.columns)
		})
	}
}
