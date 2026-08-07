package sqlutil_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/sqlutil"
)

// TestSplitSourceStatements pins what makes this different from
// [sqlutil.SplitSQLStatementsForDialect], which trims both sides.
//
// The rows that matter are the ones where the two disagree: a terminator that
// is not flush against the statement, and comments around one. Anything that
// hashes statement text needs the first distinction, because the two spellings
// are different bytes; stokaro/ptah#1196 is where that bit.
func TestSplitSourceStatements(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []sqlutil.SourceStatement
	}{
		{
			name: "a terminator flush against the statement",
			sql:  "CREATE TABLE q (id int);\n",
			want: []sqlutil.SourceStatement{{Text: "CREATE TABLE q (id int);", Terminated: true}},
		},
		{
			name: "a terminator on its own line keeps the newline",
			sql:  "CREATE TABLE q (id int)\n;\n",
			want: []sqlutil.SourceStatement{{Text: "CREATE TABLE q (id int)\n;", Terminated: true}},
		},
		{
			name: "a leading directive and blank lines are dropped",
			sql:  "-- atlas:txmode none\n\nCREATE TABLE q (id int);\n",
			want: []sqlutil.SourceStatement{{Text: "CREATE TABLE q (id int);", Terminated: true}},
		},
		{
			name: "a comment between statements belongs to neither",
			sql:  "CREATE TABLE m (id int);\n-- between\nCREATE TABLE n (v text);\n",
			want: []sqlutil.SourceStatement{
				{Text: "CREATE TABLE m (id int);", Terminated: true},
				{Text: "CREATE TABLE n (v text);", Terminated: true},
			},
		},
		{
			name: "a block comment before a statement is dropped",
			sql:  "/* header */ CREATE TABLE q (id int);\n",
			want: []sqlutil.SourceStatement{{Text: "CREATE TABLE q (id int);", Terminated: true}},
		},
		{
			name: "an unterminated final statement reports so",
			sql:  "CREATE TABLE q (id int)\n",
			want: []sqlutil.SourceStatement{{Text: "CREATE TABLE q (id int)\n", Terminated: false}},
		},
		{
			name: "an empty terminator is not a statement",
			sql:  "CREATE TABLE a (id int);;\nCREATE TABLE b (v text);\n",
			want: []sqlutil.SourceStatement{
				{Text: "CREATE TABLE a (id int);", Terminated: true},
				{Text: "CREATE TABLE b (v text);", Terminated: true},
			},
		},
		{
			name: "comments alone are not a statement",
			sql:  "-- nothing here\n",
			want: nil,
		},
		{
			name: "empty input",
			sql:  "   \n\t\n",
			want: nil,
		},
		{
			name: "a semicolon inside a string literal does not split",
			sql:  "INSERT INTO t (v) VALUES ('a;b');\n",
			want: []sqlutil.SourceStatement{{Text: "INSERT INTO t (v) VALUES ('a;b');", Terminated: true}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			c.Assert(sqlutil.SplitSourceStatements(test.sql, "sqlite"), qt.DeepEquals, test.want)
		})
	}
}
