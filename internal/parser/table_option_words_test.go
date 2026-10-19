package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// wordOptionDialects are the dialects a table option spelled as words is read
// in: every dialect whose documents the shared grammar reads, and a parse with
// no dialect. SQLite's descriptor states STRICT and WITHOUT ROWID, and a file
// that writes them parses the same whatever dialect it names.
var wordOptionDialects = []string{
	"", platform.Postgres, platform.MySQL, platform.MariaDB, platform.SQLite,
	platform.SQLServer, platform.ClickHouse, platform.CockroachDB,
}

func TestParser_TableOptionWords_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want map[string]string
	}{
		{name: "both, after a comma", sql: "CREATE TABLE events (id integer PRIMARY KEY) WITHOUT ROWID, STRICT;",
			want: map[string]string{"WITHOUT_ROWID": "true", "STRICT": "true"}},
		{name: "lower case, a comment between the words", sql: "CREATE TABLE events (id integer PRIMARY KEY) without /* key */ rowid;",
			want: map[string]string{"WITHOUT_ROWID": "true"}},
		{name: "one word", sql: "CREATE TABLE events (id integer) Strict;",
			want: map[string]string{"STRICT": "true"}},
	}
	for _, dialect := range wordOptionDialects {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				statements, err := parser.NewParser(test.sql, parser.WithDialect(dialect)).Parse()

				c.Assert(err, qt.IsNil)
				c.Assert(statements.Statements, qt.HasLen, 1)
				table, isTable := statements.Statements[0].(*ast.CreateTableNode)
				c.Assert(isTable, qt.IsTrue)
				c.Assert(table.Options, qt.DeepEquals, test.want)
			})
		}
	}
}

func TestParser_TableOptionWords_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{name: "another word after WITHOUT", sql: "CREATE TABLE t (id int) WITHOUT OIDS;",
			wantErr: `expected ROWID after WITHOUT: expected 'ROWID', got 'OIDS' at position 32`},
		{name: "nothing after WITHOUT", sql: "CREATE TABLE t (id int) WITHOUT;",
			wantErr: `expected ROWID after WITHOUT: expected Identifier, got Semicolon at position 31`},
		{name: "a longer word", sql: "CREATE TABLE t (id int) STRICTLY;",
			wantErr: `unsupported table option: STRICTLY at position 24`},
	}
	for _, dialect := range wordOptionDialects {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				statements, err := parser.NewParser(test.sql, parser.WithDialect(dialect)).Parse()

				c.Assert(err, qt.ErrorMatches, test.wantErr)
				c.Assert(statements, qt.IsNil)
			})
		}
	}
}
