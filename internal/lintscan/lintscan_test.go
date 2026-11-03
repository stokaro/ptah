package lintscan_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/lintscan"
)

// commentsOf is the comments above each statement of sql under mode.
func commentsOf(sql string, mode lintscan.Mode) [][]string {
	var comments [][]string
	for _, statement := range lintscan.Split(sql, mode) {
		comments = append(comments, statement.Comments)
	}
	return comments
}

// A statement's comments are the ones directly above it: a blank line
// detaches a comment, and a comment on the line the previous statement ends
// on is that statement's.
func TestSplit_Comments(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
		want [][]string
	}{
		{"directly above", "-- a\n-- b\nDROP TABLE t;", [][]string{{"-- a", "-- b"}}},
		{"detached by a blank line", "-- a\n\nDROP TABLE t;", [][]string{nil}},
		{"the blank line between two", "-- a\n\n-- b\nDROP TABLE t;", [][]string{{"-- b"}}},
		{"on the previous statement's line", "DROP TABLE a; -- a\nDROP TABLE b;", [][]string{nil, nil}},
		{"inside a statement", "DROP -- a\nTABLE t;", [][]string{nil}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(commentsOf(test.sql, lintscan.ForDialect("postgres")), qt.DeepEquals, test.want)
		})
	}
}

// The dialect decides what is a comment and where a statement ends: '#'
// starts a comment on MySQL and is an operator elsewhere, and a
// dollar-quoted body keeps its semicolons on PostgreSQL.
func TestSplit_FollowsTheDialect(t *testing.T) {
	for _, test := range []struct {
		name, dialect, sql string
		want               []string
	}{
		{"a MySQL hash comment", "mysql", "# x;\nDROP TABLE t;", []string{"DROP TABLE t"}},
		{"a hash elsewhere", "", "SELECT a #> b; DROP TABLE t;", []string{"SELECT a #> b", "DROP TABLE t"}},
		{"a PostgreSQL dollar-quoted body", "postgres", "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql; DROP TABLE t;",
			[]string{"CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql", "DROP TABLE t"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var texts []string
			for _, statement := range lintscan.Split(test.sql, lintscan.ForDialect(test.dialect)) {
				texts = append(texts, statement.SQL)
			}
			c.Assert(texts, qt.DeepEquals, test.want)
		})
	}
}

// A statement carries every form rules read.
func TestSplit_Forms(t *testing.T) {
	c := qt.New(t)

	statements := lintscan.Split("\n  drop /* x */ table \"Users\";", lintscan.ForDialect("postgres"))

	c.Assert(statements, qt.DeepEquals, []lintext.Statement{{
		Start: 3, End: 29, Line: 2, SQL: `drop /* x */ table "Users"`, Canonical: `DROP TABLE "Users"`,
		Words: []string{"DROP", "TABLE", `"Users"`}, SourceWords: []string{"drop", "table", `"Users"`},
	}})
}

// The splitter splits every source of a request for its own dialect.
func TestSplitter_SplitStatements_HappyPath(t *testing.T) {
	c := qt.New(t)
	request := lintext.SplitRequest{Target: "mysql", Sources: []lintext.Source{{Name: "1.up.sql", SQL: "# a;\nDROP TABLE t;"}, {Name: "2.up.sql"}}}

	result, err := lintscan.Splitter{Dialect: "mysql"}.SplitStatements(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Files, qt.HasLen, 2)
	c.Assert(result.Files[0], qt.HasLen, 1)
	c.Assert(result.Files[0][0].Comments, qt.DeepEquals, []string{"# a;"})
	c.Assert(result.Files[1], qt.HasLen, 0)
	c.Assert(lintext.ValidateSplit(request, result), qt.IsNil)
}

func TestSplitter_SplitStatements_FailurePath(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, test := range []struct {
		name     string
		ctx      context.Context
		splitter lintscan.Splitter
		wantIs   error
	}{
		{"another target", context.Background(), lintscan.Splitter{Dialect: "postgres"}, ptaherr.ErrUnsupportedDialect},
		{"no dialect", context.Background(), lintscan.Splitter{}, ptaherr.ErrUnsupportedDialect},
		{"cancellation", canceled, lintscan.Splitter{Dialect: "mysql"}, context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := test.splitter.SplitStatements(test.ctx, lintext.SplitRequest{Target: "mysql", Sources: []lintext.Source{{SQL: "DROP TABLE t;"}}})

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(result, qt.DeepEquals, lintext.SplitResult{})
		})
	}
}

// splitStatement is the part of a split statement the batch tests compare.
type splitStatement struct {
	SQL      string
	Line     int
	Comments []string
}

// A GO on a line of its own ends a SQL Server batch, and the statement in it,
// as the migrator's splitter ends them. A statement-bodied trigger therefore
// ends at the GO rather than taking the DROP TABLE after it into its body; a
// count and a comment on the GO line belong to no statement; and a comment on
// the line after the GO is the next statement's. Read as a word, GO opened the
// next statement, and no rule saw a DROP TABLE behind one.
func TestSplit_EndsABatchAtTheSeparator(t *testing.T) {
	for _, test := range []struct {
		name, dialect, sql string
		want               []splitStatement
	}{
		{
			name:    "SQL Server",
			dialect: "sqlserver",
			sql: "CREATE TRIGGER trg ON t AFTER INSERT AS SET NOCOUNT ON;\nGO\nDROP TABLE users;\n" +
				"  go 2 -- twice\n-- audit\nDROP TABLE audit\nGO",
			want: []splitStatement{
				{SQL: "CREATE TRIGGER trg ON t AFTER INSERT AS SET NOCOUNT ON;", Line: 1},
				{SQL: "DROP TABLE users", Line: 3},
				{SQL: "DROP TABLE audit", Line: 6, Comments: []string{"-- audit"}},
			},
		},
		{
			name:    "a GO that shares its line is a word",
			dialect: "sqlserver",
			sql:     "SELECT 1 AS GO;\nDROP TABLE users;",
			want:    []splitStatement{{SQL: "SELECT 1 AS GO", Line: 1}, {SQL: "DROP TABLE users", Line: 2}},
		},
		{
			name:    "a dialect without a separator",
			dialect: "postgres",
			sql:     "SELECT 1;\nGO\nDROP TABLE users;",
			want:    []splitStatement{{SQL: "SELECT 1", Line: 1}, {SQL: "GO\nDROP TABLE users", Line: 2}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var got []splitStatement

			for _, statement := range lintscan.Split(test.sql, lintscan.ForDialect(test.dialect)) {
				got = append(got, splitStatement{SQL: statement.SQL, Line: statement.Line, Comments: statement.Comments})
			}

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
