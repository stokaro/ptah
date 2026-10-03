package sqlutil_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
)

// Each script was run whole on YDB 26.2.1.14 and the server ran it as the
// statements on the right: DEFINE ACTION and DEFINE SUBQUERY bodies, DO BEGIN
// blocks under EVALUATE FOR, EVALUATE IF and a plain DO, and lambda bodies all
// hold semicolons that do not end the statement. A row split anywhere else
// sends the server fragments it refuses on their own.
func TestSplitSQLStatementsForDialect_YDBCompoundBodies(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "an action and the statement that runs it",
			sql:  "DEFINE ACTION $a($x) AS SELECT $x AS v; SELECT 2 AS w; END DEFINE; DO $a(1);",
			want: []string{"DEFINE ACTION $a($x) AS SELECT $x AS v; SELECT 2 AS w; END DEFINE", "DO $a(1)"},
		},
		{
			name: "a subquery",
			sql:  "DEFINE SUBQUERY $s() AS SELECT 1 AS x; END DEFINE; SELECT * FROM $s();",
			want: []string{"DEFINE SUBQUERY $s() AS SELECT 1 AS x; END DEFINE", "SELECT * FROM $s()"},
		},
		{
			name: "EVALUATE FOR with a block",
			sql:  "EVALUATE FOR $i IN AsList(1, 2) DO BEGIN SELECT $i AS i; SELECT 0 AS z; END DO;\nSELECT 3;",
			want: []string{"EVALUATE FOR $i IN AsList(1, 2) DO BEGIN SELECT $i AS i; SELECT 0 AS z; END DO", "SELECT 3"},
		},
		{
			name: "EVALUATE IF with two blocks",
			sql:  "EVALUATE IF true DO BEGIN SELECT 1 AS a; END DO ELSE DO BEGIN SELECT 2 AS b; END DO; SELECT 3;",
			want: []string{"EVALUATE IF true DO BEGIN SELECT 1 AS a; END DO ELSE DO BEGIN SELECT 2 AS b; END DO", "SELECT 3"},
		},
		{
			name: "a plain DO block",
			sql:  "DO BEGIN SELECT 1 AS a; SELECT 2 AS b; END DO;",
			want: []string{"DO BEGIN SELECT 1 AS a; SELECT 2 AS b; END DO"},
		},
		{
			// CASE ... END inside the block closes the CASE, not the block,
			// and END DO inside the action closes the block, not the action.
			name: "a block inside an action, holding a CASE",
			sql: "DEFINE ACTION $a() AS DO BEGIN SELECT CASE WHEN true THEN 1 ELSE 2 END AS c; END DO; END DEFINE;\n" +
				"DO $a();",
			want: []string{
				"DEFINE ACTION $a() AS DO BEGIN SELECT CASE WHEN true THEN 1 ELSE 2 END AS c; END DO; END DEFINE",
				"DO $a()",
			},
		},
		{
			name: "a lambda body",
			sql:  "$f = ($x) -> { $y = $x + 1; RETURN $y; }; SELECT $f(1) AS r;",
			want: []string{"$f = ($x) -> { $y = $x + 1; RETURN $y; }", "SELECT $f(1) AS r"},
		},
		{
			name: "a lambda inside a lambda",
			sql:  "$f = ($x) -> { $g = ($y) -> { RETURN $y; }; RETURN $g($x); }; SELECT 1;",
			want: []string{"$f = ($x) -> { $g = ($y) -> { RETURN $y; }; RETURN $g($x); }", "SELECT 1"},
		},
		{
			// A comment between the two words of a closing pair is
			// whitespace to the server.
			name: "a comment inside END DEFINE",
			sql:  "DEFINE ACTION $a() AS SELECT 1; END /* x */ DEFINE; SELECT 2;",
			want: []string{"DEFINE ACTION $a() AS SELECT 1; END /* x */ DEFINE", "SELECT 2"},
		},
		{
			// END; DO is two statements, not a closing pair.
			name: "END and DO on either side of a semicolon",
			sql:  "SELECT CASE WHEN true THEN 1 ELSE 2 END; DO $a();",
			want: []string{"SELECT CASE WHEN true THEN 1 ELSE 2 END", "DO $a()"},
		},
		{
			// Inside a body too: the CASE's END and the next statement's DO
			// are separated by a semicolon, so they close nothing.
			name: "END and DO on either side of a semicolon inside a body",
			sql:  "DEFINE ACTION $b() AS SELECT CASE WHEN true THEN 1 ELSE 2 END; DO EMPTY_ACTION(); END DEFINE; DO $b();",
			want: []string{
				"DEFINE ACTION $b() AS SELECT CASE WHEN true THEN 1 ELSE 2 END; DO EMPTY_ACTION(); END DEFINE",
				"DO $b()",
			},
		},
		{
			// A backticked word is a name, never the keyword.
			name: "a quoted END is a name",
			sql:  "DEFINE ACTION $a() AS SELECT 1 AS `END`; SELECT 2; END DEFINE; SELECT 3;",
			want: []string{"DEFINE ACTION $a() AS SELECT 1 AS `END`; SELECT 2; END DEFINE", "SELECT 3"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlutil.SplitSQLStatementsForDialect(test.sql, platform.YDB), qt.DeepEquals, test.want)
		})
	}
}

// The literal and comment rows are read the way YDB 26.2.1.14 read them; see
// internal/lexer's YQL tests for each measurement.
func TestSplitSQLStatementsForDialect_YDBLiteralsAndComments(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "a double-quoted string holds a semicolon",
			sql:  `SELECT "a;b" AS x; SELECT 2;`,
			want: []string{`SELECT "a;b" AS x`, "SELECT 2"},
		},
		{
			name: "an escaped quote does not pair with the quote after it",
			sql:  `SELECT 'a\''; SELECT 2;`,
			want: []string{`SELECT 'a\''`, "SELECT 2"},
		},
		{
			name: "strings holding comment markers and a backtick",
			sql:  "SELECT '-- x', \"/* y\", '`'; SELECT 2;",
			want: []string{"SELECT '-- x', \"/* y\", '`'", "SELECT 2"},
		},
		{
			name: "a multiline string holds a semicolon and an escaped @@",
			sql:  "SELECT @@a;\n@@@@;b@@j; SELECT 2;",
			want: []string{"SELECT @@a;\n@@@@;b@@j", "SELECT 2"},
		},
		{
			name: "a backtick identifier holds a semicolon and an escaped backtick",
			sql:  "SELECT * FROM `dir/t;\\`x`; SELECT 2;",
			want: []string{"SELECT * FROM `dir/t;\\`x`", "SELECT 2"},
		},
		{
			name: "comments hold semicolons",
			sql:  "SELECT 1 -- a; b\n/* c; d */; SELECT 2;",
			want: []string{"SELECT 1 -- a; b\n/* c; d */", "SELECT 2"},
		},
		{
			name: "a hash does not start a comment",
			sql:  "SELECT 1 # x; SELECT 2;",
			want: []string{"SELECT 1 # x", "SELECT 2"},
		},
		{
			name: "a dollar sign does not open a dollar-quoted string",
			sql:  "SELECT $a$; SELECT $a$;",
			want: []string{"SELECT $a$", "SELECT $a$"},
		},
		{
			name: "a translation setting stays with the statement it heads",
			sql:  "--!syntax_v1\nSELECT 1; SELECT 2;",
			want: []string{"--!syntax_v1\nSELECT 1", "SELECT 2"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlutil.SplitSQLStatementsForDialect(test.sql, "ydbs"), qt.DeepEquals, test.want)
		})
	}
}

// Text YDB refuses is never cut into fragments it would run one by one. An
// unterminated literal or body keeps everything after it in one statement,
// which the server then refuses whole, and a stray closing brace is not an
// opening one.
func TestSplitSQLStatementsForDialect_YDBMalformedTextStaysWhole(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "an unterminated string",
			sql:  "SELECT 'a; DROP TABLE t; SELECT 2;",
			want: []string{"SELECT 'a; DROP TABLE t; SELECT 2;"},
		},
		{
			name: "an unterminated multiline string",
			sql:  "SELECT @@a; DROP TABLE t;",
			want: []string{"SELECT @@a; DROP TABLE t;"},
		},
		{
			name: "an action with no END DEFINE",
			sql:  "DEFINE ACTION $a() AS SELECT 1; DROP TABLE t;",
			want: []string{"DEFINE ACTION $a() AS SELECT 1; DROP TABLE t;"},
		},
		{
			name: "a lambda with no closing brace",
			sql:  "$f = ($x) -> { RETURN $x; DROP TABLE t;",
			want: []string{"$f = ($x) -> { RETURN $x; DROP TABLE t;"},
		},
		{
			name: "a stray closing brace",
			sql:  "SELECT }; SELECT 2;",
			want: []string{"SELECT }", "SELECT 2"},
		},
		{
			name: "an END DEFINE with nothing open",
			sql:  "SELECT 1 END DEFINE; SELECT 2;",
			want: []string{"SELECT 1 END DEFINE", "SELECT 2"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlutil.SplitSQLStatementsForDialect(test.sql, platform.YDB), qt.DeepEquals, test.want)
		})
	}
}

// The compound bodies decide the source split as they decide the executable
// one, so a statement's source text runs through its own body.
func TestSplitSourceStatements_YDBKeepsABodyWhole(t *testing.T) {
	c := qt.New(t)

	got := sqlutil.SplitSourceStatements("DEFINE ACTION $a() AS SELECT 1; END DEFINE;\nDO $a();", platform.YDB)

	c.Assert(got, qt.DeepEquals, []sqlutil.SourceStatement{
		{Text: "DEFINE ACTION $a() AS SELECT 1; END DEFINE;", Terminated: true},
		{Text: "DO $a();", Terminated: true},
	})
}

// Stripping removes YQL's two comment forms and nothing that only looks like
// one, and it keeps a translation setting, which changes how the server reads
// the text after it.
func TestStripCommentsForDialect_YDB(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "line and block comments",
			sql:  "SELECT 1 -- a\n/* b */;",
			want: "SELECT 1 \n;",
		},
		{
			name: "comment markers inside literals and names",
			sql:  "SELECT '--a', \"/*b*/\", @@-- c@@, `--d`;",
			want: "SELECT '--a', \"/*b*/\", @@-- c@@, `--d`;",
		},
		{
			name: "a hash is not a comment",
			sql:  "SELECT 1 # x",
			want: "SELECT 1 # x",
		},
		{
			name: "a translation setting is kept",
			sql:  "--!ansi_lexer\n-- note\nSELECT 1;",
			want: "--!ansi_lexer\n\nSELECT 1;",
		},
		{
			name: "a bang comment after the head is an ordinary comment",
			sql:  "SELECT 1; --!ansi_lexer",
			want: "SELECT 1; ",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlutil.StripCommentsForDialect(test.sql, platform.YDB), qt.Equals, test.want)
		})
	}
}
