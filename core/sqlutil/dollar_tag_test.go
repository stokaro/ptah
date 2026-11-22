package sqlutil_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlutil"
)

// The tag of a dollar-quoted body is spelled by the engine's own rule, and the
// statement splitter and the client delimiter rewrite read the same one. On
// the PostgreSQL family a tag may hold a character outside ASCII and may not
// begin with a digit: PostgreSQL 19, CockroachDB v26.4.1, YugabyteDB 2026.1.2
// and psql 19 read `$1abc$` as the parameter `$1` followed by a name, and
// `$é$` as the start of a string. A ClickHouse heredoc's tag may begin with a
// digit, ClickHouse 24.10 reads `$é$` as a tag, and a tag no closing tag
// follows opens no heredoc: 24.10, 26.9 and 26.10 read `SELECT 1 AS $ab$` as
// a column alias, and `SELECT 1 AS $ab$ab$` too, since a closing tag starts
// after the opening one. A reader that took the other engine's rule would
// split a body in two, keep a delimiter inside what the server reads as two
// statements, rewrite one inside a body, or run a name to the end of the
// text.
func TestSplitSQLStatementsForDialect_DollarTagRule_HappyPath(t *testing.T) {
	digitFirst := "SELECT $1abc$x; y$1abc$;\nSELECT 2;\n"
	digitFirstDelimited := "-- atlas:delimiter |\nSELECT $1abc$x| y$1abc$|\nSELECT 2|\n"
	outsideASCIIDelimited := "-- atlas:delimiter |\nSELECT $é$x| y$é$|\nSELECT 2|\n"
	unclosedDigitFirst := "SELECT 1 AS $1abc$;\nSELECT 2;\n"
	unclosed := "SELECT 1 AS $ab$;\nSELECT 2;\n"
	unclosedDelimited := "-- atlas:delimiter |\nSELECT 1 AS $ab$|\nSELECT 2|\n"
	for _, test := range []struct {
		name    string
		dialect string
		script  string
		want    []string
	}{
		{
			name:    "postgres splits after a digit-first tag",
			dialect: "postgres",
			script:  digitFirst,
			want:    []string{"SELECT $1abc$x", "y$1abc$", "SELECT 2"},
		},
		{
			name:    "clickhouse reads a digit-first heredoc",
			dialect: "clickhouse",
			script:  digitFirst,
			want:    []string{"SELECT $1abc$x; y$1abc$", "SELECT 2"},
		},
		{
			name:    "postgres rewrites the delimiter after a digit-first tag",
			dialect: "postgres",
			script:  digitFirstDelimited,
			want:    []string{"SELECT $1abc$x", "y$1abc$", "SELECT 2"},
		},
		{
			name:    "clickhouse keeps the delimiter inside a digit-first heredoc",
			dialect: "clickhouse",
			script:  digitFirstDelimited,
			want:    []string{"SELECT $1abc$x| y$1abc$", "SELECT 2"},
		},
		{
			name:    "postgres keeps the delimiter inside a tag outside ASCII",
			dialect: "postgres",
			script:  outsideASCIIDelimited,
			want:    []string{"SELECT $é$x| y$é$", "SELECT 2"},
		},
		{
			name:    "cockroachdb keeps the delimiter inside a tag outside ASCII",
			dialect: "cockroachdb",
			script:  outsideASCIIDelimited,
			want:    []string{"SELECT $é$x| y$é$", "SELECT 2"},
		},
		{
			name:    "yugabytedb keeps the delimiter inside a tag outside ASCII",
			dialect: "yugabytedb",
			script:  outsideASCIIDelimited,
			want:    []string{"SELECT $é$x| y$é$", "SELECT 2"},
		},
		{
			name:    "clickhouse keeps the delimiter inside a tag outside ASCII",
			dialect: "clickhouse",
			script:  outsideASCIIDelimited,
			want:    []string{"SELECT $é$x| y$é$", "SELECT 2"},
		},
		{
			name:    "clickhouse reads a heredoc whose tag is outside ASCII",
			dialect: "clickhouse",
			script:  "SELECT $é$x; y$é$;\nSELECT 2;\n",
			want:    []string{"SELECT $é$x; y$é$", "SELECT 2"},
		},
		{
			name:    "clickhouse ends the statement after a digit-first tag never closed",
			dialect: "clickhouse",
			script:  unclosedDigitFirst,
			want:    []string{"SELECT 1 AS $1abc$", "SELECT 2"},
		},
		{
			name:    "clickhouse ends the statement after a tag never closed",
			dialect: "clickhouse",
			script:  unclosed,
			want:    []string{"SELECT 1 AS $ab$", "SELECT 2"},
		},
		{
			name:    "clickhouse ends the statement where a tag repeats only inside itself",
			dialect: "clickhouse",
			script:  "SELECT 1 AS $ab$ab$;\nSELECT 2;\n",
			want:    []string{"SELECT 1 AS $ab$ab$", "SELECT 2"},
		},
		{
			name:    "clickhouse rewrites the delimiter after a tag never closed",
			dialect: "clickhouse",
			script:  unclosedDelimited,
			want:    []string{"SELECT 1 AS $ab$", "SELECT 2"},
		},
		{
			name:    "postgres reads a tag never closed to the end",
			dialect: "postgres",
			script:  unclosed,
			want:    []string{"SELECT 1 AS $ab$;\nSELECT 2;"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := sqlutil.SplitSQLStatementsForDialect(test.script, test.dialect)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
