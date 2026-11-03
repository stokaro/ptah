package sqlscript_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlutil"
	"ptah.run/internal/sqlscript"
)

func TestTerminator(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      string
		// lineComment is what EndsInLineComment answers.
		lineComment bool
	}{
		{
			name:      "an executable statement",
			statement: `DROP POLICY IF EXISTS "tenant_only" ON "site_media_settings"`,
			want:      ";",
		},
		{
			name:      "a statement led by its comment",
			statement: "-- Drop RLS policy tenant_only\nDROP POLICY IF EXISTS \"tenant_only\" ON \"site_media_settings\"",
			want:      ";",
		},
		{
			name:        "a line comment alone",
			statement:   "-- NOTE: RLS policies were removed from table site_media_settings - verify if RLS should be disabled",
			want:        "",
			lineComment: true,
		},
		{
			name:      "several comments and blank lines",
			statement: "-- WARNING: first\n\n-- second\n/* block */",
			want:      "",
		},
		{
			name:      "a comment marker inside a string literal",
			statement: "SELECT '-- not a comment'",
			want:      ";",
		},
		{
			name:      "an empty statement",
			statement: "",
			want:      "",
		},
		{
			name:        "a statement that ends in a line comment",
			statement:   "CREATE MATERIALIZED VIEW \"order_lc\" AS\nSELECT count(*) AS n FROM orders WITH NO DATA -- empty too",
			want:        "\n;",
			lineComment: true,
		},
		{
			name:        "a statement that ends in a hash comment",
			statement:   "CREATE VIEW v AS SELECT 1 # one",
			want:        "\n;",
			lineComment: true,
		},
		{
			name:      "a statement whose line comment ends its line",
			statement: "CREATE VIEW v AS SELECT 1 -- one\n",
			want:      ";",
		},
		{
			name:      "a statement with a line comment before its last line",
			statement: "CREATE VIEW v AS -- one\nSELECT 1",
			want:      ";",
		},
		{
			name:      "a statement that ends in a block comment",
			statement: "CREATE VIEW v AS SELECT 1 /* one */",
			want:      ";",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlscript.Terminator(test.statement), qt.Equals, test.want)
			c.Assert(sqlscript.CommentOnly(test.statement), qt.Equals, test.want == "")
			c.Assert(sqlscript.EndsInLineComment(test.statement), qt.Equals, test.lineComment)
		})
	}
}

// TestTerminator_EndsTheStatement writes each executable statement with its
// terminator and another statement after it, and splits the script again. The
// script holds two statements only where the terminator ended the first one: a
// semicolon behind a line comment would leave the second statement inside the
// first.
func TestTerminator_EndsTheStatement(t *testing.T) {
	statements := []string{
		`DROP POLICY IF EXISTS "tenant_only" ON "site_media_settings"`,
		"SELECT count(*) AS n FROM orders WITH NO DATA -- empty too",
		"SELECT 1 # one",
		"SELECT 1 /* one */",
		"SELECT '-- not a comment'",
	}
	for _, statement := range statements {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)
			script := statement + sqlscript.Terminator(statement) + " SELECT 2;"
			c.Assert(sqlutil.SplitSQLStatements(script), qt.DeepEquals, []string{statement, "SELECT 2"})
		})
	}
}
