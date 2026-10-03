package ptahdirective_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/ptahdirective"
)

func TestHasMarkerDistinguishesDirectiveCommentsFromSQLLookalikes(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want bool
	}{
		{
			name: "known directive",
			sql:  "-- +ptah no_transaction\nSELECT 1;\n",
			want: true,
		},
		{
			name: "bare marker",
			sql:  "  -- +ptah\nSELECT 1;\n",
			want: true,
		},
		{
			name: "unknown directive body",
			sql:  "-- +ptah future_directive\nSELECT 1;\n",
			want: true,
		},
		{
			name: "multiline string literal lookalike",
			sql:  "INSERT INTO notes (body) VALUES ('runbook:\n-- +ptah future_directive\ndone');\n",
		},
		{
			name: "block comment lookalike",
			sql:  "/*\n-- +ptah future_directive\n*/\nSELECT 1;\n",
		},
		{
			name: "trailing comment lookalike",
			sql:  "SELECT 1; -- +ptah future_directive\n",
		},
		{
			name: "near prefix",
			sql:  "-- +ptahx future_directive\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := ptahdirective.HasMarker(test.sql, lexer.Options{StandardStrings: true})

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestConservativeBodiesKeepsOnlyCrossDialectMarkers(t *testing.T) {
	c := qt.New(t)
	sql := "SELECT 'prefix \\'\n-- +ptah check name=\"fake\"\nsuffix';\n"

	got := slices.Collect(ptahdirective.ConservativeBodies(sql))

	c.Assert(got, qt.HasLen, 0)
}

// A YDB file is read by YQL's rules: a marker-looking line inside a
// multiline @@...@@ string, or inside a double-quoted string, is string
// content, and a real directive line is a directive.
func TestBodies_YDBReadsYQLStrings(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{name: "a directive", sql: "-- +ptah no_transaction\nSELECT 1;\n", want: []string{" no_transaction"}},
		{name: "inside a multiline string", sql: "SELECT @@runbook\n-- +ptah no_transaction\n@@;\n"},
		{name: "inside a double-quoted string", sql: "SELECT \"a\\\"\n-- +ptah no_transaction\n\";\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := slices.Collect(ptahdirective.Bodies(test.sql, dialectlexer.Options("ydb")))

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// The conservative reading leaves YQL out, so MySQL system variables on either
// side of a directive line do not hide it. Read by YQL, the two @@ open and
// close one string around the line.
func TestConservativeBodies_KeepsADirectiveBetweenMySQLSystemVariables(t *testing.T) {
	c := qt.New(t)
	sql := "SELECT @@session.sql_mode;\n-- +ptah no_transaction\nSELECT @@session.time_zone;\n"

	c.Assert(slices.Collect(ptahdirective.ConservativeBodies(sql)), qt.DeepEquals, []string{" no_transaction"})
	c.Assert(slices.Collect(ptahdirective.Bodies(sql, dialectlexer.Options("ydb"))), qt.HasLen, 0)
}
