package ptahdirective_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/lexer"
	"go.5x5.cz/ptah/internal/ptahdirective"
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
