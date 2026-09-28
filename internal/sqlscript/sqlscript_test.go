package sqlscript_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlscript"
)

func TestTerminator(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      string
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
			name:      "a line comment alone",
			statement: "-- NOTE: RLS policies were removed from table site_media_settings - verify if RLS should be disabled",
			want:      "",
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
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlscript.Terminator(test.statement), qt.Equals, test.want)
			c.Assert(sqlscript.CommentOnly(test.statement), qt.Equals, test.want == "")
		})
	}
}
